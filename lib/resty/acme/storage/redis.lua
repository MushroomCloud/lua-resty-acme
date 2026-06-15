local redis = require "resty.redis"
local util = require "resty.acme.util"
local fmt   = string.format
local log   = util.log
local ngx_ERR = ngx.ERR
local unpack = unpack

local _M = {}
local mt = {__index = _M}

-- Server-side scripts keep the data key and the secondary index consistent
-- in a single atomic operation, so a key can never end up stored without a
-- matching index entry (or indexed without the key existing).
local SET_INDEX_SCRIPT = [[
redis.call('set', KEYS[1], ARGV[1])
redis.call('sadd', KEYS[2], KEYS[1])
return true
]]

-- Returns true if the key was added (did not exist), false otherwise.
local ADD_INDEX_SCRIPT = [[
if redis.call('set', KEYS[1], ARGV[1], 'nx') then
  redis.call('sadd', KEYS[2], KEYS[1])
  return true
end
return false
]]

local DELETE_INDEX_SCRIPT = [[
redis.call('del', KEYS[1])
redis.call('srem', KEYS[2], KEYS[1])
return true
]]

function _M.new(conf)
  conf = conf or {}
  local self =
    setmetatable(
    {
      host = conf.host or '127.0.0.1',
      port = conf.port or 6379,
      database = conf.database,
      auth = conf.auth,
      ssl = conf.ssl or false,
      ssl_verify = conf.ssl_verify or false,
      ssl_server_name = conf.ssl_server_name,
      namespace = conf.namespace or "",
      scan_count = conf.scan_count or 10,
      username = conf.username,
      password = conf.password,
      -- Connection pooling. Without this every operation opened and tore down
      -- a fresh TCP connection; a single list() over a large keyspace then
      -- churned thousands of connections and exhausted local ephemeral ports
      -- (connect() failing with EADDRNOTAVAIL / "cannot assign requested
      -- address"). set_keepalive() returns the connection to the per-worker
      -- pool so it is reused instead.
      keepalive_timeout = conf.keepalive_timeout or 60000,
      pool_size = conf.pool_size or 100,
      -- Secondary index for list(). When enabled, every persistent key is also
      -- recorded in a Redis SET, and list() scans that small SET instead of the
      -- whole keyspace, turning enumeration from O(total keys) into O(our keys).
      -- Defaults on; set use_index = false to fall back to a keyspace SCAN.
      use_index = conf.use_index ~= false,
    },
    mt
  )
  -- The index SET and the "backfill done" marker. Both live under the
  -- namespace and use a prefix that never collides with a real cache key.
  self.index_key = self.namespace .. "__domains_index"
  self.index_built_key = self.namespace .. "__domains_index_built"
  return self, nil
end

local function op(self, operation, ...)
  local client = redis:new()
  client:set_timeouts(1000, 1000, 1000) -- 1 sec

  local sock_opts = {
    ssl = self.ssl,
    ssl_verify = self.ssl_verify,
    server_name = self.ssl_server_name,
    -- Keep the pool distinct per host/port/database so a reused connection is
    -- never on the wrong logical database.
    pool = fmt("%s:%s:%s", self.host, self.port, self.database or 0),
    pool_size = self.pool_size,
  }
  local ok, err = client:connect(self.host, self.port, sock_opts)
  if not ok then
    return nil, err
  end

  -- A reused (pooled) connection has already authenticated and selected the
  -- database, so only do it on a freshly established connection.
  if client:get_reused_times() == 0 then
    if self.username and self.password then
      local _, auth_err = client:auth(self.username, self.password)
      if auth_err then
        client:close()
        return nil, "authentication failed " .. auth_err
      end
    elseif self.password then
      local _, auth_err = client:auth(self.password)
      if auth_err then
        client:close()
        return nil, "authentication failed " .. auth_err
      end
    elseif self.auth then
      local _, auth_err = client:auth(self.auth)
      if auth_err then
        client:close()
        return nil, "authentication failed " .. auth_err
      end
    end

    if self.database then
      local select_ok, select_err = client:select(self.database)
      if not select_ok then
        client:close()
        return nil, "can't select database " .. (select_err or "")
      end
    end
  end

  local res, op_err = client[operation](client, ...)
  if op_err then
    -- The connection may be in an undefined state after an error; don't pool it.
    client:close()
  else
    local keepalive_ok, keepalive_err =
      client:set_keepalive(self.keepalive_timeout, self.pool_size)
    if not keepalive_ok then
      log(ngx_ERR, "failed to set keepalive on redis connection: ", tostring(keepalive_err))
    end
  end
  return res, op_err
end

local function remove_namespace(namespace, keys)
  if namespace == "" then
    return keys
  else
    -- <namespace><real_key>
    local len = #namespace
    local start = len + 1
    for k, v in ipairs(keys) do
      if v:sub(1, len) == namespace then
        keys[k] = v:sub(start)
      else
        local msg = fmt("found a key '%s', expected to be prefixed with namespace '%s'",
                        v, namespace)
        log(ngx_ERR, msg)
      end
    end

    return keys
  end
end

local empty_table = {}

-- The original behaviour: walk the entire keyspace with SCAN MATCH. Used when
-- the secondary index is disabled.
local function scan_keyspace(self, prefix)
  local cursor = "0"
  local data = {}
  local res, err

  repeat
    res, err = op(self, 'scan', cursor, 'match', prefix .. "*", 'count', self.scan_count)

    if not res or res == ngx.null then
      return empty_table, err
    end

    local keys
    cursor, keys = unpack(res)

    for i=1,#keys do
      data[#data+1] = keys[i]
    end

  until cursor == "0"

  return remove_namespace(self.namespace, data), err
end

-- One-time migration so existing deployments don't lose their certs when the
-- index is first enabled: scan the keyspace once, but only under `prefix`
-- (e.g. "domain:") so an empty namespace never sweeps in unrelated application
-- keys, and record what we find in the index. Idempotent: SADD is a no-op for
-- keys already present, so a crash before the marker is set just re-runs it.
local function ensure_index_built(self, prefix)
  local built, err = op(self, 'get', self.index_built_key)
  if err then
    return err
  end
  if built and built ~= ngx.null then
    return nil
  end

  local cursor = "0"
  repeat
    local res, scan_err = op(self, 'scan', cursor, 'match', prefix .. "*", 'count', self.scan_count)
    if not res or res == ngx.null then
      return scan_err
    end

    local keys
    cursor, keys = unpack(res)

    -- SADD index_key key1 key2 ... in one round trip per page, skipping the
    -- index/marker keys themselves so the index never references itself.
    local args = { self.index_key }
    for i = 1, #keys do
      local key = keys[i]
      if key ~= self.index_key and key ~= self.index_built_key then
        args[#args+1] = key
      end
    end
    if #args > 1 then
      local _, sadd_err = op(self, 'sadd', unpack(args))
      if sadd_err then
        return sadd_err
      end
    end
  until cursor == "0"

  local _, marker_err = op(self, 'set', self.index_built_key, "1")
  return marker_err
end

function _M:add(k, v, ttl)
  k = self.namespace .. k
  local ok, err
  if ttl then
    -- TTL keys (locks, challenges) are transient and are never list()ed, so
    -- they are not indexed.
    ok, err = op(self, 'set', k, v, "nx", "px", math.floor(ttl * 1000))
    if err then
      return err
    elseif ok == ngx.null then
      return "exists"
    end
  elseif self.use_index then
    ok, err = op(self, 'eval', ADD_INDEX_SCRIPT, 2, k, self.index_key, v)
    if err then
      return err
    elseif ok == ngx.null then
      -- ADD_INDEX_SCRIPT returned false (key already existed)
      return "exists"
    end
  else
    ok, err = op(self, 'set', k, v, "nx")
    if err then
      return err
    elseif ok == ngx.null then
      return "exists"
    end
  end
end

function _M:set(k, v, ttl)
  k = self.namespace .. k
  local err, _
  if ttl then
    _, err = op(self, 'set', k, v, "px", math.floor(ttl * 1000))
  elseif self.use_index then
    _, err = op(self, 'eval', SET_INDEX_SCRIPT, 2, k, self.index_key, v)
  else
    _, err = op(self, 'set', k, v)
  end
  if err then
    return err
  end
end

function _M:delete(k)
  k = self.namespace .. k
  local _, err
  if self.use_index then
    _, err = op(self, 'eval', DELETE_INDEX_SCRIPT, 2, k, self.index_key)
  else
    _, err = op(self, 'del', k)
  end
  if err then
    return err
  end
end

function _M:get(k)
  k = self.namespace .. k
  local res, err = op(self, 'get', k)
  if res == ngx.null then
    return nil, err
  end
  return res, err
end

function _M:list(prefix)
  prefix = prefix or ""
  prefix = self.namespace .. prefix

  if not self.use_index then
    return scan_keyspace(self, prefix)
  end

  local build_err = ensure_index_built(self, prefix)
  if build_err then
    return empty_table, build_err
  end

  -- SSCAN the (small) index SET rather than the whole keyspace.
  local cursor = "0"
  local data = {}
  local res, err

  repeat
    res, err = op(self, 'sscan', self.index_key, cursor, 'match', prefix .. "*", 'count', self.scan_count)

    if not res or res == ngx.null then
      return empty_table, err
    end

    local keys
    cursor, keys = unpack(res)

    for i=1,#keys do
      data[#data+1] = keys[i]
    end

  until cursor == "0"

  return remove_namespace(self.namespace, data), err
end

return _M
