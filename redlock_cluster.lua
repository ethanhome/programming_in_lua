-- redlock-lua library
-- Distributed locks with Redis: https://redis.io/topics/distlock
--

local setmetatable = setmetatable
local rawget = rawget

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 10)
local mt = {__index = _M}


function _M.new(self, client, retry_cnt, retry_delay)
    retry_cnt = retry_cnt or 3
    retry_delay = retry_delay or 0.2 -- 200ms
    if not client then
        return nil, 'client is nil'
    end
    return setmetatable({ _client = client, _retry_cnt = retry_cnt, _retry_delay = retry_delay }, mt)
end

local function lock_instance(self, client, resource, val, ttl)
    return client:set(resource, val, "NX", "PX", ttl)
end

function _M.lock(self, resource, ttl)
    --local val = self.get_unique_id()
    local val = ngx.now()
    local max_retry_cnt = rawget(self, '_retry_cnt')
    local retry_delay = rawget(self, "_retry_delay")
    local client = rawget(self, "_client")
    local lock = {resource = resource, val = val}
    local ret, err 
    for i=1, max_retry_cnt do 
        ret, err = lock_instance(self, client, resource, val, ttl) 
        ngx.log(ngx.DEBUG, 'lock_ret, ret:', ret, ', err:', err)
        if type(ret) == 'string' and ret == 'OK' then
            return lock
        else
            self:unlock(lock)
            ngx.sleep(retry_delay) 
        end
    end

    return false
end

function _M.unlock(self, lock)

    local unlock_script = [[
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    ]]
    
    local client = rawget(self, "_client")
    local resource = lock.resource
    local val = lock.val

    local ret, err = client:eval(unlock_script, 1, resource, val)
    
    ngx.log(ngx.DEBUG, 'unlock_ret, ret:', ret, ', err:', err)
    if ret ~= 1 then
        ngx.log(ngx.ERR, 'unlock_failed')
    end
end

return _M
