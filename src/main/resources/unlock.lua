-- 得到锁的key
local key = KEYS[1]

-- 当前线程标识
local threadId = ARGV[1]

-- 获取锁的线程标识 get key
local id = redis.call('get', key)

-- 比较两者是否一致
if(id == threadId) then
    -- 释放锁
    return redis.call('del', key)
end
return 0