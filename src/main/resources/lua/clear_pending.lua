-- Clear pending + permit when local work for this key is idle.
-- KEYS[1] = wait zset
-- KEYS[2] = pending set
-- KEYS[3] = permit key
-- ARGV[1] = machineId

local waitKey = KEYS[1]
local pendingKey = KEYS[2]
local permitKey = KEYS[3]
local machineId = ARGV[1]

redis.call('SREM', pendingKey, machineId)
redis.call('ZREM', waitKey, machineId)
redis.call('DEL', permitKey)
return 1
