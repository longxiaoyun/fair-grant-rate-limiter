-- Register pending waiter without consuming a token.
-- KEYS[1] = wait zset
-- KEYS[2] = pending set
-- ARGV[1] = machineId
-- ARGV[2] = nowMs

local waitKey = KEYS[1]
local pendingKey = KEYS[2]
local machineId = ARGV[1]
local nowMs = tonumber(ARGV[2])
if nowMs == nil or nowMs <= 0 then
  local t = redis.call('TIME')
  nowMs = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

redis.call('SADD', pendingKey, machineId)
if redis.call('ZSCORE', waitKey, machineId) == false then
  redis.call('ZADD', waitKey, nowMs, machineId)
end
return 1
