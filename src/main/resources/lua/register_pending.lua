-- v4: KEYS = bucket, FIFO wait zset, lease zset, window; ARGV = client, pendingTtlMs, stateTtlMs.
-- Only register work that can execute as soon as a grant arrives.
local t = redis.call('TIME')
local wallNow = tonumber(t[1]) * 1000000 + tonumber(t[2])
local clock = math.max(tonumber(redis.call('HGET', KEYS[1], 'clockUs')) or wallNow,
  tonumber(redis.call('HGET', KEYS[1], 'tsUs')) or wallNow)
local now = math.max(wallNow, clock)
local function integer(value) return (string.format('%.0f', value):gsub('%.0$', '')) end
redis.call('HSET', KEYS[1], 'clockUs', integer(now))
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', integer(now))
for _, mid in ipairs(expired) do
  redis.call('ZREM', KEYS[3], mid)
  redis.call('ZREM', KEYS[2], mid)
end
if not redis.call('ZSCORE', KEYS[2], ARGV[1]) then
  redis.call('ZADD', KEYS[2], redis.call('HINCRBY', KEYS[1], 'seq', 1), ARGV[1])
end
redis.call('ZADD', KEYS[3], integer(now + tonumber(ARGV[2]) * 1000), ARGV[1])
local retention = math.max(tonumber(ARGV[3]), tonumber(redis.call('HGET', KEYS[1], 'retentionMs')) or 0)
redis.call('HSET', KEYS[1], 'retentionMs', integer(retention))
local expiry = integer(retention + math.ceil((now - wallNow) / 1000))
for _, key in ipairs(KEYS) do redis.call('PEXPIRE', key, expiry) end
return 1
