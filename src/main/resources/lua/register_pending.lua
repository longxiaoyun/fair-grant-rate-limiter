-- KEYS = bucket, FIFO wait zset, lease zset; ARGV = client, pendingTtlMs.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now)
for _, mid in ipairs(expired) do
  redis.call('ZREM', KEYS[3], mid)
  redis.call('ZREM', KEYS[2], mid)
end
if not redis.call('ZSCORE', KEYS[2], ARGV[1]) then
  redis.call('ZADD', KEYS[2], redis.call('HINCRBY', KEYS[1], 'seq', 1), ARGV[1])
end
redis.call('ZADD', KEYS[3], now + tonumber(ARGV[2]), ARGV[1])
return 1
