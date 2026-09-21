-- v2: KEYS = bucket, FIFO wait zset, lease zset, request receipt.
-- ARGV = client, rate, burst, receiptTtlMs, pendingTtlMs.
-- All time comes from Redis; queue order comes from an incrementing sequence.
local bucket, wait, pending, receipt = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local client = ARGV[1]
local rate, burst = tonumber(ARGV[2]), tonumber(ARGV[3])
local receiptTtl, leaseTtl = tonumber(ARGV[4]), tonumber(ARGV[5])
local t = redis.call('TIME')
local wallNow = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local savedRate = tonumber(redis.call('HGET', bucket, 'rate'))
local savedBurst = tonumber(redis.call('HGET', bucket, 'burst'))
if savedRate and (savedRate ~= rate or savedBurst ~= burst) then
  return { 'ERROR', '200', '0', 'config_mismatch' }
end
if redis.call('EXISTS', receipt) == 1 then
  return { 'GRANTED', '0', tostring(redis.call('HGET', bucket, 'tokens') or '0'), 'existing_permit' }
end
-- Clean dead waiters BEFORE renewal so an expired client rejoins at the tail.
local expired = redis.call('ZRANGEBYSCORE', pending, '-inf', wallNow)
for _, mid in ipairs(expired) do
  redis.call('ZREM', pending, mid)
  redis.call('ZREM', wait, mid)
end
if not redis.call('ZSCORE', wait, client) then
  local seq = redis.call('HINCRBY', bucket, 'seq', 1)
  redis.call('ZADD', wait, seq, client)
end
redis.call('ZADD', pending, wallNow + leaseTtl, client)
local last = tonumber(redis.call('HGET', bucket, 'ts')) or wallNow
local now = math.max(wallNow, last)
local tokens = tonumber(redis.call('HGET', bucket, 'tokens')) or burst
-- Clamp elapsed before multiplication to avoid overflow for very large rates.
tokens = math.min(burst, tokens + rate * math.min((now - last) / 1000, burst / rate))
redis.call('HSET', bucket, 'tokens', tokens, 'ts', now, 'rate', rate, 'burst', burst)
local heartbeat = math.max(1, math.floor(leaseTtl / 2))
if tokens < 1 then
  local retry = math.max(1, math.ceil((1 - tokens) / rate * 1000))
  return { 'WAIT', tostring(math.min(retry, heartbeat)), tostring(tokens), 'no_token' }
end
local first = redis.call('ZRANGE', wait, 0, 0)[1]
if first ~= client then
  return { 'WAIT', tostring(math.min(50, heartbeat)), tostring(tokens), 'not_selected:' .. first }
end
tokens = tokens - 1
redis.call('HSET', bucket, 'tokens', tokens)
redis.call('SET', receipt, tostring(now), 'PX', receiptTtl)
-- A grant completes this queue turn. In-flight work cannot block other clients.
redis.call('ZREM', wait, client)
redis.call('ZREM', pending, client)
return { 'GRANTED', '0', tostring(tokens), 'ok' }
