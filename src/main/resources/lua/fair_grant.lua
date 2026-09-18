-- Fair grant: refill shared tokens + pick longest-waiting pending client.
-- KEYS[1] = bucket hash
-- KEYS[2] = wait zset (score = lastGrantAtMs)
-- KEYS[3] = pending set
-- KEYS[4] = permit key for this machine
-- ARGV[1] = machineId
-- ARGV[2] = ratePerSec
-- ARGV[3] = burst
-- ARGV[4] = permitTtlMs
-- ARGV[5] = nowMs (caller may pass; Redis TIME used if <=0)
--
-- Returns: { status, retryAfterMs, tokens, detail }
-- status: GRANTED | WAIT

local bucketKey = KEYS[1]
local waitKey = KEYS[2]
local pendingKey = KEYS[3]
local permitKey = KEYS[4]

local machineId = ARGV[1]
local rate = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local permitTtlMs = tonumber(ARGV[4])
local nowMs = tonumber(ARGV[5])

if nowMs == nil or nowMs <= 0 then
  local t = redis.call('TIME')
  nowMs = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

if rate == nil or rate <= 0 then
  return { 'WAIT', '1000', '0', 'invalid_rate' }
end
if burst == nil or burst <= 0 then
  burst = rate
end
if permitTtlMs == nil or permitTtlMs <= 0 then
  permitTtlMs = 20000
end

-- Already holding a valid permit: grant again (idempotent within TTL)
if redis.call('EXISTS', permitKey) == 1 then
  return { 'GRANTED', '0', tostring(redis.call('HGET', bucketKey, 'tokens') or '0'), 'existing_permit' }
end

redis.call('SADD', pendingKey, machineId)
if redis.call('ZSCORE', waitKey, machineId) == false then
  redis.call('ZADD', waitKey, nowMs, machineId)
end

local tokens = tonumber(redis.call('HGET', bucketKey, 'tokens'))
local lastTs = tonumber(redis.call('HGET', bucketKey, 'ts'))
if tokens == nil then
  tokens = burst
end
if lastTs == nil then
  lastTs = nowMs
end

local elapsed = nowMs - lastTs
if elapsed < 0 then
  elapsed = 0
end
tokens = math.min(burst, tokens + (rate * elapsed / 1000.0))
redis.call('HSET', bucketKey, 'tokens', tokens, 'ts', nowMs)

if tokens < 1.0 then
  local need = 1.0 - tokens
  local retryAfterMs = math.ceil((need / rate) * 1000.0)
  if retryAfterMs < 1 then
    retryAfterMs = 1
  end
  return { 'WAIT', tostring(retryAfterMs), tostring(tokens), 'no_token' }
end

-- Fair pick: lowest lastGrantAt among pending members
local candidates = redis.call('ZRANGE', waitKey, 0, -1)
local selected = nil
for i = 1, #candidates do
  local mid = candidates[i]
  if redis.call('SISMEMBER', pendingKey, mid) == 1 then
    selected = mid
    break
  else
    redis.call('ZREM', waitKey, mid)
  end
end

if selected == nil then
  return { 'WAIT', '50', tostring(tokens), 'no_pending' }
end

if selected ~= machineId then
  return { 'WAIT', '50', tostring(tokens), 'not_selected:' .. selected }
end

tokens = tokens - 1.0
redis.call('HSET', bucketKey, 'tokens', tokens, 'ts', nowMs)
redis.call('SET', permitKey, '1', 'PX', permitTtlMs)
redis.call('ZADD', waitKey, nowMs, machineId)

return { 'GRANTED', '0', tostring(tokens), 'ok' }
