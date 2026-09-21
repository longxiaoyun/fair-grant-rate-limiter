-- v4: KEYS = bucket, FIFO wait zset, lease zset, request receipt, window zset.
-- ARGV = client, rate, burst, receiptTtlMs, pendingTtlMs, windowMs, maxPermits, stateTtlMs.
-- Redis microsecond time defines the half-open rolling interval (now - window, now].
local bucket, wait, pending, receipt, window = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local client = ARGV[1]
local rate, burst = tonumber(ARGV[2]), tonumber(ARGV[3])
local receiptTtl, leaseTtl = tonumber(ARGV[4]), tonumber(ARGV[5])
local windowMs, maxPermits = tonumber(ARGV[6]) or 0, tonumber(ARGV[7]) or 0
local savedRate = tonumber(redis.call('HGET', bucket, 'rate'))
local savedBurst = tonumber(redis.call('HGET', bucket, 'burst'))
local savedWindow = tonumber(redis.call('HGET', bucket, 'windowMs')) or 0
local savedMax = tonumber(redis.call('HGET', bucket, 'windowMaxPermits')) or 0
if savedRate and (savedRate ~= rate or savedBurst ~= burst
    or savedWindow ~= windowMs or savedMax ~= maxPermits) then
  return { 'ERROR', '200', '0', 'config_mismatch' }
end
if redis.call('EXISTS', receipt) == 1 then
  return { 'GRANTED', '0', redis.call('HGET', bucket, 'tokens') or '0', 'existing_permit' }
end
-- Format microsecond integers explicitly: Lua tostring can round large timestamps.
local function integer(value) return (string.format('%.0f', value):gsub('%.0$', '')) end
local t = redis.call('TIME')
local wallNow = tonumber(t[1]) * 1000000 + tonumber(t[2])
local last = tonumber(redis.call('HGET', bucket, 'tsUs')) or wallNow
local clock = tonumber(redis.call('HGET', bucket, 'clockUs')) or wallNow
local now = math.max(wallNow, last, clock)
local expired = redis.call('ZRANGEBYSCORE', pending, '-inf', integer(now))
for _, mid in ipairs(expired) do
  redis.call('ZREM', pending, mid)
  redis.call('ZREM', wait, mid)
end
if not redis.call('ZSCORE', wait, client) then
  local seq = redis.call('HINCRBY', bucket, 'seq', 1)
  redis.call('ZADD', wait, seq, client)
end
redis.call('ZADD', pending, integer(now + leaseTtl * 1000), client)
local tokens = tonumber(redis.call('HGET', bucket, 'tokens')) or burst
tokens = math.min(burst, tokens + rate * math.min((now - last) / 1000000, burst / rate))
redis.call('HSET', bucket, 'tokens', string.format('%.17g', tokens),
  'tsUs', integer(now), 'clockUs', integer(now), 'rate', ARGV[2], 'burst', ARGV[3],
  'windowMs', integer(windowMs), 'windowMaxPermits', integer(maxPermits))
-- Preserve the longest horizon ever used by this live bucket (mixed client TTLs).
local retention = math.max(tonumber(ARGV[8]), tonumber(redis.call('HGET', bucket, 'retentionMs')) or 0)
redis.call('HSET', bucket, 'retentionMs', integer(retention))
-- Logical time can lead wall time after clock rollback. Do not expire early then.
local expiry = integer(retention + math.ceil((now - wallNow) / 1000))
local function refreshState()
  redis.call('PEXPIRE', bucket, expiry)
  redis.call('PEXPIRE', wait, expiry)
  redis.call('PEXPIRE', pending, expiry)
  redis.call('PEXPIRE', window, expiry)
end
local tokenDelay = 0
if tokens < 1 then tokenDelay = math.max(1, math.ceil((1 - tokens) / rate * 1000)) end
local windowDelay = 0
if maxPermits > 0 then
  redis.call('ZREMRANGEBYSCORE', window, '-inf', integer(now - windowMs * 1000))
  if redis.call('ZCARD', window) >= maxPermits then
    local oldest = redis.call('ZRANGE', window, 0, 0, 'WITHSCORES')
    windowDelay = math.max(1, math.ceil((tonumber(oldest[2]) + windowMs * 1000 - now) / 1000))
  end
end
if tokenDelay > 0 or windowDelay > 0 then
  local delay = math.max(tokenDelay, windowDelay)
  local heartbeat = math.max(1, math.floor(leaseTtl / 2))
  local detail = windowDelay >= tokenDelay and 'window_full' or 'no_token'
  refreshState()
  return { 'WAIT', tostring(math.min(delay, heartbeat)), string.format('%.17g', tokens), detail }
end
local first = redis.call('ZRANGE', wait, 0, 0)[1]
if first ~= client then
  -- Poll quickly for high-rate resources, without spinning low-quota clients.
  local effectiveRate = rate
  if maxPermits > 0 then effectiveRate = math.min(rate, maxPermits * 1000 / windowMs) end
  local poll = math.max(1, math.min(50, math.ceil(1000 / effectiveRate), math.floor(leaseTtl / 2)))
  refreshState()
  return { 'WAIT', tostring(poll),
    string.format('%.17g', tokens), 'not_selected:' .. first }
end
-- Both budgets and the queue head are checked BEFORE either budget is consumed.
tokens = tokens - 1
redis.call('HSET', bucket, 'tokens', string.format('%.17g', tokens))
if maxPermits > 0 then
  redis.call('HINCRBY', bucket, 'grantSeq', 1)
  -- Unique per grant, independent of request IDs and receipt expiration.
  redis.call('ZADD', window, integer(now), redis.call('HGET', bucket, 'grantSeq'))
end
redis.call('SET', receipt, integer(now), 'PX', receiptTtl)
redis.call('ZREM', wait, client)
redis.call('ZREM', pending, client)
refreshState()
return { 'GRANTED', '0', string.format('%.17g', tokens), 'ok' }
