-- KEYS = wait zset, lease zset; ARGV = client. Receipts remain until TTL.
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
