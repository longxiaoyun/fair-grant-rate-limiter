package io.github.longxiaoyun.fairgrant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisNoScriptException;
import java.util.UUID;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Redis-backed fair grant limiter (Jedis + Lua).
 * <p>
 * Thread-safe. One instance can be shared across threads for many resource keys.
 * On Redis failure, behavior follows {@link FairGrantConfig.FallbackMode}.
 */
public final class RedisFairGrantLimiter implements FairGrantLimiter, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RedisFairGrantLimiter.class);

    private static final String SCRIPT_GRANT = "lua/fair_grant.lua";
    private static final String SCRIPT_REGISTER = "lua/register_pending.lua";
    private static final String SCRIPT_CLEAR = "lua/clear_pending.lua";

    private final JedisPool jedisPool;
    private final FairGrantConfig config;
    private final FairGrantKeys keys;
    private final LocalShareFairGrantLimiter localFallback;
    private final boolean ownPool;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final String grantScript;
    private final String registerScript;
    private final String clearScript;

    private volatile String grantSha;
    private volatile String registerSha;
    private volatile String clearSha;

    public RedisFairGrantLimiter(JedisPool jedisPool, FairGrantConfig config) {
        this(jedisPool, config, false);
    }

    public RedisFairGrantLimiter(JedisPool jedisPool, FairGrantConfig config, boolean ownPool) {
        this.jedisPool = Objects.requireNonNull(jedisPool, "jedisPool");
        this.config = Objects.requireNonNull(config, "config");
        this.keys = new FairGrantKeys(config.getKeyPrefix());
        this.ownPool = ownPool;
        this.localFallback = new LocalShareFairGrantLimiter(config);
        this.grantScript = LuaScriptLoader.load(SCRIPT_GRANT);
        this.registerScript = LuaScriptLoader.load(SCRIPT_REGISTER);
        this.clearScript = LuaScriptLoader.load(SCRIPT_CLEAR);
    }

    @Override
    public AcquireResult tryAcquire(String project, String table, String clientId) {
        return tryAcquire(keys.resourceKey(project, table), clientId);
    }

    @Override
    public AcquireResult tryAcquire(String resourceKey, String clientId) {
        return tryAcquireRequest(resourceKey, clientId, UUID.randomUUID().toString());
    }

    @Override
    public AcquireResult tryAcquireRequest(String resourceKey, String clientId, String requestId) {
        ensureOpen();
        String resource = keys.normalizeResource(resourceKey);
        String client = requireClient(clientId);
        String request = FairGrantKeys.requireId(requestId, "requestId");
        try {
            return evalGrant(resource, client, request);
        } catch (JedisDataException e) {
            return AcquireResult.error("redis_data_error:" + e.getMessage());
        } catch (RuntimeException e) {
            LOG.warn("fair-grant Redis acquire failed, fallback={}, resource={}, client={}: {}",
                    config.getFallbackMode(), resource, client, e.toString());
            return fallbackAcquire(resource, client, request, e);
        }
    }

    @Override
    public void registerPending(String resourceKey, String clientId) {
        ensureOpen();
        String resource = keys.normalizeResource(resourceKey);
        String client = requireClient(clientId);
        try {
            evalRegister(resource, client);
        } catch (RuntimeException e) {
            LOG.warn("fair-grant registerPending failed, resource={}, client={}: {}",
                    resource, client, e.toString());
        }
    }

    @Override
    public void clearPending(String resourceKey, String clientId) {
        ensureOpen();
        String resource = keys.normalizeResource(resourceKey);
        String client = requireClient(clientId);
        try {
            evalClear(resource, client);
        } catch (RuntimeException e) {
            LOG.warn("fair-grant clearPending failed, resource={}, client={}: {}",
                    resource, client, e.toString());
        } finally {
            localFallback.clearPending(resource, client);
        }
    }

    @Deprecated
    @Override
    public void invalidatePermit(String resourceKey, String clientId) {
        ensureOpen();
        keys.normalizeResource(resourceKey);
        requireClient(clientId);
        // Receipts must survive completion. They expire automatically.
    }

    private enum ScriptKind {
        GRANT, REGISTER, CLEAR
    }

    private AcquireResult evalGrant(String resource, String client, String request) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object raw = evalshaOrEval(jedis, ScriptKind.GRANT, grantScript, 4,
                    new String[]{
                            keys.bucket(resource),
                            keys.wait(resource),
                            keys.pending(resource),
                            keys.permit(resource, client, request)
                    },
                    new String[]{
                            client,
                            Double.toString(config.getRatePerSec()),
                            Double.toString(config.getBurst()),
                            Long.toString(config.getPermitTtlMs()),
                            Long.toString(config.getPendingTtlMs())
                    });
            return parseGrantResult(raw);
        }
    }

    private void evalRegister(String resource, String client) {
        try (Jedis jedis = jedisPool.getResource()) {
            evalshaOrEval(jedis, ScriptKind.REGISTER, registerScript, 3,
                    new String[]{
                            keys.bucket(resource),
                            keys.wait(resource),
                            keys.pending(resource)
                    },
                    new String[]{
                            client,
                            Long.toString(config.getPendingTtlMs())
                    });
        }
    }

    private void evalClear(String resource, String client) {
        try (Jedis jedis = jedisPool.getResource()) {
            evalshaOrEval(jedis, ScriptKind.CLEAR, clearScript, 2,
                    new String[]{
                            keys.wait(resource),
                            keys.pending(resource)
                    },
                    new String[]{client});
        }
    }

    private Object evalshaOrEval(Jedis jedis,
                                 ScriptKind kind,
                                 String script,
                                 int keyCount,
                                 String[] keyArr,
                                 String[] argv) {
        String[] keysAndArgs = concat(keyArr, argv);
        String sha = shaOf(kind);
        try {
            if (sha != null) {
                return jedis.evalsha(sha, keyCount, keysAndArgs);
            }
        } catch (JedisException e) {
            if (!isNoScript(e)) {
                throw e;
            }
        }
        Object result = jedis.eval(script, keyCount, keysAndArgs);
        storeSha(kind, sha1(script));
        return result;
    }

    private String shaOf(ScriptKind kind) {
        switch (kind) {
            case GRANT:
                return grantSha;
            case REGISTER:
                return registerSha;
            case CLEAR:
                return clearSha;
            default:
                return null;
        }
    }

    private void storeSha(ScriptKind kind, String sha) {
        switch (kind) {
            case GRANT:
                grantSha = sha;
                break;
            case REGISTER:
                registerSha = sha;
                break;
            case CLEAR:
                clearSha = sha;
                break;
            default:
                break;
        }
    }

    private static String sha1(String script) {
        try {
            byte[] hash = java.security.MessageDigest.getInstance("SHA-1")
                    .digest(script.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) hex.append(String.format("%02x", b & 0xff));
            return hex.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean isNoScript(Throwable e) {
        String msg = e.getMessage();
        return e instanceof JedisNoScriptException || (msg != null && msg.startsWith("NOSCRIPT"));
    }

    @SuppressWarnings("unchecked")
    private static AcquireResult parseGrantResult(Object raw) {
        if (!(raw instanceof List)) {
            return AcquireResult.error("unexpected_lua_result:" + raw);
        }
        List<Object> list = (List<Object>) raw;
        if (list.size() < 4) {
            return AcquireResult.error("short_lua_result:" + list);
        }
        String status = String.valueOf(list.get(0));
        long retryAfter = parseLong(list.get(1), 50L);
        double tokens = parseDouble(list.get(2), 0D);
        String detail = String.valueOf(list.get(3));
        if ("GRANTED".equalsIgnoreCase(status)) {
            return AcquireResult.granted(tokens, detail);
        }
        if ("ERROR".equalsIgnoreCase(status)) return AcquireResult.error(detail);
        if ("WAIT".equalsIgnoreCase(status)) {
            return AcquireResult.waitFor(retryAfter, tokens, detail);
        }
        return AcquireResult.error("unknown_status:" + status + ":" + detail);
    }

    private AcquireResult fallbackAcquire(String resource, String client, String request, Exception cause) {
        switch (config.getFallbackMode()) {
            case LOCAL_SHARE:
                AcquireResult local = localFallback.tryAcquireRequest(resource, client, request);
                if (!local.isGranted()) return AcquireResult.waitFor(local.getRetryAfterMs(), 0D, "redis_down_local_share_wait");
                return AcquireResult.degradedLocal(0L,
                        "redis_down:" + cause.getClass().getSimpleName() + ":" + local.getDetail());
            case DENY:
                return AcquireResult.waitFor(config.getRedisTimeoutMs(), 0D,
                        "redis_down_deny:" + cause.getClass().getSimpleName());
            case ALLOW:
                return AcquireResult.degradedLocal(0L,
                        "redis_down_allow:" + cause.getClass().getSimpleName());
            default:
                return AcquireResult.error("unknown_fallback");
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("RedisFairGrantLimiter is closed");
        }
    }

    private static String requireClient(String clientId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalArgumentException("clientId is blank");
        }
        return clientId.trim();
    }

    private static String[] concat(String[] a, String[] b) {
        String[] out = new String[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static long parseLong(Object o, long def) {
        try {
            return Long.parseLong(String.valueOf(o));
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static double parseDouble(Object o, double def) {
        try {
            return Double.parseDouble(String.valueOf(o));
        } catch (NumberFormatException e) {
            return def;
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (ownPool) {
            jedisPool.close();
        }
    }
}
