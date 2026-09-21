package io.github.longxiaoyun.fairgrant.springboot;

import io.github.longxiaoyun.fairgrant.*;
import java.util.Objects;

/** Application-instance facade. WAIT/ERROR never execute the callback or schedule a retry. */
public final class FairGrantOperations {
    private final FairGrantLimiter limiter;
    private final FairGrantExecutor executor;
    private final String clientId;
    public FairGrantOperations(FairGrantLimiter limiter, FairGrantExecutor executor, String clientId) {
        this.limiter = Objects.requireNonNull(limiter, "limiter");
        this.executor = Objects.requireNonNull(executor, "executor");
        if (clientId == null || clientId.trim().isEmpty()) throw new IllegalArgumentException("clientId must not be blank");
        this.clientId = clientId.trim();
    }
    public String getClientId() { return clientId; }
    public AcquireResult tryAcquire(String resourceKey) { return limiter.tryAcquire(resourceKey, clientId); }
    public AcquireResult tryExecute(String resourceKey, FairGrantExecutor.Action action) throws Exception {
        return executor.tryExecute(resourceKey, clientId, action);
    }
}
