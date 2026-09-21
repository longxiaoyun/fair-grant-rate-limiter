package io.github.longxiaoyun.fairgrant;

import java.util.Objects;

/**
 * Acquires NEW quota and immediately invokes one ready operation on the calling thread.
 * Does not queue, retry, interpret business results, or replay request receipts.
 */
public final class FairGrantExecutor {
    @FunctionalInterface
    public interface Action {
        void execute() throws Exception;
    }

    private final FairGrantLimiter limiter;

    public FairGrantExecutor(FairGrantLimiter limiter) {
        this.limiter = Objects.requireNonNull(limiter, "limiter");
    }

    /**
     * Invoke only after all preparation is complete. The action runs exactly once
     * if this acquisition is granted, and never on WAIT/ERROR. An action exception
     * propagates to the caller; consumed quota is not refunded. To retry a business
     * operation, invoke this method again so it acquires another token.
     * SDK-internal retries must still be configured by the application.
     */
    public AcquireResult tryExecute(String resourceKey, String clientId, Action action) throws Exception {
        Objects.requireNonNull(action, "action");
        AcquireResult result = limiter.tryAcquire(resourceKey, clientId);
        if (result.isGranted()) action.execute();
        return result;
    }
}
