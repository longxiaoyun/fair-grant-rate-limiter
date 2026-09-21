package io.github.longxiaoyun.fairgrant;

/** Shared token grants. Fairness is between live waiting client identities. */
public interface FairGrantLimiter {
    /** Each invocation is a NEW attempt; successful calls consume separate tokens. */
    AcquireResult tryAcquire(String resourceKey, String clientId);

    /** Convenience overload for project + ':' + table. */
    AcquireResult tryAcquire(String project, String table, String clientId);

    /**
     * Retry the SAME logical operation with the same requestId. Distinct operations
     * MUST use distinct IDs. A grant receipt is retained for permitTtlMs; replaying
     * it does not consume another token. This does not make business execution
     * exactly-once. Do not retry an old ID after its receipt retention expires.
     */
    AcquireResult tryAcquireRequest(String resourceKey, String clientId, String requestId);

    /** Join/renew only for work ready to execute immediately; does not consume a token. */
    void registerPending(String resourceKey, String clientId);

    /** Cancel waiting. Does not refund tokens, erase receipts, or reset local rate state. */
    void clearPending(String resourceKey, String clientId);

    /**
     * @deprecated Grants no longer hold a client's queue position. Receipts expire
     * automatically and must survive completion for retry safety. This is a no-op.
     */
    @Deprecated
    void invalidatePermit(String resourceKey, String clientId);
}
