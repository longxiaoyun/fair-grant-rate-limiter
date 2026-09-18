package io.github.longxiaoyun.fairgrant;

/**
 * Distributed fair grant limiter API.
 * <p>
 * Typical ODPS / shared-quota usage:
 * <pre>
 *   AcquireResult r = limiter.tryAcquire("project", "table", machineId);
 *   if (r.isGranted()) {
 *     try {
 *       doCommit();
 *     } finally {
 *       limiter.invalidatePermit(resourceKey, machineId);
 *       // when local queue for this key is empty:
 *       limiter.clearPending(resourceKey, machineId);
 *     }
 *   } else {
 *     requeueLocally(r.getRetryAfterMs());
 *   }
 * </pre>
 */
public interface FairGrantLimiter {

    /**
     * Try to acquire one permit for {@code resourceKey} on behalf of {@code clientId}.
     * Non-blocking: returns {@link AcquireResult.Status#WAIT} instead of sleeping.
     *
     * @param resourceKey logical rate-limit key (e.g. {@code project:table})
     * @param clientId    stable client identity (e.g. host IP)
     */
    AcquireResult tryAcquire(String resourceKey, String clientId);

    /**
     * Convenience: resourceKey = project + ':' + table.
     */
    AcquireResult tryAcquire(String project, String table, String clientId);

    /** Announce that this client has pending local work for the key. */
    void registerPending(String resourceKey, String clientId);

    /** Remove pending + wait + permit when this client has no more local work. */
    void clearPending(String resourceKey, String clientId);

    /** Drop current permit after the protected action finishes (success or fail). */
    void invalidatePermit(String resourceKey, String clientId);
}
