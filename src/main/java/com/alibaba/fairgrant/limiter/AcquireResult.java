package com.alibaba.fairgrant.limiter;

/**
 * Result of a fair-grant acquire attempt.
 * <p>
 * Callers should treat {@link Status#WAIT} as non-blocking: keep local work queued
 * and retry after {@link #getRetryAfterMs()} instead of sleeping on a worker thread.
 */
public final class AcquireResult {

    public enum Status {
        /** Permit granted; caller may perform the rate-limited action. */
        GRANTED,
        /** No permit now; retry later (fairness or token refill). */
        WAIT,
        /** Redis unavailable; limiter fell back to local share mode. */
        DEGRADED_LOCAL,
        /** Unexpected error; caller should requeue / back off. */
        ERROR
    }

    private final Status status;
    private final long retryAfterMs;
    private final double tokens;
    private final String detail;

    public AcquireResult(Status status, long retryAfterMs, double tokens, String detail) {
        this.status = status;
        this.retryAfterMs = Math.max(0L, retryAfterMs);
        this.tokens = tokens;
        this.detail = detail == null ? "" : detail;
    }

    public static AcquireResult granted(double tokens, String detail) {
        return new AcquireResult(Status.GRANTED, 0L, tokens, detail);
    }

    public static AcquireResult waitFor(long retryAfterMs, double tokens, String detail) {
        return new AcquireResult(Status.WAIT, retryAfterMs, tokens, detail);
    }

    public static AcquireResult degradedLocal(long retryAfterMs, String detail) {
        return new AcquireResult(Status.DEGRADED_LOCAL, retryAfterMs, -1D, detail);
    }

    public static AcquireResult error(String detail) {
        return new AcquireResult(Status.ERROR, 200L, -1D, detail);
    }

    public Status getStatus() {
        return status;
    }

    public boolean isGranted() {
        return status == Status.GRANTED || status == Status.DEGRADED_LOCAL;
    }

    public long getRetryAfterMs() {
        return retryAfterMs;
    }

    public double getTokens() {
        return tokens;
    }

    public String getDetail() {
        return detail;
    }

    @Override
    public String toString() {
        return "AcquireResult{status=" + status
                + ", retryAfterMs=" + retryAfterMs
                + ", tokens=" + tokens
                + ", detail='" + detail + "'}";
    }
}
