package io.github.longxiaoyun.fairgrant;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;

final class FairGrantKeys {
    private final String prefix;
    FairGrantKeys(String prefix) { this.prefix = prefix == null ? "fair:grant:" : prefix; }

    String normalizeResource(String resourceKey) {
        return requireId(resourceKey, "resourceKey").toLowerCase(Locale.ROOT);
    }
    String resourceKey(String project, String table) {
        return normalizeResource(requireId(project, "project") + ":" + requireId(table, "table"));
    }
    static String requireId(String value, String name) {
        if (value == null || value.trim().isEmpty()) throw new IllegalArgumentException(name + " is blank");
        return value.trim();
    }
    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
    private String base(String resource) { return prefix + "v3:{" + encode(resource) + "}:"; }
    String bucket(String resource) { return base(resource) + "bucket"; }
    String wait(String resource) { return base(resource) + "wait"; }
    String window(String resource) { return base(resource) + "window"; }
    String pending(String resource) { return base(resource) + "pending"; }
    String permit(String resource, String client, String request) {
        return base(resource) + "permit:" + encode(client) + ":" + encode(request);
    }
}
