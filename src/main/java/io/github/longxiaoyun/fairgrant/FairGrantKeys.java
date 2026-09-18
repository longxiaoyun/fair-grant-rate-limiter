package io.github.longxiaoyun.fairgrant;

import java.util.Locale;

final class FairGrantKeys {

    private final String prefix;

    FairGrantKeys(String prefix) {
        this.prefix = prefix == null ? "fair:grant:" : prefix;
    }

    String normalizeResource(String resourceKey) {
        if (resourceKey == null) {
            throw new IllegalArgumentException("resourceKey is null");
        }
        String k = resourceKey.trim();
        if (k.isEmpty()) {
            throw new IllegalArgumentException("resourceKey is blank");
        }
        return k.toLowerCase(Locale.ROOT);
    }

    String resourceKey(String project, String table) {
        if (project == null || table == null) {
            throw new IllegalArgumentException("project/table is null");
        }
        return normalizeResource(project.trim() + ":" + table.trim());
    }

    String bucket(String resource) {
        return prefix + resource + ":bucket";
    }

    String wait(String resource) {
        return prefix + resource + ":wait";
    }

    String pending(String resource) {
        return prefix + resource + ":pending";
    }

    String permit(String resource, String clientId) {
        return prefix + resource + ":permit:" + clientId;
    }
}
