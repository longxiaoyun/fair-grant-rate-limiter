package io.github.longxiaoyun.fairgrant;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

final class LuaScriptLoader {

    private LuaScriptLoader() {
    }

    static String load(String classpathLocation) {
        InputStream in = LuaScriptLoader.class.getClassLoader().getResourceAsStream(classpathLocation);
        if (in == null) {
            throw new IllegalStateException("Lua script not found on classpath: " + classpathLocation);
        }
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] tmp = new byte[4096];
            int n;
            while ((n = in.read(tmp)) >= 0) {
                buf.write(tmp, 0, n);
            }
            return new String(buf.toByteArray(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read Lua script: " + classpathLocation, e);
        } finally {
            try {
                in.close();
            } catch (IOException ignore) {
                // ignore
            }
        }
    }
}
