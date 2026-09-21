package io.github.longxiaoyun.fairgrant;

import redis.clients.jedis.Jedis;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;

/** Starts an isolated Redis, never connects to an existing developer instance. */
final class RealRedisProcess implements AutoCloseable {
    final int port;
    private Process process;
    private final Path directory;
    RealRedisProcess() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            port = socket.getLocalPort();
        }
        directory = Files.createTempDirectory(Paths.get("target"), "redis-it-");
    }
    void start() throws Exception {
        process = new ProcessBuilder(System.getProperty("redis.server", "redis-server"),
                "--bind", "127.0.0.1", "--port", Integer.toString(port),
                "--save", "", "--appendonly", "no", "--dir", directory.toAbsolutePath().toString())
                .redirectErrorStream(true).redirectOutput(directory.resolve("redis.log").toFile()).start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) throw new IOException("Redis exited; see " + directory);
            try (Jedis jedis = new Jedis("127.0.0.1", port, 100)) {
                if ("PONG".equals(jedis.ping())) return;
            } catch (RuntimeException ignored) { Thread.sleep(20); }
        }
        throw new IOException("Redis startup timed out; see " + directory);
    }
    @Override public void close() throws Exception {
        if (process != null) {
            process.destroy();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                if (!process.waitFor(5, TimeUnit.SECONDS)) throw new IOException("Redis did not stop");
            }
        }
    }
}
