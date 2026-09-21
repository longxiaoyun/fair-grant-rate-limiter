package example;

import io.github.longxiaoyun.fairgrant.*;
import redis.clients.jedis.*;
import java.lang.management.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.io.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/** Closed-loop load client. This is an integration benchmark, not a JMH microbenchmark. */
public final class LoadWorker {
    private static final class Stats {
        volatile long calls, grants, waits, errors, degraded, down, nanos, maxNanos;
        long measuredStart, measuredEnd;
        final long[] samples = new long[20000];
        int filled;
        void record(AcquireResult r, long elapsed) {
            long count = ++calls;
            nanos += elapsed;
            maxNanos = Math.max(maxNanos, elapsed);
            if (r.isGranted()) grants++;
            if (r.getStatus() == AcquireResult.Status.WAIT) waits++;
            if (r.getStatus() == AcquireResult.Status.ERROR) errors++;
            if (r.getStatus() == AcquireResult.Status.DEGRADED_LOCAL) degraded++;
            if (r.getDetail().startsWith("redis_down_deny:")) down++;
            if (filled < samples.length) samples[filled++] = elapsed;
            else {
                long slot = ThreadLocalRandom.current().nextLong(count);
                if (slot < samples.length) samples[(int) slot] = elapsed;
            }
        }
        String json(boolean includeSamples) {
            StringBuilder out = new StringBuilder("{");
            out.append("\"calls\":").append(calls).append(",\"grants\":").append(grants)
                .append(",\"waits\":").append(waits).append(",\"errors\":").append(errors)
                .append(",\"degraded\":").append(degraded).append(",\"down\":").append(down)
                .append(",\"latency_sum_ns\":").append(nanos).append(",\"latency_max_ns\":").append(maxNanos)
                .append(",\"measured_ns\":").append(Math.max(0, measuredEnd - measuredStart));
            if (includeSamples) {
                out.append(",\"latency_sample_ns\":[");
                for (int i=0; i<filled; i++) { if(i>0) out.append(','); out.append(samples[i]); }
                out.append(']');
            }
            return out.append('}').toString();
        }
    }
    private static long epochUs() {
        Instant t = Instant.now();
        return t.getEpochSecond() * 1_000_000L + t.getNano() / 1000;
    }
    private static long processCpu() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        return os instanceof com.sun.management.OperatingSystemMXBean
                ? ((com.sun.management.OperatingSystemMXBean) os).getProcessCpuTime() : -1;
    }
    private static String snapshot(Stats[] stats, boolean samples) {
        long gcCount=0, gcMs=0;
        for(GarbageCollectorMXBean gc:ManagementFactory.getGarbageCollectorMXBeans()) {
            if(gc.getCollectionCount() >= 0) gcCount += gc.getCollectionCount();
            if(gc.getCollectionTime() >= 0) gcMs += gc.getCollectionTime();
        }
        StringBuilder out=new StringBuilder("{\"at_us\":").append(epochUs())
            .append(",\"heap_used\":").append(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed())
            .append(",\"heap_max\":").append(Runtime.getRuntime().maxMemory())
            .append(",\"cpu_ns\":").append(processCpu()).append(",\"gc_count\":").append(gcCount)
            .append(",\"gc_ms\":").append(gcMs).append(",\"threads\":[");
        for(int i=0;i<stats.length;i++){if(i>0)out.append(',');out.append(stats[i].json(samples));}
        return out.append("]}").toString();
    }
    public static void main(String[] args) throws Exception {
        int port=Integer.parseInt(args[0]);
        Path dir=Paths.get(args[1]);
        String node=args[2];
        int threadCount=Integer.parseInt(args[3]), resources=Integer.parseInt(args[4]);
        int warmup=Integer.parseInt(args[5]), seconds=Integer.parseInt(args[6]);
        boolean window=Boolean.parseBoolean(args[7]), respectWait=Boolean.parseBoolean(args[8]);
        boolean ping=Boolean.parseBoolean(args[9]), trace=Boolean.parseBoolean(args[10]);
        FairGrantConfig.Builder builder=FairGrantConfig.builder().keyPrefix("load:")
            .ratePerSec(Double.parseDouble(args[11])).burst(Double.parseDouble(args[12]))
            .permitTtlMs(2000).pendingTtlMs(5000);
        if(window) builder.slidingWindow(Integer.parseInt(args[13]),Integer.parseInt(args[14]));
        FairGrantConfig config=builder.build();
        JedisPoolConfig poolConfig=new JedisPoolConfig();
        poolConfig.setMaxTotal(32);poolConfig.setMaxIdle(8);poolConfig.setMaxWaitMillis(200);
        poolConfig.setTestOnBorrow(ping);
        Stats[] stats=new Stats[threadCount];
        CountDownLatch ready=new CountDownLatch(threadCount), go=new CountDownLatch(1);
        long[] base=new long[1];
        ExecutorService threads=Executors.newFixedThreadPool(threadCount);
        List<Future<?>> tasks=new ArrayList<Future<?>>();
        try(JedisPool pool=new JedisPool(poolConfig,"127.0.0.1",port,200);
            RedisFairGrantLimiter limiter=new RedisFairGrantLimiter(pool,config);
            PrintWriter traces=new PrintWriter(Files.newBufferedWriter(dir.resolve(node+"-grants.csv"),StandardCharsets.UTF_8))) {
            for(int i=0;i<threadCount;i++) {
                final int id=i;
                stats[i]=new Stats();
                tasks.add(threads.submit(() -> {
                    Stats s=stats[id];String client=node+"-"+id;
                    limiter.registerPending("resource-0",client);ready.countDown();
                    try {
                        go.await();
                        long start=base[0]+warmup*1_000_000_000L, end=start+seconds*1_000_000_000L;
                        long turn=0;
                        while(System.nanoTime()<end) {
                            String resource="resource-"+(turn++ % resources);
                            long before=System.nanoTime(), wallBefore=trace?epochUs():0;
                            AcquireResult r=limiter.tryAcquire(resource,client);
                            long after=System.nanoTime();
                            if(before>=start) {
                                if(s.measuredStart==0)s.measuredStart=before;
                                s.measuredEnd=after;s.record(r,after-before);
                                if(trace && r.isGranted()) {
                                    synchronized(traces){traces.println(wallBefore+","+epochUs()+","+client);traces.flush();}
                                }
                            }
                            if(!r.isGranted() && (respectWait || r.getStatus()==AcquireResult.Status.ERROR)) {
                                Thread.sleep(Math.max(1,Math.min(r.getRetryAfterMs(),Math.max(1,(end-System.nanoTime())/1_000_000))));
                            }
                        }
                        // Finite benchmark only: cancels its own remaining ready work.
                        for(int key=0;key<resources;key++)limiter.clearPending("resource-"+key,client);
                    } catch(InterruptedException interrupted){Thread.currentThread().interrupt();}
                }));
            }
            if(!ready.await(20,TimeUnit.SECONDS))throw new IllegalStateException("ready timeout");
            Files.write(dir.resolve(node+".ready"),new byte[0]);
            long deadline=System.nanoTime()+30_000_000_000L;
            while(!Files.exists(dir.resolve("start"))) {
                if(System.nanoTime()>deadline)throw new IllegalStateException("start timeout");
                Thread.sleep(10);
            }
            base[0]=System.nanoTime();go.countDown();
            while(true) {
                String current=snapshot(stats,false);
                Path temp=dir.resolve(node+".tmp");Files.write(temp,current.getBytes(StandardCharsets.UTF_8));
                Files.move(temp,dir.resolve(node+".snapshot.json"),StandardCopyOption.REPLACE_EXISTING);
                boolean done=true;for(Future<?> f:tasks)if(!f.isDone())done=false;
                if(done)break;
                Thread.sleep(1000);
            }
            for(Future<?> f:tasks)f.get();
            Files.write(dir.resolve(node+".result.json"),snapshot(stats,true).getBytes(StandardCharsets.UTF_8));
        } finally {threads.shutdownNow();}
    }
}
