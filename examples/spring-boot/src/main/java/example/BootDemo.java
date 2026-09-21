package example;

import io.github.longxiaoyun.fairgrant.*;
import io.github.longxiaoyun.fairgrant.springboot.FairGrantOperations;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;

@SpringBootApplication
public class BootDemo {
    public static void main(String[] args) {
        try (ConfigurableApplicationContext ignored = SpringApplication.run(BootDemo.class, args)) {
            // Spring closes the limiter and its pool, including the idle validation thread.
        }
    }
    private static long micros() {
        Instant now = Instant.now(); return now.getEpochSecond()*1000000L + now.getNano()/1000;
    }
    @Bean
    CommandLineRunner demo(FairGrantOperations grants, BatchWriter writer, FairGrantLimiter limiter, Environment env) {
        return args -> {
            System.out.println("BOOT_VERSION " + SpringBootVersion.getVersion());
            String node = env.getProperty("demo.node", "standalone");
            String folder = env.getProperty("demo.run-dir");
            long deadline = System.nanoTime() + 45000000000L;
            if (folder != null) {
                limiter.registerPending("table-a", grants.getClientId());
                Files.write(Paths.get(folder,node+".ready"), grants.getClientId().getBytes(StandardCharsets.UTF_8));
                while(!Files.exists(Paths.get(folder,"go"))) {
                    if(System.nanoTime()>deadline) throw new IllegalStateException("start deadline exceeded");
                    limiter.registerPending("table-a", grants.getClientId());
                    Thread.sleep(100);
                }
            }
            for(int task=0;task<3;) {
                if(System.nanoTime()>deadline) throw new IllegalStateException("demo deadline exceeded");
                final int currentTask = task;
                final long before = micros();
                AcquireResult result = writer.submit("table-a", () -> {
                    long after = micros();
                    String target = env.getProperty("demo.url");
                    if(target != null) {
                        HttpURLConnection c = (HttpURLConnection)new URL(target).openConnection();
                        c.setRequestMethod("POST"); c.setDoOutput(true); c.setConnectTimeout(1000); c.setReadTimeout(1000);
                        try {
                            byte[] body = (node+":"+currentTask).getBytes(StandardCharsets.UTF_8);
                            c.setFixedLengthStreamingMode(body.length);
                            try(java.io.OutputStream out=c.getOutputStream()){out.write(body);}
                            if(c.getResponseCode()!=204) throw new IllegalStateException("HTTP submission failed");
                        } finally { c.disconnect(); }
                    }
                    System.out.println("GRANT " + grants.getClientId()+" " + currentTask+" " + before+" " +after);
                });
                if(result.isGranted()) task++;
                else if(result.getStatus()==AcquireResult.Status.ERROR) throw new IllegalStateException(result.toString());
                else Thread.sleep(Math.max(1,result.getRetryAfterMs()));
            }
            System.out.println("COMPLETE " + node);
        };
    }
}
