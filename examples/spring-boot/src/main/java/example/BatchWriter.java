package example;

import io.github.longxiaoyun.fairgrant.*;
import io.github.longxiaoyun.fairgrant.springboot.FairGrantOperations;
import org.springframework.stereotype.Service;

@Service
public class BatchWriter {
    private final FairGrantOperations grants;

    public BatchWriter(FairGrantOperations grants) { this.grants = grants; }

    public AcquireResult submit(String table, FairGrantExecutor.Action commitBatch) throws Exception {
        return grants.tryExecute(table, commitBatch);
    }
}
