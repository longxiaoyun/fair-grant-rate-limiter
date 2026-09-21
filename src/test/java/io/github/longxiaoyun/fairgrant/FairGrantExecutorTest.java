package io.github.longxiaoyun.fairgrant;
import org.junit.Test;
import static org.junit.Assert.*;
public class FairGrantExecutorTest {
    private static class Stub implements FairGrantLimiter {
        AcquireResult next=AcquireResult.granted(0,"ok"); int acquisitions;
        public AcquireResult tryAcquire(String r,String c){ acquisitions++; return next; }
        public AcquireResult tryAcquire(String p,String t,String c){throw new AssertionError();}
        public AcquireResult tryAcquireRequest(String r,String c,String id){throw new AssertionError("must acquire new quota");}
        public void registerPending(String r,String c){throw new AssertionError();}
        public void clearPending(String r,String c){throw new AssertionError("must not refund");}
        public void invalidatePermit(String r,String c){throw new AssertionError("must not refund");}
    }
    @Test public void invokesOnceOnlyWhenGrantedAndReacquiresForEveryOperation() throws Exception {
        Stub stub=new Stub(); FairGrantExecutor executor=new FairGrantExecutor(stub); int[] calls={0};
        executor.tryExecute("r","c",()->calls[0]++);
        executor.tryExecute("r","c",()->calls[0]++);
        stub.next=AcquireResult.waitFor(10,0,"window_full"); executor.tryExecute("r","c",()->calls[0]++);
        stub.next=AcquireResult.error("error"); executor.tryExecute("r","c",()->calls[0]++);
        assertEquals(2,calls[0]); assertEquals(4,stub.acquisitions);
    }
    @Test public void businessFailurePropagatesAndRetryAcquiresFreshQuota() throws Exception {
        Stub stub=new Stub(); FairGrantExecutor executor=new FairGrantExecutor(stub); Exception failure=new Exception("business");
        try { executor.tryExecute("r","c",()->{throw failure;}); fail(); } catch(Exception e){assertSame(failure,e);}
        executor.tryExecute("r","c",()->{}); assertEquals(2,stub.acquisitions);
    }
    @Test public void nullActionCannotConsumeQuota() throws Exception {
        Stub stub=new Stub();
        try { new FairGrantExecutor(stub).tryExecute("r","c",null); fail(); } catch(NullPointerException expected){}
        assertEquals(0,stub.acquisitions);
    }
}
