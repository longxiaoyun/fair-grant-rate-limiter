package com.alibaba.fairgrant.limiter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FairGrantKeysTest {

    @Test
    public void normalizesCaseAndBuildsKeys() {
        FairGrantKeys keys = new FairGrantKeys("fg:");
        String resource = keys.resourceKey("MyProj", "MyTable");
        assertEquals("myproj:mytable", resource);
        assertEquals("fg:myproj:mytable:bucket", keys.bucket(resource));
        assertEquals("fg:myproj:mytable:wait", keys.wait(resource));
        assertEquals("fg:myproj:mytable:pending", keys.pending(resource));
        assertEquals("fg:myproj:mytable:permit:host-1", keys.permit(resource, "host-1"));
    }

    @Test
    public void defaultPrefixWhenNull() {
        FairGrantKeys keys = new FairGrantKeys(null);
        assertTrue(keys.bucket("a").startsWith("fair:grant:"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNullResource() {
        new FairGrantKeys("p:").normalizeResource(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsBlankResource() {
        new FairGrantKeys("p:").normalizeResource("  ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNullProject() {
        new FairGrantKeys("p:").resourceKey(null, "t");
    }
}
