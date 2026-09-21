package io.github.longxiaoyun.fairgrant;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FairGrantKeysTest {

    @Test
    public void normalizesCaseAndBuildsKeys() {
        FairGrantKeys keys = new FairGrantKeys("fg:");
        String resource = keys.resourceKey("MyProj", "MyTable");
        assertEquals("myproj:mytable", resource);
        assertEquals("fg:v2:{bXlwcm9qOm15dGFibGU}:bucket", keys.bucket(resource));
        assertEquals("fg:v2:{bXlwcm9qOm15dGFibGU}:wait", keys.wait(resource));
        assertEquals("fg:v2:{bXlwcm9qOm15dGFibGU}:pending", keys.pending(resource));
        assertEquals("fg:v2:{bXlwcm9qOm15dGFibGU}:permit:aG9zdC0x:cmVx", keys.permit(resource, "host-1", "req"));
    }

    @Test public void identitiesCannotCollideAtSeparators() {
        FairGrantKeys keys = new FairGrantKeys("fg:");
        org.junit.Assert.assertNotEquals(keys.permit("a", "b:c", "d"), keys.permit("a", "b", "c:d"));
    }
    @Test(expected = IllegalArgumentException.class) public void rejectsEmptyTable() {
        new FairGrantKeys("fg:").resourceKey("p", " ");
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
