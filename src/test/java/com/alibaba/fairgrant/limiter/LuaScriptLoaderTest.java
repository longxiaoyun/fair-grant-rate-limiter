package com.alibaba.fairgrant.limiter;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class LuaScriptLoaderTest {

    @Test
    public void loadsBundledScripts() {
        String grant = LuaScriptLoader.load("lua/fair_grant.lua");
        assertTrue(grant.contains("KEYS[1]"));
        assertTrue(grant.contains("GRANTED"));

        String register = LuaScriptLoader.load("lua/register_pending.lua");
        assertTrue(register.contains("SADD"));

        String clear = LuaScriptLoader.load("lua/clear_pending.lua");
        assertTrue(clear.contains("SREM"));
    }

    @Test(expected = IllegalStateException.class)
    public void missingScriptThrows() {
        LuaScriptLoader.load("lua/does_not_exist.lua");
    }
}
