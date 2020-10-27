package org.caffinitas.ohc.linked;

import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.HashAlgorithm;
import org.caffinitas.ohc.OHCache;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class CrossCheckContainsKeyTest extends CrossCheckTestBase {
    @Test(dataProvider = "types")
    public void testContainsKey(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            cache.put(42, "foo");
            assertTrue(cache.containsKey(42));
            Assert.assertFalse(cache.containsKey(11));
        }
    }
}
