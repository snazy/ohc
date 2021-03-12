package org.caffinitas.ohc.linked;

import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.HashAlgorithm;
import org.caffinitas.ohc.OHCache;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class CrossCheckPutIfAbsentTest extends CrossCheckTestBase {
    @Test(dataProvider = "types")
    public void testPutIfAbsent(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            for (int i = 0; i < TestUtils.manyCount; i++)
                assertTrue(cache.putIfAbsent(i, Integer.toHexString(i)));

            assertTrue(cache.putIfAbsent(-42, "foo"));
            Assert.assertEquals(cache.get(-42), "foo");
            Assert.assertFalse(cache.putIfAbsent(-42, "foo"));
        }
    }
}
