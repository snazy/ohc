package org.caffinitas.ohc.linked;

import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.HashAlgorithm;
import org.caffinitas.ohc.OHCache;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class CrossCheckAddOrReplaceTest extends CrossCheckTestBase {
    @Test(dataProvider = "types")
    public void testAddOrReplace(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            for (int i = 0; i < TestUtils.manyCount; i++)
                assertTrue(cache.addOrReplace(i, "", Integer.toHexString(i)));

            assertTrue(cache.addOrReplace(42, Integer.toHexString(42), "foo"));
            Assert.assertEquals(cache.get(42), "foo");
            assertTrue(cache.addOrReplace(42, "foo", "bar"));
            Assert.assertEquals(cache.get(42), "bar");
            Assert.assertFalse(cache.addOrReplace(42, "foo", "bar"));
            Assert.assertEquals(cache.get(42), "bar");
        }
    }
}
