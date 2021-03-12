package org.caffinitas.ohc.linked;

import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.HashAlgorithm;
import org.caffinitas.ohc.OHCache;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class CrossCheckGetPutTest extends CrossCheckTestBase {
    @Test(dataProvider = "types")
    public void testGet_Put(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            cache.put(42, "foo");
            Assert.assertEquals(cache.get(42), "foo");
            Assert.assertNull(cache.get(11));

            cache.put(11, "bar baz");
            Assert.assertEquals(cache.get(42), "foo");
            Assert.assertEquals(cache.get(11), "bar baz");

            cache.put(11, "dog");
            Assert.assertEquals(cache.get(42), "foo");
            Assert.assertEquals(cache.get(11), "dog");
        }
    }

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
