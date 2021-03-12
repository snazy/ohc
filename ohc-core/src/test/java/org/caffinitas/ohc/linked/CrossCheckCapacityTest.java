package org.caffinitas.ohc.linked;

import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.HashAlgorithm;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheStats;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class CrossCheckCapacityTest extends CrossCheckTestBase {

    @Test(dataProvider = "types")
    public void testFreeCapacity(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            long cap = cache.capacity();

            cache.put(42, "foo");
            long free1 = cache.freeCapacity();
            assertTrue(cap > free1);

            cache.put(11, "bar baz");
            long free2 = cache.freeCapacity();
            assertTrue(free1 > free2);
            assertTrue(cap > free2);

            cache.put(11, "bar baz dog mud forest");
            long free3 = cache.freeCapacity();
            assertTrue(free2 > free3);
            assertTrue(cap > free3);

            cache.remove(11);
            long free4 = cache.freeCapacity();
            Assert.assertEquals(free1, free4);
            assertTrue(cap > free4);

            cache.remove(42);
            long free5 = cache.freeCapacity();
            Assert.assertEquals(free5, cap);
        }
    }

    @Test(dataProvider = "types")
    public void testSetCapacity(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            long cap = cache.capacity();

            cache.put(42, "foo");

            long free = cache.freeCapacity();

            cache.setCapacity(cap + TestUtils.ONE_MB);
            Assert.assertEquals(cache.capacity(), cap + TestUtils.ONE_MB);
            Assert.assertEquals(cache.freeCapacity(), free + TestUtils.ONE_MB);

            cache.setCapacity(cap - TestUtils.ONE_MB);
            Assert.assertEquals(cache.capacity(), cap - TestUtils.ONE_MB);
            Assert.assertEquals(cache.freeCapacity(), free - TestUtils.ONE_MB);

            cache.setCapacity(0L);
            Assert.assertEquals(cache.capacity(), 0L);
            assertTrue(cache.freeCapacity() < 0L);

            Assert.assertEquals(cache.size(), 1);
            cache.put(42, "bar");
            Assert.assertEquals(cache.size(), 0);
            Assert.assertEquals(cache.freeCapacity(), 0L);
        }
    }

    @Test(dataProvider = "types")
    public void testResetStatistics(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toString(i));

            for (int i = 0; i < 30; i++)
                cache.put(i, Integer.toString(i));

            for (int i = 0; i < 50; i++)
                cache.get(i);

            for (int i = 100; i < 120; i++)
                cache.get(i);

            for (int i = 0; i < 25; i++)
                cache.remove(i);

            OHCacheStats stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), 100);
            Assert.assertEquals(stats.getPutReplaceCount(), 30);
            Assert.assertEquals(stats.getHitCount(), 50);
            Assert.assertEquals(stats.getMissCount(), 20);
            Assert.assertEquals(stats.getRemoveCount(), 25);

            cache.resetStatistics();

            stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), 0);
            Assert.assertEquals(stats.getPutReplaceCount(), 0);
            Assert.assertEquals(stats.getHitCount(), 0);
            Assert.assertEquals(stats.getMissCount(), 0);
            Assert.assertEquals(stats.getRemoveCount(), 0);
        }
    }

    @Test(dataProvider = "types")
    public void testTooBigEntryOnPut(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm, 8, -1, -1, Util.roundUpTo8(TestUtils.intSerializer.serializedSize(1)) + Util.ENTRY_OFF_DATA + 5))
        {
            cache.put(1, new String(new byte[100]));
            Assert.assertEquals(cache.size(), 0);

            cache.putIfAbsent(1, new String(new byte[100]));
            Assert.assertEquals(cache.size(), 0);

            cache.addOrReplace(1, "foo", new String(new byte[100]));
            Assert.assertEquals(cache.size(), 0);

            cache.addOrReplace(1, "bar", "foo");
            Assert.assertEquals(cache.size(), 1);
            Assert.assertEquals(cache.get(1), "foo");

            cache.addOrReplace(1, "foo", new String(new byte[100]));
            Assert.assertEquals(cache.size(), 0);
            Assert.assertEquals(cache.get(1), null);
        }
    }

    @Test(dataProvider = "none")
    public void testEvictionNone(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm, 1, -1, 1, -1))
        {
            String v = longString();
            int i = 0;

            for (; cache.freeCapacity() >= 1000; i++)
                assertTrue(cache.put(i++, v));

            assertFalse(cache.put(i++, v));

            cache.remove(0);

            assertTrue(cache.put(i++, v));

            assertFalse(cache.put(i++, v));
        }
    }
}
