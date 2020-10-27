package org.caffinitas.ohc.linked;

import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.HashAlgorithm;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertTrue;

public abstract class CrossCheckTestBase {
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    static DoubleCheckCacheImpl<Integer, String> cache(Eviction eviction, HashAlgorithm hashAlgorithm)
    {
        return cache(eviction, hashAlgorithm, 256);
    }

    static DoubleCheckCacheImpl<Integer, String> cache(Eviction eviction, HashAlgorithm hashAlgorithm, long capacity)
    {
        return cache(eviction, hashAlgorithm, capacity, -1);
    }

    static DoubleCheckCacheImpl<Integer, String> cache(Eviction eviction, HashAlgorithm hashAlgorithm, long capacity, int hashTableSize)
    {
        return cache(eviction, hashAlgorithm, capacity, hashTableSize, -1, -1);
    }

    static DoubleCheckCacheImpl<Integer, String> cache(Eviction eviction, HashAlgorithm hashAlgorithm, long capacity, int hashTableSize, int segments, long maxEntrySize)
    {
        OHCacheBuilder<Integer, String> builder = OHCacheBuilder.<Integer, String>newBuilder()
                .keySerializer(TestUtils.intSerializer)
                .valueSerializer(TestUtils.stringSerializer)
                .eviction(eviction)
                .hashMode(hashAlgorithm)
                .capacity(capacity * TestUtils.ONE_MB);
        if (hashTableSize > 0)
            builder.hashTableSize(hashTableSize);
        if (segments > 0)
            builder.segmentCount(segments);
        else
            // use 16 segments by default to prevent differing test behaviour on varying test hardware
            builder.segmentCount(16);
        if (maxEntrySize > 0)
            builder.maxEntrySize(maxEntrySize);

        return new DoubleCheckCacheImpl<>(builder);
    }

    @DataProvider(name = "types")
    public Object[][] cacheEviction()
    {
        return new Object[][]{
                { Eviction.LRU, HashAlgorithm.MURMUR3 },
                { Eviction.LRU, HashAlgorithm.CRC32 },
                { Eviction.LRU, HashAlgorithm.CRC32C },
                { Eviction.LRU, HashAlgorithm.XX },
                { Eviction.W_TINY_LFU, HashAlgorithm.MURMUR3 },
                { Eviction.W_TINY_LFU, HashAlgorithm.CRC32 },
                { Eviction.W_TINY_LFU, HashAlgorithm.CRC32C },
                { Eviction.W_TINY_LFU, HashAlgorithm.XX },
                { Eviction.NONE, HashAlgorithm.MURMUR3 },
                { Eviction.NONE, HashAlgorithm.CRC32 },
                { Eviction.NONE, HashAlgorithm.CRC32C },
                { Eviction.NONE, HashAlgorithm.XX }
        };
    }

    @DataProvider(name = "lru")
    public Object[][] cacheLru()
    {
        return new Object[][]{
                { Eviction.LRU, HashAlgorithm.MURMUR3 },
                { Eviction.LRU, HashAlgorithm.CRC32 },
                { Eviction.LRU, HashAlgorithm.CRC32C },
                { Eviction.LRU, HashAlgorithm.XX }
        };
    }

    @DataProvider(name = "none")
    public Object[][] cacheNone()
    {
        return new Object[][]{
                { Eviction.NONE, HashAlgorithm.MURMUR3 },
                { Eviction.NONE, HashAlgorithm.CRC32 },
                { Eviction.NONE, HashAlgorithm.CRC32C },
                { Eviction.NONE, HashAlgorithm.XX }
        };
    }

    @Test(dataProvider = "types")
    public void testBasics(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());

            cache.put(11, "hello world \u00e4\u00f6\u00fc\u00df");

            assertTrue(cache.freeCapacity() < cache.capacity());

            String v = cache.get(11);
            Assert.assertEquals(v, "hello world \u00e4\u00f6\u00fc\u00df");

            cache.remove(11);

            Assert.assertEquals(cache.freeCapacity(), cache.capacity());

            TestUtils.fill5(cache);

            TestUtils.check5(cache);

            // implicitly compares stats
            cache.stats();
        }
    }

    static String longString()
    {
        char[] chars = new char[900];
        for (int i = 0; i < chars.length; i++)
            chars[i] = (char) ('A' + i % 26);
        return new String(chars);
    }

    static int sum(int[] ints)
    {
        int r = 0;
        for (int i : ints)
            r += i;
        return r;
    }
}
