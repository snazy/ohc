/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.caffinitas.ohc.linked;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.caffinitas.ohc.CloseableIterator;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.HashAlgorithm;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.OHCacheStats;
import org.caffinitas.ohc.histo.EstimatedHistogram;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

// This unit test uses the production cache implementation and an independent OHCache implementation used to
// cross-check the production implementation.
public class CrossCheckTest
{
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

    @Test(dataProvider = "types", dependsOnMethods = "testBasics")
    public void testManyValues(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm, 64, -1))
        {
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());

            TestUtils.fillMany(cache);

            OHCacheStats stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
                Assert.assertEquals(cache.get(i), Integer.toHexString(i), "for i="+i);

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
            {
                Assert.assertEquals(cache.get(i), Integer.toHexString(i), "for i="+i);
                assertTrue(cache.containsKey(i), "for i="+i);
                cache.put(i, Integer.toOctalString(i));
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i), "for i="+i);
                Assert.assertEquals(cache.size(), TestUtils.manyCount, "for i="+i);
                assertTrue(cache.containsKey(i), "for i="+i);
            }

            stats = cache.stats();
            Assert.assertEquals(stats.getPutReplaceCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i), "for i="+i);

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), TestUtils.manyCount * 6);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
            {
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i), "for i=" + i);
                assertTrue(cache.containsKey(i), "for i=" + i);
                cache.remove(i);
                Assert.assertNull(cache.get(i), "for i=" + i);
                Assert.assertFalse(cache.containsKey(i), "for i=" + i);
                Assert.assertEquals(cache.stats().getRemoveCount(), i + 1);
                Assert.assertEquals(cache.size(), TestUtils.manyCount - i - 1, "for i=" + i);
            }

            stats = cache.stats();
            Assert.assertEquals(stats.getRemoveCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), 0);
            Assert.assertEquals(stats.getFree(), stats.getCapacity());
        }
    }

    @Test(dataProvider = "types", dependsOnMethods = "testBasics")
    public void testHotN(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            TestUtils.fill5(cache);

            try (CloseableIterator<Integer> iter = cache.hotKeyIterator(1))
            {
                Assert.assertNotNull(iter.next());
            }
        }
    }

    @Test(dataProvider = "lru", dependsOnMethods = "testManyValues")
    public void testCleanUpLRU(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException, InterruptedException
    {
        String v = longString();

        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm, 4, -1, 1, -1))
        {
            int i;
            for (i = 0; cache.freeCapacity() > 950; i++)
            {
                cache.put(i, v);
                if ((i % 10000) == 0)
                    Assert.assertEquals(cache.stats().getEvictionCount(), 0L, "oops - cleanup triggered - fix the unit test!");
            }

            Assert.assertEquals(cache.stats().getEvictionCount(), 0L, "oops - cleanup triggered - fix the unit test!");

            cache.put(i++, v);

            Assert.assertEquals(cache.stats().getEvictionCount(), 1L, "cleanup did not run");

            for (int j = 0; j < 10000; j++, i++)
                cache.put(i, v);

            assertTrue(cache.stats().getEvictionCount() >= 10000L, "cleanup did not run");
        }
    }

    private String longString()
    {
        char[] chars = new char[900];
        for (int i = 0; i < chars.length; i++)
            chars[i] = (char) ('A' + i % 26);
        return new String(chars);
    }

    @Test(dataProvider = "lru", dependsOnMethods = "testManyValues")
    public void testLRU(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException, InterruptedException
    {
        String v = longString();

        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm, 4, -1, 1, -1))
        {
            int i;
            for (i = 0; cache.freeCapacity() > 950; i++)
            {
                cache.put(i, v);
                if ((i % 10000) == 0)
                    Assert.assertEquals(cache.stats().getEvictionCount(), 0L, "oops - cleanup triggered - fix the unit test!");
            }
            int k = i;

            // reference ~50%
            for (int j = 0; j < k / 2; j++)
                cache.get(j);

            for (int j = 0; j < k / 2; j++, i++)
                cache.put(i, v);

            for (int j = 0; j < k / 2 - 1; j++)
                Assert.assertEquals(cache.containsKey(j), true, Integer.toString(j));

            for (int j = k / 2; j < k - 2; j++)
                Assert.assertEquals(cache.containsKey(j), false, Integer.toString(j));
        }
    }

    @Test(dataProvider = "types", dependsOnMethods = "testBasics")
    public void testPutTooLarge(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException, InterruptedException
    {
        char[] c940 = new char[8192];
        for (int i = 0; i < c940.length; i++)
            c940[i] = (char) ('A' + i % 26);
        String v = new String(c940);

        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm, 1, -1, -1, 8192))
        {
            cache.put(88, v);

            Assert.assertNull(cache.get(88));
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());
            Assert.assertEquals(cache.stats().getEvictionCount(), 0L, "eviction must not be performed");
        }
    }

    // per-method tests

    @Test(dataProvider = "types", dependsOnMethods = "testBasics")
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

    @Test(dataProvider = "types")
    public void testPutAll(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            Map<Integer, String> map = new HashMap<>();
            map.put(1, "one");
            map.put(2, "two");
            map.put(3, "three");
            cache.putAll(map);
            Assert.assertEquals(cache.get(1), map.get(1));
            Assert.assertEquals(cache.get(2), map.get(2));
            Assert.assertEquals(cache.get(3), map.get(3));
            Assert.assertEquals(cache.stats().getSize(), 3);
        }
    }

    @Test(dataProvider = "types")
    public void testRemove(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            TestUtils.fillMany(cache);

            cache.put(42, "foo");
            Assert.assertEquals(cache.get(42), "foo");
            cache.remove(42);
            Assert.assertNull(cache.get(42));

            Random r = new Random();
            for (int i = 0; i < TestUtils.manyCount; i++)
                cache.remove(r.nextInt(TestUtils.manyCount));
        }
    }

    @Test(dataProvider = "types")
    public void testRemoveAll(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));
            for (int i = 0; i < 100; i++)
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i));

            long lastFree = cache.freeCapacity();
            assertTrue(lastFree < cache.capacity());
            assertTrue(cache.size() > 0);

            List<Integer> coll = new ArrayList<>();
            for (int i = 0; i < 10; i++)
                coll.add(i);
            cache.removeAll(coll);

            assertTrue(cache.freeCapacity() > lastFree);
            Assert.assertEquals(cache.size(), 90);

            for (int i = 10; i < 50; i++)
                coll.add(i);
            cache.removeAll(coll);

            assertTrue(cache.freeCapacity() > lastFree);
            Assert.assertEquals(cache.size(), 50);

            for (int i = 50; i < 100; i++)
                coll.add(i);
            cache.removeAll(coll);

            Assert.assertEquals(cache.freeCapacity(), cache.capacity());
            Assert.assertEquals(cache.size(), 0);
        }
    }

    @Test(dataProvider = "types")
    public void testClear(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));
            for (int i = 0; i < 100; i++)
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i));

            assertTrue(cache.freeCapacity() < cache.capacity());
            assertTrue(cache.size() > 0);

            cache.clear();

            Assert.assertEquals(cache.freeCapacity(), cache.capacity());
            Assert.assertEquals(cache.size(), 0);
        }
    }

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

    @Test(dataProvider = "types")
    public void testHotKeyIterator(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (DoubleCheckCacheImpl<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            Assert.assertFalse(cache.hotKeyIterator(10).hasNext());

            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));

            Assert.assertEquals(cache.stats().getSize(), 100);

            try (CloseableIterator<Integer> iProd = cache.prod.hotKeyIterator(10))
            {
                try (CloseableIterator<Integer> iCheck = cache.check.hotKeyIterator(10))
                {
                    while (iProd.hasNext())
                    {
                        assertTrue(iCheck.hasNext());

                        Integer kProd = iProd.next();
                        Integer kCheck = iCheck.next();

                        Assert.assertEquals(kProd, kCheck);
                    }

                    Assert.assertFalse(iCheck.hasNext());
                }
            }
        }
    }

    @Test(dataProvider = "types", dependsOnMethods = "testBasics")
    public void testKeyIterator1(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm, 32))
        {
            long capacity = cache.capacity();
            Assert.assertEquals(cache.freeCapacity(), capacity);

            TestUtils.fill5(cache);

            Set<Integer> returned = new TreeSet<>();
            Iterator<Integer> iter = cache.keyIterator();
            for (int i = 0; i < 5; i++)
            {
                assertTrue(iter.hasNext());
                returned.add(iter.next());
            }
            Assert.assertFalse(iter.hasNext());
            Assert.assertEquals(returned.size(), 5);

            assertTrue(returned.contains(1));
            assertTrue(returned.contains(2));
            assertTrue(returned.contains(3));
            assertTrue(returned.contains(4));
            assertTrue(returned.contains(5));

            returned.clear();

            iter = cache.keyIterator();
            for (int i = 0; i < 5; i++)
                returned.add(iter.next());
            Assert.assertFalse(iter.hasNext());
            Assert.assertEquals(returned.size(), 5);

            assertTrue(returned.contains(1));
            assertTrue(returned.contains(2));
            assertTrue(returned.contains(3));
            assertTrue(returned.contains(4));
            assertTrue(returned.contains(5));

            iter = cache.keyIterator();
            for (int i = 0; i < 5; i++)
            {
                iter.next();
                iter.remove();
            }

            Assert.assertEquals(cache.freeCapacity(), capacity);

            Assert.assertEquals(cache.size(), 0);
            Assert.assertNull(cache.get(1));
            Assert.assertNull(cache.get(2));
            Assert.assertNull(cache.get(3));
            Assert.assertNull(cache.get(4));
            Assert.assertNull(cache.get(5));
        }
    }

    @Test(dataProvider = "types")
    public void testKeyIterator2(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            Assert.assertFalse(cache.keyIterator().hasNext());

            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));

            Assert.assertEquals(cache.stats().getSize(), 100);

            Set<Integer> keys = new HashSet<>();
            try (CloseableIterator<Integer> iter = cache.keyIterator())
            {
                while (iter.hasNext())
                {
                    Integer k = iter.next();
                    assertTrue(keys.add(k));
                }
            }
            Assert.assertEquals(keys.size(), 100);
            for (int i = 0; i < 100; i++)
                assertTrue(keys.contains(i));

            cache.clear();

            Assert.assertEquals(cache.stats().getSize(), 0);
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());

            // add again and remove via iterator

            Assert.assertFalse(cache.keyBufferIterator().hasNext());

            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));

            try (CloseableIterator<ByteBuffer> iter = cache.keyBufferIterator())
            {
                while (iter.hasNext())
                {
                    iter.next();
                    iter.remove();
                }
            }

            Assert.assertFalse(cache.keyBufferIterator().hasNext());

            Assert.assertEquals(cache.stats().getSize(), 0);
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());
        }
    }

    @Test(dataProvider = "types")
    public void testHotKeyBufferIterator(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (DoubleCheckCacheImpl<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            Assert.assertFalse(cache.hotKeyIterator(10).hasNext());

            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));

            Assert.assertEquals(cache.stats().getSize(), 100);

            try (CloseableIterator<ByteBuffer> iProd = cache.prod.hotKeyBufferIterator(10))
            {
                try (CloseableIterator<ByteBuffer> iCheck = cache.check.hotKeyBufferIterator(10))
                {
                    while (iProd.hasNext())
                    {
                        assertTrue(iCheck.hasNext());

                        ByteBuffer kProd = iProd.next();
                        ByteBuffer kCheck = iCheck.next();

                        Assert.assertEquals(kProd, kCheck);
                    }

                    Assert.assertFalse(iCheck.hasNext());
                }
            }
        }
    }

    @Test(dataProvider = "types")
    public void testKeyBufferIterator(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            Assert.assertFalse(cache.keyBufferIterator().hasNext());

            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));

            Assert.assertEquals(cache.stats().getSize(), 100);

            Set<Integer> keys = new HashSet<>();
            try (CloseableIterator<ByteBuffer> iter = cache.keyBufferIterator())
            {
                while (iter.hasNext())
                {
                    ByteBuffer k = iter.next();
                    ByteBuffer k2 = ByteBuffer.allocate(k.remaining());
                    k2.put(k);
                    Integer key = TestUtils.intSerializer.deserialize(ByteBuffer.wrap(k2.array()));
                    assertTrue(keys.add(key));
                }
            }
            Assert.assertEquals(keys.size(), 100);
            for (int i = 0; i < 100; i++)
                assertTrue(keys.contains(i));

            cache.clear();

            Assert.assertEquals(cache.stats().getSize(), 0);
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());

            // add again and remove via iterator

            Assert.assertFalse(cache.keyBufferIterator().hasNext());

            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));

            try (CloseableIterator<ByteBuffer> iter = cache.keyBufferIterator())
            {
                while (iter.hasNext())
                {
                    iter.next();
                    iter.remove();
                }
            }

            Assert.assertFalse(cache.keyBufferIterator().hasNext());

            Assert.assertEquals(cache.stats().getSize(), 0);
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());
        }
    }

    @Test(dataProvider = "types")
    public void testGetBucketHistogram(Eviction eviction, HashAlgorithm hashAlgorithm) throws Exception
    {
        try (DoubleCheckCacheImpl<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            Assert.assertFalse(cache.keyIterator().hasNext());

            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));

            Assert.assertEquals(cache.stats().getSize(), 100);

            EstimatedHistogram hProd = cache.prod.getBucketHistogram();
            Assert.assertEquals(hProd.count(), sum(cache.prod.hashTableSizes()));
            long[] offsets = hProd.getBucketOffsets();
            Assert.assertEquals(offsets.length, 3);
            Assert.assertEquals(offsets[0], -1);
            Assert.assertEquals(offsets[1], 0);
            Assert.assertEquals(offsets[2], 1);
            // hProd.log(LoggerFactory.getLogger(CrossCheckTest.class));
            // System.out.println(Arrays.toString(offsets));
            Assert.assertEquals(hProd.min(), 0);
            Assert.assertEquals(hProd.max(), 1);
        }
    }

    private static int sum(int[] ints)
    {
        int r = 0;
        for (int i : ints)
            r += i;
        return r;
    }

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
