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
package org.caffinitas.ohc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.io.ByteStreams;

import org.caffinitas.ohc.histo.EstimatedHistogram;
import org.testng.Assert;
import org.testng.annotations.Test;

// This unit test uses the production cache implementation and an independent OHCache implementation used to
// cross-check the production implementation.
public class CrossCheckTest
{
    static DoubleCheckCacheImpl<Integer, String> cache()
    {
        return cache(256);
    }

    static DoubleCheckCacheImpl<Integer, String> cache(long capacity)
    {
        return cache(capacity, -1);
    }

    static DoubleCheckCacheImpl<Integer, String> cache(long capacity, int hashTableSize)
    {
        return cache(capacity, hashTableSize, -1, -1);
    }

    static DoubleCheckCacheImpl<Integer, String> cache(long capacity, int hashTableSize, int segments, long maxEntrySize)
    {
        OHCacheBuilder<Integer, String> builder = OHCacheBuilder.<Integer, String>newBuilder()
                                                                .keySerializer(Utils.complexSerializer)
                                                                .valueSerializer(Utils.stringSerializer)
                                                                .capacity(capacity * Utils.ONE_MB);
        if (hashTableSize > 0)
            builder.hashTableSize(hashTableSize);
        if (segments > 0)
            builder.segmentCount(segments);
        if (maxEntrySize > 0)
            builder.maxEntrySize(maxEntrySize);

        return new DoubleCheckCacheImpl<>(builder);
    }

    @Test
    public void basic() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache())
        {
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());

            cache.put(11, "hello world \u00e4\u00f6\u00fc\u00df");

            Assert.assertTrue(cache.freeCapacity() < cache.capacity());

            String v = cache.get(11);
            Assert.assertEquals(v, "hello world \u00e4\u00f6\u00fc\u00df");

            cache.remove(11);

            Assert.assertEquals(cache.freeCapacity(), cache.capacity());

            Utils.fill5(cache);

            Utils.check5(cache);

            // implicitly compares stats
            cache.stats();
        }
    }

    @Test(dependsOnMethods = "basic")
    public void manyValues() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache(64, -1))
        {
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());

            Utils.fillMany(cache);

            OHCacheStats stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), Utils.manyCount);
            Assert.assertEquals(stats.getSize(), Utils.manyCount);

            for (int i = 0; i < Utils.manyCount; i++)
                Assert.assertEquals(cache.get(i), Integer.toHexString(i));

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), Utils.manyCount);
            Assert.assertEquals(stats.getSize(), Utils.manyCount);

            for (int i = 0; i < Utils.manyCount; i++)
                cache.put(i, Integer.toOctalString(i));

            stats = cache.stats();
            Assert.assertEquals(stats.getPutReplaceCount(), Utils.manyCount);
            Assert.assertEquals(stats.getSize(), Utils.manyCount);

            for (int i = 0; i < Utils.manyCount; i++)
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i));

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), Utils.manyCount * 2);
            Assert.assertEquals(stats.getSize(), Utils.manyCount);

            for (int i = 0; i < Utils.manyCount; i++)
                cache.remove(i);

            stats = cache.stats();
            Assert.assertEquals(stats.getRemoveCount(), Utils.manyCount);
            Assert.assertEquals(stats.getSize(), 0);
            Assert.assertEquals(stats.getFree(), stats.getCapacity());
        }
    }

    @Test(dependsOnMethods = "basic")
    public void keyIterator() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache(32))
        {
            long capacity = cache.capacity();
            Assert.assertEquals(cache.freeCapacity(), capacity);

            Utils.fill5(cache);

            Set<Integer> returned = new TreeSet<>();
            Iterator<Integer> iter = cache.keyIterator();
            for (int i = 0; i < 5; i++)
            {
                Assert.assertTrue(iter.hasNext());
                returned.add(iter.next());
            }
            Assert.assertFalse(iter.hasNext());
            Assert.assertEquals(returned.size(), 5);

            Assert.assertTrue(returned.contains(1));
            Assert.assertTrue(returned.contains(2));
            Assert.assertTrue(returned.contains(3));
            Assert.assertTrue(returned.contains(4));
            Assert.assertTrue(returned.contains(5));

            returned.clear();

            iter = cache.keyIterator();
            for (int i = 0; i < 5; i++)
                returned.add(iter.next());
            Assert.assertFalse(iter.hasNext());
            Assert.assertEquals(returned.size(), 5);

            Assert.assertTrue(returned.contains(1));
            Assert.assertTrue(returned.contains(2));
            Assert.assertTrue(returned.contains(3));
            Assert.assertTrue(returned.contains(4));
            Assert.assertTrue(returned.contains(5));

            iter = cache.keyIterator();
            for (int i = 0; i < 5; i++)
            {
                iter.next();
                iter.remove();
            }

            Assert.assertEquals(cache.freeCapacity(), capacity);

            Assert.assertEquals(0, cache.size());
            Assert.assertNull(cache.get(1));
            Assert.assertNull(cache.get(2));
            Assert.assertNull(cache.get(3));
            Assert.assertNull(cache.get(4));
            Assert.assertNull(cache.get(5));
        }
    }

    @Test(dependsOnMethods = "basic")
    public void hotN() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache())
        {
            Utils.fill5(cache);

            Assert.assertNotNull(cache.hotKeyIterator(1).next());
        }
    }

    @Test(dependsOnMethods = "manyValues")
    public void cleanUpTest() throws IOException, InterruptedException
    {
        char[] chars = new char[900];
        for (int i = 0; i < chars.length; i++)
            chars[i] = (char) ('A' + i % 26);
        String v = new String(chars);

        try (OHCache<Integer, String> cache = cache(4, -1, 1, -1))
        {
            int i;
            for (i = 0; cache.freeCapacity() > 950; i++)
            {
                cache.put(i, v);
                if ((i % 10000) == 0)
                    Assert.assertEquals(cache.stats().getEvictionCount(), 0L, "oops - cleanup triggered - fix the unit test!");
            }

            Assert.assertEquals(cache.stats().getEvictionCount(), 0L, "oops - cleanup triggered - fix the unit test!");

            cache.put(i, v);

            Assert.assertEquals(cache.stats().getEvictionCount(), 1L, "cleanup did not run");
        }
    }

    @Test(dependsOnMethods = "manyValues")
    public void lruTest() throws IOException, InterruptedException
    {
        // TODO add many values, reference ~50%, add more, check that (most) referenced are still there
        Assert.fail();
    }

    @Test(dependsOnMethods = "basic")
    public void putTooLarge() throws IOException, InterruptedException
    {
        char[] c940 = new char[8192];
        for (int i = 0; i < c940.length; i++)
            c940[i] = (char) ('A' + i % 26);
        String v = new String(c940);

        // Build cache with 64MB capacity and trigger on less than 8 MB free capacity
        try (OHCache<Integer, String> cache = cache(1, -1, -1, 8192))
        {
            cache.put(88, v);

            Assert.assertNull(cache.get(88));
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());
            Assert.assertEquals(cache.stats().getEvictionCount(), 0L, "eviction must not be performed");
        }
    }

    // per-method tests

    @Test
    public void testAddOrReplace() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            // TODO add more elements and call more ops

            Assert.assertTrue(cache.addOrReplace(42, "baz", "foo"));
            Assert.assertEquals(cache.get(42), "foo");
            Assert.assertTrue(cache.addOrReplace(42, "foo", "bar"));
            Assert.assertEquals(cache.get(42), "bar");
            Assert.assertFalse(cache.addOrReplace(42, "foo", "bar"));
            Assert.assertEquals(cache.get(42), "bar");
        }
    }

    @Test
    public void testPutIfAbsent() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            // TODO add more elements and call more ops

            Assert.assertTrue(cache.putIfAbsent(42, "foo"));
            Assert.assertEquals(cache.get(42), "foo");
            Assert.assertFalse(cache.putIfAbsent(42, "foo"));
        }
    }

    @Test
    public void testPutAll() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
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

    @Test
    public void testRemove() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            // TODO add more elements and call more remove ops

            cache.put(42, "foo");
            Assert.assertEquals(cache.get(42), "foo");
            cache.remove(42);
            Assert.assertNull(cache.get(42));
        }
    }

    @Test
    public void testRemoveAll() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));
            for (int i = 0; i < 100; i++)
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i));

            long lastFree = cache.freeCapacity();
            Assert.assertTrue(lastFree < cache.capacity());
            Assert.assertTrue(cache.size() > 0);

            List<Integer> coll = new ArrayList<>();
            for (int i = 0; i < 10; i++)
                coll.add(i);
            cache.removeAll(coll);

            Assert.assertTrue(cache.freeCapacity() > lastFree);
            Assert.assertEquals(cache.size(), 90);

            for (int i = 10; i < 50; i++)
                coll.add(i);
            cache.removeAll(coll);

            Assert.assertTrue(cache.freeCapacity() > lastFree);
            Assert.assertEquals(cache.size(), 50);

            for (int i = 50; i < 100; i++)
                coll.add(i);
            cache.removeAll(coll);

            Assert.assertEquals(cache.freeCapacity(), cache.capacity());
            Assert.assertEquals(cache.size(), 0);
        }
    }

    @Test
    public void testClear() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));
            for (int i = 0; i < 100; i++)
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i));

            Assert.assertTrue(cache.freeCapacity() < cache.capacity());
            Assert.assertTrue(cache.size() > 0);

            cache.clear();

            Assert.assertEquals(cache.freeCapacity(), cache.capacity());
            Assert.assertEquals(cache.size(), 0);
        }
    }

    @Test
    public void testGet_Put() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
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

    @Test
    public void testContainsKey() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            cache.put(42, "foo");
            Assert.assertTrue(cache.containsKey(42));
            Assert.assertFalse(cache.containsKey(11));
        }
    }

    @Test
    public void testHotKeyIterator() throws Exception
    {
        try (DoubleCheckCacheImpl<Integer, String> cache = cache())
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
                        Assert.assertTrue(iCheck.hasNext());

                        Integer kProd = iProd.next();
                        Integer kCheck = iCheck.next();

                        Assert.assertEquals(kProd, kCheck);
                    }

                    Assert.assertFalse(iCheck.hasNext());
                }
            }

        }
    }

    @Test
    public void testKeyIterator() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
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
                    Assert.assertTrue(keys.add(k));
                }
            }
            Assert.assertEquals(keys.size(), 100);
            for (int i = 0; i < 100; i++)
                Assert.assertTrue(keys.contains(i));

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

    @Test
    public void testHotKeyBufferIterator() throws Exception
    {
        try (DoubleCheckCacheImpl<Integer, String> cache = cache())
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
                        Assert.assertTrue(iCheck.hasNext());

                        ByteBuffer kProd = iProd.next();
                        ByteBuffer kCheck = iCheck.next();

                        Assert.assertEquals(kProd, kCheck);
                    }

                    Assert.assertFalse(iCheck.hasNext());
                }
            }

        }
    }

    @Test
    public void testKeyBufferIterator() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
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
                    Integer key = Utils.complexSerializer.deserialize(ByteStreams.newDataInput(k2.array()));
                    Assert.assertTrue(keys.add(key));
                }
            }
            Assert.assertEquals(keys.size(), 100);
            for (int i = 0; i < 100; i++)
                Assert.assertTrue(keys.contains(i));

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

    @Test
    public void testGetBucketHistogram() throws Exception
    {
        try (DoubleCheckCacheImpl<Integer, String> cache = cache())
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

    @Test
    public void testFreeCapacity() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            long cap = cache.capacity();

            cache.put(42, "foo");
            long free1 = cache.freeCapacity();
            Assert.assertTrue(cap > free1);

            cache.put(11, "bar baz");
            long free2 = cache.freeCapacity();
            Assert.assertTrue(free1 > free2);
            Assert.assertTrue(cap > free2);

            cache.put(11, "bar baz dog mud forest");
            long free3 = cache.freeCapacity();
            Assert.assertTrue(free2 > free3);
            Assert.assertTrue(cap > free3);

            cache.remove(11);
            long free4 = cache.freeCapacity();
            Assert.assertEquals(free1, free4);
            Assert.assertTrue(cap > free4);

            cache.remove(42);
            long free5 = cache.freeCapacity();
            Assert.assertEquals(free5, cap);
        }
    }

    @Test
    public void testSetCapacity() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            long cap = cache.capacity();

            cache.put(42, "foo");

            long free = cache.freeCapacity();

            cache.setCapacity(cap + Utils.ONE_MB);
            Assert.assertEquals(cache.capacity(), cap + Utils.ONE_MB);
            Assert.assertEquals(cache.freeCapacity(), free + Utils.ONE_MB);
            try
            {
                cache.setCapacity(cache.capacity() - Utils.ONE_MB);
                Assert.fail();
            }
            catch (IllegalArgumentException ignored)
            {
            }
            Assert.assertEquals(cache.capacity(), cap + Utils.ONE_MB);
            Assert.assertEquals(cache.freeCapacity(), free + Utils.ONE_MB);
        }
    }

    @Test
    public void testResetStatistics()
    {
        Assert.fail();
    }
}
