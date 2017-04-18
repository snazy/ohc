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
package org.caffinitas.ohc.chunked;

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
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.OHCacheStats;
import org.caffinitas.ohc.histo.EstimatedHistogram;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

// This unit test uses the production cache implementation and an independent OHCache implementation used to
// cross-check the production implementation.
public class ChunkedCacheImplTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    static OHCache<Integer, String> cache()
    {
        return cache(256);
    }

    static OHCache<Integer, String> cache(long capacity)
    {
        return cache(capacity, -1);
    }

    static OHCache<Integer, String> cache(long capacity, int hashTableSize)
    {
        return cache(capacity, hashTableSize, -1, -1);
    }

    static OHCache<Integer, String> cache(long capacity, int hashTableSize, int segments, long maxEntrySize)
    {
        OHCacheBuilder<Integer, String> builder = OHCacheBuilder.<Integer, String>newBuilder()
                                                                .segmentCount(16)
                                                                .keySerializer(TestUtils.intSerializer)
                                                                .valueSerializer(TestUtils.stringSerializer)
                                                                .chunkSize(65536)
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

        return builder.build();
    }

    @Test
    public void testBufferRealloc() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache())
        {
            String strings[] = {
                               stringWithLength(1024),
                               stringWithLength(2048),
                               stringWithLength(3072),
                               stringWithLength(4096),
                               stringWithLength(5120),
                               stringWithLength(8192),
                               stringWithLength(12288),
                               stringWithLength(16384),
            };
            for (int i = 0; i < strings.length; i++)
            {
                cache.put(i, strings[i]);
            }
            for (int i = 0; i < strings.length; i++)
            {
                assertEquals(strings[i], cache.get(i));
            }
        }
    }

    private static String stringWithLength(int len)
    {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
        {
            sb.append((char)('A' + i % 26));
        }
        return sb.toString();
    }

    @Test
    public void testBasics() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache())
        {
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());

            cache.put(11, "hello world \u00e4\u00f6\u00fc\u00df");

            assertTrue(cache.freeCapacity() < cache.capacity());

            String v = cache.get(11);
            Assert.assertEquals(v, "hello world \u00e4\u00f6\u00fc\u00df");

            cache.remove(11);

            TestUtils.fill5(cache);

            TestUtils.check5(cache);

            // implicitly compares stats
            cache.stats();
        }
    }

    @Test(dependsOnMethods = "testBasics")
    public void testRemoveAndReAdd() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            cache.put(1, "one thing");
            Assert.assertEquals(cache.get(1), "one thing");

            cache.remove(1);
            Assert.assertFalse(cache.containsKey(1));

            Assert.assertEquals(cache.size(), 0);

            cache.put(1, "one more");
            Assert.assertEquals(cache.get(1), "one more");

            cache.remove(1);
            Assert.assertFalse(cache.containsKey(1));

            Assert.assertEquals(cache.size(), 0);
        }
    }

    @Test(dependsOnMethods = "testBasics")
    public void testManyValues() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache(64, -1))
        {
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());

            TestUtils.fillMany(cache);

            OHCacheStats stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
                Assert.assertEquals(cache.get(i), Integer.toHexString(i), "for i=" + i);

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
            {
                Assert.assertEquals(cache.get(i), Integer.toHexString(i), "for i=" + i);
                assertTrue(cache.containsKey(i), "for i=" + i);
                cache.put(i, Integer.toOctalString(i));
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i), "for i=" + i);
                Assert.assertEquals(cache.size(), TestUtils.manyCount, "for i=" + i);
                assertTrue(cache.containsKey(i), "for i=" + i);
            }

            stats = cache.stats();
            Assert.assertEquals(stats.getPutReplaceCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i), "for i=" + i);

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
            //Assert.assertEquals(stats.getFree(), stats.getCapacity());
        }
    }

    @Test(dependsOnMethods = "testBasics")
    public void testHotN() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache())
        {
            TestUtils.fill5(cache);

            try (CloseableIterator<Integer> iter = cache.hotKeyIterator(1))
            {
                Assert.assertNotNull(iter.next());
            }
        }
    }

    // per-method tests

    @Test
    public void testAddOrReplace() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
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

    @Test
    public void testPutIfAbsent() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            for (int i = 0; i < TestUtils.manyCount; i++)
                assertTrue(cache.putIfAbsent(i, Integer.toHexString(i)));

            assertTrue(cache.putIfAbsent(-42, "foo"));
            Assert.assertEquals(cache.get(-42), "foo");
            Assert.assertFalse(cache.putIfAbsent(-42, "foo"));
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
            assertTrue(lastFree < cache.capacity());
            assertTrue(cache.size() > 0);

            List<Integer> coll = new ArrayList<>();
            for (int i = 0; i < 10; i++)
                coll.add(i);
            cache.removeAll(coll);

            //assertTrue(cache.freeCapacity() > lastFree);
            Assert.assertEquals(cache.size(), 90);

            for (int i = 10; i < 50; i++)
                coll.add(i);
            cache.removeAll(coll);

            //assertTrue(cache.freeCapacity() > lastFree);
            Assert.assertEquals(cache.size(), 50);

            for (int i = 50; i < 100; i++)
                coll.add(i);
            cache.removeAll(coll);

            //Assert.assertEquals(cache.freeCapacity(), cache.capacity());
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

            assertTrue(cache.freeCapacity() < cache.capacity());
            assertTrue(cache.size() > 0);

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
            assertTrue(cache.containsKey(42));
            Assert.assertFalse(cache.containsKey(11));
        }
    }

    @Test
    public void testHotKeyIterator() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            Assert.assertFalse(cache.hotKeyIterator(10).hasNext());

            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));

            Assert.assertEquals(cache.stats().getSize(), 100);

            try (CloseableIterator<Integer> iProd = cache.hotKeyIterator(10))
            {
                int count = 0;
                while (iProd.hasNext())
                {
                    iProd.next();
                    count++;
                }
                assertTrue(count >= 10, "count is " + count + " but should be >= 10");
            }
        }
    }

    @Test(dependsOnMethods = "testBasics")
    public void testKeyIterator1() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = cache(32))
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

            Assert.assertEquals(cache.size(), 0);
            Assert.assertNull(cache.get(1));
            Assert.assertNull(cache.get(2));
            Assert.assertNull(cache.get(3));
            Assert.assertNull(cache.get(4));
            Assert.assertNull(cache.get(5));
        }
    }

    @Test
    public void testKeyIterator2() throws Exception
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

            List<ByteBuffer> got = new ArrayList<>();
            int serSize = TestUtils.intSerializer.serializedSize(0);
            int cnt = 0;
            try (CloseableIterator<ByteBuffer> iter = cache.keyBufferIterator())
            {
                while (iter.hasNext())
                {
                    ByteBuffer bb = ByteBuffer.allocate(serSize);
                    bb.put(iter.next());
                    bb.flip();
                    got.add(bb);
                    iter.remove();
                    cnt ++;
                }
            }
            Assert.assertEquals(cnt, 100);
            Assert.assertEquals(got.size(), 100);
            for (int i = 0; i < 100; i++)
            {
                ByteBuffer bb = ByteBuffer.allocate(serSize);
                TestUtils.intSerializer.serialize(i, bb);
                bb.flip();
                Assert.assertTrue(got.indexOf(bb) != -1);
            }

            for (CloseableIterator<ByteBuffer> iter = cache.keyBufferIterator();iter.hasNext();)
                System.out.println(iter.next());

            for (CloseableIterator<Integer> iter = cache.keyIterator();iter.hasNext();)
                System.out.println(iter.next());

            Assert.assertFalse(cache.keyBufferIterator().hasNext());

            Assert.assertEquals(cache.stats().getSize(), 0);
        }
    }

    @Test
    public void testHotKeyBufferIterator() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            Assert.assertFalse(cache.hotKeyIterator(10).hasNext());

            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));

            Assert.assertEquals(cache.stats().getSize(), 100);

            try (CloseableIterator<ByteBuffer> iProd = cache.hotKeyBufferIterator(10))
            {
                int count = 0;
                while (iProd.hasNext())
                {
                    iProd.next();
                    count++;
                }

                assertTrue(count >= 10, "count is " + count + " but should be >= 10");
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
                    Integer key = TestUtils.intSerializer.deserialize(ByteBuffer.wrap(k2.array()));
                    Assert.assertTrue(keys.add(key));
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
                }
            }
        }
    }

    @Test
    public void testGetBucketHistogram() throws Exception
    {
        try (OHCache<Integer, String> cache = cache())
        {
            for (int i = 0; i < 100; i++)
                cache.put(i, Integer.toOctalString(i));

            Assert.assertEquals(cache.stats().getSize(), 100);

            EstimatedHistogram hProd = cache.getBucketHistogram();
            Assert.assertEquals(hProd.count(), sum(cache.hashTableSizes()));
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
            assertTrue(cap > free1);

            cache.put(11, "bar baz");
            long free2 = cache.freeCapacity();
            assertTrue(free1 > free2);
            assertTrue(cap > free2);

            cache.put(11, "bar baz dog mud forest");
            long free3 = cache.freeCapacity();
            assertTrue(free2 > free3);
            assertTrue(cap > free3);
        }
    }

    @Test
    public void testResetStatistics() throws IOException
    {
        try (OHCache<Integer, String> cache = cache())
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

    @Test
    public void testTooBigEntryOnPut() throws IOException
    {
        try (OHCache<Integer, String> cache = cache(8, -1, -1, TestUtils.intSerializer.serializedSize(1) + Util.entryOffData(false) + 5))
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
}
