package org.caffinitas.ohc.linked;

import org.caffinitas.ohc.CloseableIterator;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.HashAlgorithm;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.histo.EstimatedHistogram;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static org.testng.Assert.assertTrue;

public class CrossCheckIteratorsTest extends CrossCheckTestBase {
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

    @Test(dataProvider = "types")
    public void testKeyIterator1(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException
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
    }}
