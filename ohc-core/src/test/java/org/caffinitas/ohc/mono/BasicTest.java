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
package org.caffinitas.ohc.mono;

import java.io.IOException;
import java.util.Iterator;

import org.caffinitas.ohc.ThirteenBytesSource;
import org.caffinitas.ohc.ThirteenSource;
import org.caffinitas.ohc.api.BytesSink;
import org.caffinitas.ohc.api.BytesSource;
import org.caffinitas.ohc.api.CacheSerializers;
import org.caffinitas.ohc.api.OHCache;
import org.caffinitas.ohc.api.OHCacheBuilder;
import org.caffinitas.ohc.api.OHCacheStats;
import org.caffinitas.ohc.api.OutOfOffHeapMemoryException;
import org.caffinitas.ohc.api.PutResult;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BasicTest extends AbstractTest
{

    public static final int CAPACITY_64M = 1024 * 1024 * 64;

    @Test(expectedExceptions = OutOfOffHeapMemoryException.class)
    public void tooBig() throws IOException
    {
        System.err.println("DO NOT BOTHER ABOUT SOMETHING LIKE 'java(10093,0x103960000) malloc: *** mach_vm_map(size=9223372036854775808) failed (error code=3)' !!!");
        // Note: this test may produce something like this on stderr:
        //
        // java(10093,0x103960000) malloc: *** mach_vm_map(size=9223372036854775808) failed (error code=3)
        // *** error: can't allocate region
        // *** set a breakpoint in malloc_error_break to debug

        newBuilder().capacity(Long.MAX_VALUE)
                    .build();
    }

    @Test
    public void cleanupTest() throws IOException
    {
        try (OHCache cache = newBuilder()
                             .capacity(CAPACITY_64M)
                             .hashTableSize(256)
                             .entriesPerSegmentTrigger(4)
                             .build())
        {
            OHCacheStats stats = cache.extendedStats();

            byte[] data = new byte[CAPACITY_64M / 256 - 70];
            for (int i = 0; i < 256; i++)
            {
                String s = Integer.toString(i);
                Assert.assertSame(cache.put(i, new BytesSource.StringSource(Integer.toString(i)), new BytesSource.ByteArraySource(data)), PutResult.ADD, s);
            }

            cache.cleanUp();

            OHCacheStats stats2 = cache.extendedStats();
            Assert.assertTrue(stats.getCleanupCount() < stats2.getCleanupCount());
            Assert.assertTrue(stats.getCacheStats().evictionCount() < stats2.getCacheStats().evictionCount());
        }
    }

    @Test
    public void rehashTest() throws IOException, InterruptedException
    {
        try (OHCache cache = newBuilder()
                             .capacity(CAPACITY_64M)
                             .hashTableSize(32)
                             .entriesPerSegmentTrigger(4)
                             .build())
        {
            for (int i = 0; i < 4 * 32; i++)
            {
                String s = Integer.toString(i);
                Assert.assertSame(cache.put(i, new BytesSource.StringSource(Integer.toString(i)), new BytesSource.StringSource(Integer.toString(i))), PutResult.ADD, s);
            }

            for (int i = 0; i < 4 * 32; i++)
            {
                String s = Integer.toString(i);
                BytesSink.ByteArraySink valueSink = new BytesSink.ByteArraySink();
                Assert.assertTrue(cache.get(i, new BytesSource.StringSource(s), valueSink), s);
                Assert.assertEquals(valueSink.toString(), s, s);
            }

            Assert.assertEquals(cache.getHashTableSize(), 32);
            Assert.assertTrue(((MonoCacheImpl) cache).rehash());
            Assert.assertEquals(cache.getHashTableSize(), 64);

            for (int i = 0; i < 4 * 32; i++)
            {
                String s = Integer.toString(i);
                BytesSink.ByteArraySink valueSink = new BytesSink.ByteArraySink();
                Assert.assertTrue(cache.get(i, new BytesSource.StringSource(s), valueSink), s);
                Assert.assertEquals(valueSink.toString(), s, s);
            }

            for (int i = 4 * 32; i < 4 * 128; i++)
            {
                String s = Integer.toString(i);
                Assert.assertSame(cache.put(i, new BytesSource.StringSource(s), new BytesSource.StringSource(s)), PutResult.ADD, s);
            }

            for (int i = 0; i < 4 * 128; i++)
            {
                String s = Integer.toString(i);
                BytesSink.ByteArraySink valueSink = new BytesSink.ByteArraySink();
                Assert.assertTrue(cache.get(i, new BytesSource.StringSource(s), valueSink), s);
                Assert.assertEquals(valueSink.toString(), s, s);
            }

            Thread.sleep(3000L);
            Assert.assertEquals(cache.getHashTableSize(), 128);

            for (int i = 0; i < 4 * 128; i++)
            {
                String s = Integer.toString(i);
                BytesSink.ByteArraySink valueSink = new BytesSink.ByteArraySink();
                Assert.assertTrue(cache.get(i, new BytesSource.StringSource(s), valueSink), s);
                Assert.assertEquals(valueSink.toString(), s, s);
            }
        }
    }

    @Test
    public void basic() throws IOException
    {
        try (OHCache cache = nonEvicting())
        {
            Assert.assertEquals(cache.freeCapacity(), cache.getCapacity());

            String k = "123";
            cache.put(k.hashCode(), new BytesSource.StringSource(k), new BytesSource.StringSource("hello world"));

//            if (cache.getDataManagement() == DataManagement.FIXED_BLOCKS)
//                Assert.assertEquals(cache.freeCapacity(), cache.getCapacity() - cache.getBlockSize());

            BytesSink.ByteArraySink valueSink = new BytesSink.ByteArraySink();
            cache.get(k.hashCode(), new BytesSource.StringSource(k), valueSink);
            String v = valueSink.toString();
            Assert.assertEquals(v, "hello world");

            cache.remove(k.hashCode(), new BytesSource.StringSource(k));

            Assert.assertEquals(cache.freeCapacity(), cache.getCapacity());
        }
    }

    @Test
    public void serializing() throws IOException
    {
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                                                           .keySerializer(CacheSerializers.stringSerializer)
                                                           .valueSerializer(CacheSerializers.stringSerializer)
                                                           .build())
        {
            Assert.assertEquals(cache.freeCapacity(), cache.getCapacity());

            String k = "123";
            cache.put(k, "hello world \u00e4\u00f6\u00fc\u00df");

//            if (cache.getDataManagement() == DataManagement.FIXED_BLOCKS)
//                Assert.assertEquals(cache.freeCapacity(), cache.getCapacity() - cache.getBlockSize());

            String v = cache.getIfPresent(k);
            Assert.assertEquals(v, "hello world \u00e4\u00f6\u00fc\u00df");

            Iterator<String> iter = cache.hotN(1);
            Assert.assertTrue(iter.hasNext());
            Assert.assertEquals(iter.next(), "123");
            Assert.assertFalse(iter.hasNext());

            cache.invalidate(k);

            Assert.assertEquals(cache.freeCapacity(), cache.getCapacity());

            for (int i = 0; i < 100000; i++)
                cache.put("key-" + i, "" + i);
            for (int i = 0; i < 100000; i++)
                cache.invalidate("key-" + i);
        }
    }

    @Test(enabled = false)
    public void fillWithSmall() throws IOException
    {
        try (OHCache cache = nonEvicting())
        {
            int cnt = 0; // TODO

            for (int i = 0; i < cnt; i++)
            {
                BytesSource.StringSource src = new BytesSource.StringSource(Integer.toString(i));
                Assert.assertSame(cache.put(i, src, src), PutResult.ADD, Integer.toString(i));
            }

            BytesSource.StringSource src = new BytesSource.StringSource(Integer.toString(cnt));
            Assert.assertSame(cache.put(cnt, src, src), PutResult.ADD, Integer.toString(cnt));

            src = new BytesSource.StringSource(Integer.toString(-1));
            Assert.assertSame(cache.put(-1, src, src), PutResult.NO_MORE_FREE_CAPACITY, Integer.toString(-1));

            src = new BytesSource.StringSource(Integer.toString(cnt));
            Assert.assertTrue(cache.remove(cnt, src));

            for (int i = 0; i < cnt; i++)
            {
                BytesSink.ByteArraySink val = new BytesSink.ByteArraySink();
                Assert.assertTrue(cache.get(i, new BytesSource.StringSource(Integer.toString(i)), val));
                Assert.assertEquals(val.toString(), Integer.toString(i));
            }

            // once again

            for (int i = 0; i < cnt; i++)
            {
                src = new BytesSource.StringSource(Integer.toString(i));
                Assert.assertSame(cache.put(i, src, src), PutResult.REPLACE, Integer.toString(i));
            }

            for (int i = 0; i < cnt; i++)
            {
                BytesSink.ByteArraySink val = new BytesSink.ByteArraySink();
                Assert.assertTrue(cache.get(i, new BytesSource.StringSource(Integer.toString(i)), val));
                Assert.assertEquals(val.toString(), Integer.toString(i));
            }
        }
    }

    @Test(enabled = false)
    public void fillWithBig() throws IOException
    {
        try (OHCache cache = nonEvicting())
        {
            int garbage = 1024; // TODO 2 * cache.getBlockSize();
            int cnt = 0; // TODO dataBlockCount / 5 - 1;

            for (int i = 0; i < cnt; i++)
            {
                BytesSource src = bigSourceFor(garbage, i);
                Assert.assertSame(cache.put(i, src, src), PutResult.ADD, Integer.toString(i));
            }

            BytesSource src = bigSourceFor(garbage, cnt);
            Assert.assertSame(cache.put(cnt, src, src), PutResult.ADD, Integer.toString(cnt));

            src = bigSourceFor(garbage, -1);
            Assert.assertSame(cache.put(-1, src, src), PutResult.NO_MORE_FREE_CAPACITY, Integer.toString(-1));

            src = bigSourceFor(garbage, cnt);
            Assert.assertTrue(cache.remove(cnt, src));

            for (int i = 0; i < cnt; i++)
            {
                BytesSink.ByteArraySink val = new BytesSink.ByteArraySink();
                Assert.assertTrue(cache.get(i, bigSourceFor(garbage, i), val));
                Assert.assertEquals(val.toString(), bigFor(garbage, i));
            }

            // once again

            for (int i = 0; i < cnt; i++)
            {
                src = bigSourceFor(garbage, i);
                Assert.assertSame(cache.put(i, src, src), PutResult.REPLACE, Integer.toString(i));
            }

            for (int i = 0; i < cnt; i++)
            {
                BytesSink.ByteArraySink val = new BytesSink.ByteArraySink();
                Assert.assertTrue(cache.get(i, bigSourceFor(garbage, i), val));
                Assert.assertEquals(val.toString(), bigFor(garbage, i));
            }
        }
    }

    @Test
    public void bigThingSerial() throws IOException
    {
        try (OHCache cache = nonEvicting())
        {
            withKeyAndValLen(cache,
                             4321,
                             987654,
                             false);
        }
    }

    @Test
    public void bigThingArray() throws IOException
    {
        try (OHCache cache = nonEvicting())
        {
            withKeyAndValLen(cache,
                             4321,
                             987654,
                             true);
        }
    }

    private BytesSource bigSourceFor(int prefix, int v)
    {
        String s = bigFor(prefix, v);
        return new BytesSource.StringSource(s);
    }

    private String bigFor(int prefix, int v)
    {
        StringBuilder sb = new StringBuilder(prefix + 6);
        for (int i = 0; i < prefix; i++)
            sb.append('a');
        sb.append(v);
        return sb.toString();
    }

    private void withKeyAndValLen(OHCache cache, final int keyLen, final int valLen, boolean array)
    {
        Assert.assertEquals(cache.freeCapacity(), cache.getCapacity());

        BytesSource key = array
                          ? new ThirteenBytesSource(keyLen)
                          : new ThirteenSource(keyLen);
        BytesSource val = array
                          ? new ThirteenBytesSource(valLen)
                          : new ThirteenSource(valLen);
        BytesSink valSink = new BytesSink.AbstractSink()
        {
            public void setSize(int size)
            {
                Assert.assertEquals(size, valLen);
            }

            public void putByte(int pos, byte value)
            {
                Assert.assertEquals(value, (pos % 13), "at position " + pos + " with keyLen=" + keyLen + " and valLen=" + valLen);
            }
        };

        int hash = Integer.MAX_VALUE - 1;

        cache.put(hash, key, val);

//        int len = keyLen + valLen;
//        int blk = 1;
//        len -= cache.getBlockSize() - ENTRY_OFF_DATA;
//        for (; len > 0; blk++)
//            len -= cache.getBlockSize() - ENTRY_OFF_DATA_IN_NEXT;
//
//        if (cache.getDataManagement() == DataManagement.FIXED_BLOCKS)
//            Assert.assertEquals(cache.freeCapacity(), cache.getCapacity() - cache.getBlockSize() * blk);

        Assert.assertTrue(cache.get(hash, key, valSink));

        cache.remove(hash, key);

        Assert.assertEquals(cache.freeCapacity(), cache.getCapacity());
    }
}
