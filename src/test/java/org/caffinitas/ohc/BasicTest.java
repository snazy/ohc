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

import org.testng.Assert;
import org.testng.annotations.Test;

public class BasicTest
{
    @Test
    public void basic() throws IOException
    {
        try (OHCache cache = OHCacheBuilder.newBuilder()
                                           .build())
        {
            int dataBlockCount = (int) cache.getTotalCapacity() / cache.getBlockSize();
            Assert.assertEquals(cache.calcFreeBlockCount(), dataBlockCount);

            String k = "123";
            cache.put(k.hashCode(), new BytesSource.StringSource(k), new BytesSource.StringSource("hello world"));

            Assert.assertEquals(cache.calcFreeBlockCount(), dataBlockCount - 1);

            BytesSink.ByteArraySink valueSink = new BytesSink.ByteArraySink();
            cache.get(k.hashCode(), new BytesSource.StringSource(k), valueSink);
            String v = valueSink.toString();
            Assert.assertEquals(v, "hello world");

            cache.remove(k.hashCode(), new BytesSource.StringSource(k));

            Assert.assertEquals(cache.calcFreeBlockCount(), dataBlockCount);
        }
    }

    @Test
    public void fillWithSmall() throws IOException
    {
        try (OHCache cache = OHCacheBuilder.newBuilder()
                                           .build())
        {
            int dataBlockCount = (int) cache.getTotalCapacity() / cache.getBlockSize();

            for (int i = 0; i < dataBlockCount; i++)
            {
                BytesSource.StringSource src = new BytesSource.StringSource(Integer.toString(i));
                Assert.assertSame(cache.put(i, src, src), PutResult.ADD, Integer.toString(i));
            }

            BytesSource.StringSource src = new BytesSource.StringSource(Integer.toString(-1));
            Assert.assertSame(cache.put(-1, src, src), PutResult.NO_MORE_SPACE, Integer.toString(-1));

            for (int i = 0; i < dataBlockCount; i++)
            {
                BytesSink.ByteArraySink val = new BytesSink.ByteArraySink();
                Assert.assertTrue(cache.get(i, new BytesSource.StringSource(Integer.toString(i)), val));
                Assert.assertEquals(val.toString(), Integer.toString(i));
            }

            // once again

            for (int i = 0; i < dataBlockCount; i++)
            {
                src = new BytesSource.StringSource(Integer.toString(i));
                Assert.assertSame(cache.put(i, src, src), PutResult.REPLACE, Integer.toString(i));
            }

            for (int i = 0; i < dataBlockCount; i++)
            {
                BytesSink.ByteArraySink val = new BytesSink.ByteArraySink();
                Assert.assertTrue(cache.get(i, new BytesSource.StringSource(Integer.toString(i)), val));
                Assert.assertEquals(val.toString(), Integer.toString(i));
            }
        }
    }

    @Test
    public void fillWithBig() throws IOException
    {
        try (OHCache cache = OHCacheBuilder.newBuilder()
                                           .build())
        {
            int dataBlockCount = (int) cache.getTotalCapacity() / cache.getBlockSize();

            int garbage = 2 * cache.getBlockSize();
            int cnt = dataBlockCount / 5;

            for (int i = 0; i < cnt; i++)
            {
                BytesSource src = bigSourceFor(garbage, i);
                Assert.assertSame(cache.put(i, src, src), PutResult.ADD, Integer.toString(i));
            }

            BytesSource src = bigSourceFor(garbage, -1);
            Assert.assertSame(cache.put(-1, src, src), PutResult.NO_MORE_SPACE, Integer.toString(-1));

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

    @Test
    public void bigThingSerial() throws IOException
    {
        try (OHCache cache = OHCacheBuilder.newBuilder()
                                           .build())
        {
            withKeyAndValLen(cache,
                             4321,
                             987654,
                             false);

            // on first block boundary
            withKeyAndValLen(cache,
                             cache.getBlockSize() - 64, // HashPartitionAccess.OFF_DATA_IN_FIRST
                             987654,
                             false);

            // on second block boundary
            withKeyAndValLen(cache,
                             cache.getBlockSize() - 64 + // HashPartitionAccess.OFF_DATA_IN_FIRST
                             cache.getBlockSize() - 8, // HashPartitionAccess.OFF_DATA_IN_NEXT
                             987654,
                             false);
        }
    }

    @Test
    public void bigThingArray() throws IOException
    {
        try (OHCache cache = OHCacheBuilder.newBuilder()
                                           .build())
        {
            withKeyAndValLen(cache,
                             4321,
                             987654,
                             true);

            // on first block boundary
            withKeyAndValLen(cache,
                             cache.getBlockSize() - 64, // HashPartitionAccess.OFF_DATA_IN_FIRST
                             987654,
                             true);

            // on second block boundary
            withKeyAndValLen(cache,
                             cache.getBlockSize() - 64 + // HashPartitionAccess.OFF_DATA_IN_FIRST
                             cache.getBlockSize() - 8, // HashPartitionAccess.OFF_DATA_IN_NEXT
                             987654,
                             true);
        }
    }

    private void withKeyAndValLen(OHCache cache, final int keyLen, final int valLen, boolean array)
    {
        int dataBlockCount = (int) cache.getTotalCapacity() / cache.getBlockSize();
        Assert.assertEquals(cache.calcFreeBlockCount(), dataBlockCount);

        BytesSource key = array
                          ? new ThirteenBytesSource(keyLen)
                          : new ThirteenSource(keyLen);
        BytesSource val = array
                          ? new ThirteenBytesSource(valLen)
                          : new ThirteenSource(valLen);
        BytesSink valSink = new BytesSink()
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

        int len = keyLen + valLen;
        int blk = 1;
        len -= cache.getBlockSize() - 64; // HashPartitionAccess.OFF_DATA_IN_FIRST
        for (; len > 0; blk++)
            len -= cache.getBlockSize() - 8; // HashPartitionAccess.OFF_DATA_IN_NEXT

        Assert.assertEquals(cache.calcFreeBlockCount(), dataBlockCount - blk);

        Assert.assertTrue(cache.get(hash, key, valSink));

        cache.remove(hash, key);

        Assert.assertEquals(cache.calcFreeBlockCount(), dataBlockCount);
    }
}
