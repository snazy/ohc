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

import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.OHCacheStats;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class EvictionTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    @Test
    public void testEviction() throws IOException
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                            .keySerializer(TestUtils.intSerializer)
                                                            .valueSerializer(TestUtils.stringSerializer)
                                                            .hashTableSize(64)
                                                            .segmentCount(4)
                                                            .capacity(8 * 1024 * 1024)
                                                            .chunkSize(65536)
                                                            .build())
        {
            for (int i = 0; i < 10000000; i++)
            {
                cache.put(i, Integer.toOctalString(i));
                if (cache.stats().getEvictionCount() > 1000 * 150)
                    return;
            }

            fail();
        }
    }

    @Test
    public void testEvictionFixed() throws IOException
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                            .keySerializer(TestUtils.intSerializer)
                                                            .valueSerializer(TestUtils.fixedValueSerializer)
                                                            .hashTableSize(64)
                                                            .segmentCount(4)
                                                            .capacity(8 * 1024 * 1024)
                                                            .chunkSize(65536)
                                                            .fixedEntrySize(TestUtils.INT_SERIALIZER_LEN, TestUtils.FIXED_VALUE_LEN)
                                                            .build())
        {
            for (int i = 0; i < 10000000; i++)
            {
                cache.put(i, Integer.toOctalString(i));
                if (cache.stats().getEvictionCount() > 1000 * 150)
                    return;
            }

            fail();
        }
    }

    @Test
    public void testSize() throws Exception
    {
        ByteArrayCacheSerializer serializer = new ByteArrayCacheSerializer();
        try (OHCache<byte[], byte[]> ohCache = OHCacheBuilder.<byte[], byte[]>newBuilder().capacity(1024 * 4)
                                                                                          .throwOOME(true)
                                                                                          .keySerializer(serializer)
                                                                                          .valueSerializer(serializer)
                                                                                          .segmentCount(1)
                                                                                          .chunkSize(1024)
                                                                                          .hashTableSize(256)
                                                                                          .build())
        {

            for (int i = 0; i < 12; ++i)
            {
                byte[] key = longToBytes(i);
                byte[] value = new byte[256];
                ohCache.put(key, value);
                OHCacheStats pre = ohCache.stats();
                ohCache.remove(key);
                OHCacheStats post = ohCache.stats();
                assertEquals(post.getSize(), 0);
                assertEquals(post.getRemoveCount(), pre.getRemoveCount() + 1);
            }

            for (int i = 12; i < 16; ++i)
            {
                byte[] key = longToBytes(i);
                byte[] value = new byte[256];
                ohCache.put(key, value);
            }
            OHCacheStats stats = ohCache.stats();
            assertEquals(stats.getSize(), 4);
            assertTrue(stats.getEvictionCount() >= 0);
        }
    }

    private byte[] longToBytes(long x)
    {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(x);
        return buffer.array();
    }

    static private class ByteArrayCacheSerializer implements CacheSerializer<byte[]>
    {
        @Override
        public void serialize(byte[] value, ByteBuffer buf)
        {
            buf.put(value);
        }

        @Override
        public byte[] deserialize(ByteBuffer buf)
        {
            byte[] bytes = new byte[buf.capacity()];
            buf.get(bytes);
            return bytes;
        }

        @Override
        public int serializedSize(byte[] value)
        {
            return value.length;
        }
    }
}
