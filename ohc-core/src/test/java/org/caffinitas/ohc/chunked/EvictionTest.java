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

import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

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
}
