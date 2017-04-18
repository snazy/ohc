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

import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class LinkedImplTest
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
                                                  .keySerializer(TestUtils.intSerializer)
                                                  .valueSerializer(TestUtils.stringSerializer)
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

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testExtremeHashTableSize() throws IOException
    {
        OHCacheBuilder<Object, Object> builder = OHCacheBuilder.newBuilder()
                                                               .hashTableSize(1 << 30);
        builder.build().close();
    }

}
