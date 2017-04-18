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

import org.caffinitas.ohc.CloseableIterator;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CacheSerializerTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    @DataProvider(name = "types")
    public Object[][] cacheEviction()
    {
        return new Object[][]{ { Eviction.LRU }, { Eviction.W_TINY_LFU }, { Eviction.NONE} };
    }

    @Test(dataProvider = "types")
    public void testFailingKeySerializer(Eviction eviction) throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                            .keySerializer(TestUtils.intSerializerFailSerialize)
                                                            .valueSerializer(TestUtils.stringSerializer)
                                                            .capacity(512L * 1024 * 1024)
                                                            .eviction(eviction)
                                                            .build())
        {
            try
            {
                cache.put(1, "hello");
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.get(1);
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.containsKey(1);
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.putIfAbsent(1, "hello");
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.addOrReplace(1, "hello", "world");
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }
        }
    }

    @Test
    public void testFailingKeySerializerInKeyIterator() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                            .keySerializer(TestUtils.intSerializerFailDeserialize)
                                                            .valueSerializer(TestUtils.stringSerializer)
                                                            .capacity(512L * 1024 * 1024)
                                                            .build())
        {
            cache.put(1, "hello");
            cache.put(2, "hello");
            cache.put(3, "hello");

            try
            {
                try (CloseableIterator<Integer> keyIter = cache.keyIterator())
                {
                    while (keyIter.hasNext())
                        keyIter.next();
                }
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                try (CloseableIterator<Integer> keyIter = cache.hotKeyIterator(10))
                {
                    while (keyIter.hasNext())
                        keyIter.next();
                }
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }
        }
    }

    @Test
    public void testFailingValueSerializerOnPut() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                            .keySerializer(TestUtils.intSerializer)
                                                            .valueSerializer(TestUtils.stringSerializerFailSerialize)
                                                            .capacity(512L * 1024 * 1024)
                                                            .build())
        {
            try
            {
                cache.put(1, "hello");
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.putIfAbsent(1, "hello");
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

            try
            {
                cache.addOrReplace(1, "hello", "world");
                Assert.fail();
            }
            catch (RuntimeException ignored)
            {
                // ok
            }

        }
    }
}
