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

public class BasicTest extends AbstractTest
{

    public static final long ONE_MB = 1024 * 1024;

    @Test
    public void basic() throws IOException, InterruptedException
    {
        try (OHCache cache = nonEvicting())
        {
            Assert.assertEquals(cache.freeCapacity(), cache.getCapacity());

            String k = "123";
            cache.put(k.hashCode(), new BytesSource.StringSource(k), new BytesSource.StringSource("hello world"));

            BytesSink.ByteArraySink valueSink = new BytesSink.ByteArraySink();
            cache.get(k.hashCode(), new BytesSource.StringSource(k), valueSink);
            String v = valueSink.toString();
            Assert.assertEquals(v, "hello world");

            cache.remove(k.hashCode(), new BytesSource.StringSource(k));

            Thread.sleep(300L);

            Assert.assertEquals(cache.freeCapacity(), cache.getCapacity());
        }
    }

    @Test(dependsOnMethods = "basic")
    public void serializing() throws IOException, InterruptedException
    {
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                                                           .keySerializer(stringSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .build())
        {
            Assert.assertEquals(cache.freeCapacity(), cache.getCapacity());

            String k = "123";
            cache.put(k, "hello world \u00e4\u00f6\u00fc\u00df");

            String v = cache.getIfPresent(k);
            Assert.assertEquals(v, "hello world \u00e4\u00f6\u00fc\u00df");

            cache.invalidate(k);

            Thread.sleep(300L);

            Assert.assertEquals(cache.freeCapacity(), cache.getCapacity());
        }
    }

    @Test(dependsOnMethods = "serializing")
    public void serialize100k() throws IOException, InterruptedException
    {
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                                                           .keySerializer(stringSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .statisticsEnabled(true)
                                                           .build())
        {
            for (int i = 0; i < 100000; i++)
                cache.put("key-" + i, "" + i);

            OHCacheStats stats = cache.extendedStats();
            Assert.assertEquals(stats.getPutAddCount(), 100000);

            Assert.assertEquals(cache.size(), 100000);

            for (int i = 0; i < 100000; i++)
                Assert.assertEquals(cache.getIfPresent("key-" + i), "" + i);

            stats = cache.extendedStats();
            Assert.assertEquals(stats.getCacheStats().hitCount(), 100000);

            for (int i = 0; i < 100000; i++)
                cache.invalidate("key-" + i);

            Assert.assertEquals(cache.size(), 0);

            stats = cache.extendedStats();
            Assert.assertEquals(stats.getUnlinkCount(), 100000);
        }
    }

    @Test(dependsOnMethods = "serialize100k")
    public void serialize100kReplace() throws IOException, InterruptedException
    {
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                                                           .keySerializer(stringSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .build())
        {
            for (int i = 0; i < 100000; i++)
                cache.put("key-" + i, "" + i);

            Assert.assertEquals(cache.size(), 100000);

            for (int i = 0; i < 100000; i++)
                Assert.assertEquals(cache.getIfPresent("key-" + i), "" + i);

            // replace the stuff

            for (int i = 0; i < 100000; i++)
                cache.put("key-" + i, "" + i);

            Assert.assertEquals(cache.size(), 100000);

            for (int i = 0; i < 100000; i++)
                Assert.assertEquals(cache.getIfPresent("key-" + i), "" + i);

            for (int i = 0; i < 100000; i++)
                cache.invalidate("key-" + i);

            Assert.assertEquals(cache.size(), 0);
        }
    }

    @Test(dependsOnMethods = "serialize100k")
    public void cleanUpTest() throws IOException, InterruptedException
    {
        char[] c940 = new char[940];
        for (int i = 0; i < c940.length; i++)
            c940[i] = (char) ('A' + i % 26);
        String v = new String(c940);

        // Build cache with 64MB capacity and trigger on less than 8 MB free capacity
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                                                           .keySerializer(stringSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .capacity(32 * ONE_MB)
                                                           .cleanUpTriggerMinFree(.125d)
                                                           .build())
        {
            int i;
            for (i = 0; cache.freeCapacity() > 4 * ONE_MB + 1024; i++)
                cache.put(Integer.toString(i), v);
            // no eviction yet !!

            Thread.sleep(1500L);

            Assert.assertEquals(cache.extendedStats().getCleanupCount(), 0L, "oops - cleanup triggered - fix the unit test!");

            // this should trigger a cleanup (eviction/replacement)
            cache.put(Integer.toString(i), v);

            long free = cache.freeCapacity();

            Thread.sleep(1500L);

            Assert.assertEquals(cache.extendedStats().getCleanupCount(), 1L, "cleanup did not run");
            Assert.assertTrue(free < cache.freeCapacity(), "free capacity did not increase");
        }
    }
}