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

    @Test()
    public void serializing() throws IOException, InterruptedException
    {
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                                                           .keySerializer(stringSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .build())
        {
            Assert.assertEquals(cache.getFreeCapacity(), cache.getCapacity());

            String k = "123";
            cache.put(k, "hello world \u00e4\u00f6\u00fc\u00df");

            Assert.assertTrue(cache.getFreeCapacity() < cache.getCapacity());

            String v = cache.getIfPresent(k);
            Assert.assertEquals(v, "hello world \u00e4\u00f6\u00fc\u00df");

            cache.invalidate(k);

            Thread.sleep(300L);

            Assert.assertEquals(cache.getFreeCapacity(), cache.getCapacity());
        }
    }

    @Test(dependsOnMethods = "serializing")
    public void hotN() throws IOException, InterruptedException
    {
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                                                           .keySerializer(stringSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .build())
        {
            cache.put("1", "one");
            cache.put("2", "two");
            cache.put("3", "two");
            cache.put("4", "two");
            cache.put("5", "two");

            Assert.assertNotNull(cache.hotN(1).next());
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

            OHCacheStats stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), 100000);

            Assert.assertEquals(cache.size(), 100000);

            for (int i = 0; i < 100000; i++)
                Assert.assertEquals(cache.getIfPresent("key-" + i), "" + i);

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), 100000);

            for (int i = 0; i < 100000; i++)
                cache.invalidate("key-" + i);

            Assert.assertEquals(cache.size(), 0);

            stats = cache.stats();
            Assert.assertEquals(stats.getRemoveCount(), 100000);
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
                                                           .segmentCount(1)
                                                           .capacity(32 * ONE_MB)
                                                           .cleanUpTriggerFree(.125d)
                                                           .build())
        {
            int i;
            for (i = 0; cache.getFreeCapacity() > 4 * ONE_MB + 1000; i++)
                cache.put(Integer.toString(i), v);

            Assert.assertEquals(cache.stats().getCleanupCount(), 0L, "oops - cleanup triggered - fix the unit test!");

            cache.put(Integer.toString(i), v);

            Assert.assertEquals(cache.stats().getCleanupCount(), 1L, "cleanup did not run");
            Assert.assertEquals(cache.stats().getEvictionCount(), 1L, "cleanup did not run");
        }
    }

    @Test(dependsOnMethods = "serializing")
    public void putTooLarge() throws IOException, InterruptedException
    {
        char[] c940 = new char[8192];
        for (int i = 0; i < c940.length; i++)
            c940[i] = (char) ('A' + i % 26);
        String v = new String(c940);

        // Build cache with 64MB capacity and trigger on less than 8 MB free capacity
        try (OHCache<String, String> cache = OHCacheBuilder.<String, String>newBuilder()
                                                           .keySerializer(stringSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .segmentCount(1)
                                                           .capacity(ONE_MB)
                                                           .maxEntrySize((double)ONE_MB / 128) // == 8kB
                                                           .cleanUpTriggerFree(.125d)
                                                           .build())
        {
            cache.put("foobar", v);

            Assert.assertNull(cache.getIfPresent("foobar"));
            Assert.assertEquals(cache.getFreeCapacity(), cache.getCapacity());
            Assert.assertEquals(cache.stats().getCleanupCount(), 0L, "cleanup did run");
        }
    }
}