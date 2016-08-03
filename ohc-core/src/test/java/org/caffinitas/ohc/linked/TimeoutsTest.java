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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.caffinitas.ohc.CacheLoader;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.PermanentLoadException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TimeoutsTest
{
    @Test
    public void testTimeouts() throws InterruptedException
    {
        Timeouts timeouts = new Timeouts(64, 128);
        try
        {
            long in1000 = System.currentTimeMillis() + 1000L;
            for (int i = 0; i < 1000; i++)
                timeouts.add(5000 + i, in1000);

            timeouts.add(42L, System.currentTimeMillis());
            long in50 = System.currentTimeMillis() + 50;
            timeouts.add(142L, in50);
            timeouts.add(143L, in50);
            timeouts.add(144L, in50);

            final List<Long> ll = new ArrayList<>();
            timeouts.removeExpired(new Timeouts.TimeoutHandler()
            {
                public void expired(long hashEntryAdr)
                {
                    ll.add(hashEntryAdr);
                }
            });

            assertEquals(ll.size(), 1);
            assertEquals(ll.get(0), Long.valueOf(42L));

            ll.clear();
            timeouts.removeExpired(new Timeouts.TimeoutHandler()
            {
                public void expired(long hashEntryAdr)
                {
                    ll.add(hashEntryAdr);
                }
            });
            assertEquals(ll.size(), 0);

            //

            Thread.sleep(100L);

            ll.clear();
            timeouts.removeExpired(new Timeouts.TimeoutHandler()
            {
                public void expired(long hashEntryAdr)
                {
                    ll.add(hashEntryAdr);
                }
            });

            timeouts.remove(143L, in50);

            assertEquals(ll.size(), 3);
            assertTrue(ll.containsAll(Arrays.asList(142L, 144L)));

            ll.clear();
            timeouts.removeExpired(new Timeouts.TimeoutHandler()
            {
                public void expired(long hashEntryAdr)
                {
                    ll.add(hashEntryAdr);
                }
            });
            assertEquals(ll.size(), 0);

            //

            Thread.sleep(1000L);

            ll.clear();
            timeouts.removeExpired(new Timeouts.TimeoutHandler()
            {
                public void expired(long hashEntryAdr)
                {
                    ll.add(hashEntryAdr);
                }
            });
            assertEquals(ll.size(), 1000);
        }
        finally
        {
            timeouts.release();
        }
    }

    @Test
    public void testGet() throws Exception
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                              .keySerializer(TestUtils.intSerializer)
                                              .valueSerializer(TestUtils.stringSerializer)
                                              .build())
        {
            cache.put(1, "one", System.currentTimeMillis() - 1);
            assertNull(cache.get(1));

            cache.put(2, "two", System.currentTimeMillis() + 5);
            assertEquals(cache.get(2), "two");
            Thread.sleep(10);
            assertNull(cache.get(2));
        }
    }

    @Test
    public void testExpireVsEvict() throws Exception
    {
        char[] chars = new char[900];
        for (int i = 0; i < chars.length; i++)
            chars[i] = (char) ('A' + i % 26);
        String v = new String(chars);

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                              .keySerializer(TestUtils.intSerializer)
                                              .valueSerializer(TestUtils.stringSerializer)
                                              .capacity(1024 * 1024)
                                              .segmentCount(1)
                                              .build())
        {
            long expireAt = System.currentTimeMillis() + 250;

            int i;

            // fill half of the cache with expiring entries
            for (i = 0; cache.freeCapacity() > cache.capacity() / 2 + 950; i++)
                cache.put(i, v, expireAt);
            int k = i;

            assertEquals(((OHCacheLinkedImpl)cache).usedTimeouts(), k);

            // fill other half of the cache with non-expiring entries
            for (int n = 0; n < k; n++, i++)
                cache.put(i, v);

            long remain = expireAt - System.currentTimeMillis();
            assertTrue(remain >= 100, "Sorry, your machine is a bit too slow...");
            assertEquals(cache.stats().getExpireCount(), 0, "wrong expired entries count");
            assertEquals(cache.stats().getEvictionCount(), 0L, "cleanup triggered");

            // let the expiring entries expire
            Thread.sleep(remain);

            assertEquals(((OHCacheLinkedImpl)cache).usedTimeouts(), k);

            // add as many entries as expiring entries are there
            for (int n = 0; n < k; n++, i++)
            {
                cache.put(i, v);
                if ((n % 10000) == 0)
                    assertEquals(cache.stats().getEvictionCount(), 0L, "cleanup triggered");
            }

            assertEquals(cache.stats().getExpireCount(), k, "wrong expired entries count");
            assertEquals(cache.stats().getEvictionCount(), 0L, "cleanup triggered");
            assertEquals(((OHCacheLinkedImpl)cache).usedTimeouts(), 0);
        }
    }

    @Test
    public void testGetWithLoader() throws Exception
    {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                              .keySerializer(TestUtils.intSerializer)
                                              .valueSerializer(TestUtils.stringSerializer)
                                              .executorService(executorService)
                                              .build())
        {
            // expires before loader starts
            Future<String> f = cache.getWithLoaderAsync(1, new CacheLoader<Integer, String>()
            {
                public String load(Integer key) throws PermanentLoadException, Exception
                {
                    return "one";
                }
            }, System.currentTimeMillis() - 1);
            assertNull(f.get());
            assertNull(cache.get(1));

            // expires after loader finishes
            f = cache.getWithLoaderAsync(2, new CacheLoader<Integer, String>()
            {
                public String load(Integer key) throws PermanentLoadException, Exception
                {
                    Thread.sleep(50);
                    return "two";
                }
            }, System.currentTimeMillis() + 100);
            Thread.sleep(60);
            assertEquals(f.get(0, TimeUnit.MILLISECONDS), "two");
            assertEquals(cache.get(2), "two");
            Thread.sleep(60);
            assertNull(cache.get(2));

            // expires before loader finishes
            f = cache.getWithLoaderAsync(3, new CacheLoader<Integer, String>()
            {
                public String load(Integer key) throws PermanentLoadException, Exception
                {
                    Thread.sleep(5);
                    return "three";
                }
            }, System.currentTimeMillis() + 2);
            assertNull(f.get());
            assertNull(cache.get(3));
        }
        finally
        {
            executorService.shutdown();
        }
    }
}
