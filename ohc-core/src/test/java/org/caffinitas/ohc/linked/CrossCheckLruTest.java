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

import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.HashAlgorithm;
import org.caffinitas.ohc.OHCache;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertTrue;

// This unit test uses the production cache implementation and an independent OHCache implementation used to
// cross-check the production implementation.
public class CrossCheckLruTest extends CrossCheckTestBase
{
    @Test(dataProvider = "lru")
    public void testCleanUpLRU(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException
    {
        String v = longString();

        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm, 4, -1, 1, -1))
        {
            int i;
            for (i = 0; cache.freeCapacity() > 950; i++)
            {
                cache.put(i, v);
                if ((i % 10000) == 0)
                    Assert.assertEquals(cache.stats().getEvictionCount(), 0L, "oops - cleanup triggered - fix the unit test!");
            }

            Assert.assertEquals(cache.stats().getEvictionCount(), 0L, "oops - cleanup triggered - fix the unit test!");

            cache.put(i++, v);

            Assert.assertEquals(cache.stats().getEvictionCount(), 1L, "cleanup did not run");

            for (int j = 0; j < 10000; j++, i++)
                cache.put(i, v);

            assertTrue(cache.stats().getEvictionCount() >= 10000L, "cleanup did not run");
        }
    }

    @Test(dataProvider = "lru")
    public void testLRU(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException
    {
        String v = longString();

        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm, 4, -1, 1, -1))
        {
            int i;
            for (i = 0; cache.freeCapacity() > 950; i++)
            {
                cache.put(i, v);
                if ((i % 10000) == 0)
                    Assert.assertEquals(cache.stats().getEvictionCount(), 0L, "oops - cleanup triggered - fix the unit test!");
            }
            int k = i;

            // reference ~50%
            for (int j = 0; j < k / 2; j++)
                cache.get(j);

            for (int j = 0; j < k / 2; j++, i++)
                cache.put(i, v);

            for (int j = 0; j < k / 2 - 1; j++)
                Assert.assertEquals(cache.containsKey(j), true, Integer.toString(j));

            for (int j = k / 2; j < k - 2; j++)
                Assert.assertEquals(cache.containsKey(j), false, Integer.toString(j));
        }
    }
}
