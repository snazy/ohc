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
import org.caffinitas.ohc.OHCacheStats;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertTrue;

// This unit test uses the production cache implementation and an independent OHCache implementation used to
// cross-check the production implementation.
public class CrossCheckManyTest extends CrossCheckTestBase
{
    @Test(dataProvider = "types")
    public void testManyValues(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm, 64, -1))
        {
            Assert.assertEquals(cache.freeCapacity(), cache.capacity());

            TestUtils.fillMany(cache);

            OHCacheStats stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
                Assert.assertEquals(cache.get(i), Integer.toHexString(i), "for i="+i);

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
            {
                Assert.assertEquals(cache.get(i), Integer.toHexString(i), "for i="+i);
                assertTrue(cache.containsKey(i), "for i="+i);
                cache.put(i, Integer.toOctalString(i));
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i), "for i="+i);
                Assert.assertEquals(cache.size(), TestUtils.manyCount, "for i="+i);
                assertTrue(cache.containsKey(i), "for i="+i);
            }

            stats = cache.stats();
            Assert.assertEquals(stats.getPutReplaceCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i), "for i="+i);

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), TestUtils.manyCount * 6);
            Assert.assertEquals(stats.getSize(), TestUtils.manyCount);

            for (int i = 0; i < TestUtils.manyCount; i++)
            {
                Assert.assertEquals(cache.get(i), Integer.toOctalString(i), "for i=" + i);
                assertTrue(cache.containsKey(i), "for i=" + i);
                cache.remove(i);
                Assert.assertNull(cache.get(i), "for i=" + i);
                Assert.assertFalse(cache.containsKey(i), "for i=" + i);
                Assert.assertEquals(cache.stats().getRemoveCount(), i + 1);
                Assert.assertEquals(cache.size(), TestUtils.manyCount - i - 1, "for i=" + i);
            }

            stats = cache.stats();
            Assert.assertEquals(stats.getRemoveCount(), TestUtils.manyCount);
            Assert.assertEquals(stats.getSize(), 0);
            Assert.assertEquals(stats.getFree(), stats.getCapacity());
        }
    }
}
