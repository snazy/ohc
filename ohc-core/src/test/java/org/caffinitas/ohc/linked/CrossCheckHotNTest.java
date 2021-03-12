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

import org.caffinitas.ohc.CloseableIterator;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.HashAlgorithm;
import org.caffinitas.ohc.OHCache;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

// This unit test uses the production cache implementation and an independent OHCache implementation used to
// cross-check the production implementation.
public class CrossCheckHotNTest extends CrossCheckTestBase
{
    @Test(dataProvider = "types", dependsOnMethods = "testBasics")
    public void testHotN(Eviction eviction, HashAlgorithm hashAlgorithm) throws IOException
    {
        try (OHCache<Integer, String> cache = cache(eviction, hashAlgorithm))
        {
            TestUtils.fill5(cache);

            try (CloseableIterator<Integer> iter = cache.hotKeyIterator(1))
            {
                Assert.assertNotNull(iter.next());
            }
        }
    }
}
