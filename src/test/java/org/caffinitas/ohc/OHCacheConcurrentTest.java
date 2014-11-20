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
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OHCacheConcurrentTest
{
    private OHCache cache;

    private final BytesSource val = new ThirteenSource(1);

    @BeforeTest
    public void setup()
    {
        cache = OHCacheBuilder.newBuilder()
                              .build();
    }

    @AfterTest
    public void cleanup() throws IOException
    {
        System.out.println("lock-partition-spins: "+((OHCacheImpl)cache).getLockPartitionSpins());
        System.out.println("free-block-spins:     "+((OHCacheImpl)cache).getFreeBlockSpins());
        cache.close();
    }

    @Test(threadPoolSize = 1, invocationCount = 4)
    public void threadCount1() throws IOException
    {
        withSmallPieceOfData();
    }

    @Test(threadPoolSize = 2, invocationCount = 4)
    public void threadCount2() throws IOException
    {
        withSmallPieceOfData();
    }

    @Test(threadPoolSize = 4, invocationCount = 4)
    public void threadCount4() throws IOException
    {
        withSmallPieceOfData();
    }

    @Test(threadPoolSize = 8, invocationCount = 8)
    public void threadCount8() throws IOException
    {
        withSmallPieceOfData();
    }

    @Test(threadPoolSize = 16, invocationCount = 16)
    public void threadCount16() throws IOException
    {
        withSmallPieceOfData();
    }

    @Test(threadPoolSize = 32, invocationCount = 32)
    public void threadCount32() throws IOException
    {
        withSmallPieceOfData();
    }

    private void withSmallPieceOfData()
    {
        for (int o = 0; o < 1000; o++)
            for (int i = 0; i < 1000; i++)
            {
                String s = "" + i;
                BytesSource.StringSource key = new BytesSource.StringSource(s);

                switch (cache.put(s.hashCode(), key, val))
                {
                    case ADD:
                        Assert.assertTrue(cache.remove(s.hashCode(), key));
                        break;
                    case REPLACE:
                        break;
                    case NO_MORE_SPACE:
                        Assert.fail();
                        break;
                }
            }
    }
}
