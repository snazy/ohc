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

public class ConcurrentTest extends AbstractTest
{
    private OHCache cache;

    private final BytesSource val = new BytesSource.ByteArraySource(new byte[1024]);

    @BeforeTest
    public void setup()
    {
        cache = newBuilder()
                .hashTableSize(1024)
                .build();
    }

    @AfterTest
    public void cleanup() throws IOException
    {
        cache.close();
    }

    @Test(threadPoolSize = 1, invocationCount = 4)
    public void threadCount01() throws IOException
    {
        withPieceOfData();
    }

    @Test(threadPoolSize = 2, invocationCount = 4, dependsOnMethods = "threadCount01")
    public void threadCount02() throws IOException
    {
        withPieceOfData();
    }

    @Test(threadPoolSize = 4, invocationCount = 16, dependsOnMethods = "threadCount02")
    public void threadCount04() throws IOException
    {
        withPieceOfData();
    }

    @Test(threadPoolSize = 8, invocationCount = 32, dependsOnMethods = "threadCount04")
    public void threadCount08() throws IOException
    {
        withPieceOfData();
    }

    @Test(threadPoolSize = 16, invocationCount = 16, dependsOnMethods = "threadCount08")
    public void threadCount16() throws IOException
    {
        withPieceOfData();
    }

    @Test(threadPoolSize = 32, invocationCount = 32, dependsOnMethods = "threadCount16", enabled = false)
    public void threadCount32() throws IOException
    {
        withPieceOfData();
    }

    private void withPieceOfData()
    {
        byte[] arr = new byte[4];
        BytesSource.ByteArraySource key = new BytesSource.ByteArraySource(arr);
        for (int o = 0; o < 1000; o++)
            for (int i = 0; i < 1000; i++)
            {
                arr[0] = (byte) (i & 0xff);
                arr[1] = (byte) ((i >> 8) & 0xff);
                arr[2] = (byte) ((i >> 16) & 0xff);
                arr[3] = (byte) ((i >> 24) & 0xff);

                switch (cache.put(i, key, val))
                {
                    case ADD:
                        Assert.assertTrue(cache.get(i, key, null));
                        cache.remove(i, key);
                        break;
                    case REPLACE:
                        break;
                    case NO_MORE_FREE_CAPACITY:
                        Assert.fail(cache.extendedStats().toString());
                        break;
                    case ALLOCATION_FAILED:
                        Assert.fail(cache.extendedStats().toString());
                        System.exit(1);
                }
            }
    }
}
