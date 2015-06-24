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

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ReadOnlyBufferException;

import org.caffinitas.ohc.DirectValueAccess;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class DirectAccessTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    @Test
    public void testDirectPutGet() throws Exception
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                            .keySerializer(TestUtils.intSerializer)
                                                            .valueSerializer(TestUtils.stringSerializer)
                                                            .capacity(64 * 1024 * 1024)
                                                            .build())
        {
            for (int i = 0; i < 100; i++)
            {
                try (DirectValueAccess direct = cache.putDirect(i, i + 10))
                {
                    for (int c = 0; c < i + 10; c++)
                        direct.buffer().put((byte) i);

                    try
                    {
                        direct.buffer().put((byte) 0);
                        Assert.fail();
                    }
                    catch (BufferOverflowException e)
                    {
                        // fine
                    }
                }
            }

            for (int i = 0; i < 100; i++)
            {
                Assert.assertTrue(cache.containsKey(i));
                try (DirectValueAccess direct = cache.getDirect(i))
                {
                    Assert.assertEquals(direct.buffer().capacity(), i + 10);
                    Assert.assertEquals(direct.buffer().limit(), i + 10);

                    for (int c = 0; c < i + 10; c++)
                        Assert.assertEquals(direct.buffer().get(), i);

                    try
                    {
                        direct.buffer().get();
                        Assert.fail();
                    }
                    catch (BufferUnderflowException e)
                    {
                        // fine
                    }

                    try
                    {
                        direct.buffer().put(0, (byte) 0);
                        Assert.fail();
                    }
                    catch (ReadOnlyBufferException e)
                    {
                        // fine
                    }
                }
            }
        }
    }

    @Test
    public void testDirectPutIfAbsent() throws Exception
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                            .keySerializer(TestUtils.intSerializer)
                                                            .valueSerializer(TestUtils.stringSerializer)
                                                            .capacity(64 * 1024 * 1024)
                                                            .build())
        {
            for (int i = 0; i < 100; i++)
            {
                DirectValueAccess direct = cache.putIfAbsentDirect(i, i + 10);
                try
                {
                    for (int j = 0; j < 100; j++)
                        Assert.assertNull(cache.getDirect(i));

                    for (int c = 0; c < i + 10; c++)
                        direct.buffer().put((byte) i);
                }
                finally
                {
                    Assert.assertTrue(direct.commit());
                }
            }

            for (int i = 0; i < 100; i++)
            {
                Assert.assertNull(cache.putIfAbsentDirect(i, i + 10));
            }
        }
    }

    @Test
    public void testDirectAddOrReplace() throws Exception
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                            .keySerializer(TestUtils.intSerializer)
                                                            .valueSerializer(TestUtils.stringSerializer)
                                                            .capacity(64 * 1024 * 1024)
                                                            .build())
        {

            // simple put-if-absent
            for (int i = 0; i < 100; i++)
            {
                Assert.assertNull(cache.addOrReplaceDirect(i, null, i + 10));

                DirectValueAccess direct = cache.putIfAbsentDirect(i, i + 10);
                try
                {
                    for (int j = 0; j < 100; j++)
                        Assert.assertNull(cache.getDirect(i));

                    for (int c = 0; c < i + 10; c++)
                        direct.buffer().put((byte) i);
                }
                finally
                {
                    Assert.assertTrue(direct.commit());
                }

                // do it again - must fail
                Assert.assertNull(cache.putIfAbsentDirect(i, i + 10));
            }

            cache.clear();

            // put-if-absent - but added concurrently
            for (int i = 0; i < 100; i++)
            {
                Assert.assertNull(cache.addOrReplaceDirect(i, null, i + 10));

                DirectValueAccess direct = cache.putIfAbsentDirect(i, i + 10);
                try
                {
                    // do the put that will prevent direct.commit() to succeed
                    try (DirectValueAccess conc = cache.putDirect(i, i + 10))
                    {
                        for (int c = 0; c < i + 10; c++)
                            conc.buffer().put((byte) i);
                    }

                    for (int j = 0; j < 100; j++)
                        try (DirectValueAccess chk = cache.getDirect(i))
                        {
                            Assert.assertNotNull(chk);
                        }

                    for (int c = 0; c < i + 10; c++)
                        direct.buffer().put((byte) i);
                }
                finally
                {
                    Assert.assertFalse(direct.commit());
                }

                // do it again - must fail
                Assert.assertNull(cache.putIfAbsentDirect(i, i + 10));
            }

            // replace with compare of other value
            for (int i = 0; i < 100; i++)
            {
                try (DirectValueAccess ex = cache.getDirect(i))
                {
                    Assert.assertNotNull(ex);

                    DirectValueAccess direct = cache.addOrReplaceDirect(i, ex, i + 10);
                    try
                    {
                        for (int c = 0; c < i + 10; c++)
                            direct.buffer().put((byte) i);
                    }
                    finally
                    {
                        Assert.assertTrue(direct.commit());
                    }
                }
            }

            // replace with compare of other value - but changed concurrently
            for (int i = 0; i < 100; i++)
            {
                try (DirectValueAccess ex = cache.getDirect(i))
                {
                    Assert.assertNotNull(ex);

                    cache.put(i, "xx" + i);

                    DirectValueAccess direct = cache.addOrReplaceDirect(i, ex, i + 10);
                    try
                    {
                        for (int c = 0; c < i + 10; c++)
                            direct.buffer().put((byte) i);
                    }
                    finally
                    {
                        Assert.assertFalse(direct.commit());
                    }

                    Assert.assertEquals(cache.get(i), "xx" + i);
                }
            }

            // unconditional add
            for (int i = 0; i < 100; i++)
            {
                DirectValueAccess direct = cache.addOrReplaceDirect(i, null, i + 10);
                try
                {
                    for (int c = 0; c < i + 10; c++)
                        direct.buffer().put((byte) i);
                }
                finally
                {
                    Assert.assertTrue(direct.commit());
                }
            }
        }
    }
}
