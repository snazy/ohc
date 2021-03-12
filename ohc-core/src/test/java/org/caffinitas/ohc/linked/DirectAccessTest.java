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

import com.google.common.base.Charsets;
import org.caffinitas.ohc.DirectValueAccess;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.nio.BufferUnderflowException;
import java.nio.ReadOnlyBufferException;

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
                String s = "";
                for (int c = 0; c < i + 10; c++)
                    s = s + "42";
                cache.put(i, s);
            }

            for (int i = 0; i < 100; i++)
            {
                Assert.assertTrue(cache.containsKey(i));
                try (DirectValueAccess direct = cache.getDirect(i))
                {
                    String s = "";
                    for (int c = 0; c < i + 10; c++)
                        s = s + "42";
                    byte[] bytes = s.getBytes(Charsets.UTF_8);

                    Assert.assertEquals(direct.buffer().capacity(), bytes.length + 2);
                    Assert.assertEquals(direct.buffer().limit(), bytes.length + 2);

                    Assert.assertEquals(TestUtils.stringSerializer.deserialize(direct.buffer()), s);

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
}
