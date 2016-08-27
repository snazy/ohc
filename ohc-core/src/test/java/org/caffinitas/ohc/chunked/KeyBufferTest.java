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
package org.caffinitas.ohc.chunked;

import java.nio.ByteBuffer;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.caffinitas.ohc.HashAlgorithm;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class KeyBufferTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    @Test
    public void testHashFinish() throws Exception
    {
        byte[] ref = TestUtils.randomBytes(10);
        ByteBuffer buf = ByteBuffer.allocate(12);
        buf.put((byte)(42 & 0xff));
        buf.put(ref);
        buf.put((byte)(0xf0 & 0xff));
        KeyBuffer out = new KeyBuffer(buf).finish(org.caffinitas.ohc.chunked.Hasher.create(HashAlgorithm.MURMUR3));

        Hasher hasher = Hashing.murmur3_128().newHasher();
        hasher.putByte((byte) 42);
        hasher.putBytes(ref);
        hasher.putByte((byte) 0xf0);

        assertEquals(out.hash(), hasher.hash().asLong());
    }

    @Test(dependsOnMethods = "testHashFinish")
    public void testHashFinish16() throws Exception
    {
        byte[] ref = TestUtils.randomBytes(14);
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.put((byte)(42 & 0xff));
        buf.put(ref);
        buf.put((byte)(0xf0 & 0xff));
        KeyBuffer out = new KeyBuffer(buf).finish(org.caffinitas.ohc.chunked.Hasher.create(HashAlgorithm.MURMUR3));

        Hasher hasher = Hashing.murmur3_128().newHasher();
        hasher.putByte((byte) 42);
        hasher.putBytes(ref);
        hasher.putByte((byte) 0xf0);

        assertEquals(out.hash(), hasher.hash().asLong());
    }

    @Test(dependsOnMethods = "testHashFinish16")
    public void testHashRandom() throws Exception
    {
        for (int i = 1; i < 4100; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                byte[] ref = TestUtils.randomBytes(i);
                ByteBuffer buf = ByteBuffer.allocate(i);
                buf.put(ref);
                KeyBuffer out = new KeyBuffer(buf).finish(org.caffinitas.ohc.chunked.Hasher.create(HashAlgorithm.MURMUR3));

                Hasher hasher = Hashing.murmur3_128().newHasher();
                hasher.putBytes(ref);

                assertEquals(out.hash(), hasher.hash().asLong());
            }
        }
    }
}
