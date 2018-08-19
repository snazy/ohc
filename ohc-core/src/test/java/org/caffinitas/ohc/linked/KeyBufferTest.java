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

import java.nio.ByteBuffer;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.caffinitas.ohc.HashAlgorithm;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class KeyBufferTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    @DataProvider
    public Object[][] hashes()
    {
        return new Object[][]{
        { HashAlgorithm.MURMUR3 },
        { HashAlgorithm.CRC32 },
        // TODO { HashAlgorithm.CRC32C }, testing this with Java 8 yields to test failures (as the implementation falls back to CRC32)
        // TODO { HashAlgorithm.XX }
        };
    }

    @Test(dataProvider = "hashes")
    public void testHashFinish(HashAlgorithm hashAlgorithm) throws Exception
    {
        KeyBuffer out = new KeyBuffer(12);

        ByteBuffer buf = out.byteBuffer();
        byte[] ref = TestUtils.randomBytes(10);
        buf.put((byte) (42 & 0xff));
        buf.put(ref);
        buf.put((byte) (0xf0 & 0xff));
        out.finish(org.caffinitas.ohc.linked.Hasher.create(hashAlgorithm));

        Hasher hasher = hasher(hashAlgorithm);
        hasher.putByte((byte) 42);
        hasher.putBytes(ref);
        hasher.putByte((byte) 0xf0);
        long longHash = hash(hasher);

        assertEquals(out.hash(), longHash);
    }

    private long hash(Hasher hasher)
    {
        HashCode hash = hasher.hash();
        if (hash.bits() == 32)
        {
            long longHash = hash.asInt();
            longHash = longHash << 32 | (longHash & 0xffffffffL);
            return longHash;
        }
        return hash.asLong();
    }

    private Hasher hasher(HashAlgorithm hashAlgorithm)
    {
        switch (hashAlgorithm)
        {
            case MURMUR3:
                return Hashing.murmur3_128().newHasher();
            case CRC32:
                return Hashing.crc32().newHasher();
            case CRC32C:
                return Hashing.crc32c().newHasher();
            default:
                throw new IllegalArgumentException();
        }
    }

    @Test(dataProvider = "hashes", dependsOnMethods = "testHashFinish")
    public void testHashFinish16(HashAlgorithm hashAlgorithm) throws Exception
    {
        KeyBuffer out = new KeyBuffer(16);

        byte[] ref = TestUtils.randomBytes(14);
        ByteBuffer buf = out.byteBuffer();
        buf.put((byte) (42 & 0xff));
        buf.put(ref);
        buf.put((byte) (0xf0 & 0xff));
        out.finish(org.caffinitas.ohc.linked.Hasher.create(hashAlgorithm));

        Hasher hasher = hasher(hashAlgorithm);
        hasher.putByte((byte) 42);
        hasher.putBytes(ref);
        hasher.putByte((byte) 0xf0);
        long longHash = hash(hasher);

        assertEquals(out.hash(), longHash);
    }

    @Test(dataProvider = "hashes", dependsOnMethods = "testHashFinish16")
    public void testHashRandom(HashAlgorithm hashAlgorithm) throws Exception
    {
        for (int i = 1; i < 4100; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                KeyBuffer out = new KeyBuffer(i);

                byte[] ref = TestUtils.randomBytes(i);
                ByteBuffer buf = out.byteBuffer();
                buf.put(ref);
                out.finish(org.caffinitas.ohc.linked.Hasher.create(hashAlgorithm));

                Hasher hasher = hasher(hashAlgorithm);
                hasher.putBytes(ref);
                long longHash = hash(hasher);

                assertEquals(out.hash(), longHash);
            }
        }
    }
}
