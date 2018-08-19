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

import java.util.Random;

import org.caffinitas.ohc.HashAlgorithm;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HasherTest
{
    @Test
    public void testMurmur3()
    {
        test(HashAlgorithm.MURMUR3);
    }

    @Test
    public void testCRC32()
    {
        test(HashAlgorithm.CRC32);
    }

    @Test
    public void testCRC32C()
    {
        test(HashAlgorithm.CRC32C);
    }

    @Test
    public void testXX()
    {
        test(HashAlgorithm.XX);
    }

    private void test(HashAlgorithm hash)
    {
        Random rand = new Random();

        byte[] buf = new byte[3211];
        rand.nextBytes(buf);

        Hasher hasher = Hasher.create(hash);
        long arrayVal = hasher.hash(buf);
        long memAddr = Uns.allocate(buf.length + 99);
        try
        {
            Uns.copyMemory(buf, 0, memAddr, 99L, buf.length);

            long memoryVal = hasher.hash(memAddr, 99L, buf.length);

            Assert.assertEquals(memoryVal, arrayVal);
        }
        finally
        {
            Uns.free(memAddr);
        }
    }
}
