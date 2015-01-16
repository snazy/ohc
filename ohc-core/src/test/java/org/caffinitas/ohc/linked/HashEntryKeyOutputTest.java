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

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class HashEntryKeyOutputTest
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
        HashEntryKeyOutput out = build(12);
        try
        {
            assertEquals(out.avail(), 12);
            out.write(42);
            assertEquals(out.avail(), 11);
            out.write(ref);
            assertEquals(out.avail(), 1);
            out.write(0xf0);
            assertEquals(out.avail(), 0);

            Hasher hasher = Hashing.murmur3_128().newHasher();
            hasher.putByte((byte) 42);
            hasher.putBytes(ref);
            hasher.putByte((byte) 0xf0);

            assertEquals(out.murmur3hash(), hasher.hash().asLong());
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test(dependsOnMethods = "testHashFinish")
    public void testHashFinish16() throws Exception
    {
        byte[] ref = TestUtils.randomBytes(14);
        HashEntryKeyOutput out = build(16);
        try
        {
            assertEquals(out.avail(), 16);
            out.write(42);
            assertEquals(out.avail(), 15);
            out.write(ref);
            assertEquals(out.avail(), 1);
            out.write(0xf0);
            assertEquals(out.avail(), 0);

            Hasher hasher = Hashing.murmur3_128().newHasher();
            hasher.putByte((byte) 42);
            hasher.putBytes(ref);
            hasher.putByte((byte) 0xf0);

            assertEquals(out.murmur3hash(), hasher.hash().asLong());
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test(dependsOnMethods = "testHashFinish16")
    public void testHashRandom() throws Exception
    {
        for (int i = 1; i < 4100; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                byte[] ref = TestUtils.randomBytes(i);
                HashEntryKeyOutput out = build(i);
                try
                {
                    out.write(ref);

                    Hasher hasher = Hashing.murmur3_128().newHasher();
                    hasher.putBytes(ref);

                    assertEquals(out.murmur3hash(), hasher.hash().asLong());
                }
                finally
                {
                    Uns.free(out.blkAdr);
                }
            }
        }
    }

    @Test
    public void testOwnHash() throws IOException
    {
        String ref = "ewoifjeoif jewoifj oiewjfio ejwiof jeowijf oiewhiuf \u00e4\u00f6\u00fc \uff02 ";
        int len = TestUtils.writeUTFLen(ref);
        HashEntryKeyOutput out = build(len);
        try
        {
            long off = out.blkOff;

            out.writeUTF(ref);
            assertEquals(out.avail(), 0);

            long h2 = out.murmur3hash();

            KeyBuffer kb = new KeyBuffer(len);
            kb.writeUTF(ref);

            long h3 = kb.murmur3hash();

            Hasher hasher = Hashing.murmur3_128().newHasher();

            for (int i = 0; i < len; i++)
                hasher.putByte(Uns.getByte(out.blkAdr, off + i));

            long h1 = hasher.hash().asLong();

            Assert.assertEquals(h2, h1);
            Assert.assertEquals(h3, h1);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testWrite() throws Exception
    {
        int ref = 42;
        HashEntryKeyOutput out = build(1);
        try
        {
            out.write(ref);
            assertEquals(out.avail(), 0);
            HashEntryKeyInput in = new HashEntryKeyInput(out.blkAdr);
            assertEquals(in.readByte(), ref);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testWriteByte() throws Exception
    {
        int ref = 42;
        HashEntryKeyOutput out = build(1);
        try
        {
            out.writeByte(ref);
            assertEquals(out.avail(), 0);
            HashEntryKeyInput in = new HashEntryKeyInput(out.blkAdr);
            assertEquals(in.readByte(), ref);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testWriteArr() throws Exception
    {
        byte[] ref = TestUtils.randomBytes(1234);
        HashEntryKeyOutput out = build(ref.length - 200);
        try
        {
            out.write(ref, 100, 1034);
            assertEquals(out.avail(), 0);
            HashEntryKeyInput in = new HashEntryKeyInput(out.blkAdr);
            byte[] b = new byte[ref.length];
            in.readFully(b, 100, 1034);
            assertEquals(Arrays.copyOfRange(b, 100, 1134), Arrays.copyOfRange(ref, 100, 1134));
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testWriteShort() throws Exception
    {
        short ref = (short) 0x9876;
        HashEntryKeyOutput out = build(2);
        try
        {
            out.writeShort(ref);
            assertEquals(out.avail(), 0);
            HashEntryKeyInput in = new HashEntryKeyInput(out.blkAdr);
            assertEquals(in.readShort(), ref);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testWriteChar() throws Exception
    {
        char ref = 'R';
        HashEntryKeyOutput out = build(2);
        try
        {
            out.writeChar(ref);
            assertEquals(out.avail(), 0);
            HashEntryKeyInput in = new HashEntryKeyInput(out.blkAdr);
            assertEquals(in.readChar(), ref);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testWriteInt() throws Exception
    {
        int ref = 0x9f8e1317;
        HashEntryKeyOutput out = build(4);
        try
        {
            out.writeInt(ref);
            assertEquals(out.avail(), 0);
            HashEntryKeyInput in = new HashEntryKeyInput(out.blkAdr);
            assertEquals(in.readInt(), ref);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testWriteLong() throws Exception
    {
        long ref = 0x9876deafbeefaddfL;
        HashEntryKeyOutput out = build(8);
        try
        {
            out.writeLong(ref);
            assertEquals(out.avail(), 0);
            HashEntryKeyInput in = new HashEntryKeyInput(out.blkAdr);
            assertEquals(in.readLong(), ref);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testWriteFloat() throws Exception
    {
        float ref = 9.876f;
        HashEntryKeyOutput out = build(4);
        try
        {
            out.writeFloat(ref);
            assertEquals(out.avail(), 0);
            HashEntryKeyInput in = new HashEntryKeyInput(out.blkAdr);
            assertEquals(in.readFloat(), ref);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testWriteDouble() throws Exception
    {
        double ref = 9.87633d;
        HashEntryKeyOutput out = build(8);
        try
        {
            out.writeDouble(ref);
            assertEquals(out.avail(), 0);
            HashEntryKeyInput in = new HashEntryKeyInput(out.blkAdr);
            assertEquals(in.readDouble(), ref);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testWriteBoolean() throws Exception
    {
        boolean ref = true;
        HashEntryKeyOutput out = build(1);
        try
        {
            out.writeBoolean(ref);
            assertEquals(out.avail(), 0);
            HashEntryKeyInput in = new HashEntryKeyInput(out.blkAdr);
            assertEquals(in.readBoolean(), ref);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testWriteUTF() throws Exception
    {
        String ref = "ewoifjeoif jewoifj oiewjfio ejwiof jeowijf oiewhiuf \u00e4\u00f6\u00fc \uff02 ";
        HashEntryKeyOutput out = build(TestUtils.writeUTFLen(ref));
        try
        {
            out.writeUTF(ref);
            assertEquals(out.avail(), 0);
            HashEntryKeyInput in = new HashEntryKeyInput(out.blkAdr);
            assertEquals(in.readUTF(), ref);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test(dependsOnMethods = "testWriteUTF")
    public void testWriteUTFAllChars() throws Exception
    {
        StringBuilder sb = new StringBuilder(65536);
        for (int i=0; i<=65535; i++)
            sb.append((char) i);
        String ref1 = sb.substring(0, 16384);
        String ref2 = sb.substring(16384, 32768);
        String ref3 = sb.substring(32768, 49152);
        String ref4 = sb.substring(49152);

        HashEntryKeyOutput out = build(TestUtils.writeUTFLen(ref1) +
                                       TestUtils.writeUTFLen(ref2) +
                                       TestUtils.writeUTFLen(ref3) +
                                       TestUtils.writeUTFLen(ref4));
        try
        {
            out.writeUTF(ref1);
            out.writeUTF(ref2);
            out.writeUTF(ref3);
            out.writeUTF(ref4);
            assertEquals(out.avail(), 0);
            HashEntryKeyInput in = new HashEntryKeyInput(out.blkAdr);
            assertEquals(in.readUTF(), ref1);
            assertEquals(in.readUTF(), ref2);
            assertEquals(in.readUTF(), ref3);
            assertEquals(in.readUTF(), ref4);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test(expectedExceptions = EOFException.class)
    public void testAssertAvail() throws Exception
    {
        String ref = "ewoifjeoif jewoifj oiewjfio ejwiof jeowijf oiewhiuf \u00e4\u00f6\u00fc \uff02 ";
        HashEntryKeyOutput out = build(TestUtils.writeUTFLen(ref));
        try
        {
            out.writeUTF(ref);
            assertEquals(out.avail(), 0);
            out.assertAvail(1);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    private static HashEntryKeyOutput build(int len)
    {
        long adr = Uns.allocate(Util.ENTRY_OFF_DATA + len);
        HashEntries.init(0, len, 0, adr, 0);
        HashEntryKeyOutput out = new HashEntryKeyOutput(adr, len);
        assertEquals(out.avail(), len);
        return out;
    }
}