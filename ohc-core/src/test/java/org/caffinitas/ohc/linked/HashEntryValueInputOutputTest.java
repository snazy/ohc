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
import java.util.Arrays;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class HashEntryValueInputOutputTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    @Test
    public void testReadBoolean() throws Exception
    {
        boolean ref = true;

        HashEntryValueOutput out = build(1);
        try
        {
            out.writeBoolean(ref);

            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            boolean rd = input.readBoolean();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadByte() throws Exception
    {
        int ref = 0xffffff9e;

        HashEntryValueOutput out = build(1);
        try
        {
            out.writeByte(ref);

            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            int rd = input.readByte();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadUnsignedByte() throws Exception
    {
        int ref = 0x9e;

        HashEntryValueOutput out = build(1);
        out.writeByte(ref);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            int rd = input.readUnsignedByte();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadShort() throws Exception
    {
        short ref = 0x7654;

        HashEntryValueOutput out = build(2);
        out.writeShort(ref);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            short rd = input.readShort();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadUnsignedShort() throws Exception
    {
        int ref = 0x9f8e;

        HashEntryValueOutput out = build(2);
        out.writeShort(ref);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            int rd = input.readUnsignedShort();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadChar() throws Exception
    {
        char ref = 'R';

        HashEntryValueOutput out = build(2);
        out.writeChar(ref);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            char rd = input.readChar();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadInt() throws Exception
    {
        int ref = 0x76543210;

        HashEntryValueOutput out = build(4);
        out.writeInt(ref);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            int rd = input.readInt();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadLong() throws Exception
    {
        long ref = 0xdeafbeef42110815L;

        HashEntryValueOutput out = build(8);
        out.writeLong(ref);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            long rd = input.readLong();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadFloat() throws Exception
    {
        float ref = 42.8364f;

        HashEntryValueOutput out = build(4);
        out.writeFloat(ref);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            float rd = input.readFloat();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadDouble() throws Exception
    {
        double ref = .386533d;

        HashEntryValueOutput out = build(8);
        out.writeDouble(ref);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            double rd = input.readDouble();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadUTF() throws Exception
    {
        String ref = "aiehwfuiewh oifjewo ifjoiewj foijew f jioew fio \u00e4\u00f6\u00fc \uff02 ";

        HashEntryValueOutput out = build(TestUtils.writeUTFLen(ref));
        out.writeUTF(ref);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            String rd = input.readUTF();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test(dependsOnMethods = "testReadUTF")
    public void testReadUTFAllChars() throws Exception
    {
        StringBuilder sb = new StringBuilder(65536);
        for (int i = 0; i <= 65535; i++)
            sb.append((char) i);
        String ref1 = sb.substring(0, 16384);
        String ref2 = sb.substring(16384, 32768);
        String ref3 = sb.substring(32768, 49152);
        String ref4 = sb.substring(49152);

        HashEntryValueOutput out = build(TestUtils.writeUTFLen(ref1) +
                                         TestUtils.writeUTFLen(ref2) +
                                         TestUtils.writeUTFLen(ref3) +
                                         TestUtils.writeUTFLen(ref4));
        out.writeUTF(ref1);
        out.writeUTF(ref2);
        out.writeUTF(ref3);
        out.writeUTF(ref4);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            String rd = input.readUTF();
            assertEquals(rd, ref1);
            rd = input.readUTF();
            assertEquals(rd, ref2);
            rd = input.readUTF();
            assertEquals(rd, ref3);
            rd = input.readUTF();
            assertEquals(rd, ref4);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadMixed() throws Exception
    {
        String ref = "aiehwfuiewh oifjewo ifjoiewj foijew f jioew fio \u00e4\u00f6\u00fc \uff02 ";

        HashEntryValueOutput out = build(TestUtils.writeUTFLen(ref) +
                                         3 + 6 + 12 + 12 + 12345 + 5432 + 321 + TestUtils.writeUTFLen(ref));
        out.writeUTF(ref);
        out.writeBoolean(false);
        out.writeByte(0x8f);
        out.writeByte(0x8f);
        out.writeChar('R');
        out.writeShort(0x9eab);
        out.writeShort(0x9eab);
        out.writeInt(0x11223344);
        out.writeLong(0xaabbccddeeff1213L);
        out.writeFloat(.234234324f);
        out.writeDouble(49.374673728d);
        out.write(new byte[12345]);
        byte[] bytes1 = TestUtils.randomBytes(5432);
        out.write(bytes1);
        byte[] bytes2 = TestUtils.randomBytes(321);
        out.write(bytes2);
        out.writeUTF(ref);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            assertEquals(input.readUTF(), ref);
            assertEquals(input.readBoolean(), false);
            assertEquals(input.readByte(), (int) (byte) 0x8f);
            assertEquals(input.readUnsignedByte(), 0x8f);
            assertEquals(input.readChar(), 'R');
            assertEquals(input.readShort(), (int) (short) 0x9eab);
            assertEquals(input.readUnsignedShort(), 0x9eab);
            assertEquals(input.readInt(), 0x11223344);
            assertEquals(input.readLong(), 0xaabbccddeeff1213L);
            assertEquals(input.readFloat(), .234234324f);
            assertEquals(input.readDouble(), 49.374673728d);
            input.skipBytes(12345);
            byte[] b = new byte[bytes1.length];
            input.readFully(b);
            assertEquals(b, bytes1);
            b = new byte[bytes2.length];
            input.readFully(b);
            assertEquals(b, bytes2);
            assertEquals(input.readUTF(), ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test(expectedExceptions = EOFException.class)
    public void testAssertAvail() throws Exception
    {
        HashEntryValueOutput out = build(24);
        out.writeInt(0x11223344);
        out.writeLong(0xaabbccddeeff1213L);
        out.writeFloat(.234234324f);
        out.writeDouble(49.374673728d);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            input.assertAvail(24);
            assertEquals(input.readInt(), 0x11223344);
            input.assertAvail(20);
            assertEquals(input.readLong(), 0xaabbccddeeff1213L);
            input.assertAvail(12);
            assertEquals(input.readFloat(), .234234324f);
            input.assertAvail(8);
            assertEquals(input.readDouble(), 49.374673728d);
            assertEquals(input.available(), 0);

            input.assertAvail(1);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadFully() throws Exception
    {
        HashEntryValueOutput out = build(12345 + 5432 + 321);
        out.write(new byte[12345]);
        byte[] bytes1 = TestUtils.randomBytes(5432);
        out.write(bytes1);
        byte[] bytes2 = TestUtils.randomBytes(321);
        out.write(bytes2);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            input.skipBytes(12345);
            byte[] b = new byte[bytes1.length];
            input.readFully(b);
            assertEquals(b, bytes1);
            b = new byte[bytes2.length];
            input.readFully(b);
            assertEquals(b, bytes2);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testReadFullyOffLen() throws Exception
    {
        HashEntryValueOutput out = build(12345 + 5432 - 1000 + 321 - 100);
        out.write(new byte[12345]);
        byte[] bytes1 = TestUtils.randomBytes(5432);
        out.write(bytes1, 1000, bytes1.length - 1000);
        byte[] bytes2 = TestUtils.randomBytes(321);
        out.write(bytes2, 100, bytes2.length - 100);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            input.skipBytes(12345);
            byte[] b = new byte[bytes1.length * 2];
            input.assertAvail(bytes1.length - 1000 + bytes2.length - 100);
            assertEquals(input.available(), bytes1.length - 1000 + bytes2.length - 100);
            input.readFully(b, 1000, bytes1.length - 1000);
            assertEquals(Arrays.copyOfRange(b, 1000, bytes1.length), Arrays.copyOfRange(bytes1, 1000, bytes1.length));
            input.assertAvail(bytes2.length - 100);
            assertEquals(input.available(), bytes2.length - 100);
            input.readFully(b, 100, bytes2.length - 100);
            assertEquals(Arrays.copyOfRange(b, 100, bytes2.length), Arrays.copyOfRange(bytes2, 100, bytes2.length));
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    @Test
    public void testSkipBytes() throws Exception
    {
        HashEntryValueOutput out = build(12345 + 321);
        out.write(new byte[12345]);
        byte[] bytes2 = TestUtils.randomBytes(321);
        out.write(bytes2);
        try
        {
            HashEntryValueInput input = new HashEntryValueInput(out.blkAdr);
            input.skipBytes(10000);
            input.skipBytes(2000);
            input.skipBytes(300);
            input.skipBytes(40);
            input.skipBytes(5);
            byte[] b = new byte[bytes2.length];
            input.assertAvail(bytes2.length);
            assertEquals(input.available(), bytes2.length);
            input.readFully(b);
            assertEquals(b, bytes2);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(out.blkAdr);
        }
    }

    private static HashEntryValueOutput build(int len)
    {
        long adr = Uns.allocate(Util.ENTRY_OFF_DATA + 16 + len);
        HashEntries.init(0, 11, len, adr, 0);
        HashEntryValueOutput out = new HashEntryValueOutput(adr, 16, len);
        assertEquals(out.avail(), len);
        return out;
    }
}