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
package org.caffinitas.ohc.tables;

import java.io.EOFException;
import java.util.Arrays;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class HashEntryKeyInputTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    static final long MIN_ALLOC_LEN = 1024;

    @Test
    public void testReadBoolean() throws Exception
    {
        boolean ref = true;

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeBoolean(ref);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
            boolean rd = input.readBoolean();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testReadByte() throws Exception
    {
        int ref = 0xffffff9e;

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeByte(ref);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
            int rd = input.readByte();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testReadUnsignedByte() throws Exception
    {
        int ref = 0x9e;

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeByte(ref);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
            int rd = input.readUnsignedByte();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testReadShort() throws Exception
    {
        short ref = 0x7654;

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeShort(ref);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
            short rd = input.readShort();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testReadUnsignedShort() throws Exception
    {
        int ref = 0x9f8e;

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeShort(ref);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
            int rd = input.readUnsignedShort();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testReadChar() throws Exception
    {
        char ref = 'R';

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeChar(ref);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
            char rd = input.readChar();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testReadInt() throws Exception
    {
        int ref = 0x76543210;

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeInt(ref);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
            int rd = input.readInt();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testReadLong() throws Exception
    {
        long ref = 0xdeafbeef42110815L;

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeLong(ref);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
            long rd = input.readLong();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testReadFloat() throws Exception
    {
        float ref = 42.8364f;

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeFloat(ref);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
            float rd = input.readFloat();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testReadDouble() throws Exception
    {
        double ref = .386533d;

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeDouble(ref);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
            double rd = input.readDouble();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testReadUTF() throws Exception
    {
        String ref = "aiehwfuiewh oifjewo ifjoiewj foijew f jioew fio \u00e4\u00f6\u00fc \uff02 ";

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeUTF(ref);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
            String rd = input.readUTF();
            assertEquals(rd, ref);
            assertEquals(input.available(), 0);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testReadMixed() throws Exception
    {
        String ref = "aiehwfuiewh oifjewo ifjoiewj foijew f jioew fio \u00e4\u00f6\u00fc \uff02 ";

        ByteArrayDataOutput out = ByteStreams.newDataOutput();
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
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
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
            Uns.free(adr);
        }
    }

    @Test(expectedExceptions = EOFException.class)
    public void testAssertAvail() throws Exception
    {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeInt(0x11223344);
        out.writeLong(0xaabbccddeeff1213L);
        out.writeFloat(.234234324f);
        out.writeDouble(49.374673728d);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
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
            Uns.free(adr);
        }
    }

    @Test
    public void testReadFully() throws Exception
    {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.write(new byte[12345]);
        byte[] bytes1 = TestUtils.randomBytes(5432);
        out.write(bytes1);
        byte[] bytes2 = TestUtils.randomBytes(321);
        out.write(bytes2);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
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
            Uns.free(adr);
        }
    }

    @Test
    public void testReadFullyOffLen() throws Exception
    {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.write(new byte[12345]);
        byte[] bytes1 = TestUtils.randomBytes(5432);
        out.write(bytes1, 1000, bytes1.length - 1000);
        byte[] bytes2 = TestUtils.randomBytes(321);
        out.write(bytes2, 100, bytes2.length - 100);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
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
            Uns.free(adr);
        }
    }

    @Test
    public void testSkipBytes() throws Exception
    {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.write(new byte[12345]);
        byte[] bytes2 = TestUtils.randomBytes(321);
        out.write(bytes2);
        long adr = convertToKey(out);
        try
        {
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
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
            Uns.free(adr);
        }
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testReadLine() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {
            HashEntries.init(0L, 10L, 10L, adr);
            HashEntryKeyInput input = new HashEntryKeyInput(adr);
            input.readLine();
        }
        finally
        {
            Uns.free(adr);
        }
    }

    private static long convertToKey(ByteArrayDataOutput out)
    {
        byte[] arr = out.toByteArray();
        long adr = Uns.allocate(Util.ENTRY_OFF_DATA + Util.roundUpTo8(arr.length));
        HashEntries.init(0L, arr.length, 0L, adr);
        Uns.copyMemory(arr, 0, adr, Util.ENTRY_OFF_DATA, arr.length);
        return adr;
    }
}