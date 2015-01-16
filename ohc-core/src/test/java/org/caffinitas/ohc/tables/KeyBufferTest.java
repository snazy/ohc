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

import java.util.Arrays;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;

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
        KeyBuffer out = build(12);
        out.write(42);
        out.write(ref);
        out.write(0xf0);

        Hasher hasher = Hashing.murmur3_128().newHasher();
        hasher.putByte((byte) 42);
        hasher.putBytes(ref);
        hasher.putByte((byte) 0xf0);

        out.finish();

        assertEquals(out.hash(), hasher.hash().asLong());
    }

    @Test
    public void testHashFinish16() throws Exception
    {
        byte[] ref = TestUtils.randomBytes(14);
        KeyBuffer out = build(16);
        out.write(42);
        out.write(ref);
        out.write(0xf0);

        Hasher hasher = Hashing.murmur3_128().newHasher();
        hasher.putByte((byte) 42);
        hasher.putBytes(ref);
        hasher.putByte((byte) 0xf0);

        out.finish();

        assertEquals(out.hash(), hasher.hash().asLong());
    }

    @Test
    public void testWrite() throws Exception
    {
        int ref = 42;
        KeyBuffer out = build(1);
        out.write(ref);
        ByteArrayDataInput in = ByteStreams.newDataInput(out.array());
        assertEquals(in.readByte(), ref);
    }

    @Test
    public void testWriteByte() throws Exception
    {
        int ref = 42;
        KeyBuffer out = build(1);
        out.writeByte(ref);
        ByteArrayDataInput in = ByteStreams.newDataInput(out.array());
        assertEquals(in.readByte(), ref);
    }

    @Test
    public void testWriteArr() throws Exception
    {
        byte[] ref = TestUtils.randomBytes(1234);
        KeyBuffer out = build(ref.length - 200);
        out.write(ref, 100, 1034);
        ByteArrayDataInput in = ByteStreams.newDataInput(out.array());
        byte[] b = new byte[ref.length];
        in.readFully(b, 100, 1034);
        assertEquals(Arrays.copyOfRange(b, 100, 1134), Arrays.copyOfRange(ref, 100, 1134));
    }

    @Test
    public void testWriteShort() throws Exception
    {
        short ref = (short) 0x9876;
        KeyBuffer out = build(2);
        out.writeShort(ref);
        ByteArrayDataInput in = ByteStreams.newDataInput(out.array());
        assertEquals(in.readShort(), ref);
    }

    @Test
    public void testWriteChar() throws Exception
    {
        char ref = 'R';
        KeyBuffer out = build(2);
        out.writeChar(ref);
        ByteArrayDataInput in = ByteStreams.newDataInput(out.array());
        assertEquals(in.readChar(), ref);
    }

    @Test
    public void testWriteInt() throws Exception
    {
        int ref = 0x9f8e1317;
        KeyBuffer out = build(4);
        out.writeInt(ref);
        ByteArrayDataInput in = ByteStreams.newDataInput(out.array());
        assertEquals(in.readInt(), ref);
    }

    @Test
    public void testWriteLong() throws Exception
    {
        long ref = 0x9876deafbeefaddfL;
        KeyBuffer out = build(8);
        out.writeLong(ref);
        ByteArrayDataInput in = ByteStreams.newDataInput(out.array());
        assertEquals(in.readLong(), ref);
    }

    @Test
    public void testWriteFloat() throws Exception
    {
        float ref = 9.876f;
        KeyBuffer out = build(4);
        out.writeFloat(ref);
        ByteArrayDataInput in = ByteStreams.newDataInput(out.array());
        assertEquals(in.readFloat(), ref);
    }

    @Test
    public void testWriteDouble() throws Exception
    {
        double ref = 9.87633d;
        KeyBuffer out = build(8);
        out.writeDouble(ref);
        ByteArrayDataInput in = ByteStreams.newDataInput(out.array());
        assertEquals(in.readDouble(), ref);
    }

    @Test
    public void testWriteBoolean() throws Exception
    {
        boolean ref = true;
        KeyBuffer out = build(1);
        out.writeBoolean(ref);
        ByteArrayDataInput in = ByteStreams.newDataInput(out.array());
        assertEquals(in.readBoolean(), ref);
    }

    @Test
    public void testWriteUTF() throws Exception
    {
        String ref = "ewoifjeoif jewoifj oiewjfio ejwiof jeowijf oiewhiuf äöü ＂ ";
        KeyBuffer out = build(TestUtils.writeUTFLen(ref));
        out.writeUTF(ref);
        ByteArrayDataInput in = ByteStreams.newDataInput(out.array());
        assertEquals(in.readUTF(), ref);
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

        KeyBuffer out = build(TestUtils.writeUTFLen(ref1) +
                              TestUtils.writeUTFLen(ref2) +
                              TestUtils.writeUTFLen(ref3) +
                              TestUtils.writeUTFLen(ref4));
        assertEquals(out.position(), 0);
        out.writeUTF(ref1);
        assertEquals(out.position(), TestUtils.writeUTFLen(ref1));
        out.writeUTF(ref2);
        assertEquals(out.position(), TestUtils.writeUTFLen(ref1) +
                                     TestUtils.writeUTFLen(ref2));
        out.writeUTF(ref3);
        assertEquals(out.position(), TestUtils.writeUTFLen(ref1) +
                                     TestUtils.writeUTFLen(ref2) +
                                     TestUtils.writeUTFLen(ref3));
        out.writeUTF(ref4);
        assertEquals(out.position(), TestUtils.writeUTFLen(ref1) +
                                     TestUtils.writeUTFLen(ref2) +
                                     TestUtils.writeUTFLen(ref3) +
                                     TestUtils.writeUTFLen(ref4));
        ByteArrayDataInput in = ByteStreams.newDataInput(out.array());
        assertEquals(in.readUTF(), ref1);
        assertEquals(in.readUTF(), ref2);
        assertEquals(in.readUTF(), ref3);
        assertEquals(in.readUTF(), ref4);
    }

    private static KeyBuffer build(int len)
    {
        return new KeyBuffer(len);
    }
}