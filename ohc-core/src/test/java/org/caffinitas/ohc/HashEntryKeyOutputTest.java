package org.caffinitas.ohc;

import java.io.EOFException;
import java.util.Arrays;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class HashEntryKeyOutputTest
{

    @Test
    public void testHashFinish() throws Exception
    {
        byte[] ref = Utils.randomBytes(10);
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

            out.finish();

            assertEquals(out.hash(), hasher.hash().asLong());
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
        byte[] ref = Utils.randomBytes(1234);
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
        String ref = "ewoifjeoif jewoifj oiewjfio ejwiof jeowijf oiewhiuf ";
        HashEntryKeyOutput out = build(ref.length() + 2);
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

    @Test(expectedExceptions = EOFException.class)
    public void testAssertAvail() throws Exception
    {
        String ref = "ewoifjeoif jewoifj oiewjfio ejwiof jeowijf oiewhiuf ";
        HashEntryKeyOutput out = build(ref.length() + 2);
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
        HashEntries.init(0, len, 0, adr);
        HashEntryKeyOutput out = new HashEntryKeyOutput(adr, len);
        assertEquals(out.avail(), len);
        return out;
    }
}