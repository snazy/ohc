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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Random;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class UnsTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    private static final Unsafe unsafe;

    static final int CAPACITY = 65536;
    static final ByteBuffer directBuffer;

    static
    {
        try
        {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe.addressSize() > 8)
                throw new RuntimeException("Address size " + unsafe.addressSize() + " not supported yet (max 8 bytes)");

            directBuffer = ByteBuffer.allocateDirect(CAPACITY);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    private static void fillRandom()
    {
        Random r = new Random();
        directBuffer.clear();
        while (directBuffer.remaining() >= 4)
            directBuffer.putInt(r.nextInt());
        directBuffer.clear();
    }

    @Test
    public void testDirectBufferFor() throws Exception
    {
        fillRandom();

        ByteBuffer buf = Uns.directBufferFor(((DirectBuffer) directBuffer).address(), directBuffer.capacity());

        for (int i = 0; i < CAPACITY; i++)
        {
            byte b = buf.get();
            byte d = directBuffer.get();
            assertEquals(b, d);

            assertEquals(buf.position(), directBuffer.position());
            assertEquals(buf.limit(), directBuffer.limit());
            assertEquals(buf.remaining(), directBuffer.remaining());
            assertEquals(buf.capacity(), directBuffer.capacity());
        }

        buf.clear();
        directBuffer.clear();

        while (buf.remaining() >= 8)
        {
            long b = buf.getLong();
            long d = directBuffer.getLong();
            assertEquals(b, d);

            assertEquals(buf.position(), directBuffer.position());
            assertEquals(buf.remaining(), directBuffer.remaining());
        }

        while (buf.remaining() >= 4)
        {
            int b = buf.getInt();
            int d = directBuffer.getInt();
            assertEquals(b, d);

            assertEquals(buf.position(), directBuffer.position());
            assertEquals(buf.remaining(), directBuffer.remaining());
        }

        for (int i = 0; i < CAPACITY; i++)
        {
            byte b = buf.get(i);
            byte d = directBuffer.get(i);
            assertEquals(b, d);

            if (i >= CAPACITY - 1)
                continue;

            char bufChar = buf.getChar(i);
            char dirChar = directBuffer.getChar(i);
            short bufShort = buf.getShort(i);
            short dirShort = directBuffer.getShort(i);

            assertEquals(bufChar, dirChar);
            assertEquals(bufShort, dirShort);

            if (i >= CAPACITY - 3)
                continue;

            int bufInt = buf.getInt(i);
            int dirInt = directBuffer.getInt(i);
            float bufFloat = buf.getFloat(i);
            float dirFloat = directBuffer.getFloat(i);

            assertEquals(bufInt, dirInt);
            assertEquals(bufFloat, dirFloat);

            if (i >= CAPACITY - 7)
                continue;

            long bufLong = buf.getLong(i);
            long dirLong = directBuffer.getLong(i);
            double bufDouble = buf.getDouble(i);
            double dirDouble = directBuffer.getDouble(i);

            assertEquals(bufLong, dirLong);
            assertEquals(bufDouble, dirDouble);
        }
    }

    @Test
    public void testAllocate() throws Exception
    {
        ByteBuffer adr = Uns.allocate(100, true);
        assertNotNull(adr);
        Uns.free(adr);
    }

    @Test
    public void testGetTotalAllocated() throws Exception
    {
        long before = Uns.getTotalAllocated();
        if (before < 0L)
            return;

        // TODO Uns.getTotalAllocated() seems not to respect "small" areas - need to check that ... eventually.
//        long[] adrs = new long[10000];
//        try
//        {
//            for (int i=0;i<adrs.length;i++)
//                adrs[i] = Uns.allocate(100);
//            assertTrue(Uns.getTotalAllocated() > before);
//        }
//        finally
//        {
//            for (long adr : adrs)
//                Uns.free(adr);
//        }

        ByteBuffer adr = Uns.allocate(128 * 1024 * 1024, true);
        try
        {
            assertTrue(Uns.getTotalAllocated() > before);
        }
        finally
        {
            Uns.free(adr);
        }
    }
}
