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
package org.caffinitas.ohc;

import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Unsafe;

final class Uns
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Uns.class);

    static final Unsafe unsafe;
    private static final IAllocator allocator;

    static
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
            if (unsafe.addressSize() > 8)
                throw new RuntimeException("Address size " + unsafe.addressSize() + " not supported yet (max 8 bytes)");

            IAllocator alloc;
            try
            {
                alloc = new JEMallocAllocator();
            }
            catch (Throwable t)
            {
                LOGGER.warn("jemalloc native library not found (" + t + ")");
                alloc = new NativeAllocator();
            }
            allocator = alloc;
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    private Uns()
    {
    }

    static void putChar(long address, char value)
    {
        if (address == 0L)
            throw new NullPointerException();
        unsafe.putChar(null, address, value);
    }

    static void putShort(long address, short value)
    {
        if (address == 0L)
            throw new NullPointerException();
        unsafe.putShort(null, address, value);
    }

    static void putInt(long address, int value)
    {
        if (address == 0L)
            throw new NullPointerException();
        unsafe.putInt(null, address, value);
    }

    static void putLong(long address, long value)
    {
        if (address == 0L)
            throw new NullPointerException();
        unsafe.putLong(null, address, value);
    }

    static void putFloat(long address, float value)
    {
        if (address == 0L)
            throw new NullPointerException();
        unsafe.putFloat(null, address, value);
    }

    static void putDouble(long address, double value)
    {
        if (address == 0L)
            throw new NullPointerException();
        unsafe.putDouble(null, address, value);
    }

    static void putLongVolatile(long address, long value)
    {
        if (address == 0L)
            throw new NullPointerException();
        unsafe.putLongVolatile(null, address, value);
    }

    static void putLongVolatile(Object obj, long offset, long value)
    {
        if (obj == null)
            throw new NullPointerException();
        unsafe.putLongVolatile(obj, offset, value);
    }

    static long getLongFromByteArray(byte[] array, int offset)
    {
        if (array == null)
            throw new NullPointerException();
        if (offset < 0 || offset > array.length - 8)
            throw new ArrayIndexOutOfBoundsException();
        return unsafe.getLong(array, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset);
    }

    static char getChar(long address)
    {
        if (address == 0L)
            throw new NullPointerException();
        return unsafe.getChar(null, address);
    }

    static short getShort(long address)
    {
        if (address == 0L)
            throw new NullPointerException();
        return unsafe.getShort(null, address);
    }

    static int getInt(long address)
    {
        if (address == 0L)
            throw new NullPointerException();
        return unsafe.getInt(null, address);
    }

    static long getLong(long address)
    {
        if (address == 0L)
            throw new NullPointerException();
        return unsafe.getLong(null, address);
    }

    static float getFloat(long address)
    {
        if (address == 0L)
            throw new NullPointerException();
        return unsafe.getFloat(null, address);
    }

    static double getDouble(long address)
    {
        if (address == 0L)
            throw new NullPointerException();
        return unsafe.getDouble(null, address);
    }

    static long getLongVolatile(long address)
    {
        if (address == 0L)
            throw new NullPointerException();
        return unsafe.getLongVolatile(null, address);
    }

    static void putByte(long address, byte value)
    {
        if (address == 0L)
            throw new NullPointerException();
        unsafe.putByte(null, address, value);
    }

    static byte getByte(long address)
    {
        if (address == 0L)
            throw new NullPointerException();
        return unsafe.getByte(null, address);
    }

    static boolean compareAndSwap(long address, long expected, long value)
    {
        if (address == 0L)
            throw new NullPointerException();
        return unsafe.compareAndSwapLong(null, address, expected, value);
    }

    static boolean compareAndSwap(Object obj, long offset, long expected, long value)
    {
        if (obj == null)
            throw new NullPointerException();
        return unsafe.compareAndSwapLong(obj, offset, expected, value);
    }

    static void copyMemory(byte[] arr, int off, long address, long len)
    {
        if (arr == null)
            throw new NullPointerException();
        if (address == 0L)
            throw new NullPointerException();
        unsafe.copyMemory(arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, null, address, len);
    }

    static void copyMemory(long address, byte[] arr, int off, long len)
    {
        if (arr == null)
            throw new NullPointerException();
        if (address == 0L)
            throw new NullPointerException();
        unsafe.copyMemory(null, address, arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, len);
    }

    static void setMemory(long address, int len, byte val)
    {
        if (address == 0L)
            throw new NullPointerException();
        unsafe.setMemory(address, len, val);
    }

    static long fieldOffset(Class<?> clazz, String field)
    {
        try
        {
            return unsafe.objectFieldOffset(clazz.getDeclaredField(field));
        }
        catch (NoSuchFieldException e)
        {
            throw new RuntimeException(e);
        }
    }

    static void park(long nanos)
    {
        unsafe.park(false, nanos);
    }

    static long allocate(long bytes)
    {
        if (bytes <= 0)
            throw new IllegalArgumentException();
        long address = allocator.allocate(bytes);
        return address > 0L ? address : 0L;
    }

    static void free(long address)
    {
        if (address == 0L)
            throw new NullPointerException();
        allocator.free(address);
    }

    //

    private static final int LG_READERS = 7;

    private static final long RUNIT = 1L;
    private static final long WBIT = 1L << LG_READERS;
    private static final long LRBIT = WBIT << 1L;
    private static final long FBIT = LRBIT << 1L;
    private static final long RBITS = WBIT - 1L;
    private static final long RFULL = RBITS - 1L;
    private static final long ABITS = RBITS | WBIT | LRBIT | FBIT;
    private static final long SBITS = ~RBITS; // note overlap with ABITS
    private static final long ORIGIN = FBIT << 1;

    public static final long INVALID_LOCK = FBIT;

    static void initStamped(long address)
    {
        putLongVolatile(address, ORIGIN);
    }

    static long lockStampedRead(long address)
    {
        if (address == 0L)
            throw new NullPointerException();

        for (int spin = spinSeed(); ; spin++)
        {
            long s = getLongVolatile(address);
            long m = s & ABITS;
            long next;
            if ((s & FBIT) == FBIT)
                return INVALID_LOCK;
            if (m < RFULL && compareAndSwap(address, s, next = s + RUNIT))
                return next;

            spinLock(spin, inRehash(s));
        }
    }

    static long lockStampedWrite(long address)
    {
        if (address == 0L)
            throw new NullPointerException();

        for (int spin = spinSeed(); ; spin++)
        {
            long s = getLongVolatile(address);
            long next;
            if ((s & FBIT) == FBIT)
                return INVALID_LOCK;
            if ((s & ABITS) == 0L &&
                compareAndSwap(address, s, next = s | WBIT))
                return next;

            spinLock(spin, inRehash(s));
        }
    }

    static long lockStampedLongRun(long address)
    {
        if (address == 0L)
            throw new NullPointerException();

        for (int spin = spinSeed(); ; spin++)
        {
            long s = getLongVolatile(address);
            long next;
            if ((s & FBIT) == FBIT)
                throw new IllegalMonitorStateException("Lock marked as invalid");
            if ((s & ABITS) == 0L &&
                compareAndSwap(address, s, next = s | LRBIT))
                return next;

            spinLock(spin, false);
        }
    }

    private static boolean inRehash(long s)
    {
        return (s & LRBIT) != 0;
    }

    private static int spinSeed()
    {
        return (int) Thread.currentThread().getId();
    }

    private static void spinLock(int spin, boolean inRehash)
    {
        // spin in 50ms units during rehash and 5us else
        park(((spin & 3) + 1) * (inRehash ? 50000000 : 5000));
    }

    static void unlockStampedRead(long address, long stamp)
    {
        if (address == 0L)
            throw new NullPointerException();

        long s, m; //WNode h;
        for (; ; )
        {
            if (((s = getLongVolatile(address)) & SBITS) != (stamp & SBITS) ||
                (stamp & ABITS) == 0L || (m = s & ABITS) == 0L || m == WBIT)
                throw new IllegalMonitorStateException();
            if (m < RFULL)
            {
                if (compareAndSwap(address, s, s - RUNIT))
                    break;
            }
        }
    }

    static void unlockStampedWrite(long address, long stamp)
    {
        if (address == 0L)
            throw new NullPointerException();

        if ((stamp & WBIT) == 0L || !compareAndSwap(address, stamp, ORIGIN))
            throw new IllegalMonitorStateException();
    }

    static void unlockStampedLongRun(long address, long stamp)
    {
        if (address == 0L)
            throw new NullPointerException();

        if ((stamp & LRBIT) == 0L || !compareAndSwap(address, stamp, ORIGIN))
            throw new IllegalMonitorStateException();
    }

    public static void unlockForFail(long address, long stamp)
    {
        if (address == 0L)
            throw new NullPointerException();

        if ((stamp & LRBIT) == 0L || !compareAndSwap(address, stamp, FBIT))
            throw new IllegalMonitorStateException();
    }
}
