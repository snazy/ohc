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

import sun.misc.Unsafe;

final class Uns
{

    static final Unsafe unsafe;

    static
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
            if (unsafe.addressSize() > 8)
                throw new RuntimeException("Address size " + unsafe.addressSize() + " not supported yet (max 8 bytes)");
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    final long address;

    Uns(long size)
    {
        if (size <= 0)
            throw new IllegalArgumentException();
        try
        {
            this.address = unsafe.allocateMemory(size);
        }
        catch (OutOfMemoryError oom)
        {
            throw new OutOfOffHeapMemoryException(size);
        }
    }

    void free()
    {
        unsafe.freeMemory(address);
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

    static long getLong(long address)
    {
        if (address == 0L)
            throw new NullPointerException();
        return unsafe.getLong(null, address);
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

    static void initLock(long address)
    {
        initStamped(address);
    }

    static long lock(long address, LockMode lockMode)
    {
        switch (lockMode)
        {
            case READ:
                return lockStampedRead(address);
            case WRITE:
                return lockStampedWrite(address);
            case LONG_RUN:
                return lockStampedLongRun(address);
        }
        throw new Error();
    }

    static void unlock(long address, long stamp, LockMode lockMode)
    {
        switch (lockMode)
        {
            case READ:
                unlockStampedRead(address, stamp);
                return;
            case WRITE:
                unlockStampedWrite(address, stamp);
                return;
            case LONG_RUN:
                unlockStampedLongRun(address, stamp);
                return;
        }
        throw new Error();
    }

    static long allocate(long bytes)
    {
        try
        {
            return unsafe.allocateMemory(bytes);
        }
        catch (OutOfMemoryError oom)
        {
            throw new OutOfOffHeapMemoryException(bytes);
        }
    }

    static void free(long address)
    {
        if (address == 0L)
            throw new NullPointerException();
        unsafe.freeMemory(address);
    }

    //
    private static final int LG_READERS = 7;

    private static final long RUNIT = 1L;
    private static final long WBIT  = 1L << LG_READERS;
    private static final long LRBIT = WBIT << 1L;
    private static final long RBITS = WBIT - 1L;
    private static final long RFULL = RBITS - 1L;
    private static final long ABITS = RBITS | WBIT | LRBIT;
    private static final long SBITS = ~RBITS; // note overlap with ABITS
    private static final long ORIGIN = LRBIT << 1;

    static void initStamped(long address)
    {
        putLongVolatile(address, ORIGIN);
    }

    static long lockStampedRead(long address)
    {
        if (address == 0L)
            throw new NullPointerException();

        for (int spin=spinSeed();;spin++) {
            long s = getLongVolatile(address);
            long m = s & ABITS;
            long next;
            if (m < RFULL && compareAndSwap(address, s, next = s + RUNIT))
                return next;

            spinLock(spin, inRehash(s));
        }
    }

    static long lockStampedWrite(long address)
    {
        if (address == 0L)
            throw new NullPointerException();

        for (int spin=spinSeed();;spin++) {
            long s, next;
            if (((s = getLongVolatile(address)) & ABITS) == 0L &&
                compareAndSwap(address, s, next = s | WBIT))
                return next;

            spinLock(spin, inRehash(s));
        }
    }

    static long lockStampedLongRun(long address)
    {
        if (address == 0L)
            throw new NullPointerException();

        for (int spin=spinSeed();;spin++) {
            long s, next;
            if (((s = getLongVolatile(address)) & ABITS) == 0L &&
                compareAndSwap(address, s, next = s | LRBIT))
                return next;

            spinLock(spin, false);
        }
    }

    private static boolean inRehash(long s)
    {
        return (s & LRBIT)!=0;
    }

    private static int spinSeed()
    {
        return (int)Thread.currentThread().getId();
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
        for (;;) {
            if (((s = getLongVolatile(address)) & SBITS) != (stamp & SBITS) ||
                (stamp & ABITS) == 0L || (m = s & ABITS) == 0L || m == WBIT)
                throw new IllegalMonitorStateException();
            if (m < RFULL) {
                if (compareAndSwap(address, s, s - RUNIT))
                    break;
            }
        }
    }

    static void unlockStampedWrite(long address, long stamp)
    {
        if (address == 0L)
            throw new NullPointerException();

        if (getLongVolatile(address) != stamp || (stamp & WBIT) == 0L)
            throw new IllegalMonitorStateException();
        putLongVolatile(address,ORIGIN);
    }

    static void unlockStampedLongRun(long address, long stamp)
    {
        if (address == 0L)
            throw new NullPointerException();

        if (getLongVolatile(address) != stamp || (stamp & LRBIT) == 0L)
            throw new IllegalMonitorStateException();
        putLongVolatile(address,ORIGIN);
    }
}
