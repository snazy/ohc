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
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    final long address;
    private final long last;

    Uns(long size)
    {
        address = unsafe.allocateMemory(size);
        last = address + size;
    }

    void validate(long address, int len)
    {
        if (address < this.address || address + len > last)
            throw new ArrayIndexOutOfBoundsException("Address " + address + "+" + len + " outside allowed range " + address + ".." + last);
    }

    void free()
    {
        unsafe.freeMemory(address);
    }

    void putLong(long address, long value)
    {
        unsafe.putLongVolatile(null, address, value);
    }

    void putLongVolatile(long address, long value)
    {
        validate(address, 8);
        unsafe.putLongVolatile(null, address, value);
    }

    static void putLongVolatile(Object obj, long offset, long value)
    {
        unsafe.putLongVolatile(obj, offset, value);
    }

    long getLongFromByteArray(byte[] array, int offset)
    {
        return unsafe.getLong(array, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset);
    }

    long getLong(long address)
    {
        validate(address, 8);
        return unsafe.getLong(null, address);
    }

    long getLongVolatile(long address)
    {
        validate(address, 8);
        return unsafe.getLongVolatile(null, address);
    }

    void putByte(long address, byte value)
    {
        validate(address, 1);
        unsafe.putByte(null, address, value);
    }

    byte getByte(long address)
    {
        validate(address, 1);
        return unsafe.getByte(null, address);
    }

    boolean compareAndSwap(long address, long expected, long value)
    {
        validate(address, 8);
        return unsafe.compareAndSwapLong(null, address, expected, value);
    }

    static boolean compareAndSwap(Object obj, long offset, long expected, long value)
    {
        return unsafe.compareAndSwapLong(obj, offset, expected, value);
    }

    void copyMemory(byte[] arr, int off, long address, int len)
    {
        validate(address, len);
        unsafe.copyMemory(arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, null, address, len);
    }

    void copyMemory(long address, byte[] arr, int off, int len)
    {
        validate(address, len);
        unsafe.copyMemory(null, address, arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, len);
    }

    void setMemory(long address, int len, byte val)
    {
        validate(address, len);
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

    boolean tryLock(long address)
    {
        return compareAndSwap(address, 0L, Thread.currentThread().getId());
    }

    int lock(long address)
    {
        long tid = Thread.currentThread().getId();
        for (int spin = 0; ; spin++)
        {
            if (compareAndSwap(address, 0L, tid))
                return spin;

            park(((spin & 3) + 1) * 5000);
        }
    }

    void unlock(long address)
    {
        putLongVolatile(address, 0L);
    }
}
