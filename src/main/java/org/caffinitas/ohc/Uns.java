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

    static long allocate(long size)
    {
        return unsafe.allocateMemory(size);
    }

    static void free(long address)
    {
        unsafe.freeMemory(address);
    }

    static void putLong(long address, long value)
    {
        unsafe.putLongVolatile(null, address, value);
    }

    static void putLong(Object obj, long offset, long value)
    {
        unsafe.putLongVolatile(obj, offset, value);
    }

    static long getLong(long address)
    {
        return unsafe.getLongVolatile(null, address);
    }

    static void putByte(long address, byte value)
    {
        unsafe.putByteVolatile(null, address, value);
    }

    static byte getByte(long address)
    {
        return unsafe.getByteVolatile(null, address);
    }

    static boolean compareAndSwap(long address, long expected, long value)
    {
        return unsafe.compareAndSwapLong(null, address, expected, value);
    }

    static boolean compareAndSwap(Object obj, long offset, long expected, long value)
    {
        return unsafe.compareAndSwapLong(obj, offset, expected, value);
    }

    static void copyMemory(byte[] arr, int off, long address, int len)
    {
        unsafe.copyMemory(arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, null, address, len);
    }

    static void setMemory(long address, long len, byte val)
    {
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

    private Uns()
    {
    }
}
