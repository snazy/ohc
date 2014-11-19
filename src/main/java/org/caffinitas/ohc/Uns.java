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
        unsafe.putLong(address, value);
    }

    static long getLong(long address)
    {
        return unsafe.getLong(address);
    }

    static void putByte(long address, byte value)
    {
        unsafe.putByte(address, value);
    }

    static byte getByte(long address)
    {
        return unsafe.getByte(address);
    }

    static boolean compareAndSwap(long address, long expected, long value)
    {
        return unsafe.compareAndSwapLong(null, address, expected, value);
    }

    private Uns()
    {
    }
}
