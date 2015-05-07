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

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.alloc.IAllocator;
import org.caffinitas.ohc.alloc.JNANativeAllocator;
import org.caffinitas.ohc.alloc.UnsafeAllocator;
import sun.misc.Unsafe;

final class Uns
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Uns.class);

    private static final Unsafe unsafe;
    private static final IAllocator allocator;

    private static final boolean __DEBUG_OFF_HEAP_MEMORY_ACCESS = Boolean.parseBoolean(System.getProperty(OHCacheBuilder.SYSTEM_PROPERTY_PREFIX + "debugOffHeapAccess", "false"));
    private static final String __ALLOCATOR = System.getProperty(OHCacheBuilder.SYSTEM_PROPERTY_PREFIX + "allocator");

    //
    // #ifdef __DEBUG_OFF_HEAP_MEMORY_ACCESS
    //
    private static final ConcurrentMap<Long, Long> ohDebug = __DEBUG_OFF_HEAP_MEMORY_ACCESS ? new ConcurrentHashMap<Long, Long>(16384) : null;
    private static final Map<Long, Throwable> ohFreeDebug = __DEBUG_OFF_HEAP_MEMORY_ACCESS ? new ConcurrentHashMap<Long, Throwable>(16384) : null;

    static void clearUnsDebugForTest()
    {
        if (__DEBUG_OFF_HEAP_MEMORY_ACCESS)
        {
            try
            {
                if (!ohDebug.isEmpty())
                {
                    for (Map.Entry<Long, Long> addrSize : ohDebug.entrySet())
                        System.err.printf("  still allocated: address=%d, size=%d%n", addrSize.getKey(), addrSize.getValue());
                    throw new RuntimeException("Not all allocated memory has been freed!");
                }
            }
            finally
            {
                ohDebug.clear();
                ohFreeDebug.clear();
            }
        }
    }

    private static void freed(long address)
    {
        if (__DEBUG_OFF_HEAP_MEMORY_ACCESS)
        {
            Long allocatedLen = ohDebug.remove(address);
            if (allocatedLen == null)
            {
                Throwable freedAt = ohFreeDebug.get(address);
                throw new IllegalStateException("Free of unallocated region " + address, freedAt);
            }
            ohFreeDebug.put(address, new Exception("free backtrace - t=" + System.nanoTime()));
        }
    }

    private static void allocated(long address, long bytes)
    {
        if (__DEBUG_OFF_HEAP_MEMORY_ACCESS)
        {
            Long allocatedLen = ohDebug.putIfAbsent(address, bytes);
            if (allocatedLen != null)
                throw new Error("Oops - allocate() got duplicate address");
            ohFreeDebug.remove(address);
        }
    }

    private static void validate(long address, long offset, long len)
    {
        if (__DEBUG_OFF_HEAP_MEMORY_ACCESS)
        {
            if (address == 0L)
                throw new NullPointerException();
            Long allocatedLen = ohDebug.get(address);
            if (allocatedLen == null)
            {
                Throwable freedAt = ohFreeDebug.get(address);
                throw new IllegalStateException("Access to unallocated region " + address + " - t=" + System.nanoTime(), freedAt);
            }
            if (offset < 0L)
                throw new IllegalArgumentException("Negative offset");
            if (len < 0L)
                throw new IllegalArgumentException("Negative length");
            if (offset + len > allocatedLen)
                throw new IllegalArgumentException("Access outside allocated region");
        }
    }
    //
    // #endif
    //

    private static final UnsExt ext;

    static
    {
        try
        {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe.addressSize() > 8)
                throw new RuntimeException("Address size " + unsafe.addressSize() + " not supported yet (max 8 bytes)");

            String javaVersion = System.getProperty("java.version");
            StringTokenizer st = new StringTokenizer(javaVersion, ".");
            int major = Integer.parseInt(st.nextToken());
            int minor = Integer.parseInt(st.nextToken());
            UnsExt e;
            if (major > 1 || minor >= 8)
                try
                {
                    // use new Java8 methods in sun.misc.Unsafe
                    Class<? extends UnsExt> cls = (Class<? extends UnsExt>) Class.forName(UnsExt7.class.getName().replace('7', '8'));
                    e = cls.getDeclaredConstructor(Class.class).newInstance(unsafe);
                    LOGGER.info("OHC using Java8 Unsafe API");
                }
                catch (VirtualMachineError ex)
                {
                    throw ex;
                }
                catch (Throwable ex)
                {
                    LOGGER.warn("Failed to load Java8 implementation ohc-core-j8 : " + ex);
                    e = new UnsExt7(unsafe);
                }
            else
                e = new UnsExt7(unsafe);
            ext = e;

            if (__DEBUG_OFF_HEAP_MEMORY_ACCESS)
                LOGGER.warn("Degraded performance due to off-heap memory allocations and access guarded by debug code enabled via system property " + OHCacheBuilder.SYSTEM_PROPERTY_PREFIX + "debugOffHeapAccess=true");

            IAllocator alloc;
            String allocType = __ALLOCATOR != null ? __ALLOCATOR : "jna";
            switch (allocType)
            {
                case "unsafe":
                    alloc = new UnsafeAllocator();
                    LOGGER.info("OHC using sun.misc.Unsafe memory allocation");
                    break;
                case "jna":
                default:
                    alloc = new JNANativeAllocator();
                    LOGGER.info("OHC using JNA OS native malloc/free");
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

    static long getLongFromByteArray(byte[] array, int offset)
    {
        if (offset < 0 || offset + 8 > array.length)
            throw new ArrayIndexOutOfBoundsException();
        return unsafe.getLong(array, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset);
    }

    static int getIntFromByteArray(byte[] array, int offset)
    {
        if (offset < 0 || offset + 4 > array.length)
            throw new ArrayIndexOutOfBoundsException();
        return unsafe.getInt(array, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset);
    }

    static short getShortFromByteArray(byte[] array, int offset)
    {
        if (offset < 0 || offset + 2 > array.length)
            throw new ArrayIndexOutOfBoundsException();
        return unsafe.getShort(array, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset);
    }

    static long getAndPutLong(long address, long offset, long value)
    {
        validate(address, offset, 8L);

        return ext.getAndPutLong(address, offset, value);
    }

    public static boolean compareAndSwapLong(long address, long offset, long expect, long value)
    {
        validate(address, offset, 8L);
        return unsafe.compareAndSwapLong(null, address + offset, expect, value);
    }

    static void putLong(long address, long offset, long value)
    {
        validate(address, offset, 8L);
        unsafe.putLong(null, address + offset, value);
    }

    static long getLong(long address, long offset)
    {
        validate(address, offset, 8L);
        return unsafe.getLong(null, address + offset);
    }

    static void putInt(long address, long offset, int value)
    {
        validate(address, offset, 4L);
        unsafe.putInt(null, address + offset, value);
    }

    static int getInt(long address, long offset)
    {
        validate(address, offset, 4L);
        return unsafe.getInt(null, address + offset);
    }

    static void putShort(long address, long offset, short value)
    {
        validate(address, offset, 2L);
        unsafe.putShort(null, address + offset, value);
    }

    static short getShort(long address, long offset)
    {
        validate(address, offset, 2L);
        return unsafe.getShort(null, address + offset);
    }

    static void putByte(long address, long offset, byte value)
    {
        validate(address, offset, 1L);
        unsafe.putByte(null, address + offset, value);
    }

    static byte getByte(long address, long offset)
    {
        validate(address, offset, 1L);
        return unsafe.getByte(null, address + offset);
    }

    static void putBoolean(long address, long offset, boolean value)
    {
        validate(address, offset, 1L);
        unsafe.putBoolean(null, address + offset, value);
    }

    static boolean getBoolean(long address, long offset)
    {
        validate(address, offset, 1L);
        return unsafe.getBoolean(null, address + offset);
    }

    static void putChar(long address, long offset, char value)
    {
        validate(address, offset, 2L);
        unsafe.putChar(null, address + offset, value);
    }

    static char getChar(long address, long offset)
    {
        validate(address, offset, 2L);
        return unsafe.getChar(null, address + offset);
    }

    static void putFloat(long address, long offset, float value)
    {
        validate(address, offset, 4L);
        unsafe.putFloat(null, address + offset, value);
    }

    static float getFloat(long address, long offset)
    {
        validate(address, offset, 4L);
        return unsafe.getFloat(null, address + offset);
    }

    static void putDouble(long address, long offset, double value)
    {
        validate(address, offset, 8L);
        unsafe.putDouble(null, address + offset, value);
    }

    static double getDouble(long address, long offset)
    {
        validate(address, offset, 8L);
        return unsafe.getDouble(null, address + offset);
    }

    static boolean decrement(long address, long offset)
    {
        validate(address, offset, 8L);
        long v = ext.getAndAddInt(address, offset, -1);
        return v == 1;
    }

    static void increment(long address, long offset)
    {
        validate(address, offset, 8L);
        ext.getAndAddInt(address, offset, 1);
    }

    static void copyMemory(long srcAddress, long srcOffset, long dstAddress, long dstOffset, long len)
    {
        validate(srcAddress, srcOffset, len);
        validate(dstAddress, dstOffset, len);
        unsafe.copyMemory(null, srcAddress + srcOffset, null, dstAddress + dstOffset, len);
    }

    static void copyMemory(byte[] arr, int off, long dstAddress, long dstOffset, long len)
    {
        validate(dstAddress, dstOffset, len);
        unsafe.copyMemory(arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, null, dstAddress + dstOffset, len);
    }

    static void copyMemory(long srcAddress, long srcOffset, byte[] arr, int off, long len)
    {
        validate(srcAddress, srcOffset, len);
        unsafe.copyMemory(null, srcAddress + srcOffset, arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, len);
    }

    static void setMemory(long address, long offset, long len, byte val)
    {
        validate(address, offset, len);
        unsafe.setMemory(address + offset, len, val);
    }

    static long getTotalAllocated()
    {
        return allocator.getTotalAllocated();
    }

    static long allocate(long bytes)
    {
        return allocate(bytes, false);
    }

    static long allocate(long bytes, boolean throwOOME)
    {
        long address = allocator.allocate(bytes);
        if (address != 0L)
            allocated(address, bytes);
        else if (throwOOME)
            throw new OutOfMemoryError("unable to allocate " + bytes + " in off-heap");
        return address;
    }

    static long allocateIOException(long bytes) throws IOException
    {
        return allocateIOException(bytes, false);
    }

    static long allocateIOException(long bytes, boolean throwOOME) throws IOException
    {
        long address = allocate(bytes, throwOOME);
        if (address == 0L)
            throw new IOException("unable to allocate " + bytes + " in off-heap");
        return address;
    }

    static void free(long address)
    {
        if (address == 0L)
            return;
        freed(address);
        allocator.free(address);
    }

    private static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
    private static final long DIRECT_BYTE_BUFFER_ADDRESS_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_CAPACITY_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_LIMIT_OFFSET;

    static
    {
        try
        {
            Class<?> clazz = ByteBuffer.allocateDirect(0).getClass();
            DIRECT_BYTE_BUFFER_ADDRESS_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            DIRECT_BYTE_BUFFER_CAPACITY_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));
            DIRECT_BYTE_BUFFER_LIMIT_OFFSET = unsafe.objectFieldOffset(Buffer.class.getDeclaredField("limit"));
            DIRECT_BYTE_BUFFER_CLASS = clazz;
        }
        catch (NoSuchFieldException e)
        {
            throw new RuntimeException(e);
        }
    }

    static ByteBuffer directBufferFor(long address, long offset, long len)
    {
        if (len > Integer.MAX_VALUE || len < 0L)
            throw new IllegalArgumentException();
        try
        {
            ByteBuffer bb = (ByteBuffer) unsafe.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
            unsafe.putLong(bb, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, address + offset);
            unsafe.putInt(bb, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, (int) len);
            unsafe.putInt(bb, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, (int) len);
            bb.order(ByteOrder.nativeOrder());
            return bb;
        }
        catch (Error e)
        {
            throw e;
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }
}
