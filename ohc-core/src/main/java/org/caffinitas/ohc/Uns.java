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

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.caffinitas.ohc.alloc.IAllocator;
import org.caffinitas.ohc.alloc.JEMallocAllocator;
import org.caffinitas.ohc.alloc.NativeAllocator;
import sun.misc.Unsafe;

final class Uns
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Uns.class);

    private static final Unsafe unsafe;
    private static final IAllocator allocator;

    private static final boolean __DEBUG_OFF_HEAP_MEMORY_ACCESS = Boolean.parseBoolean(System.getProperty("DEBUG_OFF_HEAP_MEMORY_ACCESS", "false"));
    private static final boolean __DISABLE_JEMALLOC = Boolean.parseBoolean(System.getProperty("DISABLE_JEMALLOC", "false"));

    //
    // #ifdef __DEBUG_OFF_HEAP_MEMORY_ACCESS
    //
    private static final ConcurrentMap<Long, Long> ohDebug = __DEBUG_OFF_HEAP_MEMORY_ACCESS ? new ConcurrentHashMap<Long, Long>(16384) : null;
    private static final Map<Long, Throwable> ohFreeDebug = __DEBUG_OFF_HEAP_MEMORY_ACCESS ? new ConcurrentHashMap<Long, Throwable>(16384) : null;

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

    static
    {
        try
        {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe.addressSize() > 8)
                throw new RuntimeException("Address size " + unsafe.addressSize() + " not supported yet (max 8 bytes)");

            IAllocator alloc = null;
            if (!__DISABLE_JEMALLOC)
                try
                {
                    alloc = new JEMallocAllocator();
                }
                catch (Throwable t)
                {
                    LOGGER.warn("jemalloc native library not found (" + t + ") - use jemalloc for better off-heap cache performance");
                }
            if (alloc == null)
                alloc = new NativeAllocator();
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

    static long getAndPutLong(long address, long offset, long value)
    {
        validate(address, offset, 8L);

        // TODO replace with Java8 Unsafe.getAndSetLong()
        // return unsafe.getAndSetLong(null, address + offset, value);

        long r = unsafe.getLong(null, address + offset);
        unsafe.putLong(null, address + offset, value);
        return r;
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
        // increment + decrement are used in concurrent contexts - so Java8 Unsafe.getAndAddLong is not applicable

        validate(address, offset, 8L);
        address += offset;
        long v;
        while (true)
        {
            v = unsafe.getLongVolatile(null, address);
            if (v == 0)
                throw new IllegalStateException("Must not decrement 0");
            if (unsafe.compareAndSwapLong(null, address, v, v - 1))
                return v == 1;
            unsafe.park(false, 1000);
        }
    }

    static void increment(long address, long offset)
    {
        // increment + decrement are used in concurrent contexts - so Java8 Unsafe.getAndAddLong is not applicable

        validate(address, offset, 8L);
        address += offset;
        long v;
        do
        {
            v = unsafe.getLongVolatile(null, address);
        } while (!unsafe.compareAndSwapLong(null, address, v, v + 1));
    }

    static void copyMemory(byte[] arr, int off, long address, long offset, long len)
    {
        validate(address, offset, len);
        unsafe.copyMemory(arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, null, address + offset, len);
    }

    static void copyMemory(long address, long offset, byte[] arr, int off, long len)
    {
        validate(address, offset, len);
        unsafe.copyMemory(null, address + offset, arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, len);
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
        long address = allocator.allocate(bytes);
        allocated(address, bytes);
        return address > 0L ? address : 0L;
    }

    static long allocateIOException(long bytes) throws IOException
    {
        long address = allocate(bytes);
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

    private static final MethodHandle directByteBufferHandle;
    private static final Field byteBufferNativeByteOrder;

    static
    {
        try
        {
            Constructor ctor = Class.forName("java.nio.DirectByteBuffer")
                                    .getDeclaredConstructor(long.class, int.class, Object.class);
            ctor.setAccessible(true);

            byteBufferNativeByteOrder = ByteBuffer.class.getDeclaredField("nativeByteOrder");
            byteBufferNativeByteOrder.setAccessible(true);

            directByteBufferHandle = MethodHandles.lookup().unreflectConstructor(ctor);
        }
        catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | NoSuchFieldException e)
        {
            throw new RuntimeException(e);
        }
    }

    static ByteBuffer directBufferFor(long address, long offset, long len)
    {
        try
        {
            ByteBuffer bb = (ByteBuffer) directByteBufferHandle.invoke(address + offset, (int) len, null);
            byteBufferNativeByteOrder.setBoolean(bb, true);
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
