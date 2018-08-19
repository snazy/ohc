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
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.alloc.IAllocator;
import org.caffinitas.ohc.alloc.JNANativeAllocator;
import org.caffinitas.ohc.alloc.UnsafeAllocator;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

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
    private static final ConcurrentMap<Long, AllocInfo> ohDebug = __DEBUG_OFF_HEAP_MEMORY_ACCESS ? new ConcurrentHashMap<>(16384) : null;
    private static final Map<Long, Throwable> ohFreeDebug = __DEBUG_OFF_HEAP_MEMORY_ACCESS ? new ConcurrentHashMap<>(16384) : null;

    private static final class AllocInfo
    {
        final long size;
        final Throwable trace;

        AllocInfo(Long size, Throwable trace)
        {
            this.size = size;
            this.trace = trace;
        }
    }

    static void clearUnsDebugForTest()
    {
        if (__DEBUG_OFF_HEAP_MEMORY_ACCESS)
        {
            try
            {
                if (!ohDebug.isEmpty())
                {
                    for (Map.Entry<Long, AllocInfo> addrSize : ohDebug.entrySet())
                    {
                        System.err.printf("  still allocated: address=%d, size=%d%n", addrSize.getKey(), addrSize.getValue().size);
                        addrSize.getValue().trace.printStackTrace();
                    }
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
            AllocInfo allocInfo = ohDebug.remove(address);
            if (allocInfo == null)
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
            AllocInfo allocatedLen = ohDebug.putIfAbsent(address, new AllocInfo(bytes, new Exception("Thread: "+Thread.currentThread())));
            if (allocatedLen != null)
                throw new Error("Oops - allocate() got duplicate address");
            ohFreeDebug.remove(address);
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

    static void copyMemory(byte[] arr, int off, long address, long offset, long len)
    {
        unsafe.copyMemory(arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, null, address + offset, len);
    }

    static void copyMemory(long address, long offset, byte[] arr, int off, long len)
    {
        unsafe.copyMemory(null, address + offset, arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, len);
    }

    static long getTotalAllocated()
    {
        return allocator.getTotalAllocated();
    }

    static ByteBuffer allocate(long bytes, boolean throwOOME)
    {
        long address = allocator.allocate(bytes);
        if (address != 0L)
            allocated(address, bytes);
        else
        {
            if (throwOOME)
                throw new OutOfMemoryError("unable to allocate " + bytes + " in off-heap");
            return null;
        }
        return directBufferFor(address, bytes);
    }

    static void free(ByteBuffer buffer)
    {
        if (buffer == null || !DIRECT_BYTE_BUFFER_CLASS.isAssignableFrom(buffer.getClass()))
            return;

        long address = ((DirectBuffer) buffer).address();

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
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(0);
            Class<?> clazz = directBuffer.getClass();
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

    static ByteBuffer directBufferFor(long address, long len)
    {
        if (len > Integer.MAX_VALUE || len < 0L)
            throw new IllegalArgumentException();
        try
        {
            ByteBuffer bb = (ByteBuffer) unsafe.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
            unsafe.putLong(bb, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, address);
            unsafe.putInt(bb, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, (int) len);
            unsafe.putInt(bb, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, (int) len);
            bb.order(ByteOrder.BIG_ENDIAN);
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
