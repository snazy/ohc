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
package org.caffinitas.ohc.segment;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.caffinitas.ohc.alloc.IAllocator;
import org.caffinitas.ohc.alloc.JEMallocAllocator;
import org.caffinitas.ohc.alloc.NativeAllocator;
import sun.misc.Unsafe;

final class Uns
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Uns.class);

    static final Unsafe unsafe;
    private static final IAllocator allocator;

    private static final boolean __DEBUG_OFF_HEAP_MEMORY_ACCESS = Boolean.parseBoolean(System.getProperty("DEBUG_OFF_HEAP_MEMORY_ACCESS", "false"));

    //
    // #ifdef __DEBUG_OFF_HEAP_MEMORY_ACCESS
    //
    private static final Map<Long, Long> ohDebug = __DEBUG_OFF_HEAP_MEMORY_ACCESS ? new ConcurrentHashMap<Long, Long>(16384) : null;
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
            ohFreeDebug.put(address, new Exception("free backtrace"));
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
            Long allocatedLen = ohDebug.get(address);
            if (allocatedLen == null)
            {
                Throwable freedAt = ohFreeDebug.get(address);
                throw new IllegalStateException("Access to unallocated region " + address, freedAt);
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

            IAllocator alloc;
            try
            {
                alloc = new JEMallocAllocator();
            }
            catch (Throwable t)
            {
                LOGGER.warn("jemalloc native library not found (" + t + ") - use jemalloc for better off-heap cache performance");
                alloc = new NativeAllocator();
            }
            allocator = alloc;
//            allocator = new NativeAllocator();
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    private Uns()
    {
    }

    static void putLongVolatile(long address, long offset, long value)
    {
        if (address == 0L)
            throw new NullPointerException();
        validate(address, offset, 8L);
        unsafe.putLongVolatile(null, address + offset, value);
    }

    static long getLongFromByteArray(byte[] array, int offset)
    {
        if (array == null)
            throw new NullPointerException();
        if (offset < 0 || offset > array.length - 8)
            throw new ArrayIndexOutOfBoundsException();
        return unsafe.getLong(array, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset);
    }

    static long getLong(long address, long offset)
    {
        if (address == 0L)
            throw new NullPointerException();
        validate(address, offset, 8L);
        return unsafe.getLong(null, address + offset);
    }

    static long getLongVolatile(long address, long offset)
    {
        if (address == 0L)
            throw new NullPointerException();
        validate(address, offset, 8L);
        return unsafe.getLongVolatile(null, address + offset);
    }

    static void putByte(long address, long offset, byte value)
    {
        if (address == 0L)
            throw new NullPointerException();
        validate(address, offset, 1L);
        unsafe.putByte(null, address + offset, value);
    }

    static byte getByte(long address, long offset)
    {
        if (address == 0L)
            throw new NullPointerException();
        validate(address, offset, 1L);
        return unsafe.getByte(null, address + offset);
    }

    static void decrement(long address, long offset)
    {
        if (address == 0L)
            throw new NullPointerException();
        validate(address, offset, 8L);
        address += offset;
        for (int spin = -1; ; )
        {
            long v = unsafe.getLongVolatile(null, address);
            if (v == 0)
                throw new IllegalStateException("Must not decrement 0");
            if (unsafe.compareAndSwapLong(null, address, v, v - 1))
                return;

            spin = spinLock(spin);
        }
    }

    static void increment(long address, long offset)
    {
        if (address == 0L)
            throw new NullPointerException();
        validate(address, offset, 8L);
        address += offset;
        for (int spin = -1; ; )
        {
            long v = unsafe.getLongVolatile(null, address);
            if (unsafe.compareAndSwapLong(null, address, v, v + 1))
                return;

            spin = spinLock(spin);
        }
    }

    static void awaitValue(long address, long offset, long value)
    {
        if (address == 0L)
            throw new NullPointerException();
        validate(address, offset, 8L);
        address += offset;
        for (int spin = -1; ; )
        {
            if (unsafe.getLongVolatile(null, address) == value)
                return;

            spin = spinLock(spin);
        }
    }

    private static int spinLock(int spin)
    {
        if (spin == -1)
            spin = (int) Thread.currentThread().getId();
        // spin in 50ms units during rehash and 5us else
        park(((spin & 3) + 1) * 5000);
        return spin + 1;
    }

    static boolean compareAndSwap(long address, long offset, long expected, long value)
    {
        if (address == 0L)
            throw new NullPointerException();
        validate(address, offset, 8L);
        return unsafe.compareAndSwapLong(null, address +  offset, expected, value);
    }

    static void copyMemory(byte[] arr, int off, long address, long offset, long len)
    {
        if (arr == null)
            throw new NullPointerException();
        if (address == 0L)
            throw new NullPointerException();
        validate(address, offset, len);
        unsafe.copyMemory(arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, null, address + offset, len);
    }

    static void copyMemory(long address, long offset, byte[] arr, int off, long len)
    {
        if (arr == null)
            throw new NullPointerException();
        if (address == 0L)
            throw new NullPointerException();
        validate(address, offset, len);
        unsafe.copyMemory(null, address + offset, arr, Unsafe.ARRAY_BYTE_BASE_OFFSET + off, len);
    }

    static void setMemory(long address, long offset, long len, byte val)
    {
        if (address == 0L)
            throw new NullPointerException();
        validate(address, offset, len);
        unsafe.setMemory(address + offset, len, val);
    }

    static void park(long nanos)
    {
        unsafe.park(false, nanos);
    }

    static long allocate(long bytes)
    {
        if (bytes <= 0)
            throw new IllegalArgumentException();

        // TODO any chance to pin the memory to RAM (never swap to disk) ?

        long address = allocator.allocate(bytes);
        allocated(address, bytes);
        return address > 0L ? address : 0L;
    }

    static void free(long address)
    {
        if (address == 0L)
            throw new NullPointerException();
        freed(address);
        allocator.free(address);
    }

    static final long[] asyncFree = new long[1024];
    static final AtomicInteger asyncFreeIndex = new AtomicInteger();

    static void asyncFree(long address)
    {
        if (address == 0L)
            throw new NullPointerException();

        // If the async-free-list has room (and is not locked), just add the address to free().
        // Otherwise fall through to immediate free().
//        if (asyncFreeIndex.get() < asyncFree.length - 1)
//        {
//            int idx = asyncFreeIndex.getAndIncrement();
//            if (idx < asyncFree.length)
//            {
//                asyncFree[idx] = address;
//                return;
//            }
//        }

        free(address);
    }

    static void processOutstandingFree()
    {
        // This is basically a locked copy of the array containing the addresses to free()
        asyncFreeIndex.set(asyncFree.length);
        long[] lst = asyncFree.clone();
        Arrays.fill(asyncFree, 0L);
        asyncFreeIndex.set(0);

        for (long adr : lst)
            if (adr != 0L)
                free(adr);
    }
}
