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

import java.util.Arrays;

import static org.caffinitas.ohc.Util.ENTRY_OFF_DATA;
import static org.caffinitas.ohc.Util.ENTRY_OFF_HASH;
import static org.caffinitas.ohc.Util.ENTRY_OFF_KEY_LENGTH;
import static org.caffinitas.ohc.Util.ENTRY_OFF_LRU_NEXT;
import static org.caffinitas.ohc.Util.ENTRY_OFF_LRU_PREV;
import static org.caffinitas.ohc.Util.ENTRY_OFF_NEXT;
import static org.caffinitas.ohc.Util.ENTRY_OFF_REFCOUNT;
import static org.caffinitas.ohc.Util.ENTRY_OFF_VALUE_LENGTH;
import static org.caffinitas.ohc.Util.allocLen;

/**
 * Encapsulates access to hash entries.
 */
public final class HashEntries
{
    static void init(long hash, long keyLen, long valueLen, long hashEntryAdr)
    {
        Uns.putLong(hashEntryAdr, ENTRY_OFF_HASH, hash);
        setNext(hashEntryAdr, 0L);
        Uns.putLong(hashEntryAdr, ENTRY_OFF_KEY_LENGTH, keyLen);
        Uns.putLong(hashEntryAdr, ENTRY_OFF_VALUE_LENGTH, valueLen);
        Uns.putLong(hashEntryAdr, ENTRY_OFF_REFCOUNT, 1L);
    }

    static boolean compareKey(long hashEntryAdr, KeyBuffer key, long serKeyLen)
    {
        if (hashEntryAdr == 0L)
            return false;

        long blkOff = ENTRY_OFF_DATA;
        int p = 0;
        byte[] arr = key.array();
        for (; p <= serKeyLen - 8; p += 8, blkOff += 8)
            if (Uns.getLong(hashEntryAdr, blkOff) != Uns.getLongFromByteArray(arr, p))
                return false;
        for (; p < serKeyLen; p++, blkOff++)
            if (Uns.getByte(hashEntryAdr, blkOff) != arr[p])
                return false;

        return true;
    }

    static boolean compare(long hashEntryAdr, long offset, long otherHashEntryAdr, long otherOffset, long len)
    {
        if (hashEntryAdr == 0L)
            return false;

        int p = 0;
        for (; p <= len - 8; p += 8, offset += 8, otherOffset += 8)
            if (Uns.getLong(hashEntryAdr, offset) != Uns.getLong(otherHashEntryAdr, otherOffset))
                return false;
        for (; p < len; p++, offset++, otherOffset++)
            if (Uns.getByte(hashEntryAdr, offset) != Uns.getByte(otherHashEntryAdr, otherOffset))
                return false;

        return true;
    }

    public static long getLRUNext(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, ENTRY_OFF_LRU_NEXT);
    }

    public static void setLRUNext(long hashEntryAdr, long replacement)
    {
        Uns.putLong(hashEntryAdr, ENTRY_OFF_LRU_NEXT, replacement);
    }

    public static long getLRUPrev(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, ENTRY_OFF_LRU_PREV);
    }

    public static void setLRUPrev(long hashEntryAdr, long replacement)
    {
        Uns.putLong(hashEntryAdr, ENTRY_OFF_LRU_PREV, replacement);
    }

    static long getHash(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, ENTRY_OFF_HASH);
    }

    static long getNext(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLong(hashEntryAdr, ENTRY_OFF_NEXT) : 0L;
    }

    static void setNext(long hashEntryAdr, long nextAdr)
    {
        if (hashEntryAdr == nextAdr)
            throw new IllegalArgumentException();
        if (hashEntryAdr != 0L)
            Uns.putLong(hashEntryAdr, ENTRY_OFF_NEXT, nextAdr);
    }

    static long getKeyLen(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, ENTRY_OFF_KEY_LENGTH);
    }

    static long getValueLen(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, ENTRY_OFF_VALUE_LENGTH);
    }

    static long getAllocLen(long address)
    {
        return allocLen(getKeyLen(address), getValueLen(address));
    }

    static void reference(long hashEntryAdr)
    {
        Uns.increment(hashEntryAdr, ENTRY_OFF_REFCOUNT);
    }

    static boolean dereference(long hashEntryAdr)
    {
        return Uns.decrement(hashEntryAdr, ENTRY_OFF_REFCOUNT);
    }

    //
    // malloc() or free() are very expensive operations. Write heavy workloads can consume most CPU time
    // (system CPU usage). To reduce this effort, the following code implements a mem-buffer cache.
    // Each free'd hash entry is added to the memBuffer array and each allocation tries to reuse such a
    // cached mem-buffer.
    //

    private static final int BLOCK_BUFFERS = 512;
    private static final long[] memBuffers = new long[BLOCK_BUFFERS * 2];

    static final long BLOCK_SIZE = 16384L;
    static final long BLOCK_MASK = BLOCK_SIZE - 1L;
    private static final long MAX_BUFFERED_SIZE = 8L * 1024 * 1024;

    static long memBufferHit;
    static long memBufferMiss;
    static long memBufferFree;
    static long memBufferExpires;
    static long memBufferClear;

    static long allocate(long bytes)
    {
        if (bytes <= MAX_BUFFERED_SIZE)
        {
            long blockAllocLen = blockAllocLen(bytes);
            long adr = memBufferAllocate(blockAllocLen);
            return adr != 0L ? adr : Uns.allocate(blockAllocLen);
        }

        return Uns.allocate(bytes);
    }

    static void free(long address, long allocLen)
    {
        if (address == 0L)
            return;

        if (allocLen <= MAX_BUFFERED_SIZE)
        {
            memBufferFree(address, allocLen);
            return;
        }

        Uns.free(address);
    }

    static long blockAllocLen(long allocLen)
    {
        if ((allocLen & BLOCK_MASK) == 0L)
            return allocLen;

        return (allocLen & ~BLOCK_MASK) + BLOCK_SIZE;
    }

    private static synchronized long memBufferAllocate(long blockAllocLen)
    {
        for (int i = 0; i < BLOCK_BUFFERS * 2; i += 2)
        {
            long mbAdr = memBuffers[i];
            if (mbAdr != 0L && memBuffers[i + 1] == blockAllocLen)
            {
                memBufferHit ++;
                memBuffers[i] = 0L;
                return mbAdr;
            }
        }

        memBufferMiss ++;

        return 0L;
    }

    private static synchronized void memBufferFree(long address, long allocLen)
    {
        memBufferFree++;

        long blockAllocLen = blockAllocLen(allocLen);
        long least = Long.MAX_VALUE;
        int min = -1;
        for (int i = 0; i < BLOCK_BUFFERS * 2; i += 2)
        {
            if (memBuffers[i] == 0L)
            {
                memBuffers[i] = address;
                memBuffers[i + 1] = blockAllocLen;
                Uns.putLong(address, 0L, System.currentTimeMillis());
                return;
            }
            else
            {
                long ts = Uns.getLong(memBuffers[i], 0L);
                if (ts < least)
                {
                    least = ts;
                    min = i;
                }
            }
        }

        assert min != -1;

        memBufferExpires++;

        Uns.free(memBuffers[min]);
        memBuffers[min] = address;
        memBuffers[min + 1] = blockAllocLen;
    }

    static synchronized void memBufferClear()
    {
        memBufferClear++;

        for (int i = 0; i < BLOCK_BUFFERS * 2; i += 2)
            Uns.free(memBuffers[i]);

        Arrays.fill(memBuffers, 0L);
    }
}
