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

import java.util.Arrays;

/**
 * Encapsulates access to hash entries.
 */
final class HashEntries
{
    static void init(long hash, long keyLen, long valueLen, long hashEntryAdr)
    {
        Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_HASH, hash);
        Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_KEY_LENGTH, keyLen);
        Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_VALUE_LENGTH, valueLen);
        Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_REFCOUNT, 1L);
    }

    static boolean compareKey(long hashEntryAdr, org.caffinitas.ohc.tables.KeyBuffer key, long serKeyLen)
    {
        if (hashEntryAdr == 0L)
            return false;

        long blkOff = Util.ENTRY_OFF_DATA;
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

    static int getLRUIndex(long hashEntryAdr)
    {
        return Uns.getInt(hashEntryAdr, Util.ENTRY_OFF_LRU_INDEX);
    }

    static void setLRUIndex(long hashEntryAdr, int lruIndex)
    {
        Uns.putInt(hashEntryAdr, Util.ENTRY_OFF_LRU_INDEX, lruIndex);
    }

    static long getHash(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, Util.ENTRY_OFF_HASH);
    }

    static long getKeyLen(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, Util.ENTRY_OFF_KEY_LENGTH);
    }

    static long getValueLen(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, Util.ENTRY_OFF_VALUE_LENGTH);
    }

    static long getAllocLen(long address)
    {
        return Util.allocLen(getKeyLen(address), getValueLen(address));
    }

    static void reference(long hashEntryAdr)
    {
        Uns.increment(hashEntryAdr, Util.ENTRY_OFF_REFCOUNT);
    }

    static boolean dereference(long hashEntryAdr)
    {
        if (Uns.decrement(hashEntryAdr, Util.ENTRY_OFF_REFCOUNT))
        {
            HashEntries.free(hashEntryAdr, HashEntries.getAllocLen(hashEntryAdr));
            return true;
        }
        return false;
    }

    //
    // malloc() or free() are very expensive operations. Write heavy workloads can spend most CPU time
    // in system (OS). To reduce this, the following code implements a mem-buffer cache.
    // Each free'd hash entry is added to the memBuffers array and each allocation tries to reuse such a
    // cached mem-buffer.
    // "Eviction" is performed on a free() operation - the oldest mem-buffer is released.
    //
    // Using direct calls to malloc()/free() can consume up to 70% in OS (system CPU usage).
    //

    private static final boolean MEM_BUFFERS_ENABLE = Boolean.parseBoolean(System.getProperty("MEM_BUFFERS_ENABLE", "false"));

    private static final int BLOCK_BUFFERS = 512;
    private static final long[] memBuffers = new long[BLOCK_BUFFERS * 3];

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
        if (MEM_BUFFERS_ENABLE && bytes <= MAX_BUFFERED_SIZE)
        {
            long blockAllocLen = blockAllocLen(bytes);
            long adr = reuseMemBuffer(blockAllocLen);
            if (adr != 0L)
            {
                memBufferHit++;
                return adr;
            }

            memBufferMiss++;

            return Uns.allocate(blockAllocLen);
        }

        return Uns.allocate(bytes);
    }

    static void free(long address, long allocLen)
    {
        if (address == 0L)
            return;

        if (MEM_BUFFERS_ENABLE && allocLen <= MAX_BUFFERED_SIZE)
            address = releaseToMemBuffer(address, blockAllocLen(allocLen));

        Uns.free(address);
    }

    private static long reuseMemBuffer(long blockAllocLen)
    {
        synchronized (memBuffers)
        {
            for (int i = 0; i < memBuffers.length; i += 3)
            {
                long mbAdr = memBuffers[i];
                if (mbAdr != 0L && memBuffers[i + 1] == blockAllocLen)
                {
                    memBuffers[i] = 0L;
                    return mbAdr;
                }
            }

            return 0L;
        }
    }

    private static long releaseToMemBuffer(long address, long allocLen)
    {
        synchronized (memBuffers)
        {
            memBufferFree++;

            long blockAllocLen = blockAllocLen(allocLen);
            long least = Long.MAX_VALUE;
            int min = -1;
            for (int i = 0; i < memBuffers.length; i += 3)
            {
                if (memBuffers[i] == 0L)
                {
                    memBuffers[i] = address;
                    memBuffers[i + 1] = blockAllocLen;
                    memBuffers[i + 2] = System.currentTimeMillis();
                    return 0L;
                }
                else
                {
                    long ts = memBuffers[i + 2];
                    if (ts < least)
                    {
                        least = ts;
                        min = i;
                    }
                }
            }

            assert min != -1;

            memBufferExpires++;

            long freeAddress = memBuffers[min];

            memBuffers[min] = address;
            memBuffers[min + 1] = blockAllocLen;

            return freeAddress;
        }
    }

    static long blockAllocLen(long allocLen)
    {
        if ((allocLen & BLOCK_MASK) == 0L)
            return allocLen;

        return (allocLen & ~BLOCK_MASK) + BLOCK_SIZE;
    }

    static synchronized void memBufferClear()
    {
        synchronized (memBuffers)
        {
            memBufferClear++;

            for (int i = 0; i < memBuffers.length; i += 3)
                Uns.free(memBuffers[i]);

            Arrays.fill(memBuffers, 0L);
        }
    }
}
