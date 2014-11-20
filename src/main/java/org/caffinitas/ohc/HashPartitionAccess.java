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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Encapsulates access to hash partitions.
 */
final class HashPartitionAccess
{
    // reference to the last-referenced hash entry (for LRU)
    static final long OFF_LRU_HEAD = 0L;
    // offset of CAS style lock field
    static final long OFF_LOCK = 8L;
    // total memory required for a hash-partition
    static final long PARTITION_ENTRY_LEN = 16L;

    private final int hashPartitionMask;
    private final long rootAddress;

    private final AtomicLong lockPartitionSpins = new AtomicLong();

    static long sizeForEntries(int hashTableSize)
    {
        return PARTITION_ENTRY_LEN * hashTableSize;
    }

    HashPartitionAccess(int hashTableSize, long rootAddress)
    {
        this.hashPartitionMask = hashTableSize - 1;
        this.rootAddress = rootAddress;

        // it's important to initialize the hash partition memory!
        // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
        Uns.setMemory(rootAddress, sizeForEntries(hashTableSize), (byte) 0);
    }

    long partitionForHash(int hash)
    {
        int partition = hash & hashPartitionMask;
        return rootAddress + partition * PARTITION_ENTRY_LEN;
    }

    long lockPartitionForHash(int hash)
    {
        long partAdr = partitionForHash(hash);

        long tid = Thread.currentThread().getId();
        for (int spin= 0;;spin++)
        {
            if (Uns.compareAndSwap(partAdr + OFF_LOCK, 0L, tid))
                return partAdr;

            lockPartitionSpins.incrementAndGet();
            Uns.park(((spin & 3) +1 ) * 5000);
        }
    }

    void unlockPartition(long partitionAdr)
    {
        if (partitionAdr != 0L)
            Uns.putLong(partitionAdr + OFF_LOCK, 0L);
    }

    long getLRUHead(long partitionAdr)
    {
        if (partitionAdr == 0L)
            return 0L;
        return Uns.getLong(partitionAdr + OFF_LRU_HEAD);
    }

    public void setLRUHead(long partitionAdr, long hashEntryAdr)
    {
        if (partitionAdr != 0L)
            Uns.putLong(partitionAdr + OFF_LRU_HEAD, hashEntryAdr);
    }

    long getLockPartitionSpins()
    {
        return lockPartitionSpins.get();
    }
}
