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
    static final int PARTITION_ENTRY_LEN = 16;

    // TODO Check whether a LRU-tail field has benefits regarding eviction and hash-partition locks during eviction.
    // Guess: no benefit if the hash-table is large (just a few entries in each hash-partition.

    private final int hashPartitionMask;
    private final long rootAddress;
    private final Uns uns;

    private final AtomicLong lockPartitionSpins = new AtomicLong();

    static int sizeForEntries(int hashTableSize)
    {
        return PARTITION_ENTRY_LEN * hashTableSize;
    }

    HashPartitionAccess(Uns uns, int hashTableSize, long rootAddress)
    {
        this.uns = uns;
        this.hashPartitionMask = hashTableSize - 1;
        this.rootAddress = rootAddress;

        // it's important to initialize the hash partition memory!
        // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
        uns.setMemory(rootAddress, sizeForEntries(hashTableSize), (byte) 0);
    }

    long partitionForHash(int hash)
    {
        int partition = hash & hashPartitionMask;
        return rootAddress + partition * PARTITION_ENTRY_LEN;
    }

    long lockPartitionForHash(int hash)
    {
        long partAdr = partitionForHash(hash);

        int spins = uns.lock(partAdr + OFF_LOCK);
        if (spins > 0)
            lockPartitionSpins.addAndGet(spins);

        return partAdr;
    }

    void unlockPartition(long partitionAdr)
    {
        if (partitionAdr != 0L)
            uns.unlock(partitionAdr + OFF_LOCK);
    }

    long getLRUHead(long partitionAdr)
    {
        if (partitionAdr == 0L)
            return 0L;
        return uns.getAddress(partitionAdr + OFF_LRU_HEAD);
    }

    void setLRUHead(long partitionAdr, long hashEntryAdr)
    {
        if (partitionAdr != 0L)
            uns.putAddress(partitionAdr + OFF_LRU_HEAD, hashEntryAdr);
    }

    long getLockPartitionSpins()
    {
        return lockPartitionSpins.get();
    }
}
