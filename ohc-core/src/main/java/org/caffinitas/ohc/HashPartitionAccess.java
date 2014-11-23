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

import java.util.concurrent.atomic.AtomicInteger;
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

    private volatile int hashPartitionMask;
    private volatile Uns unsMain;

    private volatile int hashPartitionMaskAlt;
    private volatile Uns unsAlt;

    // all hash partitions ins 'unsMain' smaller than this value must be looked up in 'unsAlt'
    private final AtomicInteger rehashSplit = new AtomicInteger();

    private final AtomicLong lockPartitionSpins = new AtomicLong();

    static int sizeForEntries(int hashTableSize)
    {
        return PARTITION_ENTRY_LEN * hashTableSize;
    }

    HashPartitionAccess(int hashTableSize)
    {
        this.unsMain = new Uns((long) hashTableSize * PARTITION_ENTRY_LEN, PARTITION_ENTRY_LEN);
        this.hashPartitionMask = hashTableSize - 1;

        // it's important to initialize the hash partition memory!
        // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
        unsMain.setMemory(unsMain.address, sizeForEntries(hashTableSize), (byte) 0);
    }

    int getHashTableSize()
    {
        return hashPartitionMask + 1;
    }

    long partitionForHash(int hash)
    {
        int partition = hash & hashPartitionMask;
        if (rehashSplit.get() <= partition)
            return unsMain.address + partition * PARTITION_ENTRY_LEN;

        // partition already rehashed - look it up in alternate location
        partition = hash & hashPartitionMaskAlt;
        return unsAlt.address + partition * PARTITION_ENTRY_LEN;
    }

    long lockPartitionForHash(int hash)
    {
        long partAdr = partitionForHash(hash);

        int spins = unsMain.lock(partAdr + OFF_LOCK);
        if (spins > 0)
            lockPartitionSpins.addAndGet(spins);

        return partAdr;
    }

    void unlockPartition(long partitionAdr)
    {
        if (partitionAdr != 0L)
            unsMain.unlock(partitionAdr + OFF_LOCK);
    }

    long getLRUHead(long partitionAdr)
    {
        if (partitionAdr == 0L)
            return 0L;
        return unsMain.getLongVolatile(partitionAdr + OFF_LRU_HEAD);
    }

    void setLRUHead(long partitionAdr, long hashEntryAdr)
    {
        if (partitionAdr != 0L)
            unsMain.putLongVolatile(partitionAdr + OFF_LRU_HEAD, hashEntryAdr);
    }

    void setLRUHeadAlt(long partitionAdr, long hashEntryAdr)
    {
        if (partitionAdr != 0L)
            unsAlt.putLongVolatile(partitionAdr + OFF_LRU_HEAD, hashEntryAdr);
    }

    long getLockPartitionSpins()
    {
        return lockPartitionSpins.get();
    }

    void prepareRehash(int newHashTableSize)
    {
        this.unsAlt = new Uns((long) newHashTableSize * PARTITION_ENTRY_LEN, PARTITION_ENTRY_LEN);
        hashPartitionMaskAlt = newHashTableSize - 1;
        // it's important to initialize the hash partition memory!
        // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
        unsAlt.setMemory(unsAlt.address, sizeForEntries(newHashTableSize), (byte) 0);
    }

    void finishRehash()
    {
        unsMain = unsAlt;
        hashPartitionMask = hashPartitionMaskAlt;
        rehashSplit.set(0);
    }

    public void rehashProgress(int partition)
    {
        rehashSplit.set(partition + 1);
    }

    long rehashPartitionForHash(int hash)
    {
        int partition = hash & hashPartitionMaskAlt;
        return unsAlt.address + partition * PARTITION_ENTRY_LEN;
    }
}
