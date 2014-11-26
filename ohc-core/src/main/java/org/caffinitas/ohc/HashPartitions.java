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

/**
 * Encapsulates access to hash partitions.
 */
final class HashPartitions implements Constants
{

    private volatile int hashPartitionMask;
    private volatile Uns unsMain;

    private volatile int hashPartitionMaskAlt;
    private volatile Uns unsAlt;

    // all hash partitions ins 'unsMain' smaller than this value must be looked up in 'unsAlt'
    private final AtomicInteger rehashSplit = new AtomicInteger(-1);

    static int sizeForEntries(int hashTableSize)
    {
        return PARTITION_ENTRY_LEN * hashTableSize;
    }

    HashPartitions(int hashTableSize)
    {
        prepareRehash(hashTableSize);
        finishRehash();
    }

    int getHashTableSize()
    {
        return hashPartitionMask + 1;
    }

    long getLRUHead(int hash)
    {
        return Uns.getLongVolatile(partitionAdrForHash(hash) + PART_OFF_LRU_HEAD);
    }

    void setLRUHead(int hash, long hashEntryAdr)
    {
        Uns.putLongVolatile(partitionAdrForHash(hash) + PART_OFF_LRU_HEAD, hashEntryAdr);
    }

    void setLRUHeadAlt(int hash, long hashEntryAdr)
    {
        Uns.putLongVolatile(unsAlt.address + partitionForHashAlt(hash) * PARTITION_ENTRY_LEN + PART_OFF_LRU_HEAD, hashEntryAdr);
    }

    void prepareRehash(int newHashTableSize)
    {
        this.unsAlt = new Uns((long) newHashTableSize * PARTITION_ENTRY_LEN);
        hashPartitionMaskAlt = newHashTableSize - 1;
        // it's important to initialize the hash partition memory!
        // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
        Uns.setMemory(unsAlt.address, sizeForEntries(newHashTableSize), (byte) 0);

        for (int partNo = 0; partNo < newHashTableSize; partNo++)
            Uns.initLock(unsAlt.address + partNo * PARTITION_ENTRY_LEN + PART_OFF_LOCK);
    }

    long[] lockForRehash(int newHashTableSize)
    {
        long[] rehashLocks = new long[newHashTableSize];
        for (int partNo = 0; partNo < newHashTableSize; partNo++)
            rehashLocks[partNo] = lockPartitionForLongRun(true, partNo);
        return rehashLocks;
    }

    void finishRehash()
    {
        unsMain = unsAlt;
        hashPartitionMask = hashPartitionMaskAlt;
        rehashSplit.set(-1);
    }

    private long partitionAdrForHash(int hash)
    {
        int partition = partitionForHash(hash);
        if (inMain(partition))
            return unsMain.address + partition * PARTITION_ENTRY_LEN;

        // partition already rehashed - look it up in alternate location
        partition = hash & hashPartitionMaskAlt;
        return unsAlt.address + partition * PARTITION_ENTRY_LEN;
    }

    long lockPartition(int hash, boolean write)
    {
        int partition = partitionForHash(hash);
        if (inMain(hash))
            return Uns.lock(unsMain.address + partition * PARTITION_ENTRY_LEN + PART_OFF_LOCK, write ? LockMode.WRITE : LockMode.READ);
        else
            return Uns.lock(unsAlt.address + partitionForHashAlt(hash) * PARTITION_ENTRY_LEN + PART_OFF_LOCK, write ? LockMode.WRITE : LockMode.READ);
    }

    long lockPartitionForLongRun(boolean alt, int partNo)
    {
        return Uns.lock((alt ? unsAlt : unsMain).address + partNo * PARTITION_ENTRY_LEN + PART_OFF_LOCK, LockMode.LONG_RUN);
    }

    void unlockPartition(long lock, int hash, boolean write)
    {
        int partition = partitionForHash(hash);
        if (inMain(hash))
            Uns.unlock(unsMain.address + partition * PARTITION_ENTRY_LEN + PART_OFF_LOCK, lock, write ? LockMode.WRITE : LockMode.READ);
        else
            Uns.unlock(unsAlt.address + partitionForHashAlt(hash) * PARTITION_ENTRY_LEN + PART_OFF_LOCK, lock, write ? LockMode.WRITE : LockMode.READ);
    }

    void unlockPartitionForLongRun(boolean alt, long lock, int partNo)
    {
        Uns.unlock((alt ? unsAlt : unsMain).address + partNo * PARTITION_ENTRY_LEN + PART_OFF_LOCK, lock, LockMode.LONG_RUN);
    }

    public void rehashProgress(long lock, int partition, long[] rehashLocks)
    {
        rehashSplit.set(partition);
        Uns.unlock(unsMain.address + partition * PARTITION_ENTRY_LEN + PART_OFF_LOCK, lock, LockMode.LONG_RUN);
        int partNew = partition * 2;
        Uns.unlock(unsAlt.address + (partition * 2) * PARTITION_ENTRY_LEN + PART_OFF_LOCK, rehashLocks[partNew], LockMode.LONG_RUN);
        partNew++;
        Uns.unlock(unsAlt.address + (partition * 2 + 1) * PARTITION_ENTRY_LEN + PART_OFF_LOCK, rehashLocks[partNew], LockMode.LONG_RUN);
    }

    private boolean inMain(int hash)
    {
        return rehashSplit.get() < partitionForHash(hash);
    }

    private int partitionForHash(int hash)
    {
        return hash & hashPartitionMask;
    }

    private int partitionForHashAlt(int hash)
    {
        return hash & hashPartitionMaskAlt;
    }
}
