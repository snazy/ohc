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
    private volatile long address;

    private volatile int hashPartitionMaskAlt;
    private volatile long addressAlt;

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

    long getPartitionHead(int hash)
    {
        return Uns.getLongVolatile(partitionAdrForHash(hash) + PART_OFF_PARTITION_HEAD);
    }

    void setPartitionHead(int hash, long hashEntryAdr)
    {
        Uns.putLongVolatile(partitionAdrForHash(hash) + PART_OFF_PARTITION_HEAD, hashEntryAdr);
    }

    void setPartitionHeadAlt(int hash, long hashEntryAdr)
    {
        Uns.putLongVolatile(address + partitionForHashAlt(hash) * PARTITION_ENTRY_LEN + PART_OFF_PARTITION_HEAD, hashEntryAdr);
    }

    void prepareRehash(int newHashTableSize)
    {
        this.addressAlt = Uns.allocate((long) newHashTableSize * PARTITION_ENTRY_LEN);
        hashPartitionMaskAlt = newHashTableSize - 1;
        // it's important to initialize the hash partition memory!
        // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
        Uns.setMemory(addressAlt, sizeForEntries(newHashTableSize), (byte) 0);

        for (int partNo = 0; partNo < newHashTableSize; partNo++)
            Uns.initStamped(addressAlt + partNo * PARTITION_ENTRY_LEN + PART_OFF_LOCK);
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
        address = addressAlt;
        hashPartitionMask = hashPartitionMaskAlt;
        rehashSplit.set(-1);
    }

    private long partitionAdrForHash(int hash)
    {
        int partition = partitionForHash(hash);
        if (inMain(partition))
            return address + partition * PARTITION_ENTRY_LEN;

        // partition already rehashed - look it up in alternate location
        partition = hash & hashPartitionMaskAlt;
        return addressAlt + partition * PARTITION_ENTRY_LEN;
    }

    long lockPartition(int hash, boolean write)
    {
        for (int i = 0; i < 256; i++)
        {
            int partition = partitionForHash(hash);
            long partLockAdr = inMain(hash)
                           ? address + partition * PARTITION_ENTRY_LEN + PART_OFF_LOCK
                           : addressAlt + partitionForHashAlt(hash) * PARTITION_ENTRY_LEN + PART_OFF_LOCK;
            long stamp = write
                         ? Uns.lockStampedWrite(partLockAdr)
                         : Uns.lockStampedRead(partLockAdr);
            if (stamp != Uns.INVALID_LOCK)
                return stamp;
        }
        // execution path might get here if too many consecutive rehashes occur (and the lock code reaches the
        // old hash partition)
        throw new IllegalStateException("Got INVALID_LOCK too often - this is very likely an internal error (race condition)");
    }

    long lockPartitionForLongRun(boolean alt, int partNo)
    {
        return Uns.lockStampedLongRun((alt ? addressAlt : address) + partNo * PARTITION_ENTRY_LEN + PART_OFF_LOCK);
    }

    void unlockPartition(long lock, int hash, boolean write)
    {
        int partition = partitionForHash(hash);
        long partLockAdr = inMain(hash)
                           ? address + partition * PARTITION_ENTRY_LEN + PART_OFF_LOCK
                           : addressAlt + partitionForHashAlt(hash) * PARTITION_ENTRY_LEN + PART_OFF_LOCK;
        if (write)
            Uns.unlockStampedWrite(partLockAdr, lock);
        else
            Uns.unlockStampedRead(partLockAdr, lock);
    }

    void unlockPartitionForLongRun(boolean alt, long lock, int partNo)
    {
        Uns.unlockStampedLongRun((alt ? addressAlt : address) + partNo * PARTITION_ENTRY_LEN + PART_OFF_LOCK, lock);
    }

    public void rehashProgress(long lock, int partition, long[] rehashLocks)
    {
        // move rehash split pointer first to let new
        rehashSplit.set(partition);

        // mark the old lock as invalid to let waiters fail immediately
        Uns.unlockForFail(address + partition * PARTITION_ENTRY_LEN + PART_OFF_LOCK, lock);

        int partNew = partition * 2;
        Uns.unlockStampedLongRun(addressAlt + (partition * 2) * PARTITION_ENTRY_LEN + PART_OFF_LOCK, rehashLocks[partNew]);
        partNew++;
        Uns.unlockStampedLongRun(addressAlt + (partition * 2 + 1) * PARTITION_ENTRY_LEN + PART_OFF_LOCK, rehashLocks[partNew]);
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
