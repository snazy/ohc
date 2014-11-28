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

import java.util.concurrent.atomic.AtomicReference;

/**
 * Encapsulates access to hash partitions.
 */
final class HashPartitions implements Constants
{

    // AtomicReference needed to make table-switch after rehash an atomic operation (thus no need for a lock)
    private final AtomicReference<Table> table = new AtomicReference<>();

    HashPartitions(int hashTableSize)
    {
        prepareHashTable(hashTableSize, false);
    }

    void release()
    {
        table.get().release();
    }

    int hashTableSize()
    {
        return table.get().hashPartitionMask + 1;
    }

    long getPartitionHead(int hash)
    {
        return Uns.getLongVolatile(partitionAddressHead(hash));
    }

    void setPartitionHead(int hash, long hashEntryAdr)
    {
        Uns.putLongVolatile(partitionAddressHead(hash) + PART_OFF_PARTITION_HEAD, hashEntryAdr);
    }

    Table prepareHashTable(int newHashTableSize, boolean forRehash)
    {
        // set new new partition table (all partitions are locked exclusively) and remember the
        // old table for rehash purpose
        return table.getAndSet(new Table(newHashTableSize, forRehash));
    }

    void rehashProgress(int partNo, int partNoNew)
    {
        Uns.unlockStampedLongRun(partitionAddressLock(partNo), Uns.longRunStamp());
        Uns.unlockStampedLongRun(partitionAddressLock(partNoNew), Uns.longRunStamp());
    }

    long lockPartition(int hash, boolean write)
    {
        for (int i = 0; i < 131072; i++)
        {
            long partLockAdr = partitionAddressLock(hash);
            long stamp = write
                         ? Uns.lockStampedWrite(partLockAdr)
                         : Uns.lockStampedRead(partLockAdr);
            if (stamp != Uns.INVALID_LOCK)
                return stamp;
            Uns.park(100000L); // 0.1ms
        }
        // execution path might get here if too many consecutive rehashes occur (and the lock code reaches the
        // old hash partition)
        throw new IllegalStateException("Got INVALID_LOCK too often - this is very likely an internal error (race condition)");
    }

    long lockPartitionForLongRun(int partNo)
    {
        // always work against main table
        return Uns.lockStampedLongRun(table.get().partitionAddressLock(partNo));
    }

    void unlockPartition(long lock, int hash, boolean write)
    {
        long partLockAdr = partitionAddressLock(hash);
        if (write)
            Uns.unlockStampedWrite(partLockAdr, lock);
        else
            Uns.unlockStampedRead(partLockAdr, lock);
    }

    void unlockPartitionForLongRun(long lock, int partNo)
    {
        Uns.unlockStampedLongRun(table.get().partitionAddressLock(partNo), lock);
    }

    static long partOffset(int partNo)
    {
        return ((long)partNo) * PARTITION_ENTRY_LEN;
    }

    int partitionForHash(int hash)
    {
        return table.get().partitionForHash(hash);
    }

    private Table table()
    {
        return table.get();
    }

    private long partitionAddressHead(int hash)
    {
        return table().partitionAddressHead(hash);
    }

    private long partitionAddressLock(int hash)
    {
        return table().partitionAddressLock(hash);
    }

    static final class Table
    {
        final int hashPartitionMask;
        final long address;

        public Table(int hashTableSize, boolean forRehash)
        {
            int msz = PARTITION_ENTRY_LEN * hashTableSize;
            this.address = Uns.allocate(msz);
            hashPartitionMask = hashTableSize - 1;
            // it's important to initialize the hash partition memory!
            // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
            Uns.setMemory(address, msz, (byte) 0);

            long adr = partitionAddressLock(0);
            for (int partNo = 0; partNo < hashTableSize; partNo++, adr += PARTITION_ENTRY_LEN)
                Uns.initStamped(adr, forRehash);
        }

        int partitionForHash(int hash)
        {
            return hash & hashPartitionMask;
        }

        long partitionAddressLock(int hash)
        {
            return partitionAddress(hash) + PART_OFF_LOCK;
        }

        long partitionAddressHead(int hash)
        {
            return partitionAddress(hash) + PART_OFF_PARTITION_HEAD;
        }

        private long partitionAddress(int hash)
        {
            return address + partOffset(partitionForHash(hash));
        }

        void release()
        {
            Uns.freeLater(address);
        }
    }
}
