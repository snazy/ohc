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

/**
 * Encapsulates access to hash partitions.
 */
final class HashPartitionAccess
{
    // reference to the last-referenced hash entry (for LRU)
    static final long OFF_LRU_HEAD = 0L;
    // offset of CAS style lock field
    static final long OFF_LOCK = 8L;
    // each partition entry is 64 bytes long (since we will perform a lot of CAS/modifying operations on LRU and LOCK
    // which are evil to CPU cache lines)
    static final long PARTITION_ENTRY_LEN = 64L;

    private final int hashPartitionMask;
    private final int blockSize;
    private final long rootAddress;

    HashPartitionAccess(int hashTableSize, int blockSize, long rootAddress)
    {
        this.hashPartitionMask = hashTableSize;
        this.blockSize = blockSize;
        this.rootAddress = rootAddress;
    }

    long partitionForHash(int hash)
    {
        int partition = hash & hashPartitionMask;
        return rootAddress + partition * blockSize;
    }

    long lockPartitionForHash(int hash)
    {
        long partAdr = partitionForHash(hash);

        while (!Uns.compareAndSwap(partAdr + OFF_LOCK, 0L, Thread.currentThread().getId()))
        {
            // TODO find a better solution than a busy-spin-lock
        }

        return partAdr;
    }

    void unlockPartition(long partitionAdr)
    {
        Uns.compareAndSwap(partitionAdr + OFF_LOCK, Thread.currentThread().getId(), 0L);
    }

    long getLRUHead(long partitionAdr)
    {
        return Uns.getLong(partitionAdr + OFF_LRU_HEAD);
    }

    public void setLRUHead(long partitionAdr, long hashEntryAdr)
    {
        Uns.putLong(partitionAdr + OFF_LRU_HEAD, hashEntryAdr);
    }
}
