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

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

final class OHCacheImpl implements OHCache
{

    static
    {
        try
        {
            Field f = AtomicLong.class.getDeclaredField("VM_SUPPORTS_LONG_CAS");
            f.setAccessible(true);
            if (!(Boolean)f.get(null))
                throw new IllegalStateException("Off Heap Cache implementation requires a JVM that supports CAS on long fields");
        }
        catch (Exception e)
        {
            throw new RuntimeException();
        }
    }

    public static final int MAX_HASH_TABLE_SIZE = 4194304;
    public static final int MIN_HASH_TABLE_SIZE = 32;
    public static final int MAX_BLOCK_SIZE = 32768;
    public static final int MIN_BLOCK_SIZE = 2048;

    private final int blockSize;
    private final int hashTableSize;
    private final long totalCapacity;

    // off-heap address offset
    private final long rootAddress;

    private final FreeBlocks freeBlocks;
    private final HashEntryAccess hashEntryAccess;
    private final HashPartitionAccess hashPartitionAccess;

    private volatile boolean closed;

    OHCacheImpl(OHCacheBuilder builder)
    {
        if (builder.getBlockSize() < MIN_BLOCK_SIZE)
            throw new IllegalArgumentException("Block size must not be less than " + MIN_BLOCK_SIZE);
        if (builder.getBlockSize() > MAX_BLOCK_SIZE)
            throw new IllegalArgumentException("Block size must not be greater than " + MAX_BLOCK_SIZE);
        int bs;
        for (bs = MIN_BLOCK_SIZE; bs < MAX_BLOCK_SIZE; bs <<= 1)
            if (bs >= builder.getBlockSize())
                break;
        blockSize = bs;

        if (builder.getHashTableSize() < MIN_HASH_TABLE_SIZE)
            throw new IllegalArgumentException("Block size must not be less than " + MIN_HASH_TABLE_SIZE);
        if (builder.getHashTableSize() > MAX_HASH_TABLE_SIZE)
            throw new IllegalArgumentException("Block size must not be greater than " + MAX_HASH_TABLE_SIZE);
        int hts;
        for (hts = MIN_HASH_TABLE_SIZE; hts < MAX_HASH_TABLE_SIZE; hts <<= 1)
            if (hts >= builder.getHashTableSize())
                break;
        hashTableSize = hts;

        long minSize = 8 * 1024 * 1024; // ugh

        long ts = builder.getTotalCapacity();
        ts /= bs;
        ts *= bs;
        if (ts < minSize)
            throw new IllegalArgumentException("Block size must not be less than " + minSize);
        totalCapacity = ts;

        long hashTableMem = HashPartitionAccess.sizeForEntries(hashTableSize);

        this.rootAddress = Uns.allocate(totalCapacity + hashTableMem);

        long blocksAddress = this.rootAddress + hashTableMem;

        this.freeBlocks = new FreeBlocks(blocksAddress, blocksAddress + totalCapacity, blockSize);
        this.hashPartitionAccess = new HashPartitionAccess(hashTableSize, rootAddress);
        this.hashEntryAccess = new HashEntryAccess(blockSize, freeBlocks, hashPartitionAccess);
    }

    public void close()
    {
        closed = true;
        Uns.free(rootAddress);
    }

    private void assertNotClosed()
    {
        if (closed)
            throw new IllegalStateException("OHCache instance already closed");
    }

    public int getBlockSize()
    {
        assertNotClosed();
        return blockSize;
    }

    public long getTotalCapacity()
    {
        assertNotClosed();
        return totalCapacity;
    }

    public int getHashTableSize()
    {
        assertNotClosed();
        return hashTableSize;
    }

    public int calcFreeBlockCount()
    {
        assertNotClosed();
        return freeBlocks.calcFreeBlockCount();
    }

    public PutResult put(int hash, BytesSource keySource, BytesSource valueSource)
    {
        return put(hash, keySource, valueSource, null);
    }

    public PutResult put(int hash, BytesSource keySource, BytesSource valueSource, BytesSink oldValueSink)
    {
        if (keySource == null)
            throw new NullPointerException();
        if (valueSource == null)
            throw new NullPointerException();
        if (keySource.size() <= 0)
            throw new ArrayIndexOutOfBoundsException();
        if (valueSource.size() < 0)
            throw new ArrayIndexOutOfBoundsException();

        // 1. find + lock hash partition
        long partitionAdr = hashPartitionAccess.lockPartitionForHash(hash);
        try
        {
            // 2. do work
            long hashEntryAdr = hashEntryAccess.findHashEntry(partitionAdr, hash, keySource);
            boolean replace = hashEntryAdr != 0L;
            if (hashEntryAdr != 0L)
            {
                // remove existing entry

                if (oldValueSink != null)
                    hashEntryAccess.writeValueToSink(hashEntryAdr, oldValueSink);

                hashEntryAccess.removeFromLRU(partitionAdr, hashEntryAdr);
                hashEntryAccess.freeHashEntryChain(hashEntryAdr);
            }
            hashEntryAdr = hashEntryAccess.allocateDataForEntry(hash, keySource, valueSource);

            if (hashEntryAdr == 0L)
                return PutResult.NO_MORE_SPACE;

            // add new entry

            hashEntryAccess.addAsLRUHead(partitionAdr, hashEntryAdr);

            return replace ? PutResult.REPLACE : PutResult.ADD;
        }
        finally
        {
            // 3. release hash partition
            hashPartitionAccess.unlockPartition(partitionAdr);
        }
    }

    public boolean get(int hash, BytesSource keySource, BytesSink valueSink)
    {
        if (keySource == null)
            throw new NullPointerException();
        if (valueSink == null)
            throw new NullPointerException();
        if (keySource.size() <= 0)
            throw new ArrayIndexOutOfBoundsException();

        // 1. find + lock hash partition
        long partitionAdr = hashPartitionAccess.lockPartitionForHash(hash);
        try
        {
            // 2. do work
            long hashEntryAdr = hashEntryAccess.findHashEntry(partitionAdr, hash, keySource);
            if (hashEntryAdr != 0L)
            {
                hashEntryAccess.updateLRU(partitionAdr, hashEntryAdr);

                hashEntryAccess.writeValueToSink(hashEntryAdr, valueSink);

                return true;
            }

            return false;
        }
        finally
        {
            // 3. release hash partition
            hashPartitionAccess.unlockPartition(partitionAdr);
        }
    }

    public boolean remove(int hash, BytesSource keySource)
    {
        if (keySource == null)
            throw new NullPointerException();
        if (keySource.size() <= 0)
            throw new ArrayIndexOutOfBoundsException();

        // 1. find + lock hash partition
        long partitionAdr = hashPartitionAccess.lockPartitionForHash(hash);
        try
        {
            // 2. do work
            long hashEntryAdr = hashEntryAccess.findHashEntry(partitionAdr, hash, keySource);
            if (hashEntryAdr != 0L)
            {
                hashEntryAccess.removeFromLRU(partitionAdr, hashEntryAdr);

                hashEntryAccess.freeHashEntryChain(hashEntryAdr);

                return true;
            }

            return false;
        }
        finally
        {
            // 3. release hash partition
            hashPartitionAccess.unlockPartition(partitionAdr);
        }
    }

    public long getFreeBlockSpins()
    {
        return freeBlocks.getFreeBlockSpins();
    }

    public long getLockPartitionSpins()
    {
        return hashPartitionAccess.getLockPartitionSpins();
    }
}
