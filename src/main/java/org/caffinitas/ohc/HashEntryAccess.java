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
 * Encapsulates access to hash entries.
 */
final class HashEntryAccess
{
    // offset of next block address in a "next block"
    static final long OFF_NEXT_BLOCK = 0L;
    // offset of serialized hash value
    static final long OFF_HASH = 8L;
    // offset of previous hash entry in LRU list for this hash partition
    static final long OFF_LRU_PREVIOUS = 16L;
    // offset of next hash entry in LRU list for this hash partition
    static final long OFF_LRU_NEXT = 24L;
    // offset of last used timestamp (System.currentTimeMillis)
    static final long OFF_LAST_USED_TIMESTAMP = 32L;
    // offset of serialized hash key length
    static final long OFF_HASH_KEY_LENGTH = 40L;
    // offset of serialized value length
    static final long OFF_VALUE_LENGTH = 48L;
    // offset of data in first block
    static final long OFF_DATA = 64L;

    // offset of data in "next block"
    static final long OFF_DATA_IN_NEXT = 8L;

    private final int blockSize;

    private final FreeBlocks freeBlocks;
    private final HashPartitionAccess hashPartitionAccess;

    private final long firstBlockDataSpace;
    private final long nextBlockDataSpace;

    HashEntryAccess(int blockSize, FreeBlocks freeBlocks, HashPartitionAccess hashPartitionAccess)
    {
        this.blockSize = blockSize;
        this.freeBlocks = freeBlocks;
        this.hashPartitionAccess = hashPartitionAccess;

        firstBlockDataSpace = blockSize - OFF_DATA;
        nextBlockDataSpace = blockSize - OFF_DATA_IN_NEXT;
    }

    int calcRequiredNumberOfBlocks(BytesSource keySource, BytesSource valueSource)
    {
        long total = keySource.size();
        total += valueSource.size();

        // TODO can be optimized (may allocate one wasted block if value ends on exact block boundary)
        return (int) (2L + (total - firstBlockDataSpace) / nextBlockDataSpace);
    }

    long allocateDataForEntry(int hash, BytesSource keySource, BytesSource valueSource)
    {
        int requiredBlocks = calcRequiredNumberOfBlocks(keySource, valueSource);
        if (requiredBlocks < 1)
            throw new InternalError();

        long adr = 0L;
        while (requiredBlocks-- > 0)
        {
            long blkAdr = freeBlocks.allocateBlock();
            if (blkAdr == 0L)
                return freeHashEntryChain(adr);

            Uns.putLong(blkAdr + OFF_NEXT_BLOCK, adr);

            adr = blkAdr;
        }

        Uns.putLong(adr + OFF_HASH, hash);
        setLRUPrevious(adr, 0L);
        setLRUNext(adr, 0L);
        Uns.putLong(adr + OFF_LAST_USED_TIMESTAMP, System.currentTimeMillis());
        Uns.putLong(adr + OFF_HASH_KEY_LENGTH, keySource.size());
        Uns.putLong(adr + OFF_VALUE_LENGTH, valueSource.size());

        // TODO write key to data
        // TODO write value to data

        return adr;
    }

    long freeHashEntryChain(long adr)
    {
        while (adr != 0L)
        {
            long next = Uns.getLong(adr + OFF_NEXT_BLOCK);
            freeBlocks.freeBlock(adr);
            adr = next;
        }
        return 0L;
    }

    long findHashEntry(long partitionAdr, int hash, BytesSource keySource)
    {
        for (long hashEntryAdr = hashPartitionAccess.getLRUHead(partitionAdr); hashEntryAdr != 0L; hashEntryAdr = getLRUNext(hashEntryAdr))
        {
            long hashEntryHash = Uns.getLong(hashEntryAdr + OFF_HASH);
            if (hashEntryHash != hash)
                continue;

            // TODO compare serialized hash

            throw new UnsupportedOperationException();
        }
        return 0L;
    }

    void writeValueToSink(long hashEntryAdr, BytesSink valueSink)
    {
        throw new UnsupportedOperationException();
    }

    public void updateLRU(long partitionAdr, long hashEntryAdr)
    {
        removeFromLRU(partitionAdr, hashEntryAdr);
        addAsLRUHead(partitionAdr, hashEntryAdr);

        Uns.putLong(hashEntryAdr + OFF_LAST_USED_TIMESTAMP, System.currentTimeMillis());
    }

    void addAsLRUHead(long partitionAdr, long hashEntryAdr)
    {
        long lruHead = hashPartitionAccess.getLRUHead(partitionAdr);

        if (lruHead != 0L)
        {
            setLRUNext(hashEntryAdr, lruHead);
            setLRUPrevious(lruHead, hashEntryAdr);
        }

        hashPartitionAccess.setLRUHead(partitionAdr, hashEntryAdr);
    }

    void removeFromLRU(long partitionAdr, long hashEntryAdr)
    {
        long lruHead = hashPartitionAccess.getLRUHead(partitionAdr);

        long entryPrev = getLRUPrevious(hashEntryAdr);
        long entryNext = getLRUNext(hashEntryAdr);

        if (entryNext != 0L)
            setLRUPrevious(entryNext, entryPrev);

        if (entryPrev != 0L)
            setLRUNext(entryPrev, entryNext);

        if (lruHead == hashEntryAdr)
            hashPartitionAccess.setLRUHead(partitionAdr, entryNext);

        setLRUPrevious(hashEntryAdr, 0L);
        setLRUNext(hashEntryAdr, 0L);
    }

    private long getLRUNext(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr + OFF_LRU_NEXT);
    }

    private long getLRUPrevious(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr + OFF_LRU_PREVIOUS);
    }

    private void setLRUPrevious(long hashEntryAdr, long previousAdr)
    {
        Uns.putLong(hashEntryAdr + OFF_LRU_PREVIOUS, previousAdr);
    }

    private void setLRUNext(long hashEntryAdr, long nextAdr)
    {
        Uns.putLong(hashEntryAdr + OFF_LRU_NEXT, nextAdr);
    }
}
