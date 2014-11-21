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
    static final int OFF_NEXT_BLOCK = 0;
    // offset of serialized hash value
    static final int OFF_HASH = 8;
    // offset of previous hash entry in LRU list for this hash partition
    static final int OFF_LRU_PREVIOUS = 16;
    // offset of next hash entry in LRU list for this hash partition
    static final int OFF_LRU_NEXT = 24;
    // offset of last used timestamp (System.currentTimeMillis)
    static final int OFF_LAST_USED_TIMESTAMP = 32;
    // offset of serialized hash key length
    static final int OFF_HASH_KEY_LENGTH = 40;
    // offset of serialized value length
    static final int OFF_VALUE_LENGTH = 48;
    // offset of data in first block
    static final int OFF_DATA_IN_FIRST = 64;

    // offset of data in "next block"
    static final int OFF_DATA_IN_NEXT = 8;

    private final int blockSize;

    private final FreeBlocks freeBlocks;
    private final HashPartitionAccess hashPartitionAccess;

    private final int firstBlockDataSpace;
    private final int nextBlockDataSpace;

    HashEntryAccess(int blockSize, FreeBlocks freeBlocks, HashPartitionAccess hashPartitionAccess)
    {
        this.blockSize = blockSize;
        this.freeBlocks = freeBlocks;
        this.hashPartitionAccess = hashPartitionAccess;

        firstBlockDataSpace = blockSize - OFF_DATA_IN_FIRST;
        nextBlockDataSpace = blockSize - OFF_DATA_IN_NEXT;
    }

    int calcRequiredNumberOfBlocks(BytesSource keySource, BytesSource valueSource)
    {
        long total = keySource.size();
        total += valueSource.size();

        total -= firstBlockDataSpace;
        if (total <= 0L)
            return 1;

        int blk = (int) (1L + total / nextBlockDataSpace);

        return (total % nextBlockDataSpace) != 0
               ? blk + 1
               : blk;
    }

    long createNewEntryChain(int hash, BytesSource keySource, BytesSource valueSource)
    {
        int requiredBlocks = calcRequiredNumberOfBlocks(keySource, valueSource);
        if (requiredBlocks < 1)
            throw new InternalError();

        // allocate memory for whole hash-entry-chain
        long hashEntryAdr = freeBlocks.allocateChain(requiredBlocks);
        if (hashEntryAdr == 0L)
            return 0L;

        // initialize hash entry fields
        initHashEntry(hash, keySource, valueSource, hashEntryAdr);

        // write key + value to data
        long blkAdr = hashEntryAdr;
        long blkOff = OFF_DATA_IN_FIRST;
        int blkRemain = firstBlockDataSpace;

        // write key to data
        long len = keySource.size();
        if (keySource.hasArray())
        {
            // keySource provides array access (can use Unsafe.copyMemory)
            byte[] arr = keySource.array();
            int arrOff = keySource.arrayOffset();
            int arrIdx = 0;
            while (arrIdx < len)
            {
                long remain = len - arrIdx;
                if (remain > blkRemain)
                {
                    Uns.copyMemory(arr, arrIdx + arrOff, blkAdr + blkOff, blkRemain);
                    arrIdx += blkRemain;
                    blkAdr = getNextBlock(blkAdr);
                    blkOff = OFF_DATA_IN_NEXT;
                    blkRemain = nextBlockDataSpace;
                }
                else
                {
                    Uns.copyMemory(arr, arrIdx + arrOff, blkAdr + blkOff, (int) remain);
                    blkOff += remain;
                    if (blkOff == blockSize)
                    {
                        blkAdr = getNextBlock(blkAdr);
                        blkOff = OFF_DATA_IN_NEXT;
                        blkRemain = nextBlockDataSpace;
                    }
                    else
                        blkRemain -= remain;
                    break;
                }
            }
        }
        else
        {
            // keySource has no array
            for (int p = 0; p < len; p++)
            {
                byte b = keySource.getByte(p);
                Uns.putByte(blkAdr + blkOff++, b);
                if (blkOff == blockSize)
                {
                    blkAdr = getNextBlock(blkAdr);
                    blkOff = OFF_DATA_IN_NEXT;
                }
            }
        }

        // write value to data
        //
        // Although the following code is similar to the one used for the key above, it would require us to
        // allocate objects to be able to use the same code for both - so...
        //
        len = valueSource.size();
        if (valueSource.hasArray())
        {
            // valueSource provides array access (can use Unsafe.copyMemory)
            byte[] arr = valueSource.array();
            int arrOff = valueSource.arrayOffset();
            int arrIdx = 0;
            while (arrIdx < len)
            {
                long remain = len - arrIdx;
                if (remain > blkRemain)
                {
                    Uns.copyMemory(arr, arrIdx + arrOff, blkAdr + blkOff, blkRemain);
                    arrIdx += blkRemain;
                    blkAdr = getNextBlock(blkAdr);
                    blkOff = OFF_DATA_IN_NEXT;
                    blkRemain = nextBlockDataSpace;
                }
                else
                {
                    Uns.copyMemory(arr, arrIdx + arrOff, blkAdr + blkOff, (int) remain);
                    break;
                }
            }
        }
        else
        {
            // valueSource has no array
            for (int p = 0; p < len; p++)
            {
                byte b = valueSource.getByte(p);
                Uns.putByte(blkAdr + blkOff++, b);
                if (blkOff == blockSize)
                {
                    blkAdr = getNextBlock(blkAdr);
                    blkOff = OFF_DATA_IN_NEXT;
                }
            }
        }

        return hashEntryAdr;
    }

    private void initHashEntry(int hash, BytesSource keySource, BytesSource valueSource, long hashEntryAdr)
    {
        setLastUsed(hashEntryAdr);
        Uns.putLongVolatile(hashEntryAdr + OFF_HASH, hash);
        setLRUPrevious(hashEntryAdr, 0L);
        setLRUNext(hashEntryAdr, 0L);
        Uns.putLongVolatile(hashEntryAdr + OFF_HASH_KEY_LENGTH, keySource.size());
        Uns.putLongVolatile(hashEntryAdr + OFF_VALUE_LENGTH, valueSource.size());
    }

    long findHashEntry(long partitionAdr, int hash, BytesSource keySource)
    {
        if (partitionAdr == 0L)
            return 0L;

        long firstHashEntryAdr = hashPartitionAccess.getLRUHead(partitionAdr);
        boolean first = true;
        for (long hashEntryAdr = firstHashEntryAdr; hashEntryAdr != 0L; hashEntryAdr = getLRUNext(hashEntryAdr))
        {
            if (!first && firstHashEntryAdr == hashEntryAdr)
                throw new InternalError("endless loop");
            first = false;

            long hashEntryHash = Uns.getLongVolatile(hashEntryAdr + OFF_HASH);
            if (hashEntryHash != hash)
                continue;

            long serKeyLen = Uns.getLongVolatile(hashEntryAdr + OFF_HASH_KEY_LENGTH);
            if (serKeyLen != keySource.size())
                continue;

            if (!compareKey(hashEntryAdr, keySource, serKeyLen))
                continue;

            return hashEntryAdr;
        }
        return 0L;
    }

//    String dumpLRUList(long partitionAdr)
//    {
//        StringBuilder sb = new StringBuilder()
//                           .append("partition=").append(partitionAdr).append("\n");
//        long firstHashEntryAdr = hashPartitionAccess.getLRUHead(partitionAdr);
//        boolean first = true;
//        for (long hashEntryAdr = firstHashEntryAdr; hashEntryAdr != 0L; hashEntryAdr = getLRUNext(hashEntryAdr))
//        {
//            sb.append("     next=").append(hashEntryAdr).append("\n");
//
//            if (!first && firstHashEntryAdr == hashEntryAdr)
//            {
//                sb.append("*** FAILURE !!! LOOOPS TO FIRST ENTRY!");
//                break;
//            }
//            first = false;
//        }
//        return sb.toString();
//    }

    private boolean compareKey(long hashEntryAdr, BytesSource keySource, long serKeyLen)
    {
        if (hashEntryAdr == 0L)
            return false;

        long blkOff = OFF_DATA_IN_FIRST;
        long blkAdr = hashEntryAdr;

        // array optimized version
        if (keySource.hasArray())
        {
            byte[] arr = keySource.array();
            int arrOff = keySource.arrayOffset();
            for (int p = 0; p < serKeyLen; )
            {
                if (serKeyLen - p >= 8)
                {
                    long lSer = Uns.getLong(blkAdr + blkOff);
                    long lKey = Uns.getLongFromByteArray(arr, arrOff + p);
                    if (lSer != lKey)
                        return false;
                    blkOff += 8;
                    p += 8;
                }
                else
                {
                    byte bSer = Uns.getByte(blkAdr + blkOff++);
                    byte bKey = keySource.getByte(p++);

                    if (bSer != bKey)
                        return false;
                }

                if (blkOff == blockSize)
                {
                    blkAdr = getNextBlock(blkAdr);
                    blkOff = OFF_DATA_IN_NEXT;
                }
            }

            return true;
        }

        // last-resort byte-by-byte compare
        for (int p = 0; p < serKeyLen; p++)
        {
            if (blkAdr == 0L)
                return false;

            byte bSer = Uns.getByte(blkAdr + blkOff++);
            byte bKey = keySource.getByte(p);

            if (bSer != bKey)
                return false;

            if (blkOff == blockSize)
            {
                blkAdr = getNextBlock(blkAdr);
                blkOff = OFF_DATA_IN_NEXT;
            }
        }

        return true;
    }

    void writeValueToSink(long hashEntryAdr, BytesSink valueSink)
    {
        if (hashEntryAdr == 0L)
            return;

        long serKeyLen = Uns.getLongVolatile(hashEntryAdr + OFF_HASH_KEY_LENGTH);
        if (serKeyLen < 0L)
            throw new InternalError();
        long valueLen = Uns.getLongVolatile(hashEntryAdr + OFF_VALUE_LENGTH);
        if (valueLen < 0L)
            throw new InternalError();
        long blkAdr = hashEntryAdr;
        long blkOff;

        // first block
        if (serKeyLen >= firstBlockDataSpace)
        {
            serKeyLen -= firstBlockDataSpace;
            blkAdr = getNextBlock(hashEntryAdr);

            while (serKeyLen >= nextBlockDataSpace && blkAdr != 0L)
            {
                serKeyLen -= nextBlockDataSpace;
                blkAdr = getNextBlock(blkAdr);
            }

            blkOff = OFF_DATA_IN_NEXT + serKeyLen;
        }
        else
            blkOff = OFF_DATA_IN_FIRST + serKeyLen;

        if (valueLen > Integer.MAX_VALUE)
            throw new IllegalStateException("integer overflow");
        valueSink.setSize((int) valueLen);

        for (int p = 0; blkAdr != 0L && p < valueLen; p++)
        {
            byte b = Uns.getByte(blkAdr + blkOff++);
            if (blkOff == blockSize)
            {
                blkAdr = getNextBlock(blkAdr);
                blkOff = OFF_DATA_IN_NEXT;
            }
            valueSink.putByte(p, b);
        }
    }

    void updatePartitionLRU(long partitionAdr, long hashEntryAdr)
    {
        if (partitionAdr == 0L || hashEntryAdr == 0L)
            return;

        long lruHead = hashPartitionAccess.getLRUHead(partitionAdr);
        if (lruHead != hashEntryAdr)
        {
            removeFromPartitionLRU(partitionAdr, hashEntryAdr, lruHead);
            addAsPartitionLRUHead(partitionAdr, hashEntryAdr, lruHead);
        }

        setLastUsed(hashEntryAdr);
    }

    void addAsPartitionLRUHead(long partitionAdr, long hashEntryAdr)
    {
        long lruHead = hashPartitionAccess.getLRUHead(partitionAdr);
        addAsPartitionLRUHead(partitionAdr, hashEntryAdr, lruHead);
    }

    void addAsPartitionLRUHead(long partitionAdr, long hashEntryAdr, long lruHead)
    {
        if (partitionAdr == 0L || hashEntryAdr == 0L)
            return;

        if (lruHead != 0L)
        {
            setLRUNext(hashEntryAdr, lruHead);
            setLRUPrevious(lruHead, hashEntryAdr);
        }
        else
            setLRUNext(hashEntryAdr, 0L);
        setLRUPrevious(hashEntryAdr, 0L);

        hashPartitionAccess.setLRUHead(partitionAdr, hashEntryAdr);
    }

    void removeFromPartitionLRU(long partitionAdr, long hashEntryAdr)
    {
        long lruHead = hashPartitionAccess.getLRUHead(partitionAdr);
        removeFromPartitionLRU(partitionAdr, hashEntryAdr, lruHead);
    }

    void removeFromPartitionLRU(long partitionAdr, long hashEntryAdr, long lruHead)
    {
        if (partitionAdr == 0L || hashEntryAdr == 0L)
            return;

        long entryPrev = getLRUPrevious(hashEntryAdr);
        long entryNext = getLRUNext(hashEntryAdr);

        if (entryNext != 0L)
            setLRUPrevious(entryNext, entryPrev);

        if (entryPrev != 0L)
            setLRUNext(entryPrev, entryNext);

        if (lruHead == hashEntryAdr)
            hashPartitionAccess.setLRUHead(partitionAdr, entryNext);

        // we can leave the LRU next+previous pointers in hashEntryAdr alone
    }

    private long getNextBlock(long blockAdr)
    {
        return blockAdr != 0L ? Uns.getLongVolatile(blockAdr + OFF_NEXT_BLOCK) : 0L;
    }

    private long getLRUNext(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLongVolatile(hashEntryAdr + OFF_LRU_NEXT) : 0L;
    }

    private long getLRUPrevious(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLongVolatile(hashEntryAdr + OFF_LRU_PREVIOUS) : 0L;
    }

    private void setLRUPrevious(long hashEntryAdr, long previousAdr)
    {
        if (hashEntryAdr != 0L)
            Uns.putLongVolatile(hashEntryAdr + OFF_LRU_PREVIOUS, previousAdr);
    }

    private void setLRUNext(long hashEntryAdr, long nextAdr)
    {
        if (hashEntryAdr != 0L)
            Uns.putLongVolatile(hashEntryAdr + OFF_LRU_NEXT, nextAdr);
    }

    private void setLastUsed(long hashEntryAdr)
    {
        if (hashEntryAdr != 0L)
            Uns.putLong(hashEntryAdr + OFF_LAST_USED_TIMESTAMP, System.currentTimeMillis());
    }
}
