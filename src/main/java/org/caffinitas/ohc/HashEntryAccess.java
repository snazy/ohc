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

    private final long firstBlockDataSpace;
    private final long nextBlockDataSpace;

    HashEntryAccess(int blockSize, FreeBlocks freeBlocks)
    {
        this.blockSize = blockSize;
        this.freeBlocks = freeBlocks;

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
        Uns.putLong(adr + OFF_LRU_PREVIOUS, 0L);
        Uns.putLong(adr + OFF_LRU_NEXT, 0L);
        Uns.putLong(adr + OFF_LAST_USED_TIMESTAMP, System.currentTimeMillis());
        Uns.putLong(adr + OFF_HASH_KEY_LENGTH, keySource.size());
        Uns.putLong(adr + OFF_VALUE_LENGTH, valueSource.size());

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

    long findHashEntry(int hash, BytesSource keySource)
    {
        return 0;
    }

    public void updateLRU(long partitionAdr, long hashEntryAdr)
    {
        // TODO update LRU using hash partition
        Uns.putLong(hashEntryAdr + OFF_LRU_PREVIOUS, 0L);
        Uns.putLong(hashEntryAdr + OFF_LRU_NEXT, 0L);
        Uns.putLong(hashEntryAdr + OFF_LAST_USED_TIMESTAMP, System.currentTimeMillis());
    }
}
