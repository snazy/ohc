package org.caffinitas.ohc;

final class OHCacheImpl implements OHCache
{

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

        long minSize = 2L * hts * blockSize * 2L;

        long ts = builder.getTotalCapacity();
        ts /= bs;
        ts *= bs;
        if (ts < minSize)
            throw new IllegalArgumentException("Block size must not be less than " + minSize);
        totalCapacity = ts;

        this.rootAddress = Uns.allocate(totalCapacity);

        // this block is directly after the hash-table
        long firstFreeBlockAddress = hashTableSize * blockSize;

        this.freeBlocks = new FreeBlocks(rootAddress + firstFreeBlockAddress, rootAddress + totalCapacity, blockSize);
        this.hashEntryAccess = new HashEntryAccess(blockSize, freeBlocks);
        this.hashPartitionAccess = new HashPartitionAccess(hashTableSize, blockSize, rootAddress, freeBlocks);
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

    public void put(int hash, BytesSource keySource, BytesSource valueSource)
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
        long partitionAdr = hashPartitionAccess.partitionForHash(hash);

        // 2. do work
        long adr = hashEntryAccess.findHashEntry(hash, keySource);
        if (adr != 0L)
        {
            // remove existing entry

            hashEntryAccess.freeHashEntryChain(adr);

            // TODO unlink from LRU list

            // TODO remove from hash partition
        }
        adr = hashEntryAccess.allocateDataForEntry(hash, keySource, valueSource);
        if (adr != 0L)
        {
            // TODO link to LRU list

            // TODO add to hash partition
        }

        // 3. release hash partition

        throw new UnsupportedOperationException();
    }

    public void get(int hash, BytesSource keySource, BytesSink valueSink)
    {
        if (keySource == null)
            throw new NullPointerException();
        if (valueSink == null)
            throw new NullPointerException();
        if (keySource.size() <= 0)
            throw new ArrayIndexOutOfBoundsException();

        // 1. find + lock hash partition
        long partitionAdr = hashPartitionAccess.partitionForHash(hash);

        // 2. do work
        long adr = hashEntryAccess.findHashEntry(hash, keySource);
        if (adr != 0L)
        {
            // TODO update LRU list
            // TODO update last accessed timestamp

            hashEntryAccess.updateLRU(partitionAdr, adr);
        }

        // 3. release hash partition

        throw new UnsupportedOperationException();
    }

    public void remove(int hash, BytesSource keySource)
    {
        if (keySource == null)
            throw new NullPointerException();
        if (keySource.size() <= 0)
            throw new ArrayIndexOutOfBoundsException();

        // 1. find + lock hash partition
        long partitionAdr = hashPartitionAccess.partitionForHash(hash);

        // 2. do work
        long adr = hashEntryAccess.findHashEntry(hash, keySource);
        if (adr != 0L)
        {
            hashEntryAccess.freeHashEntryChain(adr);

            // TODO unlink from LRU list

            // TODO remove from hash partition
        }

        // 3. release hash partition

        throw new UnsupportedOperationException();
    }
}
