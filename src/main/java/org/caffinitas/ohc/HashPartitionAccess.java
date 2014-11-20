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
