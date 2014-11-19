package org.caffinitas.ohc;

/**
 * Encapsulates access to hash partitions.
 */
final class HashPartitionAccess
{

    private final int hashPartitionMask;
    private final int blockSize;
    private final long rootAddress;
    private final FreeBlocks freeBlocks;

    HashPartitionAccess(int hashTableSize, int blockSize, long rootAddress, FreeBlocks freeBlocks)
    {
        this.hashPartitionMask = hashTableSize;
        this.blockSize = blockSize;
        this.rootAddress = rootAddress;
        this.freeBlocks = freeBlocks;
    }

    long partitionForHash(int hash)
    {
        int partition = hash & hashPartitionMask;
        return rootAddress + partition * blockSize;
    }
}
