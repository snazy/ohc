package org.caffinitas.ohc;

public class OHCacheBuilder
{
    private int blockSize = 2048;
    private int hashTableSize = 256;
    private long totalCapacity = 64L*1024L*1024L;

    private OHCacheBuilder()
    {
    }

    public static OHCacheBuilder newBuilder()
    {
        return new OHCacheBuilder();
    }

    public int getBlockSize()
    {
        return blockSize;
    }

    public OHCacheBuilder setBlockSize(int blockSize)
    {
        this.blockSize = blockSize;
        return this;
    }

    public int getHashTableSize()
    {
        return hashTableSize;
    }

    public OHCacheBuilder setHashTableSize(int hashTableSize)
    {
        this.hashTableSize = hashTableSize;
        return this;
    }

    public long getTotalCapacity()
    {
        return totalCapacity;
    }

    public OHCacheBuilder setTotalCapacity(long totalCapacity)
    {
        this.totalCapacity = totalCapacity;
        return this;
    }

    public OHCache build()
    {
        return new OHCacheImpl(this);
    }
}
