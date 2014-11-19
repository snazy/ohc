package org.caffinitas.ohc;

final class FreeBlocks
{
    private volatile long freeBlockHead;

    // TODO add more independent free-block lists to reduce concurrency issues during block allocation and replace
    // synchronized with something better

    FreeBlocks(long firstFreeBlockAddress, long firstNonUsableAddress, int blockSize)
    {
        this.freeBlockHead = firstFreeBlockAddress;

        for (long adr = firstFreeBlockAddress; adr != 0L; )
        {
            long next = adr + blockSize;
            if (next > firstNonUsableAddress)
                throw new InternalError();
            if (next == firstNonUsableAddress)
                next = 0L;
            Uns.putLong(adr, next);

            adr = next;
        }
    }

    synchronized long allocateBlock()
    {
        long blockAddress = freeBlockHead;
        if (blockAddress == 0L)
            return 0L;

        freeBlockHead = Uns.getLong(blockAddress);

        return blockAddress;
    }

    synchronized void freeBlock(long adr)
    {
        Uns.putLong(adr, freeBlockHead);
        freeBlockHead = adr;
    }

    synchronized int calcFreeBlockCount()
    {
        int free = 0;
        for (long adr = freeBlockHead; adr != 0L; adr = Uns.getLong(adr))
            free++;
        return free;
    }
}
