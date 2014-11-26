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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

final class DataMemoryFixedBlockSize extends DataMemory
{
    private static final long freeListLockOffset = Uns.fieldOffset(FreeList.class, "freeListLock");

    final class FreeList
    {
        private volatile long freeBlockHead;
        private volatile long freeListLock;
        // pad to 64 bytes (assuming 16 byte object header)
        private volatile long pad2;
        private volatile long pad3;
        private volatile long pad4;
        private volatile long pad5;

        boolean tryLock()
        {
            return Uns.compareAndSwap(this, freeListLockOffset, 0L, Thread.currentThread().getId());
        }

        void unlock()
        {
            Uns.putLongVolatile(this, freeListLockOffset, 0L);
        }

        public boolean empty()
        {
            return freeBlockHead == 0L;
        }

        int pushChain(long adr)
        {
            long root = adr;
            for (int cnt = 1; ; cnt++)
            {
                long next = Uns.getLongVolatile(adr);
                if (next == 0L)
                {
                    Uns.putLongVolatile(adr, freeBlockHead);
                    freeBlockHead = root;
                    return cnt;
                }
                adr = next;
            }
        }

        void push(long adr)
        {
            Uns.putLongVolatile(adr, freeBlockHead);
            freeBlockHead = adr;
        }

        long pull()
        {
            long adr = freeBlockHead;
            if (adr == 0L)
                return 0L;

            freeBlockHead = Uns.getLongVolatile(adr);

            return adr;
        }

        int calcFreeBlockCount()
        {
            int free = 0;
            for (long adr = freeBlockHead; adr != 0L; adr = Uns.getLongVolatile(adr))
                free++;
            return free;
        }
    }

    private static final int MASK = 7;
    private final FreeList[] freeLists = new FreeList[]{
                                                       new FreeList(),
                                                       new FreeList(),
                                                       new FreeList(),
                                                       new FreeList(),
                                                       new FreeList(),
                                                       new FreeList(),
                                                       new FreeList(),
                                                       new FreeList()
    };

    private final long address;
    private final AtomicInteger freeListPtr = new AtomicInteger();
    private final LongAdder freeBlockSpins = new LongAdder();
    private final long firstBlockDataSpace;
    private final long nextBlockDataSpace;
    private final long blockSize;

    DataMemoryFixedBlockSize(long capacity, int blockSize, long cleanUpTriggerMinFree)
    {
        super(capacity, cleanUpTriggerMinFree);
        this.address = Uns.allocate(capacity);
        this.blockSize = blockSize;
        this.firstBlockDataSpace = blockSize - 64;
        this.nextBlockDataSpace = blockSize - 8;

        int fli = 0;
        for (long adr = address; adr < address + capacity; adr += blockSize)
        {
            Uns.putLongVolatile(adr, 0L);

            freeLists[fli & MASK].push(adr);
            fli++;
        }
    }

    DataManagement getDataManagement()
    {
        return DataManagement.FIXED_BLOCKS;
    }

    long blockSize()
    {
        return blockSize;
    }

    void close()
    {
        free(address);
    }

    private int calcRequiredNumberOfBlocks(long bytes)
    {
        bytes -= firstBlockDataSpace;
        if (bytes <= 0L)
            return 1;

        int blk = (int) (1L + bytes / nextBlockDataSpace);

        return (bytes % nextBlockDataSpace) != 0
               ? blk + 1
               : blk;
    }

    long allocate(long bytes)
    {
        int requiredBlocks = calcRequiredNumberOfBlocks(bytes);
        if (requiredBlocks < 1)
            throw new InternalError();
        long blockBytes = blockSize * requiredBlocks;

        int fli = freeListPtr.getAndIncrement();
        long lastAlloc = 0L;
        for (int spin = 0; ; spin++)
        {
            boolean none = true;
            for (int i = 0; i < freeLists.length; i++, fli++)
            {
                FreeList fl = freeLists[fli & MASK];

                if (fl.empty())
                    continue;
                none = false;

                if (fl.tryLock())
                    try
                    {
                        while (requiredBlocks > 0)
                        {
                            long adr = fl.pull();
                            if (adr != 0L)
                            {
                                Uns.putLongVolatile(adr, lastAlloc);
                                lastAlloc = adr;
                                if (--requiredBlocks == 0)
                                {
                                    freeCapacity.add(-blockBytes);

                                    return lastAlloc;
                                }
                            }
                            else
                                break;
                        }
                    }
                    finally
                    {
                        fl.unlock();
                    }
            }
            if (none)
            {
                if (lastAlloc != 0L)
                    freeInternal(lastAlloc);
                return 0L;
            }

            Uns.park(((spin & 3) + 1) * 5000);
        }
    }

    long free(long adr)
    {
        long bytes = freeInternal(adr);
        freeCapacity.add(bytes);
        return bytes;
    }

    private long freeInternal(long adr)
    {
        if (adr == 0L)
            return 0L;

        int fli = freeListPtr.getAndIncrement();
        for (int spin = 0; ; spin++)
        {
            for (int i = 0; i < freeLists.length; i++, fli++)
            {
                FreeList fl = freeLists[fli & MASK];

                if (fl.tryLock())
                    try
                    {
                        int blocks = fl.pushChain(adr);
                        return blockSize * blocks;
                    }
                    finally
                    {
                        fl.unlock();
                    }
            }

            freeBlockSpins.increment();
            Uns.park(((spin & 3) + 1) * 5000);
        }
    }

    int[] calcFreeBlockCounts()
    {
        int[] free = new int[freeLists.length];
        for (int i = 0; i < free.length; i++)
        {
            FreeList freeList = freeLists[i];
            for (int spin = 0; !freeList.tryLock(); spin++)
                Uns.park(((spin & 3) + 1) * 5000);

            try
            {
                free[i] = freeList.calcFreeBlockCount();
            }
            finally
            {
                freeList.unlock();
            }
        }
        return free;
    }

    long getFreeListSpins()
    {
        return freeBlockSpins.longValue();
    }
}
