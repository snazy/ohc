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
import java.util.concurrent.atomic.AtomicLong;

final class FreeBlocks
{
    static final class FreeList
    {
        private volatile long freeBlockHead;
        private volatile long freeListLock;
        // pad to 64 bytes (assuming 16 byte object header)
        private volatile long pad2;
        private volatile long pad3;
        private volatile long pad4;
        private volatile long pad5;

        private static final long freeListLockOffset = Uns.fieldOffset(FreeList.class, "freeListLock");

        boolean tryLock()
        {
            return Uns.compareAndSwap(this, freeListLockOffset, 0L, Thread.currentThread().getId());
        }

        void unlock()
        {
            Uns.putLong(this, freeListLockOffset, 0L);
        }

        public boolean empty()
        {
            return freeBlockHead == 0L;
        }

        void push(long adr)
        {
            Uns.putLong(adr, freeBlockHead);
            freeBlockHead = adr;
        }

        long pull()
        {
            long blockAddress = freeBlockHead;
            if (blockAddress == 0L)
                return 0L;

            freeBlockHead = Uns.getLong(blockAddress);

            return blockAddress;
        }

        int calcFreeBlockCount()
        {
            int free = 0;
            for (long adr = freeBlockHead; adr != 0L; adr = Uns.getLong(adr))
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

    private final AtomicInteger freeListPtr = new AtomicInteger();
    private final AtomicLong freeBlockSpins = new AtomicLong();

    // TODO add more independent free-block lists to reduce concurrency issues during block allocation and replace
    // synchronized with something better

    FreeBlocks(long firstFreeBlockAddress, long firstNonUsableAddress, int blockSize)
    {
        int fli = 0;
        for (long adr = firstFreeBlockAddress; adr < firstNonUsableAddress; adr += blockSize)
        {
            Uns.putLong(adr, 0L);

            freeLists[fli & MASK].push(adr);
            fli++;
        }
    }

    // TODO add some inexpensive functionality to allocate multiple blocks at once

    synchronized long allocateBlock()
    {
        int fli = freeListPtr.getAndIncrement();
        long adr;
        while (true)
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
                        adr = fl.pull();
                        if (adr != 0L)
                            return adr;
                    }
                    finally
                    {
                        fl.unlock();
                    }
            }
            if (none)
                return 0L;
        }
    }

    void freeBlock(long adr)
    {
        if (adr == 0L)
            return;

        int fli = freeListPtr.getAndIncrement();
        while (true)
        {
            for (int i = 0; i < freeLists.length; i++, fli++)
            {
                FreeList fl = freeLists[fli & MASK];

                if (fl.tryLock())
                    try
                    {
                        fl.push(adr);
                        return;
                    }
                    finally
                    {
                        fl.unlock();
                    }
            }
            freeBlockSpins.incrementAndGet();
            Thread.yield();
        }
    }

    // TODO add functionality to free a complete hash-entry-chain at once

    synchronized int calcFreeBlockCount()
    {
        int free = 0;
        for (FreeList freeList : freeLists)
        {
            while (!freeList.tryLock())
                Thread.yield();
            try
            {
                free += freeList.calcFreeBlockCount();
            }
            finally
            {
                freeList.unlock();
            }
        }
        return free;
    }

    long getFreeBlockSpins()
    {
        return freeBlockSpins.get();
    }
}
