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
