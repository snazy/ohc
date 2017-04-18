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
package org.caffinitas.ohc.linked;

import org.caffinitas.ohc.OHCacheBuilder;

class OffHeapLinkedLRUMap extends OffHeapLinkedMap
{
    private long lruHead;
    private long lruTail;

    private long freeCapacity;
    private long capacity;

    OffHeapLinkedLRUMap(OHCacheBuilder builder, long freeCapacity)
    {
        super(builder);

        this.freeCapacity = freeCapacity;
        this.capacity = freeCapacity;
    }

    void addToLruAndUpdateCapacity(long hashEntryAdr)
    {
        long h = lruHead;
        HashEntries.setLRUNext(hashEntryAdr, h);
        if (h != 0L)
            HashEntries.setLRUPrev(h, hashEntryAdr);
        HashEntries.setLRUPrev(hashEntryAdr, 0L);
        lruHead = hashEntryAdr;

        if (lruTail == 0L)
            lruTail = hashEntryAdr;

        freeCapacity -= HashEntries.getAllocLen(hashEntryAdr);
    }

    void removeFromLruAndUpdateCapacity(long hashEntryAdr)
    {
        long next = HashEntries.getLRUNext(hashEntryAdr);
        long prev = HashEntries.getLRUPrev(hashEntryAdr);

        if (lruHead == hashEntryAdr)
            lruHead = next;
        if (lruTail == hashEntryAdr)
            lruTail = prev;

        if (next != 0L)
            HashEntries.setLRUPrev(next, prev);
        if (prev != 0L)
            HashEntries.setLRUNext(prev, next);

        freeCapacity += HashEntries.getAllocLen(hashEntryAdr);
    }

    void replaceSentinelInLruAndUpdateCapacity(long hashEntryAdr, long newHashEntryAdr, long bytes)
    {
        // TODO also handle the case, that the sentinel has been evicted

        long next = HashEntries.getLRUNext(hashEntryAdr);
        long prev = HashEntries.getLRUPrev(hashEntryAdr);

        HashEntries.setLRUNext(newHashEntryAdr, next);
        HashEntries.setLRUPrev(newHashEntryAdr, prev);

        if (lruHead == hashEntryAdr)
            lruHead = newHashEntryAdr;
        if (lruTail == hashEntryAdr)
            lruTail = newHashEntryAdr;

        if (next != 0L)
            HashEntries.setLRUPrev(next, newHashEntryAdr);
        if (prev != 0L)
            HashEntries.setLRUNext(prev, newHashEntryAdr);

        // note: only need to add bytes since this method only replaces a sentinel with the real value
        freeCapacity -= bytes;
    }

    void clearLruAndCapacity()
    {
        lruHead = lruTail = 0L;

        freeCapacity = capacity;
    }

    void touch(long hashEntryAdr)
    {
        long head = lruHead;

        if (head == hashEntryAdr)
            // short-cut - entry already at LRU head
            return;

        // LRU stuff

        long next = HashEntries.getAndSetLRUNext(hashEntryAdr, head);
        long prev = HashEntries.getAndSetLRUPrev(hashEntryAdr, 0L);

        long tail = lruTail;
        if (tail == hashEntryAdr)
            lruTail = prev == 0L ? hashEntryAdr : prev;
        else if (tail == 0L)
            lruTail = hashEntryAdr;

        if (next != 0L)
            HashEntries.setLRUPrev(next, prev);
        if (prev != 0L)
            HashEntries.setLRUNext(prev, next);

        // LRU stuff (basically an add to LRU linked list)

        if (head != 0L)
            HashEntries.setLRUPrev(head, hashEntryAdr);
        lruHead = hashEntryAdr;
    }

    long freeCapacity()
    {
        return freeCapacity;
    }

    void updateFreeCapacity(long diff)
    {
        boolean wasFirst = lock();
        try
        {
            freeCapacity += diff;
        }
        finally
        {
            unlock(wasFirst);
        }
    }

    LongArrayList ensureFreeSpaceForNewEntry(long bytes)
    {
        if (freeCapacity < bytes)
            removeExpired();

        LongArrayList derefList = null;
        while (freeCapacity < bytes)
        {
            long eldestHashAdr = removeEldest();
            if (eldestHashAdr == 0L)
                break;
            if (derefList == null)
                derefList = new LongArrayList();
            derefList.add(eldestHashAdr);
        }
        return derefList;
    }

    boolean hasFreeSpaceForNewEntry(long bytes)
    {
        return freeCapacity >= bytes;
    }

    private long removeEldest()
    {
        long hashEntryAdr = lruTail;
        if (hashEntryAdr == 0L)
            return 0L;

        removeInternal(hashEntryAdr, -1L, true);
        size--;
        evictedEntries++;

        return hashEntryAdr;
    }

    long[] hotN(int n)
    {
        boolean wasFirst = lock();
        try
        {
            long[] r = new long[n];
            int i = 0;
            for (long hashEntryAdr = lruHead;
                 hashEntryAdr != 0L && i < n;
                 hashEntryAdr = HashEntries.getLRUNext(hashEntryAdr))
            {
                r[i++] = hashEntryAdr;
                HashEntries.reference(hashEntryAdr);
            }
            return r;
        }
        finally
        {
            unlock(wasFirst);
        }
    }
}
