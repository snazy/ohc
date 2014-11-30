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
package org.caffinitas.ohc.replacement;

import org.caffinitas.ohc.HashEntries;

public final class LRUReplacementStrategy implements ReplacementStrategy
{
    private volatile long head;
    private volatile long tail;

    private void removeLRU(long hashEntryAdr)
    {
        long next = lruNext(hashEntryAdr);
        long prev = lruPrev(hashEntryAdr);

        if (head == hashEntryAdr)
            head = next;
        if (tail == hashEntryAdr)
            tail = prev;

        if (next != 0L)
            lruPrev(next, prev);
        if (prev != 0L)
            lruNext(prev, next);
    }

    private void addLRU(long hashEntryAdr)
    {
        long h = head;
        lruNext(hashEntryAdr, h);
        if (h != 0L)
            lruPrev(h, hashEntryAdr);
        lruPrev(hashEntryAdr, 0L);
        head = hashEntryAdr;

        if (tail == 0L)
            tail = hashEntryAdr;
    }

    private long lruNext(long hashEntryAdr)
    {
        return HashEntries.getReplacement0(hashEntryAdr);
    }

    private long lruPrev(long hashEntryAdr)
    {
        return HashEntries.getReplacement1(hashEntryAdr);
    }

    private void lruNext(long hashEntryAdr, long next)
    {
        HashEntries.setReplacement0(hashEntryAdr, next);
    }

    private void lruPrev(long hashEntryAdr, long prev)
    {
        HashEntries.setReplacement1(hashEntryAdr, prev);
    }

    public void entryUsed(long hashEntryAdr)
    {
        removeLRU(hashEntryAdr);
        addLRU(hashEntryAdr);
    }

    public void entryReplaced(long oldHashEntryAdr, long hashEntryAdr)
    {
        if (oldHashEntryAdr != 0L)
            removeLRU(oldHashEntryAdr);
        addLRU(hashEntryAdr);
    }

    public void entryRemoved(long hashEntryAdr)
    {
        removeLRU(hashEntryAdr);
    }

    public long cleanUp(long recycleGoal, ReplacementCallback cb)
    {
        long prev;
        long evicted = 0L;
        for (long hashEntryAdr = tail;
             hashEntryAdr != 0L && recycleGoal > 0L;
             hashEntryAdr = prev)
        {
            prev = lruPrev(hashEntryAdr);

            removeLRU(hashEntryAdr);

            long bytes = cb.evict(hashEntryAdr);

            recycleGoal -= bytes;

            evicted ++;
        }

        return evicted;
    }
}
