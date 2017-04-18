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

final class OffHeapLinkedWTinyLFUMap extends OffHeapLinkedMap
{
    private long edenLruHead;
    private long edenLruTail;
    private long edenFreeCapacity;
    private long edenCapacity;

    private long mainLruHead;
    private long mainLruTail;
    private long mainFreeCapacity;
    private long mainCapacity;

    private long probationCapacity;

    private FrequencySketch frequencySketch;

    private final double edenSize;

    OffHeapLinkedWTinyLFUMap(OHCacheBuilder builder, long freeCapacity)
    {
        super(builder);

        edenSize = builder.getEdenSize();
        if (edenSize <= 0d)
            throw new IllegalArgumentException("Illegal edenSize, must be > 0");

        updateFreeCapacity(freeCapacity);

        int freqSketchSize = builder.getFrequencySketchSize();
        if (freqSketchSize <= 0)
            freqSketchSize = table.size();

        frequencySketch = new FrequencySketch(freqSketchSize);
    }

    void release()
    {
        boolean wasFirst = lock();
        try
        {
            frequencySketch.release();

            super.release();
        }
        finally
        {
            unlock(wasFirst);
        }
    }

    long freeCapacity()
    {
        return edenFreeCapacity + mainFreeCapacity;
    }

    void updateFreeCapacity(long diff)
    {
        long edenPart = (long)(edenSize * diff);
        long mainPart = diff - edenPart;

        edenFreeCapacity += edenPart;
        edenCapacity += edenPart;
        mainFreeCapacity += mainPart;
        mainCapacity += mainPart;

        // capacity of probation area needs to be as big as eden because when a new entry is added, it might be
        // necessary to check all entries in the whole eden generation whether these are admitted for main generation.
        probationCapacity = (long)(edenSize * mainCapacity);
    }

    LongArrayList ensureFreeSpaceForNewEntry(long bytes)
    {
        // enough free capacity in eden generation?
        if (edenFreeCapacity >= bytes)
            return null;

        // eden generation too small for entry?
        if (edenCapacity < bytes)
            return null;

        // need to make room in eden

        long candidateAdr = edenLruTail;
        long nextCandidateAdr;

        if (bytes > edenFreeCapacity)
        {
            // main generation has enough free capacity for candidate from eden, just move candidate to main generation and return
            // (note: this happens when the cache is initially empty and new entries get added)
            if (mainLruTail == 0L || mainFreeCapacity >= bytes)
            {
                moveCandidateFromEdenToMain(candidateAdr);
                return null;
            }
        }

        // status: main generation has not enough room - need to check entries in eden generation against entries in
        // probation area.

        long victimAdr = mainLruTail;
        long nextVictimAdr;


        // TODO following code compares one entry in eden with one entry in probation (starting at the tail).
        // It feels that it is ok to do it like that - but not sure.

        LongArrayList derefList = null;
        long probationUsed = probationUsed();
        for (;bytes > edenFreeCapacity && probationUsed > 0L; candidateAdr = nextCandidateAdr, victimAdr = nextVictimAdr)
        {
            if (candidateAdr == 0L)
                throw new AssertionError();
            if (victimAdr == 0L)
                throw new AssertionError();

            nextCandidateAdr = HashEntries.getLRUPrev(candidateAdr);
            nextVictimAdr = HashEntries.getLRUPrev(victimAdr);

            int candidateFreq = frequencySketch.frequency(HashEntries.getHash(candidateAdr));
            int victimFreq = frequencySketch.frequency(HashEntries.getHash(victimAdr));

            probationUsed -= HashEntries.getAllocLen(victimAdr);

            if (admit(candidateFreq, victimFreq))
            {
                // evict victim from main generation & move candidate to main generation

                derefList = evictEntry(derefList, victimAdr);

                moveCandidateFromEdenToMain(candidateAdr);
            }
            else
            {
                // candidate not admitted, evict it

                derefList = evictEntry(derefList, candidateAdr);
            }
        }

        return derefList;
    }

    boolean hasFreeSpaceForNewEntry(long bytes)
    {
        return edenFreeCapacity >= bytes;
    }

    private LongArrayList evictEntry(LongArrayList derefList, long targetAdr)
    {
        removeInternal(targetAdr, -1L, true);
        size--;
        evictedEntries++;
        if (derefList == null)
            derefList = new LongArrayList();
        derefList.add(targetAdr);
        return derefList;
    }

    private void moveCandidateFromEdenToMain(long candidateAdr)
    {
        removeFromLruAndUpdateCapacity(candidateAdr);
        HashEntries.setGeneration(candidateAdr, Util.GEN_MAIN);
        addToLruAndUpdateCapacity(candidateAdr);
    }

    private long probationUsed()
    {
        //                                          Split           MainCapacity
        //              protected                     |    probation    |
        // -------------------------------------------|-----------------|
        //
        //
        //long protectedSize = mainCapacity - probationCapacity;
        //long mainUsed = mainCapacity - mainFreeCapacity;
        //long probationUsed = mainUsed - protectedSize;
        // -->                 (mainCapacity - mainFreeCapacity) - (mainCapacity - probationCapacity)
        // -->                 mainCapacity - mainFreeCapacity - mainCapacity + probationCapacity
        // -->                 - mainFreeCapacity + probationCapacity
        return - mainFreeCapacity + probationCapacity;
    }

    private boolean admit(int candidateFreq, int victimFreq)
    {
        if (candidateFreq > victimFreq) {
            return true;
        } else if (candidateFreq <= 5) {
            // The maximum frequency is 15 and halved to 7 after a reset to age the history. An attack
            // exploits that a hot candidate is rejected in favor of a hot victim. The threshold of a warm
            // candidate reduces the number of random acceptances to minimize the impact on the hit rate.
            return false;
        }

        // only admit a small number of entries to pass admission filter on a candidate/victim frequency tie
        return frequencySketch.tieAdmit();
    }

    void addToLruAndUpdateCapacity(long hashEntryAdr)
    {
        int gen = HashEntries.getGeneration(hashEntryAdr);

        long h = lruHead(gen);
        HashEntries.setLRUNext(hashEntryAdr, h);
        if (h != 0L)
            HashEntries.setLRUPrev(h, hashEntryAdr);
        HashEntries.setLRUPrev(hashEntryAdr, 0L);
        lruHead(gen, hashEntryAdr);

        if (lruTail(gen) == 0L)
            lruTail(gen, hashEntryAdr);

        adjustFreeCapacity(gen, -HashEntries.getAllocLen(hashEntryAdr));
    }

    void removeFromLruAndUpdateCapacity(long hashEntryAdr)
    {
        int gen = HashEntries.getGeneration(hashEntryAdr);

        long next = HashEntries.getLRUNext(hashEntryAdr);
        long prev = HashEntries.getLRUPrev(hashEntryAdr);

        if (lruHead(gen) == hashEntryAdr)
            lruHead(gen, next);
        if (lruTail(gen) == hashEntryAdr)
            lruTail(gen, prev);

        if (next != 0L)
            HashEntries.setLRUPrev(next, prev);
        if (prev != 0L)
            HashEntries.setLRUNext(prev, next);

        adjustFreeCapacity(gen, HashEntries.getAllocLen(hashEntryAdr));
    }

    void clearLruAndCapacity()
    {
        edenLruHead = edenLruTail =
        mainLruHead = mainLruTail = 0L;

        edenFreeCapacity = edenCapacity;
        mainFreeCapacity = mainCapacity;
    }

    long[] hotN(int n)
    {
        boolean wasFirst = lock();
        try
        {
            long[] r = new long[n];
            int i = 0;
            for (long hashEntryAdr = mainLruHead;
                 hashEntryAdr != 0L && i < n;
                 hashEntryAdr = HashEntries.getLRUNext(hashEntryAdr))
            {
                r[i++] = hashEntryAdr;
                HashEntries.reference(hashEntryAdr);
            }
            for (long hashEntryAdr = edenLruHead;
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

    void replaceSentinelInLruAndUpdateCapacity(long hashEntryAdr, long newHashEntryAdr, long bytes)
    {
        // TODO also handle the case, that the sentinel has been evicted

        int gen = HashEntries.getGeneration(hashEntryAdr);

        HashEntries.setGeneration(newHashEntryAdr, gen);

        long next = HashEntries.getLRUNext(hashEntryAdr);
        long prev = HashEntries.getLRUPrev(hashEntryAdr);

        HashEntries.setLRUNext(newHashEntryAdr, next);
        HashEntries.setLRUPrev(newHashEntryAdr, prev);

        if (lruHead(gen) == hashEntryAdr)
            lruHead(gen, newHashEntryAdr);
        if (lruTail(gen) == hashEntryAdr)
            lruTail(gen, newHashEntryAdr);

        if (next != 0L)
            HashEntries.setLRUPrev(next, newHashEntryAdr);
        if (prev != 0L)
            HashEntries.setLRUNext(prev, newHashEntryAdr);

        // note: only need to add bytes since this method only replaces a sentinel with the real value
        adjustFreeCapacity(gen, -bytes);
    }

    void touch(long hashEntryAdr)
    {
        frequencySketch.increment(HashEntries.getHash(hashEntryAdr));

        int gen = HashEntries.getGeneration(hashEntryAdr);

        long head = lruHead(gen);

        if (head == hashEntryAdr)
            // short-cut - entry already at LRU head
            return;

        // LRU stuff

        long next = HashEntries.getAndSetLRUNext(hashEntryAdr, head);
        long prev = HashEntries.getAndSetLRUPrev(hashEntryAdr, 0L);

        long tail = lruTail(gen);
        if (tail == hashEntryAdr)
            lruTail(gen, prev == 0L ? hashEntryAdr : prev);
        else if (tail == 0L)
            lruTail(gen, hashEntryAdr);

        if (next != 0L)
            HashEntries.setLRUPrev(next, prev);
        if (prev != 0L)
            HashEntries.setLRUNext(prev, next);

        // LRU stuff (basically an add to LRU linked list)

        if (head != 0L)
            HashEntries.setLRUPrev(head, hashEntryAdr);
        lruHead(gen, hashEntryAdr);

    }

    private void adjustFreeCapacity(int gen, long amount)
    {
        switch (gen)
        {
            case Util.GEN_EDEN: edenFreeCapacity += amount; break;
            case Util.GEN_MAIN: mainFreeCapacity += amount; break;
        }
    }

    private long lruHead(int gen)
    {
        switch (gen)
        {
            case Util.GEN_EDEN: return edenLruHead;
            case Util.GEN_MAIN: return mainLruHead;
        }
        return 0L;
    }

    private void lruHead(int gen, long hashEntryAdr)
    {
        switch (gen)
        {
            case Util.GEN_EDEN: edenLruHead = hashEntryAdr; break;
            case Util.GEN_MAIN: mainLruHead = hashEntryAdr; break;
        }
    }

    private long lruTail(int gen)
    {
        switch (gen)
        {
            case Util.GEN_EDEN: return edenLruTail;
            case Util.GEN_MAIN: return mainLruTail;
        }
        return 0L;
    }

    private void lruTail(int gen, long hashEntryAdr)
    {
        switch (gen)
        {
            case Util.GEN_EDEN: edenLruTail = hashEntryAdr; break;
            case Util.GEN_MAIN: mainLruTail = hashEntryAdr; break;
        }
    }
}
