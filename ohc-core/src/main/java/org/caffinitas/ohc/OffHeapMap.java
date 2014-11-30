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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.caffinitas.ohc.Constants.*;

final class OffHeapMap
{
    // maximum hash table size
    private static final int MAX_TABLE_SIZE = 1 << 27;
    private static final Logger LOGGER = LoggerFactory.getLogger(OffHeapMap.class);

    private final double entriesPerBucketTrigger;

    private volatile long size;

    // not nice to have a cyclic ref with ShardCacheImpl - but using an interface just for the sake of it feels too heavy
    private final SegmentedCacheImpl cache;

    private volatile Table table;

    private volatile long lruHead;
    private volatile long lruTail;

    private long rehashes;

    OffHeapMap(OHCacheBuilder builder, SegmentedCacheImpl cache)
    {
        this.cache = cache;

        int hts = builder.getHashTableSize();
        if (hts < 8192)
            hts = 8192;
        table = new Table(roundUpToPowerOf2(hts));

        this.entriesPerBucketTrigger = builder.getEntriesPerSegmentTrigger();
    }

    void release()
    {
        table.release();
    }

    long size()
    {
        return size;
    }

    void resetStatistics()
    {
        rehashes = 0L;
    }

    long rehashes()
    {
        return rehashes;
    }

    synchronized long getEntry(long hash, BytesSource keySource)
    {
        int loops = 0;
        for (long hashEntryAdr = table.first(hash);
             hashEntryAdr != 0L;
             hashEntryAdr = HashEntries.getNext(hashEntryAdr), loops++)
        {
            if (notSameKey(hash, keySource, loops, hashEntryAdr))
                continue;

            // return existing entry

            touch(hashEntryAdr);

            HashEntries.reference(hashEntryAdr);

            return hashEntryAdr;
        }

        // not found

        return 0L;
    }

    synchronized long replaceEntry(long hash, BytesSource keySource, long newHashEntryAdr)
    {
        int loops = 0;
        long hashEntryAdr;
        for (hashEntryAdr = table.first(hash);
             hashEntryAdr != 0L;
             hashEntryAdr = HashEntries.getNext(hashEntryAdr), loops++)
        {
            if (notSameKey(hash, keySource, loops, hashEntryAdr))
                continue;

            // replace existing entry

            remove(hash, hashEntryAdr);

            break;
        }

        if (hashEntryAdr == 0L)
            size++;

        add(hash, newHashEntryAdr);

        return hashEntryAdr;
    }

    synchronized void clear()
    {
        lruHead = lruTail = 0L;
        size = 0L;

        long next;
        for (int p = 0; p < table.size(); p++)
            for (long hashEntryAdr = table.first(p);
                 hashEntryAdr != 0L;
                 hashEntryAdr = next)
            {
                next = HashEntries.getNext(hashEntryAdr);

                dereference(hashEntryAdr);
            }

        table.clear();
    }

    synchronized long removeEntry(long hash, BytesSource keySource)
    {
        int loops = 0;
        for (long hashEntryAdr = table.first(hash);
             hashEntryAdr != 0L;
             hashEntryAdr = HashEntries.getNext(hashEntryAdr), loops++)
        {
            if (notSameKey(hash, keySource, loops, hashEntryAdr))
                continue;

            // remove existing entry

            remove(hash, hashEntryAdr);

            size--;

            return hashEntryAdr;
        }

        // no entry to remove

        return 0L;
    }

    private boolean notSameKey(long hash, BytesSource keySource, int loops, long hashEntryAdr)
    {
        if (loops >= entriesPerBucketTrigger)
        {
            LOGGER.warn("Degraded OHC performance! Segment linked list very long - rehash triggered");
            rehash();
        }

        long hashEntryHash = HashEntries.getHash(hashEntryAdr);
        if (hashEntryHash != hash)
            return true;

        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen != keySource.size()
               || !HashEntries.compareKey(hashEntryAdr, keySource, serKeyLen);
    }

    private void rehash()
    {
        Table tab = table;
        int tableSize = tab.size();
        if (tableSize > 1 << 24)
        {
            // already at max hash table size - keep rehashTrigger field true
            return;
        }

        Table newTable = new Table(tableSize * 2);
        long next;

        for (int part = 0; part < tableSize; part++)
            for (long hashEntryAdr = tab.first(part);
                 hashEntryAdr != 0L;
                 hashEntryAdr = next)
            {
                next = HashEntries.getNext(hashEntryAdr);

                HashEntries.setNext(hashEntryAdr, 0L);

                newTable.addLinkAsHead(HashEntries.getHash(hashEntryAdr), hashEntryAdr);
            }

        table = newTable;
        rehashes++;
    }

    static int roundUpToPowerOf2(int number)
    {
        return number >= MAX_TABLE_SIZE
               ? MAX_TABLE_SIZE
               : (number > 1) ? Integer.highestOneBit((number - 1) << 1) : 1;
    }

    static final class Table
    {
        final int mask;
        final long address;

        public Table(int hashTableSize)
        {
            int msz = (int) BUCKET_ENTRY_LEN * hashTableSize;
            this.address = Uns.allocate(msz);
            mask = hashTableSize - 1;
            clear();
        }

        void clear()
        {
            // It's important to initialize the hash table memory.
            // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
            Uns.setMemory(address, 0L, BUCKET_ENTRY_LEN * size(), (byte) 0);
        }

        void release()
        {
            Uns.free(address);
        }

        long first(long hash)
        {
            return Uns.getLongVolatile(address, bucketOffset(hash));
        }

        void first(long hash, long hashEntryAdr)
        {
            Uns.putLongVolatile(address, bucketOffset(hash), hashEntryAdr);
        }

        private long bucketOffset(long hash)
        {
            return bucketIndexForHash(hash) * BUCKET_ENTRY_LEN;
        }

        private int bucketIndexForHash(long hash)
        {
            return (int) (hash & mask);
        }

        void removeLink(long hash, long hashEntryAdr)
        {
            long prev = HashEntries.getPrevious(hashEntryAdr);
            long next = HashEntries.getNext(hashEntryAdr);

            long head = first(hash);
            if (head == hashEntryAdr)
            {
                if (prev != 0L)
                    throw new IllegalStateException("head must not have a previous entry");
                first(hash, next);
            }

            if (prev != 0L)
                HashEntries.setNext(prev, next);
            if (next != 0L)
                HashEntries.setPrevious(next, prev);

            // just for safety
            HashEntries.setPrevious(hashEntryAdr, 0L);
            HashEntries.setNext(hashEntryAdr, 0L);
        }

        void addLinkAsHead(long hash, long hashEntryAdr)
        {
            long head = first(hash);
            HashEntries.setNext(hashEntryAdr, head);
            HashEntries.setPrevious(hashEntryAdr, 0L); // just for safety
            first(hash, hashEntryAdr);
            if (head != 0L)
                HashEntries.setPrevious(head, hashEntryAdr);
        }

        int size()
        {
            return mask + 1;
        }
    }

    //
    // eviction/replacement/cleanup
    //

    private void remove(long hash, long hashEntryAdr)
    {
        table.removeLink(hash, hashEntryAdr);

        // LRU stuff

        long next = lruNext(hashEntryAdr);
        long prev = lruPrev(hashEntryAdr);

        if (lruHead == hashEntryAdr)
            lruHead = next;
        if (lruTail == hashEntryAdr)
            lruTail = prev;

        if (next != 0L)
            lruPrev(next, prev);
        if (prev != 0L)
            lruNext(prev, next);
    }

    private void add(long hash, long hashEntryAdr)
    {
        table.addLinkAsHead(hash, hashEntryAdr);

        // LRU stuff

        long h = lruHead;
        lruNext(hashEntryAdr, h);
        if (h != 0L)
            lruPrev(h, hashEntryAdr);
        lruPrev(hashEntryAdr, 0L);
        lruHead = hashEntryAdr;

        if (lruTail == 0L)
            lruTail = hashEntryAdr;
    }

    private void touch(long hashEntryAdr)
    {
        if (lruHead == hashEntryAdr)
            // short-cut - entry already at LRU head
            return;

        // LRU stuff (basically a remove from LRU linked list)

        long next = lruNext(hashEntryAdr);
        long prev = lruPrev(hashEntryAdr);

        if (lruTail == hashEntryAdr)
            lruTail = prev;

        if (next != 0L)
            lruPrev(next, prev);
        if (prev != 0L)
            lruNext(prev, next);

        // LRU stuff (basically an add to LRU linked list)

        long head = lruHead;
        lruNext(hashEntryAdr, head);
        if (head != 0L)
            lruPrev(head, hashEntryAdr);
        lruPrev(hashEntryAdr, 0L);
        lruHead = hashEntryAdr;

        if (lruTail == 0L)
            lruTail = hashEntryAdr;
    }

    private long lruNext(long hashEntryAdr)
    {
        return HashEntries.getLRUNext(hashEntryAdr);
    }

    private long lruPrev(long hashEntryAdr)
    {
        return HashEntries.getLRUPrev(hashEntryAdr);
    }

    private void lruNext(long hashEntryAdr, long next)
    {
        HashEntries.setLRUNext(hashEntryAdr, next);
    }

    private void lruPrev(long hashEntryAdr, long prev)
    {
        HashEntries.setLRUPrev(hashEntryAdr, prev);
    }

    synchronized long cleanUp(long recycleGoal)
    {
        long prev;
        long evicted = 0L;
        for (long hashEntryAdr = lruTail;
             hashEntryAdr != 0L && recycleGoal > 0L;
             hashEntryAdr = prev)
        {
            prev = lruPrev(hashEntryAdr);

            long bytes = HashEntries.getAllocLen(hashEntryAdr);
            long hash = HashEntries.getHash(hashEntryAdr);
            remove(hash, hashEntryAdr);

            dereference(hashEntryAdr);

            size--;

            recycleGoal -= bytes;

            evicted++;
        }

        return evicted;
    }

    private void dereference(long hashEntryAdr)
    {
        if (HashEntries.dereference(hashEntryAdr))
            cache.free(hashEntryAdr);
    }
}
