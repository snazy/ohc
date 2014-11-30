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

import static org.caffinitas.ohc.Constants.BUCKET_ENTRY_LEN;

final class OffHeapMap
{
    // maximum hash table size
    private static final int MAX_TABLE_SIZE = 1 << 27;

    private final long capacity;
    private long freeCapacity;
    private final long cleanUpTriggerFree;
    private final long cleanUpTargetFree;

    private Table table;
    private long size;
    private long threshold;
    private final double loadFactor;

    private long lruHead;
    private long lruTail;

    private long rehashes;
    private long cleanUpCount;
    private long evictedEntries;

    OffHeapMap(OHCacheBuilder builder, long capacity, long cleanUpTriggerFree, long cleanUpTargetFree)
    {
        this.capacity = capacity;
        this.freeCapacity = capacity;
        this.cleanUpTriggerFree = cleanUpTriggerFree;
        this.cleanUpTargetFree = cleanUpTargetFree;

        int hts = builder.getHashTableSize();
        if (hts <= 0)
            hts = 8192;
        if (hts < 256)
            hts = 256;
        table = new Table(roundUpToPowerOf2(hts));

        double lf = builder.getLoadFactor();
        if (lf <= .0d)
            lf = .75d;
        this.loadFactor = lf;
        threshold = (long) ((double) table.size() * loadFactor);
    }

    void release()
    {
        table.release();
    }

    long size()
    {
        return size;
    }

    long capacity()
    {
        return capacity;
    }

    long freeCapacity()
    {
        return freeCapacity;
    }

    void resetStatistics()
    {
        rehashes = 0L;
        cleanUpCount = 0L;
        evictedEntries = 0L;
    }

    long rehashes()
    {
        return rehashes;
    }

    long cleanUpCount()
    {
        return cleanUpCount;
    }

    long evictedEntries()
    {
        return evictedEntries;
    }

    synchronized long getEntry(KeyBuffer key)
    {
        for (long hashEntryAdr = table.first(key.hash());
             hashEntryAdr != 0L;
             hashEntryAdr = HashEntries.getNext(hashEntryAdr))
        {
            if (notSameKey(key, hashEntryAdr))
                continue;

            // return existing entry

            touch(hashEntryAdr);

            HashEntries.reference(hashEntryAdr);

            return hashEntryAdr;
        }

        // not found

        return 0L;
    }

    synchronized boolean replaceEntry(KeyBuffer key, long newHashEntryAdr, long bytes)
    {
        if (freeCapacity - bytes < cleanUpTriggerFree)
            cleanUp();

        freeCapacity -= bytes;

        long hashEntryAdr;
        for (hashEntryAdr = table.first(key.hash());
             hashEntryAdr != 0L;
             hashEntryAdr = HashEntries.getNext(hashEntryAdr))
        {
            if (notSameKey(key, hashEntryAdr))
                continue;

            // replace existing entry

            remove(hashEntryAdr);
            dereference(hashEntryAdr);

            break;
        }

        if (hashEntryAdr == 0L)
        {
            if (size >= threshold)
                rehash();

            size++;
        }

        add(newHashEntryAdr);

        return hashEntryAdr == 0L;
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

    synchronized boolean removeEntry(KeyBuffer key)
    {
        for (long hashEntryAdr = table.first(key.hash());
             hashEntryAdr != 0L;
             hashEntryAdr = HashEntries.getNext(hashEntryAdr))
        {
            if (notSameKey(key, hashEntryAdr))
                continue;

            // remove existing entry

            remove(hashEntryAdr);
            dereference(hashEntryAdr);

            size--;

            return true;
        }

        // no entry to remove

        return false;
    }

    private boolean notSameKey(KeyBuffer key, long hashEntryAdr)
    {
        long hashEntryHash = HashEntries.getHash(hashEntryAdr);
        if (hashEntryHash != key.hash())
            return true;

        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen != key.size()
               || !HashEntries.compareKey(hashEntryAdr, key, serKeyLen);
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

        threshold = (long) ((double) newTable.size() * loadFactor);
        table = newTable;
        rehashes++;
    }

    static int roundUpToPowerOf2(int number)
    {
        return number >= MAX_TABLE_SIZE
               ? MAX_TABLE_SIZE
               : (number > 1) ? Integer.highestOneBit((number - 1) << 1) : 1;
    }

    synchronized long[] hotN(int n)
    {
        long[] r = new long[n];
        int i = 0;
        for (long hashEntryAdr = lruHead;
             hashEntryAdr != 0L;
             hashEntryAdr = lruNext(hashEntryAdr))
        {
            r[i++] = hashEntryAdr;
            HashEntries.reference(hashEntryAdr);
        }
        return r;
    }

    double loadFactor()
    {
        return loadFactor;
    }

    int hashTableSize()
    {
        return table.size();
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

    private void remove(long hashEntryAdr)
    {
        long hash = HashEntries.getHash(hashEntryAdr);

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

    private void add(long hashEntryAdr)
    {
        long hash = HashEntries.getHash(hashEntryAdr);

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

    synchronized void freed(long bytes)
    {
        freeCapacity += bytes;
    }

    synchronized void cleanUp()
    {
        long recycleGoal = cleanUpTargetFree - freeCapacity;
        if (recycleGoal <= 0L)
            recycleGoal = 1L;

        long prev;
        long evicted = 0L;
        for (long hashEntryAdr = lruTail;
             hashEntryAdr != 0L && recycleGoal > 0L;
             hashEntryAdr = prev)
        {
            prev = lruPrev(hashEntryAdr);

            long bytes = HashEntries.getAllocLen(hashEntryAdr);

            remove(hashEntryAdr);
            dereference(hashEntryAdr);

            size--;

            recycleGoal -= bytes;

            evicted++;
        }

        cleanUpCount++;
        evictedEntries += evicted;
    }

    private void dereference(long hashEntryAdr)
    {
        if (HashEntries.dereference(hashEntryAdr))
        {
            long bytes = HashEntries.getAllocLen(hashEntryAdr);
            if (bytes == 0L)
                throw new IllegalStateException();

            Uns.free(hashEntryAdr);

            freeCapacity += bytes;
        }
    }
}
