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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.caffinitas.ohc.histo.HistogramBuilder;

import static org.caffinitas.ohc.Util.BUCKET_ENTRY_LEN;
import static org.caffinitas.ohc.Util.ENTRY_OFF_DATA;
import static org.caffinitas.ohc.Util.roundUpTo8;

final class OffHeapMap
{
    // maximum hash table size
    private static final int MAX_TABLE_SIZE = 1 << 30;

    private long lruHead;
    private long lruTail;

    private long size;
    private Table table;

    private long hitCount;
    private long missCount;
    private long putAddCount;
    private long putReplaceCount;
    private long removeCount;

    private long threshold;
    private final float loadFactor;

    private long rehashes;
    private long evictedEntries;

    private final AtomicLong freeCapacity;

    OffHeapMap(OHCacheBuilder builder, AtomicLong freeCapacity)
    {
        this.freeCapacity = freeCapacity;

        int hts = builder.getHashTableSize();
        if (hts <= 0)
            hts = 8192;
        if (hts < 256)
            hts = 256;
        table = Table.create((int) Util.roundUpToPowerOf2(hts, MAX_TABLE_SIZE));
        if (table == null)
            throw new RuntimeException("unable to allocate off-heap memory for segment");

        float lf = builder.getLoadFactor();
        if (lf <= .0d)
            lf = .75f;
        this.loadFactor = lf;
        threshold = (long) ((double) table.size() * loadFactor);
    }

    synchronized void release()
    {
        table.release();
        table = null;
    }

    long size()
    {
        return size;
    }

    long hitCount()
    {
        return hitCount;
    }

    long missCount()
    {
        return missCount;
    }

    long putAddCount()
    {
        return putAddCount;
    }

    long putReplaceCount()
    {
        return putReplaceCount;
    }

    long removeCount()
    {
        return removeCount;
    }

    void resetStatistics()
    {
        rehashes = 0L;
        evictedEntries = 0L;
        hitCount = 0L;
        missCount = 0L;
        putAddCount = 0L;
        putReplaceCount = 0L;
        removeCount = 0L;
    }

    long rehashes()
    {
        return rehashes;
    }

    long evictedEntries()
    {
        return evictedEntries;
    }

    synchronized long getEntry(KeyBuffer key, boolean reference)
    {
        for (long hashEntryAdr = table.first(key.hash());
             hashEntryAdr != 0L;
             hashEntryAdr = HashEntries.getNext(hashEntryAdr))
        {
            if (notSameKey(key, hashEntryAdr))
                continue;

            // return existing entry

            touch(hashEntryAdr);

            if (reference)
                HashEntries.reference(hashEntryAdr);

            hitCount++;
            return hashEntryAdr;
        }

        // not found
        missCount++;
        return 0L;
    }

    synchronized boolean putEntry(long newHashEntryAdr, long hash, long keyLen, long bytes, boolean ifAbsent, long oldValueAdr, long oldValueLen)
    {
        long hashEntryAdr;
        long prevEntryAdr = 0L;
        for (hashEntryAdr = table.first(hash);
             hashEntryAdr != 0L;
             prevEntryAdr = hashEntryAdr, hashEntryAdr = HashEntries.getNext(hashEntryAdr))
        {
            if (notSameKey(newHashEntryAdr, hash, keyLen, hashEntryAdr))
                continue;

            // replace existing entry

            if (ifAbsent)
                return false;

            if (oldValueAdr != 0L)
            {
                // code for replace() operation
                long valueLen = HashEntries.getValueLen(hashEntryAdr);
                if (valueLen != oldValueLen || !HashEntries.compare(hashEntryAdr, ENTRY_OFF_DATA + roundUpTo8(keyLen), oldValueAdr, 0L, oldValueLen))
                    return false;
            }

            remove(hashEntryAdr, prevEntryAdr);
            dereference(hashEntryAdr);

            break;
        }

        while (freeCapacity.get() < bytes)
            if (!removeOldest())
            {
                if (hashEntryAdr != 0L)
                    size--;
                return false;
            }

        if (hashEntryAdr == 0L)
        {
            if (size >= threshold)
                rehash();

            size++;
        }

        freeCapacity.addAndGet(-bytes);

        add(newHashEntryAdr);

        if (hashEntryAdr == 0L)
            putAddCount++;
        else
            putReplaceCount++;

        return true;
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

    synchronized void removeEntry(long removeHashEntryAdr)
    {
        long hash = HashEntries.getHash(removeHashEntryAdr);
        long prevEntryAdr = 0L;
        for (long hashEntryAdr = table.first(hash);
             hashEntryAdr != 0L;
             prevEntryAdr = hashEntryAdr, hashEntryAdr = HashEntries.getNext(hashEntryAdr))
        {
            if (hashEntryAdr != removeHashEntryAdr)
                continue;

            // remove existing entry

            remove(hashEntryAdr, prevEntryAdr);
            dereference(hashEntryAdr);

            size--;
            removeCount++;

            return;
        }
    }

    synchronized void removeEntry(KeyBuffer key)
    {
        long prevEntryAdr = 0L;
        for (long hashEntryAdr = table.first(key.hash());
             hashEntryAdr != 0L;
             prevEntryAdr = hashEntryAdr, hashEntryAdr = HashEntries.getNext(hashEntryAdr))
        {
            if (notSameKey(key, hashEntryAdr))
                continue;

            // remove existing entry

            remove(hashEntryAdr, prevEntryAdr);
            dereference(hashEntryAdr);

            size--;
            removeCount++;

            return;
        }
    }

    private static boolean notSameKey(KeyBuffer key, long hashEntryAdr)
    {
        if (notSameHash(key.hash(), hashEntryAdr)) return true;

        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen != key.size()
               || !HashEntries.compareKey(hashEntryAdr, key, serKeyLen);
    }

    private static boolean notSameKey(long newHashEntryAdr, long newHash, long newKeyLen, long hashEntryAdr)
    {
        if (notSameHash(newHash, hashEntryAdr)) return true;

        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen != newKeyLen
               || !HashEntries.compare(hashEntryAdr, ENTRY_OFF_DATA, newHashEntryAdr, ENTRY_OFF_DATA, serKeyLen);
    }

    private static boolean notSameHash(long newHash, long hashEntryAdr)
    {
        long hashEntryHash = HashEntries.getHash(hashEntryAdr);
        return hashEntryHash != newHash;
    }

    private void rehash()
    {
        Table tab = table;
        int tableSize = tab.size();
        if (tableSize > MAX_TABLE_SIZE)
        {
            // already at max hash table size - keep rehashTrigger field true
            return;
        }

        Table newTable = Table.create(tableSize * 2);
        if (newTable == null)
            return;
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

        threshold = (long) ((float) newTable.size() * loadFactor);
        table = newTable;
        rehashes++;
    }

    synchronized long[] hotN(int n)
    {
        long[] r = new long[n];
        int i = 0;
        for (long hashEntryAdr = lruHead;
             hashEntryAdr != 0L && i < n;
             hashEntryAdr = getLruNext(hashEntryAdr))
        {
            r[i++] = hashEntryAdr;
            HashEntries.reference(hashEntryAdr);
        }
        return r;
    }

    float loadFactor()
    {
        return loadFactor;
    }

    int hashTableSize()
    {
        return table.size();
    }

    synchronized void updateBucketHistogram(HistogramBuilder builder)
    {
        table.updateBucketHistogram(builder);
    }

    synchronized void getEntryAddresses(int mapSegmentIndex, int nSegments, List<Long> hashEntryAdrs)
    {
        for (; nSegments-- > 0 && mapSegmentIndex < table.size(); mapSegmentIndex++)
            for (long hashEntryAdr = table.first(mapSegmentIndex);
                 hashEntryAdr != 0L;
                 hashEntryAdr = HashEntries.getNext(hashEntryAdr))
            {
                hashEntryAdrs.add(hashEntryAdr);
                HashEntries.reference(hashEntryAdr);
            }
    }

    static final class Table
    {
        final int mask;
        final long address;
        private boolean released;

        static Table create(int hashTableSize)
        {
            int msz = (int) BUCKET_ENTRY_LEN * hashTableSize;
            long address = Uns.allocate(msz);
            return address != 0L ? new Table(address, hashTableSize) : null;
        }

        private Table(long address, int hashTableSize)
        {
            this.address = address;
            this.mask = hashTableSize - 1;
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
            released = true;
        }

        protected void finalize() throws Throwable
        {
            if (!released)
                Uns.free(address);
            super.finalize();
        }

        long first(long hash)
        {
            return Uns.getLong(address, bucketOffset(hash));
        }

        void first(long hash, long hashEntryAdr)
        {
            Uns.putLong(address, bucketOffset(hash), hashEntryAdr);
        }

        private long bucketOffset(long hash)
        {
            return bucketIndexForHash(hash) * BUCKET_ENTRY_LEN;
        }

        private int bucketIndexForHash(long hash)
        {
            return (int) (hash & mask);
        }

        /**
         * Remove entry operation for eviction - that's when the previous entry is not known.
         */
        void removeLink(long hash, long hashEntryAdr)
        {
            long next = HashEntries.getNext(hashEntryAdr);
            long head = first(hash);

            if (head == hashEntryAdr)
                first(hash, next);
            else
            {
                long prevEntryAdr = 0L;
                for (long adr = head;
                     adr != 0L;
                     prevEntryAdr = adr, adr = HashEntries.getNext(adr))
                {
                    if (adr == hashEntryAdr)
                        HashEntries.setNext(prevEntryAdr, next);
                }
            }

            // just for safety
            HashEntries.setNext(hashEntryAdr, 0L);
        }

        /**
         * Remove entry operation (not for eviction).
         */
        void removeLink(long hash, long hashEntryAdr, long prevEntryAdr)
        {
            long next = HashEntries.getNext(hashEntryAdr);

            long head = first(hash);
            if (head == hashEntryAdr)
                first(hash, next);
            else if (prevEntryAdr != 0L)
                HashEntries.setNext(prevEntryAdr, next);

            // just for safety
            HashEntries.setNext(hashEntryAdr, 0L);
        }

        void addLinkAsHead(long hash, long hashEntryAdr)
        {
            long head = first(hash);
            HashEntries.setNext(hashEntryAdr, head);
            first(hash, hashEntryAdr);
        }

        int size()
        {
            return mask + 1;
        }

        void updateBucketHistogram(HistogramBuilder h)
        {
            for (int i = 0; i < size(); i++)
            {
                int len = 0;
                for (long adr = first(i); adr != 0L; adr = HashEntries.getNext(adr))
                    len++;
                h.add(len);
            }
        }
    }

    //
    // eviction/replacement/cleanup
    //

    private void remove(long hashEntryAdr, long prevEntryAdr)
    {
        long hash = HashEntries.getHash(hashEntryAdr);

        if (prevEntryAdr == -1L)
            // cleanUp has no information about the previous hash-entry (during eviction)
            table.removeLink(hash, hashEntryAdr);
        else
            // other operations know about the previous hash-entry (since they walk through the entry-chain)
            table.removeLink(hash, hashEntryAdr, prevEntryAdr);

        // LRU stuff

        long next = getLruNext(hashEntryAdr);
        long prev = getLruPrev(hashEntryAdr);

        if (lruHead == hashEntryAdr)
            lruHead = next;
        if (lruTail == hashEntryAdr)
            lruTail = prev;

        if (next != 0L)
            setLruPrev(next, prev);
        if (prev != 0L)
            setLruNext(prev, next);
    }

    private void add(long hashEntryAdr)
    {
        long hash = HashEntries.getHash(hashEntryAdr);

        table.addLinkAsHead(hash, hashEntryAdr);

        // LRU stuff

        long h = lruHead;
        setLruNext(hashEntryAdr, h);
        if (h != 0L)
            setLruPrev(h, hashEntryAdr);
        setLruPrev(hashEntryAdr, 0L);
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

        long next = getLruNext(hashEntryAdr);
        long prev = getLruPrev(hashEntryAdr);

        if (lruTail == hashEntryAdr)
            lruTail = prev;

        if (next != 0L)
            setLruPrev(next, prev);
        if (prev != 0L)
            setLruNext(prev, next);

        // LRU stuff (basically an add to LRU linked list)

        long head = lruHead;
        setLruNext(hashEntryAdr, head);
        if (head != 0L)
            setLruPrev(head, hashEntryAdr);
        setLruPrev(hashEntryAdr, 0L);
        lruHead = hashEntryAdr;

        if (lruTail == 0L)
            lruTail = hashEntryAdr;
    }

    private static long getLruNext(long hashEntryAdr)
    {
        return HashEntries.getLRUNext(hashEntryAdr);
    }

    private static long getLruPrev(long hashEntryAdr)
    {
        return HashEntries.getLRUPrev(hashEntryAdr);
    }

    private static void setLruNext(long hashEntryAdr, long next)
    {
        HashEntries.setLRUNext(hashEntryAdr, next);
    }

    private static void setLruPrev(long hashEntryAdr, long prev)
    {
        HashEntries.setLRUPrev(hashEntryAdr, prev);
    }

    private boolean removeOldest()
    {
        long hashEntryAdr = lruTail;
        if (hashEntryAdr == 0L)
            return false;

        remove(hashEntryAdr, -1L);
        dereference(hashEntryAdr);

        size--;

        evictedEntries ++;

        return true;
    }

    void dereference(long hashEntryAdr)
    {
        if (HashEntries.dereference(hashEntryAdr))
        {
            long bytes = HashEntries.getAllocLen(hashEntryAdr);
            if (bytes == 0L)
                throw new IllegalStateException();

            Uns.free(hashEntryAdr);

            freeCapacity.addAndGet(bytes);
        }
    }
}
