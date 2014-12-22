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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.caffinitas.ohc.histo.EstimatedHistogram;

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

    private final ThreadLocal<List<Long>> dereferenceList = new ThreadLocal<List<Long>>()
    {
        protected List<Long> initialValue()
        {
            return new ArrayList<>();
        }
    };

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
        for (long hashEntryAdr = table.getFirst(key.hash());
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

    boolean putEntry(long newHashEntryAdr, long hash, long keyLen, long bytes, boolean ifAbsent, long oldValueAdr, long oldValueLen)
    {
        boolean r = putEntryInt(newHashEntryAdr, hash, keyLen, bytes, ifAbsent, oldValueAdr, oldValueLen);
        processDereferences();
        return r;
    }

    private synchronized boolean putEntryInt(long newHashEntryAdr, long hash, long keyLen, long bytes, boolean ifAbsent, long oldValueAdr, long oldValueLen)
    {
        long hashEntryAdr;
        long prevEntryAdr = 0L;
        for (hashEntryAdr = table.getFirst(hash);
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

            removeAndDereference(hashEntryAdr, prevEntryAdr);

            break;
        }

        while (freeCapacity.get() < bytes)
            if (!removeEldest())
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

        add(newHashEntryAdr, hash);

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
            for (long hashEntryAdr = table.getFirst(p);
                 hashEntryAdr != 0L;
                 hashEntryAdr = next)
            {
                next = HashEntries.getNext(hashEntryAdr);

                dereference(hashEntryAdr);
            }

        table.clear();
    }

    void removeEntry(long removeHashEntryAdr)
    {
        removeEntryInt(removeHashEntryAdr);
        processDereferences();
    }

    void removeEntry(KeyBuffer key)
    {
        removeEntryInt(key);
        processDereferences();
    }

    private synchronized void removeEntryInt(long removeHashEntryAdr)
    {
        long hash = HashEntries.getHash(removeHashEntryAdr);
        long prevEntryAdr = 0L;
        for (long hashEntryAdr = table.getFirst(hash);
             hashEntryAdr != 0L;
             prevEntryAdr = hashEntryAdr, hashEntryAdr = HashEntries.getNext(hashEntryAdr))
        {
            if (hashEntryAdr != removeHashEntryAdr)
                continue;

            // remove existing entry

            removeAndDereference(hashEntryAdr, prevEntryAdr);

            size--;
            removeCount++;

            return;
        }
    }

    private synchronized void removeEntryInt(KeyBuffer key)
    {
        long prevEntryAdr = 0L;
        for (long hashEntryAdr = table.getFirst(key.hash());
             hashEntryAdr != 0L;
             prevEntryAdr = hashEntryAdr, hashEntryAdr = HashEntries.getNext(hashEntryAdr))
        {
            if (notSameKey(key, hashEntryAdr))
                continue;

            // remove existing entry

            removeAndDereference(hashEntryAdr, prevEntryAdr);

            size--;
            removeCount++;

            return;
        }
    }

    private static boolean notSameKey(KeyBuffer key, long hashEntryAdr)
    {
        if (HashEntries.getHash(hashEntryAdr) != key.hash()) return true;

        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen != key.size()
               || !HashEntries.compareKey(hashEntryAdr, key, serKeyLen);
    }

    private static boolean notSameKey(long newHashEntryAdr, long newHash, long newKeyLen, long hashEntryAdr)
    {
        if (HashEntries.getHash(hashEntryAdr) != newHash) return true;

        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen != newKeyLen
               || !HashEntries.compare(hashEntryAdr, ENTRY_OFF_DATA, newHashEntryAdr, ENTRY_OFF_DATA, serKeyLen);
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
            for (long hashEntryAdr = tab.getFirst(part);
                 hashEntryAdr != 0L;
                 hashEntryAdr = next)
            {
                next = HashEntries.getNext(hashEntryAdr);

                HashEntries.setNext(hashEntryAdr, 0L);

                newTable.addAsHead(HashEntries.getHash(hashEntryAdr), hashEntryAdr);
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
             hashEntryAdr = HashEntries.getLRUNext(hashEntryAdr))
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

    synchronized void updateBucketHistogram(EstimatedHistogram hist)
    {
        table.updateBucketHistogram(hist);
    }

    synchronized void getEntryAddresses(int mapSegmentIndex, int nSegments, List<Long> hashEntryAdrs)
    {
        for (; nSegments-- > 0 && mapSegmentIndex < table.size(); mapSegmentIndex++)
            for (long hashEntryAdr = table.getFirst(mapSegmentIndex);
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

        long getFirst(long hash)
        {
            return Uns.getLong(address, bucketOffset(hash));
        }

        void setFirst(long hash, long hashEntryAdr)
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

        void removeLink(long hash, long hashEntryAdr, long prevEntryAdr)
        {
            long next = HashEntries.getNext(hashEntryAdr);

            long head = getFirst(hash);
            if (head == hashEntryAdr)
            {
                setFirst(hash, next);
            }
            else if (prevEntryAdr != 0L)
            {
                if (prevEntryAdr == -1L)
                {
                    for (long adr = head;
                         adr != 0L;
                         prevEntryAdr = adr, adr = HashEntries.getNext(adr))
                    {
                        if (adr == hashEntryAdr)
                            break;
                    }
                }
                HashEntries.setNext(prevEntryAdr, next);
            }
        }

        void addAsHead(long hash, long hashEntryAdr)
        {
            long head = getFirst(hash);
            HashEntries.setNext(hashEntryAdr, head);
            setFirst(hash, hashEntryAdr);
        }

        int size()
        {
            return mask + 1;
        }

        void updateBucketHistogram(EstimatedHistogram h)
        {
            for (int i = 0; i < size(); i++)
            {
                int len = 0;
                for (long adr = getFirst(i); adr != 0L; adr = HashEntries.getNext(adr))
                    len++;
                h.add(len + 1);
            }
        }
    }

    private void removeAndDereference(long hashEntryAdr, long prevEntryAdr)
    {
        long hash = HashEntries.getHash(hashEntryAdr);

        table.removeLink(hash, hashEntryAdr, prevEntryAdr);

        // LRU stuff

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

        // add to a ThreadLocal deref list since a dereference can become very expensive if a free() is involved
        dereferenceList.get().add(hashEntryAdr);
    }

    private void processDereferences()
    {
        // process ThreadLocal deref list since a dereference can become very expensive if a free() is involved
        List<Long> derefList = dereferenceList.get();
        for (long hashEntryAdr : derefList)
            dereference(hashEntryAdr);
        derefList.clear();
    }

    void dereference(long hashEntryAdr)
    {
        if (HashEntries.dereference(hashEntryAdr))
        {
            long bytes = HashEntries.getAllocLen(hashEntryAdr);

            HashEntries.free(hashEntryAdr, bytes);

            freeCapacity.addAndGet(bytes);
        }
    }

    private void add(long hashEntryAdr, long hash)
    {
        table.addAsHead(hash, hashEntryAdr);

        // LRU stuff

        long h = lruHead;
        HashEntries.setLRUNext(hashEntryAdr, h);
        if (h != 0L)
            HashEntries.setLRUPrev(h, hashEntryAdr);
        HashEntries.setLRUPrev(hashEntryAdr, 0L);
        lruHead = hashEntryAdr;

        if (lruTail == 0L)
            lruTail = hashEntryAdr;
    }

    private void touch(long hashEntryAdr)
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

    private boolean removeEldest()
    {
        long hashEntryAdr = lruTail;
        if (hashEntryAdr == 0L)
            return false;

        removeAndDereference(hashEntryAdr, -1L);

        size--;

        evictedEntries ++;

        return true;
    }
}
