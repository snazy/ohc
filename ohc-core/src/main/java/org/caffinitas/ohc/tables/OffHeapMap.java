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
package org.caffinitas.ohc.tables;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.histo.EstimatedHistogram;

final class OffHeapMap
{
    // maximum hash table size
    private static final int MAX_TABLE_SIZE = 1 << 30;

    private long lruHead;
    private long lruTail;

    private final int entriesPerBucket;
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

    private final ReentrantLock lock = new ReentrantLock();

    OffHeapMap(OHCacheBuilder builder, AtomicLong freeCapacity)
    {
        this.freeCapacity = freeCapacity;

        int hts = builder.getHashTableSize();
        if (hts <= 0)
            hts = 8192;
        if (hts < 256)
            hts = 256;
        int bl = builder.getBucketLength();
        if (bl <= 0)
            bl = 8;
        int buckets = (int) Util.roundUpToPowerOf2(hts, MAX_TABLE_SIZE);
        entriesPerBucket = (int) Util.roundUpToPowerOf2(bl, MAX_TABLE_SIZE);
        table = Table.create(buckets, entriesPerBucket);
        if (table == null)
            throw new RuntimeException("unable to allocate off-heap memory for segment");

        float lf = builder.getLoadFactor();
        if (lf <= .0d)
            lf = .75f;
        this.loadFactor = lf;
        threshold = (long) ((double) table.size() * loadFactor);
    }

    void release()
    {
        lock.lock();
        try
        {
            table.release();
            table = null;
        }
        finally
        {
            lock.unlock();
        }
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

    long getEntry(KeyBuffer key, boolean reference)
    {
        lock.lock();
        try
        {
            long ptr = table.bucketOffset(key.hash());
            for (int idx = 0; idx < entriesPerBucket; idx++, ptr += Util.BUCKET_ENTRY_LEN)
            {
                long hashEntryAdr = table.getEntryAdr(ptr);
                if (hashEntryAdr == 0L)
                    continue;

                if (table.getHash(ptr) != key.hash() || notSameKey(key, hashEntryAdr))
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
        finally
        {
            lock.unlock();
        }
    }

    boolean putEntry(long newHashEntryAdr, long hash, long keyLen, long bytes, boolean ifAbsent, long oldValueAdr, long oldValueLen)
    {
        long removeHashEntryAdr = 0L;
        List<Long> derefList = null;
        lock.lock();
        try
        {
            long oldHashEntryAdr = 0L;
            long hashEntryAdr;
            long ptr = table.bucketOffset(hash);
            for (int idx = 0; idx < entriesPerBucket; idx++, ptr += Util.BUCKET_ENTRY_LEN)
            {
                hashEntryAdr = table.getEntryAdr(ptr);
                if (hashEntryAdr == 0L)
                    continue;

                if (table.getHash(ptr) != hash || notSameKey(newHashEntryAdr, keyLen, hashEntryAdr))
                    continue;

                // replace existing entry

                if (ifAbsent)
                    return false;

                if (oldValueAdr != 0L)
                {
                    // code for replace() operation
                    long valueLen = HashEntries.getValueLen(hashEntryAdr);
                    if (valueLen != oldValueLen || !HashEntries.compare(hashEntryAdr, Util.ENTRY_OFF_DATA + Util.roundUpTo8(keyLen), oldValueAdr, 0L, oldValueLen))
                        return false;
                }

                removeInternal(hashEntryAdr, hash);
                removeHashEntryAdr = hashEntryAdr;

                oldHashEntryAdr = hashEntryAdr;

                break;
            }

            while (freeCapacity.get() < bytes)
            {
                long eldestHashAdr = removeEldest();
                if (eldestHashAdr == 0L)
                {
                    if (oldHashEntryAdr != 0L)
                        size--;
                    return false;
                }
                if (derefList == null)
                    derefList = new ArrayList<>();
                derefList.add(eldestHashAdr);
            }

            if (oldHashEntryAdr == 0L)
            {
                if (size >= threshold)
                    rehash();

                size++;
            }

            freeCapacity.addAndGet(-bytes);

            if (!add(newHashEntryAdr, hash))
                return false;

            if (oldHashEntryAdr == 0L)
                putAddCount++;
            else
                putReplaceCount++;

            return true;
        }
        finally
        {
            lock.unlock();
            if (removeHashEntryAdr != 0L)
                HashEntries.dereference(removeHashEntryAdr);
            if (derefList != null)
                for (long hashEntryAdr : derefList)
                    HashEntries.dereference(hashEntryAdr);
        }
    }

    void clear()
    {
        lock.lock();
        try
        {
            lruHead = lruTail = 0L;
            size = 0L;

            long hashEntryAdr;
            for (int p = 0; p < table.size(); p++)
            {
                long ptr = table.bucketOffset(p);
                for (int idx = 0; idx < entriesPerBucket; idx++, ptr += Util.BUCKET_ENTRY_LEN)
                {
                    hashEntryAdr = table.getEntryAdr(ptr);
                    if (hashEntryAdr == 0L)
                        continue;

                    freeCapacity.addAndGet(HashEntries.getAllocLen(hashEntryAdr));
                    HashEntries.dereference(hashEntryAdr);
                }
            }

            table.clear();
        }
        finally
        {
            lock.unlock();
        }
    }

    void removeEntry(long removeHashEntryAdr)
    {
        lock.lock();
        try
        {
            long hash = HashEntries.getHash(removeHashEntryAdr);
            long hashEntryAdr;
            long ptr = table.bucketOffset(hash);
            for (int idx = 0; idx < entriesPerBucket; idx++, ptr += Util.BUCKET_ENTRY_LEN)
            {
                hashEntryAdr = table.getEntryAdr(ptr);
                if (hashEntryAdr != removeHashEntryAdr)
                    continue;

                // remove existing entry

                removeInternal(hashEntryAdr, hash);

                size--;
                removeCount++;

                return;
            }
            removeHashEntryAdr = 0L;
        }
        finally
        {
            lock.unlock();
            if (removeHashEntryAdr != 0L)
                HashEntries.dereference(removeHashEntryAdr);
        }
    }

    void removeEntry(KeyBuffer key)
    {
        long removeHashEntryAdr = 0L;
        lock.lock();
        try
        {
            long hashEntryAdr;
            long ptr = table.bucketOffset(key.hash());
            for (int idx = 0; idx < entriesPerBucket; idx++, ptr += Util.BUCKET_ENTRY_LEN)
            {
                hashEntryAdr = table.getEntryAdr(ptr);
                if (hashEntryAdr == 0L)
                    continue;

                if (table.getHash(ptr) != key.hash() || notSameKey(key, hashEntryAdr))
                    continue;

                // remove existing entry

                removeHashEntryAdr = hashEntryAdr;
                removeInternal(hashEntryAdr, key.hash());

                size--;
                removeCount++;

                return;
            }
        }
        finally
        {
            lock.unlock();
            if (removeHashEntryAdr != 0L)
                HashEntries.dereference(removeHashEntryAdr);
        }
    }

    private static boolean notSameKey(KeyBuffer key, long hashEntryAdr)
    {
        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen != key.size()
               || !HashEntries.compareKey(hashEntryAdr, key, serKeyLen);
    }

    private static boolean notSameKey(long newHashEntryAdr, long newKeyLen, long hashEntryAdr)
    {
        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen != newKeyLen
               || !HashEntries.compare(hashEntryAdr, Util.ENTRY_OFF_DATA, newHashEntryAdr, Util.ENTRY_OFF_DATA, serKeyLen);
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

        Table newTable = Table.create(tableSize * 2, entriesPerBucket);
        if (newTable == null)
            return;

        for (int part = 0; part < tableSize; part++)
        {
            long hashEntryAdr;
            long ptr = table.bucketOffset(part);
            for (int idx = 0; idx < entriesPerBucket; idx++, ptr += Util.BUCKET_ENTRY_LEN)
            {
                hashEntryAdr = table.getEntryAdr(ptr);
                if (hashEntryAdr == 0L)
                    continue;

                if (!newTable.addToTable(table.getHash(ptr), hashEntryAdr))
                    HashEntries.dereference(hashEntryAdr);
            }
        }

        threshold = (long) ((float) newTable.size() * loadFactor);
        table.release();
        table = newTable;
        rehashes++;
    }

    long[] hotN(int n)
    {
        lock.lock();
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
            lock.unlock();
        }
    }

    float loadFactor()
    {
        return loadFactor;
    }

    int hashTableSize()
    {
        return table.size();
    }

    void updateBucketHistogram(EstimatedHistogram hist)
    {
        lock.lock();
        try
        {
            table.updateBucketHistogram(hist);
        }
        finally
        {
            lock.unlock();
        }
    }

    void getEntryAddresses(int mapSegmentIndex, int nSegments, List<Long> hashEntryAdrs)
    {
        lock.lock();
        try
        {
            for (; nSegments-- > 0 && mapSegmentIndex < table.size(); mapSegmentIndex++)
            {
                long hashEntryAdr;
                long ptr = table.bucketOffset(mapSegmentIndex);
                for (int idx = 0; idx < entriesPerBucket; idx++, ptr += Util.BUCKET_ENTRY_LEN)
                {
                    hashEntryAdr = table.getEntryAdr(ptr);
                    if (hashEntryAdr == 0L)
                        continue;
                    hashEntryAdrs.add(hashEntryAdr);
                    HashEntries.reference(hashEntryAdr);
                }
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    static final class Table
    {
        final int mask;
        final long address;
        private final int entriesPerBucket;
        private boolean released;

        static Table create(int hashTableSize, int entriesPerBucket)
        {
            int msz = (int) Util.BUCKET_ENTRY_LEN * hashTableSize * entriesPerBucket;
            long address = Uns.allocate(msz);
            return address != 0L ? new Table(address, hashTableSize, entriesPerBucket) : null;
        }

        private Table(long address, int hashTableSize, int entriesPerBucket)
        {
            this.address = address;
            this.mask = hashTableSize - 1;
            this.entriesPerBucket = entriesPerBucket;
            clear();
        }

        void clear()
        {
            // It's important to initialize the hash table memory.
            // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
            Uns.setMemory(address, 0L, Util.BUCKET_ENTRY_LEN * entriesPerBucket * size(), (byte) 0);
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

        boolean addToTable(long hash, long hashEntryAdr)
        {
            long off = bucketOffset(hash);
            for (int i = 0; i < entriesPerBucket; i++, off += Util.BUCKET_ENTRY_LEN)
                if (Uns.compareAndSwapLong(address, off, 0L, hashEntryAdr))
                {
                    Uns.putLong(address, off + Util.BUCKET_OFF_HASH, hash);
                    return true;
                }
            return false;
        }

        boolean removeFromTable(long hash, long hashEntryAdr)
        {
            long off = bucketOffset(hash);
            for (int i = 0; i < entriesPerBucket; i++, off += Util.BUCKET_ENTRY_LEN)
            {
                if (Uns.compareAndSwapLong(address, off, hashEntryAdr, 0L))
                    return true;
            }
            return false;
        }

        long bucketOffset(long hash)
        {
            return bucketIndexForHash(hash) * entriesPerBucket * Util.BUCKET_ENTRY_LEN;
        }

        private int bucketIndexForHash(long hash)
        {
            return (int) (hash & mask);
        }

        int size()
        {
            return mask + 1;
        }

        void updateBucketHistogram(EstimatedHistogram h)
        {
            long off = 0L;
            for (int p = 0; p < size(); p++)
            {
                int len = 0;
                for (int i = 0; i < entriesPerBucket; i++, off += Util.BUCKET_ENTRY_LEN)
                {
                    if (getEntryAdr(off) != 0L)
                        len++;
                }
                h.add(len + 1);
            }
        }

        long getEntryAdr(long entryOff)
        {
            return Uns.getLong(address, entryOff);
        }

        long getHash(long entryOff)
        {
            return Uns.getLong(address, entryOff + Util.BUCKET_OFF_HASH);
        }
    }

    private void removeInternal(long hashEntryAdr, long hash)
    {
        table.removeFromTable(hash, hashEntryAdr);

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

        freeCapacity.addAndGet(HashEntries.getAllocLen(hashEntryAdr));
    }

    private boolean add(long hashEntryAdr, long hash)
    {
        if (!table.addToTable(hash, hashEntryAdr))
            return false;

        // LRU stuff

        long h = lruHead;
        HashEntries.setLRUNext(hashEntryAdr, h);
        if (h != 0L)
            HashEntries.setLRUPrev(h, hashEntryAdr);
        HashEntries.setLRUPrev(hashEntryAdr, 0L);
        lruHead = hashEntryAdr;

        if (lruTail == 0L)
            lruTail = hashEntryAdr;

        return true;
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

    private long removeEldest()
    {
        long hashEntryAdr = lruTail;
        if (hashEntryAdr == 0L)
            return 0L;

        removeInternal(hashEntryAdr, HashEntries.getHash(hashEntryAdr));

        size--;

        evictedEntries++;

        return hashEntryAdr;
    }
}
