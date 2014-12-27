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
        if (lf >= 1d)
            throw new IllegalArgumentException("load factor must not be greater that 1");
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

            if (freeCapacity.get() < bytes)
                do
                {
                    derefList = new ArrayList<>();
                    if (!ensureCapacity(derefList, oldHashEntryAdr))
                        return false;
                } while (freeCapacity.get() < bytes);

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

    private boolean ensureCapacity(List<Long> derefList, long oldHashEntryAdr)
    {
        long eldestEntryAdr = table.getEldest();
        if (eldestEntryAdr == 0L)
        {
            if (oldHashEntryAdr != 0L)
                size--;
            return false;
        }

        removeInternal(eldestEntryAdr, HashEntries.getHash(eldestEntryAdr));

        size--;

        evictedEntries++;

        derefList.add(eldestEntryAdr);
        return true;
    }

    void clear()
    {
        lock.lock();
        try
        {
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
            // already at max hash table size
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
            table.fillHotN(r, n);
            for (long hashEntryAdr : r)
            {
                if (hashEntryAdr != 0L)
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

        private final long lruOffset;

        private int lruWriteTarget;
        private int lruEldestIndex;

        static Table create(int hashTableSize, int entriesPerBucket)
        {
            int msz = (int) Util.BUCKET_ENTRY_LEN * hashTableSize * entriesPerBucket;

            msz += hashTableSize * Util.POINTER_LEN;

            long address = Uns.allocate(msz);
            return address != 0L ? new Table(address, hashTableSize, entriesPerBucket) : null;
        }

        private Table(long address, int hashTableSize, int entriesPerBucket)
        {
            this.address = address;
            this.mask = hashTableSize - 1;
            this.entriesPerBucket = entriesPerBucket;

            this.lruOffset = Util.BUCKET_ENTRY_LEN * hashTableSize * entriesPerBucket;
            this.lruWriteTarget = 0;

            clear();
        }

        //

        long getEldest()
        {
            for (int i = lruEldestIndex; i < lruWriteTarget; i++ )
            {
                long hashEntryAdr = Uns.getLong(address, lruOffset(i));
                if (hashEntryAdr != 0L)
                {
                    lruEldestIndex = i + 1;
                    return hashEntryAdr;
                }
            }
            return 0;
        }

        void fillHotN(long[] r, int n)
        {
            int c = 0;
            for (int i = lruWriteTarget - 1; i >= 0; i--)
            {
                long hashEntryAdr = Uns.getLong(address, lruOffset(i));
                if (hashEntryAdr != 0L)
                {
                    r[c++] = hashEntryAdr;
                    if (c == n)
                        return;
                }
            }
        }

        void addToLRU(long hashEntryAdr)
        {
            int i = lruWriteTarget;
            if (i < size())
            {
                // try to add to current-write-target
                Uns.putLong(address, lruOffset(i), hashEntryAdr);
                HashEntries.setLRUIndex(hashEntryAdr, i);
                lruWriteTarget = i + 1;
                return;
            }

            // LRU table compaction needed

            int id = 0;
            for (int is = lruEldestIndex; is < size(); is++)
            {
                long adr = Uns.getLong(address, lruOffset(is));
                if (adr != 0L)
                {
                    Uns.putLong(address, lruOffset(id), adr);
                    HashEntries.setLRUIndex(adr, id);
                    id++;
                }
            }

            // add hash-entry to LRU
            HashEntries.setLRUIndex(hashEntryAdr, id);
            Uns.putLong(address, lruOffset(id), hashEntryAdr);

            lruWriteTarget = id + 1;
            lruEldestIndex = 0;

            // clear remaining LRU table
            Uns.setMemory(address, lruOffset + id * Util.POINTER_LEN,
                          (size() - id) * Util.POINTER_LEN,
                          (byte) 0);
        }

        void removeFromLRU(long hashEntryAdr)
        {
            int lruIndex = HashEntries.getLRUIndex(hashEntryAdr);
            if (lruEldestIndex <= lruIndex)
                lruEldestIndex = lruIndex + 1;
            Uns.putLong(address, lruOffset + lruIndex * Util.POINTER_LEN, 0L);
        }

        private long lruOffset(int i)
        {
            return lruOffset + i * Util.POINTER_LEN;
        }

        //

        void clear()
        {
            // It's important to initialize the hash table memory.
            // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
            Uns.setMemory(address, 0L,
                          Util.BUCKET_ENTRY_LEN * entriesPerBucket * size() +
                          Util.POINTER_LEN * size(),
                          (byte) 0);
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

        void removeFromTable(long hash, long hashEntryAdr)
        {
            long off = bucketOffset(hash);
            for (int i = 0; i < entriesPerBucket; i++, off += Util.BUCKET_ENTRY_LEN)
            {
                if (Uns.compareAndSwapLong(address, off, hashEntryAdr, 0L))
                    break;
            }

            removeFromLRU(hashEntryAdr);
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

        freeCapacity.addAndGet(HashEntries.getAllocLen(hashEntryAdr));
    }

    private boolean add(long hashEntryAdr, long hash)
    {
        if (!table.addToTable(hash, hashEntryAdr))
            return false;

        table.addToLRU(hashEntryAdr);
        return true;
    }

    private void touch(long hashEntryAdr)
    {
        table.removeFromLRU(hashEntryAdr);
        table.addToLRU(hashEntryAdr);
    }
}
