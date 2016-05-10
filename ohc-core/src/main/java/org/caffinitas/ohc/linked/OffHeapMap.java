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

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.histo.EstimatedHistogram;

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
    private long expiredEntries;

    private long freeCapacity;

    private final ReentrantLock lock;

    private final boolean throwOOME;

    private final Timeouts timeouts;
    private final Timeouts.TimeoutHandler timeoutsExpireHandler = new Timeouts.TimeoutHandler()
    {
        public void expired(long hashEntryAdr)
        {
            removeEntry(hashEntryAdr, false);
        }
    };

    OffHeapMap(OHCacheBuilder builder, long freeCapacity)
    {
        this.freeCapacity = freeCapacity;

        this.throwOOME = builder.isThrowOOME();

        // 64 hash slots, each for 128ms
        this.timeouts = new Timeouts(builder.getTimeoutsSlots(), builder.getTimeoutsPrecision());

        this.lock = builder.isUnlocked() ? null : new ReentrantLock();

        int hts = builder.getHashTableSize();
        if (hts <= 0)
            hts = 8192;
        if (hts < 256)
            hts = 256;
        table = Table.create((int) Util.roundUpToPowerOf2(hts, MAX_TABLE_SIZE), throwOOME);
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
        lock();
        try
        {
            table.release();
            table = null;

            timeouts.release();
        }
        finally
        {
            unlock();
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

    long freeCapacity()
    {
        return freeCapacity;
    }

    void updateFreeCapacity(long diff)
    {
        lock();
        try
        {
            freeCapacity += diff;
        }
        finally
        {
            unlock();
        }
    }

    long evictedEntries()
    {
        return evictedEntries;
    }

    long expiredEntries()
    {
        return expiredEntries;
    }

    int usedTimeouts()
    {
        return timeouts.used();
    }

    long getEntry(KeyBuffer key, boolean reference, boolean updateLRU)
    {
        lock();
        try
        {
            for (long hashEntryAdr = table.getFirst(key.hash());
                 hashEntryAdr != 0L;
                 hashEntryAdr = HashEntries.getNext(hashEntryAdr))
            {
                if (notSameKey(key, hashEntryAdr))
                    continue;

                // return existing entry

                long expireAt = HashEntries.getExpireAt(hashEntryAdr);
                if (expireAt > 0L && expireAt <= System.currentTimeMillis())
                {
                    // entry is expired, remove it and return
                    expiredEntries++;
                    removeEntry(hashEntryAdr);
                    break;
                }

                if (updateLRU)
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
            unlock();
        }
    }

    boolean putEntry(long newHashEntryAdr, long hash, long keyLen, long bytes, boolean ifAbsent, long expireAt,
                     long oldValueAddr, long oldValueOffset, long oldValueLen)
    {
        long removeHashEntryAdr = 0L;
        LongArrayList derefList = null;
        lock();
        try
        {
            long oldHashEntryAdr = 0L;
            long hashEntryAdr;
            long prevEntryAdr = 0L;
            for (hashEntryAdr = table.getFirst(hash);
                 hashEntryAdr != 0L;
                 prevEntryAdr = hashEntryAdr, hashEntryAdr = HashEntries.getNext(hashEntryAdr))
            {
                if (notSameKey(newHashEntryAdr, hash, keyLen, hashEntryAdr))
                    continue;

                long testExpireAt = HashEntries.getExpireAt(hashEntryAdr);
                if (testExpireAt == 0L || testExpireAt > System.currentTimeMillis())
                {
                    // replace existing entry
                    //
                    // Only need to do this if the existing entry is not expired, otherwise
                    // we can just remove the existing entry.

                    if (ifAbsent)
                        return false;

                    if (oldValueAddr != 0L)
                    {
                        // code for replace() operation
                        long valueLen = HashEntries.getValueLen(hashEntryAdr);
                        if (valueLen != oldValueLen || !HashEntries.compare(hashEntryAdr, Util.ENTRY_OFF_DATA + Util.roundUpTo8(keyLen), oldValueAddr, oldValueOffset, oldValueLen))
                            return false;
                    }
                }

                removeInternal(hashEntryAdr, prevEntryAdr, true);
                removeHashEntryAdr = hashEntryAdr;

                oldHashEntryAdr = hashEntryAdr;

                break;
            }

            if (freeCapacity < bytes)
                removeExpired();
            while (freeCapacity < bytes)
            {
                long eldestHashAdr = removeEldest();
                if (eldestHashAdr == 0L)
                {
                    if (oldHashEntryAdr != 0L)
                        size--;
                    return false;
                }
                if (derefList == null)
                    derefList = new LongArrayList();
                derefList.add(eldestHashAdr);
            }

            if (hashEntryAdr == 0L)
            {
                if (size >= threshold)
                    rehash();

                size++;
            }

            freeCapacity -= bytes;

            add(newHashEntryAdr, hash, expireAt);

            if (hashEntryAdr == 0L)
                putAddCount++;
            else
                putReplaceCount++;

            return true;
        }
        finally
        {
            unlock();
            if (removeHashEntryAdr != 0L)
                HashEntries.dereference(removeHashEntryAdr);
            if (derefList != null)
                for (int i = 0; i < derefList.size(); i++)
                    HashEntries.dereference(derefList.getLong(i));
        }
    }

    private void removeExpired()
    {
        expiredEntries += timeouts.removeExpired(timeoutsExpireHandler);
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

    void clear()
    {
        lock();
        try
        {
            lruHead = lruTail = 0L;
            size = 0L;

            long next;
            long freed = 0L;
            for (int p = 0; p < table.size(); p++)
                for (long hashEntryAdr = table.getFirst(p);
                     hashEntryAdr != 0L;
                     hashEntryAdr = next)
                {
                    next = HashEntries.getNext(hashEntryAdr);

                    long expireAt = HashEntries.getExpireAt(hashEntryAdr);
                    if (expireAt > 0L)
                        timeouts.remove(hashEntryAdr, expireAt);

                    freed += HashEntries.getAllocLen(hashEntryAdr);
                    HashEntries.dereference(hashEntryAdr);
                }
            freeCapacity += freed;

            table.clear();
        }
        finally
        {
            unlock();
        }
    }

    void removeEntry(long removeHashEntryAdr)
    {
        removeEntry(removeHashEntryAdr, true);
    }

    private void removeEntry(long removeHashEntryAdr, boolean removeFromTimeouts)
    {
        lock();
        try
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

                removeIt(prevEntryAdr, hashEntryAdr, removeFromTimeouts);

                return;
            }
            removeHashEntryAdr = 0L;
        }
        finally
        {
            unlock();
            if (removeHashEntryAdr != 0L)
                HashEntries.dereference(removeHashEntryAdr);
        }
    }

    void removeEntry(KeyBuffer key)
    {
        long removeHashEntryAdr = 0L;
        lock();
        try
        {
            long prevEntryAdr = 0L;
            for (long hashEntryAdr = table.getFirst(key.hash());
                 hashEntryAdr != 0L;
                 prevEntryAdr = hashEntryAdr, hashEntryAdr = HashEntries.getNext(hashEntryAdr))
            {
                if (notSameKey(key, hashEntryAdr))
                    continue;

                // remove existing entry

                removeHashEntryAdr = hashEntryAdr;
                removeIt(prevEntryAdr, hashEntryAdr, true);

                return;
            }
        }
        finally
        {
            unlock();
            if (removeHashEntryAdr != 0L)
                HashEntries.dereference(removeHashEntryAdr);
        }
    }

    private void removeIt(long prevEntryAdr, long hashEntryAdr, boolean removeFromTimeouts)
    {
        removeInternal(hashEntryAdr, prevEntryAdr, removeFromTimeouts);

        size--;
        removeCount++;
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

        Table newTable = Table.create(tableSize * 2, throwOOME);
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
        table.release();
        table = newTable;
        rehashes++;
    }

    long[] hotN(int n)
    {
        lock();
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
            unlock();
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
        lock();
        try
        {
            table.updateBucketHistogram(hist);
        }
        finally
        {
            unlock();
        }
    }

    void getEntryAddresses(int mapSegmentIndex, int nSegments, List<Long> hashEntryAdrs)
    {
        lock();
        try
        {
            long t = System.currentTimeMillis();
            for (; nSegments-- > 0 && mapSegmentIndex < table.size(); mapSegmentIndex++)
                for (long hashEntryAdr = table.getFirst(mapSegmentIndex);
                     hashEntryAdr != 0L;
                     hashEntryAdr = HashEntries.getNext(hashEntryAdr))
                {
                    long expireAt = HashEntries.getExpireAt(hashEntryAdr);
                    if (expireAt > 0L && expireAt <= t)
                    {
                        // entry is expired, remove it and continue
                        removeEntry(hashEntryAdr);
                        expiredEntries++;
                        continue;
                    }

                    hashEntryAdrs.add(hashEntryAdr);
                    HashEntries.reference(hashEntryAdr);
                }
        }
        finally
        {
            unlock();
        }
    }

    private static final class Table
    {
        final int mask;
        final long address;
        private boolean released;

        static Table create(int hashTableSize, boolean throwOOME)
        {
            int msz = (int) Util.BUCKET_ENTRY_LEN * hashTableSize;
            long address = Uns.allocate(msz, throwOOME);
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
            Uns.setMemory(address, 0L, Util.BUCKET_ENTRY_LEN * size(), (byte) 0);
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
            return bucketIndexForHash(hash) * Util.BUCKET_ENTRY_LEN;
        }

        private int bucketIndexForHash(long hash)
        {
            return (int) (hash & mask);
        }

        void removeLink(long hash, long hashEntryAdr, long prevEntryAdr)
        {
            long next = HashEntries.getNext(hashEntryAdr);

            removeLinkInternal(hash, hashEntryAdr, prevEntryAdr, next);
        }

        void replaceLink(long hash, long hashEntryAdr, long prevEntryAdr, long newHashEntryAdr)
        {
            HashEntries.setNext(newHashEntryAdr, HashEntries.getNext(hashEntryAdr));

            removeLinkInternal(hash, hashEntryAdr, prevEntryAdr, newHashEntryAdr);
        }

        private void removeLinkInternal(long hash, long hashEntryAdr, long prevEntryAdr, long next)
        {
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

    private void removeInternal(long hashEntryAdr, long prevEntryAdr, boolean removeFromTimeouts)
    {
        long hash = HashEntries.getHash(hashEntryAdr);

        table.removeLink(hash, hashEntryAdr, prevEntryAdr);

        if (removeFromTimeouts)
        {
            long expireAt = HashEntries.getExpireAt(hashEntryAdr);
            if (expireAt > 0L)
                timeouts.remove(hashEntryAdr, expireAt);
        }

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

        freeCapacity += HashEntries.getAllocLen(hashEntryAdr);
    }

    boolean replaceEntry(long hash, long oldHashEntryAdr, long newHashEntryAdr, long bytes, long expireAt)
    {
        LongArrayList derefList = null;

        lock();
        try
        {
            long prevEntryAdr = 0L;
            for (long hashEntryAdr = table.getFirst(hash);
                 hashEntryAdr != 0L;
                 prevEntryAdr = hashEntryAdr, hashEntryAdr = HashEntries.getNext(hashEntryAdr))
            {
                if (hashEntryAdr != oldHashEntryAdr)
                    continue;

                // remove existing entry

                replaceInternal(oldHashEntryAdr, prevEntryAdr, newHashEntryAdr);

                if (expireAt > 0L)
                    timeouts.add(newHashEntryAdr, expireAt);

                if (freeCapacity < bytes)
                    removeExpired();
                while (freeCapacity < bytes)
                {
                    long eldestHashAdr = removeEldest();
                    if (eldestHashAdr == 0L)
                    {
                        return false;
                    }
                    if (derefList == null)
                        derefList = new LongArrayList();
                    derefList.add(eldestHashAdr);
                }

                return true;
            }

            return false;
        }
        finally
        {
            unlock();

            if (derefList != null)
                for (int i = 0; i < derefList.size(); i++)
                    HashEntries.dereference(derefList.getLong(i));
        }
    }

    private void replaceInternal(long hashEntryAdr, long prevEntryAdr, long newHashEntryAdr)
    {
        long hash = HashEntries.getHash(hashEntryAdr);

        table.replaceLink(hash, hashEntryAdr, prevEntryAdr, newHashEntryAdr);

        // LRU stuff

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

        freeCapacity += HashEntries.getAllocLen(hashEntryAdr);
    }

    private void add(long hashEntryAdr, long hash, long expireAt)
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

        if (expireAt > 0L)
            timeouts.add(hashEntryAdr, expireAt);
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

    private void lock()
    {
        if (lock != null)
            lock.lock();
    }

    private void unlock()
    {
        if (lock != null)
            lock.unlock();
    }

    @Override
    public String toString()
    {
        return String.valueOf(size);
    }
}
