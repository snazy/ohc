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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.google.common.primitives.Ints;

import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.Ticker;
import org.caffinitas.ohc.histo.EstimatedHistogram;

abstract class OffHeapLinkedMap
{
    // maximum hash table size
    private static final int MAX_TABLE_SIZE = 1 << 30;

    long size;
    Table table;

    private long hitCount;
    private long missCount;
    private long putAddCount;
    private long putReplaceCount;
    private long removeCount;

    private long threshold;
    private final float loadFactor;

    private long rehashes;
    long evictedEntries;
    private long expiredEntries;

    // Replacement for Unsafe.monitorEnter/monitorExit. Uses the thread-ID to indicate a lock
    // using a CAS operation on the primitive instance field.
    private final boolean unlocked;
    private volatile long lock;
    private static final AtomicLongFieldUpdater<OffHeapLinkedMap> lockFieldUpdater =
    AtomicLongFieldUpdater.newUpdater(OffHeapLinkedMap.class, "lock");

    private final boolean throwOOME;

    private final Ticker ticker;

    private final Timeouts timeouts;
    private final Timeouts.TimeoutHandler timeoutsExpireHandler = new Timeouts.TimeoutHandler()
    {
        public void expired(long hashEntryAdr)
        {
            removeEntry(hashEntryAdr, false);
        }
    };

    OffHeapLinkedMap(OHCacheBuilder builder)
    {
        this.throwOOME = builder.isThrowOOME();

        this.ticker = builder.getTicker();

        // 64 hash slots, each for 128ms
        this.timeouts = builder.isTimeouts()
                        ? new Timeouts(ticker, builder.getTimeoutsSlots(), builder.getTimeoutsPrecision())
                        : null;

        this.unlocked = builder.isUnlocked();

        int hts = builder.getHashTableSize();
        if (hts <= 0)
            hts = 8192;
        if (hts < 256)
            hts = 256;
        int msz = Ints.checkedCast(Util.roundUpToPowerOf2(hts, MAX_TABLE_SIZE));
        table = Table.create(msz, throwOOME);
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
        boolean wasFirst = lock();
        try
        {
            try
            {
                table.release();
                table = null;
            }
            finally
            {
                if (timeouts != null)
                    timeouts.release();
            }
        }
        finally
        {
            unlock(wasFirst); 
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

    abstract long freeCapacity();

    abstract void updateFreeCapacity(long diff);

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
        return timeouts != null ? timeouts.used() : 0;
    }

    long getEntry(KeyBuffer key, boolean reference, boolean updateLRU)
    {
        boolean wasFirst = lock();
        try
        {
            for (long hashEntryAdr = table.getFirst(key.hash());
                 hashEntryAdr != 0L;
                 hashEntryAdr = HashEntries.getNext(hashEntryAdr))
            {
                if (!key.sameKey(hashEntryAdr))
                    continue;

                // return existing entry

                long expireAt = HashEntries.getExpireAt(hashEntryAdr);
                if (expireAt > 0L && expireAt <= ticker.currentTimeMillis())
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
            unlock(wasFirst); 
        }
    }

    boolean putEntry(long newHashEntryAdr, long hash, long keyLen, long bytes, boolean ifAbsent, long expireAt,
                     long oldValueAddr, long oldValueLen)
    {
        long removeHashEntryAdr = 0L;
        LongArrayList derefList = null;
        boolean wasFirst = lock();
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
                if (testExpireAt == 0L || testExpireAt > ticker.currentTimeMillis())
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
                        if (valueLen != oldValueLen || !Uns.memoryCompare(hashEntryAdr, Util.ENTRY_OFF_DATA + Util.roundUpTo8(keyLen), oldValueAddr, 0L, oldValueLen))
                            return false;
                    }
                }

                removeInternal(hashEntryAdr, prevEntryAdr, true);
                removeHashEntryAdr = hashEntryAdr;

                oldHashEntryAdr = hashEntryAdr;

                break;
            }

            derefList = ensureFreeSpaceForNewEntry(bytes);
            if (!hasFreeSpaceForNewEntry(bytes))
            {
                // need to decrement size since old entry has already been removed
                if (oldHashEntryAdr != 0L)
                    size--;
                return false;
            }

            if (hashEntryAdr == 0L)
            {
                if (size >= threshold)
                    rehash();

                size++;
            }

            add(newHashEntryAdr, hash, expireAt);

            if (hashEntryAdr == 0L)
                putAddCount++;
            else
                putReplaceCount++;

            return true;
        }
        finally
        {
            unlock(wasFirst); 
            if (removeHashEntryAdr != 0L)
                HashEntries.dereference(removeHashEntryAdr);
            if (derefList != null)
                for (int i = 0; i < derefList.size(); i++)
                    HashEntries.dereference(derefList.getLong(i));
        }
    }

    private static boolean notSameKey(long newHashEntryAdr, long newHash, long newKeyLen, long hashEntryAdr)
    {
        if (HashEntries.getHash(hashEntryAdr) != newHash) return true;

        long serKeyLen = HashEntries.getKeyLen(hashEntryAdr);
        return serKeyLen != newKeyLen
               || !Uns.memoryCompare(hashEntryAdr, Util.ENTRY_OFF_DATA, newHashEntryAdr, Util.ENTRY_OFF_DATA, serKeyLen);
    }

    abstract LongArrayList ensureFreeSpaceForNewEntry(long bytes);

    abstract boolean hasFreeSpaceForNewEntry(long bytes);

    void removeExpired()
    {
        if (timeouts != null)
            expiredEntries += timeouts.removeExpired(timeoutsExpireHandler);
    }

    void clear()
    {
        boolean wasFirst = lock();
        try
        {
            size = 0L;

            long next;
            for (int p = 0; p < table.size(); p++)
                for (long hashEntryAdr = table.getFirst(p);
                     hashEntryAdr != 0L;
                     hashEntryAdr = next)
                {
                    next = HashEntries.getNext(hashEntryAdr);

                    if (timeouts != null)
                    {
                        long expireAt = HashEntries.getExpireAt(hashEntryAdr);
                        if (expireAt > 0L)
                            timeouts.remove(hashEntryAdr, expireAt);
                    }

                    HashEntries.dereference(hashEntryAdr);
                }

            clearLruAndCapacity();

            table.clear();
        }
        finally
        {
            unlock(wasFirst);
        }
    }

    void removeEntry(long removeHashEntryAdr)
    {
        removeEntry(removeHashEntryAdr, true);
    }

    private void removeEntry(long removeHashEntryAdr, boolean removeFromTimeouts)
    {
        boolean wasFirst = lock();
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

                removeInternal(hashEntryAdr, prevEntryAdr, removeFromTimeouts);
                size--;
                removeCount++;

                return;
            }
            removeHashEntryAdr = 0L;
        }
        finally
        {
            unlock(wasFirst);
            if (removeHashEntryAdr != 0L)
                HashEntries.dereference(removeHashEntryAdr);
        }
    }

    boolean removeEntry(KeyBuffer key)
    {
        long removeHashEntryAdr = 0L;
        boolean wasFirst = lock();
        try
        {
            long prevEntryAdr = 0L;
            for (long hashEntryAdr = table.getFirst(key.hash());
                 hashEntryAdr != 0L;
                 prevEntryAdr = hashEntryAdr, hashEntryAdr = HashEntries.getNext(hashEntryAdr))
            {
                if (!key.sameKey(hashEntryAdr))
                    continue;

                // remove existing entry

                removeHashEntryAdr = hashEntryAdr;
                removeInternal(hashEntryAdr, prevEntryAdr, true);

                size--;
                removeCount++;

                return true;
            }

            return false;
        }
        finally
        {
            unlock(wasFirst);
            if (removeHashEntryAdr != 0L)
                HashEntries.dereference(removeHashEntryAdr);
        }
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

    abstract long[] hotN(int n);

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
        boolean wasFirst = lock();
        try
        {
            table.updateBucketHistogram(hist);
        }
        finally
        {
            unlock(wasFirst); 
        }
    }

    void getEntryAddresses(int mapSegmentIndex, int nSegments, LongArrayList hashEntryAdrs)
    {
        boolean wasFirst = lock();
        try
        {
            long t = ticker.currentTimeMillis();
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
            unlock(wasFirst); 
        }
    }

    static final class Table
    {
        final int mask;
        final long address;
        private boolean released;

        static Table create(int hashTableSize, boolean throwOOME)
        {
            int msz = Ints.checkedCast(Util.BUCKET_ENTRY_LEN * hashTableSize);
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

        void replaceSentinelLink(long hash, long hashEntryAdr, long prevEntryAdr, long newHashEntryAdr)
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

    void removeInternal(long hashEntryAdr, long prevEntryAdr, boolean removeFromTimeouts)
    {
        long hash = HashEntries.getHash(hashEntryAdr);

        table.removeLink(hash, hashEntryAdr, prevEntryAdr);

        if (removeFromTimeouts && timeouts != null)
        {
            long expireAt = HashEntries.getExpireAt(hashEntryAdr);
            if (expireAt > 0L)
                timeouts.remove(hashEntryAdr, expireAt);
        }

        removeFromLruAndUpdateCapacity(hashEntryAdr);
    }

    boolean replaceSentinelEntry(long hash, long oldHashEntryAdr, long newHashEntryAdr, long bytes, long expireAt)
    {
        LongArrayList derefList = null;

        boolean wasFirst = lock();
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

                table.replaceSentinelLink(hash, oldHashEntryAdr, prevEntryAdr, newHashEntryAdr);

                if (expireAt > 0L)
                {
                    if (timeouts != null)
                        timeouts.add(newHashEntryAdr, expireAt);
                    else
                        throw new IllegalStateException("entry TTLs not enabled on this cache instance");
                }

                derefList = ensureFreeSpaceForNewEntry(bytes);
                if (!hasFreeSpaceForNewEntry(bytes))
                    return false;

                replaceSentinelInLruAndUpdateCapacity(oldHashEntryAdr, newHashEntryAdr, bytes);

                return true;
            }

            return false;
        }
        finally
        {
            unlock(wasFirst); 

            if (derefList != null)
                for (int i = 0; i < derefList.size(); i++)
                    HashEntries.dereference(derefList.getLong(i));
        }
    }

    private void add(long hashEntryAdr, long hash, long expireAt)
    {
        table.addAsHead(hash, hashEntryAdr);

        addToLruAndUpdateCapacity(hashEntryAdr);

        if (expireAt > 0L)
        {
            if (timeouts != null)
                timeouts.add(hashEntryAdr, expireAt);
            else
                throw new IllegalStateException("entry TTLs not enabled on this cache instance");
        }
    }

    abstract void addToLruAndUpdateCapacity(long hashEntryAdr);

    abstract void removeFromLruAndUpdateCapacity(long hashEntryAdr);

    abstract void replaceSentinelInLruAndUpdateCapacity(long hashEntryAdr, long newHashEntryAdr, long bytes);

    abstract void clearLruAndCapacity();

    abstract void touch(long hashEntryAdr);

    boolean lock()
    {
        if (unlocked)
            return false;

        long t = Thread.currentThread().getId();

        if (t == lockFieldUpdater.get(this))
            return false;
        while (true)
        {
            if (lockFieldUpdater.compareAndSet(this, 0L, t))
                return true;

            // yield control to other thread.
            // Note: we cannot use LockSupport.parkNanos() as that does not
            // provide nanosecond resolution on Windows.
            Thread.yield();
        }
    }

    void unlock(boolean wasFirst)
    {
        if (unlocked || !wasFirst)
            return;

        long t = Thread.currentThread().getId();
        boolean r = lockFieldUpdater.compareAndSet(this, t, 0L);
        assert r;
    }

    @Override
    public String toString()
    {
        return String.valueOf(size);
    }
}
