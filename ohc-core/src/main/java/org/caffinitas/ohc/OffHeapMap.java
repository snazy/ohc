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

import java.util.concurrent.locks.StampedLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.caffinitas.ohc.Constants.*;

final class OffHeapMap
{
    // maximum hash table size
    private static final int MAX_TABLE_SIZE = 1 << 27;
    private static final Logger LOGGER = LoggerFactory.getLogger(OffHeapMap.class);

    private final double entriesPerSegmentTrigger;

    private volatile long size;

    // not nice to have a cyclic ref with ShardCacheImpl - but using an interface just for the sake of it feels too heavy
    private final MultiTableCacheImpl shardCache;
    private final StampedLock lock;

    private volatile Table table;

    private volatile long lruHead;
    private volatile long lruTail;

    private long rehashes;

    OffHeapMap(OHCacheBuilder builder, MultiTableCacheImpl shardCache)
    {
        this.shardCache = shardCache;
        lock = new StampedLock();

        int hts = builder.getHashTableSize();
        if (hts < 8192)
            hts = 8192;
        table = new Table(roundUpToPowerOf2(hts));

        this.entriesPerSegmentTrigger = builder.getEntriesPerSegmentTrigger();
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

    long lock()
    {
        return lock.writeLock();
    }

    void unlock(long stamp)
    {
        lock.unlockWrite(stamp);
    }

    long getEntry(long hash, BytesSource keySource)
    {
        int loops = 0;
        for (long hashEntryAdr = table.first(hash);
             hashEntryAdr != 0L;
             hashEntryAdr = HashEntries.getNext(hashEntryAdr), loops++)
        {
            if (notSameKey(hash, keySource, loops, hashEntryAdr))
                continue;

            // return existing entry

            table.removeLink(hash, hashEntryAdr);
            table.addLinkAsHead(hash, hashEntryAdr);

            entryUsed(hashEntryAdr);

            return hashEntryAdr;
        }

        // not found

        return 0L;
    }

    long replaceEntry(long hash, BytesSource keySource, long newHashEntryAdr)
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

            table.removeLink(hash, hashEntryAdr);

            break;
        }

        if (hashEntryAdr == 0L)
            size++;

        table.addLinkAsHead(hash, newHashEntryAdr);
        entryReplaced(hashEntryAdr, newHashEntryAdr);

        return hashEntryAdr;
    }

    long removeEntry(long hash, BytesSource keySource)
    {
        int loops = 0;
        for (long hashEntryAdr = table.first(hash);
             hashEntryAdr != 0L;
             hashEntryAdr = HashEntries.getNext(hashEntryAdr), loops++)
        {
            if (notSameKey(hash, keySource, loops, hashEntryAdr))
                continue;

            // remove existing entry

            table.removeLink(hash, hashEntryAdr);

            entryRemoved(hashEntryAdr);

            size--;

            return hashEntryAdr;
        }

        // no entry to remove

        return 0L;
    }

    private boolean notSameKey(long hash, BytesSource keySource, int loops, long hashEntryAdr)
    {
        if (loops >= entriesPerSegmentTrigger)
        {
            LOGGER.warn("Degraded OHC performance! Segment linked list very long - rehash triggered");
            rehash();
        }

        long hashEntryHash = HashEntries.getHash(hashEntryAdr);
        if (hashEntryHash != hash)
            return true;

        long serKeyLen = HashEntries.getHashKeyLen(hashEntryAdr);
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

        long t0 = System.currentTimeMillis();

        Table newTable = new Table(tableSize * 2);
        long next, hash;

        for (int part = 0; part < tableSize; part++)
            for (long hashEntryAdr = tab.first(part);
                 hashEntryAdr != 0L;
                 hashEntryAdr = next)
            {
                next = HashEntries.getNext(hashEntryAdr);

                HashEntries.setNext(hashEntryAdr, 0L);
                hash = HashEntries.getHash(hashEntryAdr);
                newTable.addLinkAsHead(hash, hashEntryAdr);
            }

        long t = System.currentTimeMillis() - t0;
        LOGGER.debug("Rehashed table - increased table size from {} to {} in {}ms", tableSize, tableSize * 2, t);

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
        final int segmentMask;
        final long address;

        public Table(int hashTableSize)
        {
            int msz = (int) BUCKET_ENTRY_LEN * hashTableSize;
            this.address = Uns.allocate(msz);
            segmentMask = hashTableSize - 1;
            // It's important to initialize the hash segment table memory.
            // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
            Uns.setMemory(address, 0L, msz, (byte) 0);
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
            return (int) (hash & segmentMask);
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
            return segmentMask + 1;
        }
    }

    //
    // eviction/replacement/cleanup
    //

    private void removeLRU(long hashEntryAdr)
    {
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

    private void addLRU(long hashEntryAdr)
    {
        long h = lruHead;
        lruNext(hashEntryAdr, h);
        if (h != 0L)
            lruPrev(h, hashEntryAdr);
        lruPrev(hashEntryAdr, 0L);
        lruHead = hashEntryAdr;

        if (lruTail == 0L)
            lruTail = hashEntryAdr;
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

    long cleanUp(long recycleGoal)
    {
        long prev;
        long evicted = 0L;
        for (long hashEntryAdr = lruTail;
             hashEntryAdr != 0L && recycleGoal > 0L;
             hashEntryAdr = prev)
        {
            prev = lruPrev(hashEntryAdr);

            removeLRU(hashEntryAdr);

            long bytes = evict(hashEntryAdr);

            recycleGoal -= bytes;

            evicted ++;
        }

        return evicted;
    }

    private long evict(long hashEntryAdr)
    {
        long bytes = HashEntries.getAllocLen(hashEntryAdr);
        long hash = HashEntries.getHash(hashEntryAdr);
        table.removeLink(hash, hashEntryAdr);

        if (HashEntries.dereference(hashEntryAdr))
            shardCache.free(hashEntryAdr);

        size--;

        return bytes;
    }

}
