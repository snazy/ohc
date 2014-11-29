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
package org.caffinitas.ohc.mono;

import java.io.IOError;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.caffinitas.ohc.api.ByteArrayOut;
import org.caffinitas.ohc.api.BytesSink;
import org.caffinitas.ohc.api.BytesSource;
import org.caffinitas.ohc.api.CacheSerializer;
import org.caffinitas.ohc.api.OHCache;
import org.caffinitas.ohc.api.OHCacheBuilder;
import org.caffinitas.ohc.api.OHCacheStats;
import org.caffinitas.ohc.api.PutResult;
import org.caffinitas.ohc.internal.Util;

public final class MonoCacheImpl<K, V> implements OHCache<K, V>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(MonoCacheImpl.class);

    private static final AtomicInteger ohcId = new AtomicInteger();

    static
    {
        try
        {
            Field f = AtomicLong.class.getDeclaredField("VM_SUPPORTS_LONG_CAS");
            f.setAccessible(true);
            if (!(Boolean) f.get(null))
                throw new IllegalStateException("Off Heap Cache implementation requires a JVM that supports CAS on long fields");
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static final int MIN_HASH_TABLE_SIZE = 32;
    public static final int ONE_GIGABYTE = 1024 * 1024 * 1024;

    private final long capacity;
    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;

    private final DataMemory dataMemory;
    private final HashEntryAccess hashEntryAccess;
    private final HashPartitions hashPartitions;
    private final int entriesPerPartitionTrigger;
    private final long cleanUpTriggerMinFree;

    volatile boolean closed;

    private final Thread scheduler;

    private boolean statisticsEnabled;

    private final LongAdder size = new LongAdder();
    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder putAddCount = new LongAdder();
    private final LongAdder putReplaceCount = new LongAdder();
    private final LongAdder putFailCount = new LongAdder();
    private final LongAdder unlinkCount = new LongAdder();
    private final LongAdder loadSuccessCount = new LongAdder();
    private final LongAdder loadExceptionCount = new LongAdder();
    private final LongAdder totalLoadTime = new LongAdder();
    private final LongAdder evictedEntries = new LongAdder();
    private final LongAdder evictionCount = new LongAdder();
    private final LongAdder rehashCount = new LongAdder();

    private final AtomicBoolean globalLock = new AtomicBoolean();

    final Signals signals = new Signals();

    public MonoCacheImpl(OHCacheBuilder<K, V> builder)
    {
        long minSize = 8 * 1024 * 1024; // very small

        long cap = builder.getCapacity();
        if (cap < minSize)
            throw new IllegalArgumentException("Total size must not be less than " + minSize + " is (" + builder.getCapacity() + ')');
        capacity = cap;

        int hts = builder.getHashTableSize();
        if (hts > 0)
        {
            if (hts < MIN_HASH_TABLE_SIZE)
                throw new IllegalArgumentException("Block size must not be less than " + MIN_HASH_TABLE_SIZE + " is (" + hts + ')');
            hts = Util.roundUpToPowerOf2(hts);
            if (hts != builder.getHashTableSize())
                LOGGER.warn("Using hash table size {} instead of configured hash table size {} - adjust your configuration to be precise", hts, builder.getHashTableSize());
        }
        else
        {
            // auto-size hash table
            if (cap / 1024 > Integer.MAX_VALUE)
                hts = 2 ^ 24;
            else
                hts = (int) (cap / 4096);
        }

        int entriesPerPartitionTrigger = builder.getEntriesPerPartitionTrigger();
        if (entriesPerPartitionTrigger < 1)
            entriesPerPartitionTrigger = 1;
        this.entriesPerPartitionTrigger = entriesPerPartitionTrigger;

        this.keySerializer = builder.getKeySerializer();
        this.valueSerializer = builder.getValueSerializer();

        double cut = builder.getCleanUpTriggerMinFree();
        long cleanUpTriggerMinFree;
        if (cut < 0d)
        {
            // auto-sizing

            // 12.5% if capacity less than 8GB
            // 10% if capacity less than 16 GB
            // 5% if capacity is higher than 16GB
            if (capacity < 8L * ONE_GIGABYTE)
                cleanUpTriggerMinFree = (long) (.125d * capacity);
            else if (capacity < 16L * ONE_GIGABYTE)
                cleanUpTriggerMinFree = (long) (.10d * capacity);
            else
                cleanUpTriggerMinFree = (long) (.05d * capacity);
        }
        else
        {
            if (cut >= 1d)
                throw new IllegalArgumentException("Invalid clean-up percentage trigger value " + String.format("%.2f", cut));
            cut *= capacity;
            cleanUpTriggerMinFree = (long) cut;
        }
        this.cleanUpTriggerMinFree = cleanUpTriggerMinFree;

        this.statisticsEnabled = builder.isStatisticsEnabled();

        this.dataMemory = new DataMemory(capacity);

        try
        {
            this.hashPartitions = new HashPartitions(hts);
            this.hashEntryAccess = new HashEntryAccess(dataMemory, hashPartitions, entriesPerPartitionTrigger, signals);

            scheduler = new Thread(new Runnable()
            {
                public void run()
                {
                    while (true)
                    {
                        if (!signals.waitFor())
                            return;

                        if (closed)
                            return;

                        try
                        {
                            Uns.processOutstandingFree();

                            if (signals.cleanupTrigger.compareAndSet(true, false))
                                cleanUp();
                            if (signals.rehashTrigger.compareAndSet(true, false))
                                rehash();
                        }
                        catch (Throwable t)
                        {
                            LOGGER.error("Failure during triggered cleanup or rehash", t);
                        }
                    }
                }
            }, "OHC cleanup #" + ohcId.incrementAndGet());
            scheduler.start();

            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Initialized OHC with capacity={}, hash-table-size={}", cap, hts);
        }
        catch (Throwable t)
        {
            if (t instanceof RuntimeException)
                throw (RuntimeException) t;
            throw (Error) t;
        }
    }

    public void close()
    {
        if (closed)
            return;

        closed = true;

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Closing OHC instance");

        try
        {

            if (scheduler != null)
            {
                // just a hint to stop waiting
                signals.signalHousekeeping();

                try
                {
                    scheduler.join(60000);
                    scheduler.interrupt();
                    if (scheduler.isAlive())
                        throw new RuntimeException("Background OHC scheduler did not terminate normally. This usually indicates a bug.");
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }

                Uns.processOutstandingFree();
            }
        }
        finally
        {
            // releasing memory immediately is dangerous since other threads may still access the data
            // Need to orderly clear the cache before releasing (this involves hash-partition and hash-entry locks,
            // which ensure that no other thread is accessing OHC)
            removeAllInt();

            hashPartitions.release();
        }
    }

    private void removeAllInt()
    {
        for (int partNo = 0; partNo < getHashTableSize(); partNo++)
        {
            long partHead;
            long lock = hashPartitions.lockPartition(partNo, true);
            try
            {
                partHead = hashPartitions.getPartitionHead(partNo);
                hashPartitions.setPartitionHead(partNo, 0L);
            }
            finally
            {
                hashPartitions.unlockPartition(lock, partNo, true);
            }

            long next;
            long cnt = 0;
            for (long hashEntryAdr = partHead; hashEntryAdr != 0L; hashEntryAdr = next, cnt++)
            {
                next = HashEntryAccess.getNextEntry(hashEntryAdr);
                // need to lock the entry since another thread might still read from it
                HashEntryAccess.lockEntryWrite(hashEntryAdr);
                dataMemory.free(hashEntryAdr, false);
            }
            unlinkCount.add(cnt);
            size.add(-cnt);
        }
    }

    long[] calcListLengths(boolean longestOnly)
    {
        long[] ll = new long[longestOnly ? 1 : getHashTableSize()];
        for (int partNo = 0; partNo < ll.length; partNo++)
        {
            long lock = hashPartitions.lockPartition(partNo, false);
            try
            {
                int l = 0;
                for (long hashEntryAdr = hashPartitions.getPartitionHead(partNo); hashEntryAdr != 0L; hashEntryAdr = HashEntryAccess.getNextEntry(hashEntryAdr))
                    l++;
                if (longestOnly)
                {
                    if (l > ll[0])
                        ll[0] = l;
                }
                else
                    ll[partNo] = l;
            }
            finally
            {
                hashPartitions.unlockPartition(lock, partNo, false);
            }
        }
        return ll;
    }

    public boolean isStatisticsEnabled()
    {
        return statisticsEnabled;
    }

    public void setStatisticsEnabled(boolean statisticsEnabled)
    {
        this.statisticsEnabled = statisticsEnabled;
    }

    public void resetStatistics()
    {
        hitCount.reset();
        missCount.reset();
        putAddCount.reset();
        putReplaceCount.reset();
        putFailCount.reset();
        unlinkCount.reset();
        loadSuccessCount.reset();
        loadExceptionCount.reset();
        totalLoadTime.reset();
        evictedEntries.reset();
        evictionCount.reset();
        rehashCount.reset();
    }

    private void assertNotClosed()
    {
        if (closed)
            throw new IllegalStateException("OHCache instance already closed");
    }

    public long getCapacity()
    {
        return capacity;
    }

    public int getHashTableSize()
    {
        return hashPartitions.hashTableSize();
    }

    public long getMemUsed()
    {
        return capacity - freeCapacity();
    }

    public PutResult put(long hash, BytesSource keySource, BytesSource valueSource)
    {
        return put(hash, keySource, valueSource, null);
    }

    public PutResult put(long hash, BytesSource keySource, BytesSource valueSource, BytesSink oldValueSink)
    {
        assertNotClosed();

        if (keySource == null)
            throw new NullPointerException();
        if (valueSource == null)
            throw new NullPointerException();
        if (keySource.size() <= 0)
            throw new ArrayIndexOutOfBoundsException();
        if (valueSource.size() < 0)
            throw new ArrayIndexOutOfBoundsException();

        // Allocate and fill new hash entry.
        // Do this outside of the hash-partition lock to hold that lock no longer than necessary.
        long newHashEntryAdr = hashEntryAccess.createNewEntry(hash, keySource, valueSource, -1L);
        if (newHashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                putFailCount.increment();
            remove(hash, keySource);
            return PutResult.NO_MORE_FREE_CAPACITY;
        }

        return putInternal(hash, keySource, oldValueSink, newHashEntryAdr);
    }

    private PutResult putInternal(long hash, BytesSource keySource, BytesSink oldValueSink, long newHashEntryAdr)
    {
        long oldHashEntryAdr;

        maybeTriggerCleanup();

        // lock hash partition
        long lock = hashPartitions.lockPartition(hash, true);
        try
        {
            // find existing entry
            oldHashEntryAdr = hashEntryAccess.findHashEntry(hash, keySource);

            // remove existing entry
            if (oldHashEntryAdr != 0L)
            {
                hashEntryAccess.unlinkFromPartition(hash, oldHashEntryAdr);
                if (statisticsEnabled)
                    unlinkCount.increment();
            }

            // add new entry
            hashEntryAccess.addEntryToPartition(hash, newHashEntryAdr);
        }
        finally
        {
            // release hash partition
            hashPartitions.unlockPartition(lock, hash, true);
        }

        // No old entry - just return.
        if (oldHashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                putAddCount.increment();
            size.add(1);
            return PutResult.ADD;
        }

        try
        {
            // We have to lock the old entry before we can actually free the allocated blocks.
            // There's no need for a corresponding unlock because we use CAS on a field for locking.
            HashEntryAccess.lockEntryWrite(oldHashEntryAdr);

            // Write old value (if wanted).
            if (oldValueSink != null)
                HashEntryAccess.writeValueToSink(oldHashEntryAdr, oldValueSink);
        }
        finally
        {
            // release old entry
            dataMemory.free(oldHashEntryAdr, true);
        }

        if (statisticsEnabled)
            putReplaceCount.increment();
        return PutResult.REPLACE;
    }

    public boolean get(long hash, BytesSource keySource, BytesSink valueSink)
    {
        assertNotClosed();

        if (keySource == null)
            throw new NullPointerException();
        if (keySource.size() <= 0)
            throw new ArrayIndexOutOfBoundsException();

        // find + lock hash partition
        long lock = hashPartitions.lockPartition(hash, false);
        long hashEntryAdr;
        long entryLock = 0L;
        try
        {
            // find hash entry
            hashEntryAdr = hashEntryAccess.findHashEntry(hash, keySource);
            if (hashEntryAdr != 0L)
            {
                if (valueSink == null)
                {
                    hitCount.increment();
                    return true;
                }

                // to keep the hash-partition lock short, lock the entry here
                entryLock = HashEntryAccess.lockEntryRead(hashEntryAdr);
            }
        }
        finally
        {
            // release hash partition
            hashPartitions.unlockPartition(lock, hash, false);
        }

        if (statisticsEnabled)
            (hashEntryAdr == 0L ? missCount : hitCount).increment();

        if (hashEntryAdr == 0L)
            return false;

        // Write the value to the caller and unlock the entry.
        try
        {
            HashEntryAccess.touchEntry(hashEntryAdr);

            HashEntryAccess.writeValueToSink(hashEntryAdr, valueSink);
        }
        finally
        {
            HashEntryAccess.unlockEntryRead(hashEntryAdr, entryLock);
        }

        return true;
    }

    public boolean remove(long hash, BytesSource keySource)
    {
        assertNotClosed();

        if (keySource == null)
            throw new NullPointerException();
        if (keySource.size() <= 0)
            throw new ArrayIndexOutOfBoundsException();

        // find + lock hash partition
        long hashEntryAdr;
        long lock = hashPartitions.lockPartition(hash, true);
        try
        {
            // find hash entry
            hashEntryAdr = hashEntryAccess.findHashEntry(hash, keySource);
            if (hashEntryAdr == 0L)
                return false;

            hashEntryAccess.unlinkFromPartition(hash, hashEntryAdr);
            if (statisticsEnabled)
                unlinkCount.increment();

            // We have to lock the old entry before we can actually free the allocated blocks.
            // There's no need for a corresponding unlock because we use CAS on a field for locking.
            HashEntryAccess.lockEntryWrite(hashEntryAdr);
        }
        finally
        {
            // release hash partition
            hashPartitions.unlockPartition(lock, hash, true);
        }

        // free memory
        dataMemory.free(hashEntryAdr, true);

        size.add(-1);

        return true;
    }

    public void invalidate(Object o)
    {
        BytesSource.ByteArraySource ks = keySource((K) o);

        remove(ks.hash(), ks);
    }

    public V getIfPresent(Object o)
    {
        assertNotClosed();

        if (valueSerializer == null)
            throw new NullPointerException("no valueSerializer configured");

        BytesSource.ByteArraySource keySource = keySource((K) o);
        long hash = keySource.hash();

        // find + lock hash partition
        long hashEntryAdr;
        long entryLock = 0L;
        long lock = hashPartitions.lockPartition(hash, false);
        try
        {
            // find hash entry
            hashEntryAdr = hashEntryAccess.findHashEntry(hash, keySource);
            if (hashEntryAdr != 0L)
            {
                // to keep the hash-partition lock short, lock the entry here
                entryLock = HashEntryAccess.lockEntryRead(hashEntryAdr);
            }
        }
        finally
        {
            // release hash partition
            hashPartitions.unlockPartition(lock, hash, false);
        }

        if (statisticsEnabled)
            (hashEntryAdr == 0L ? missCount : hitCount).increment();

        if (hashEntryAdr == 0L)
            return null;

        HashEntryAccess.touchEntry(hashEntryAdr);

        try
        {
            return valueSerializer.deserialize(HashEntryAccess.readValueFrom(hashEntryAdr));
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finally
        {
            HashEntryAccess.unlockEntryRead(hashEntryAdr, entryLock);
        }
    }

    private BytesSource.ByteArraySource keySource(K o)
    {
        if (keySerializer == null)
            throw new NullPointerException("no keySerializer configured");
        long size = keySerializer.serializedSize(o);
        if (size < 0)
            throw new IllegalArgumentException();
        if (size >= Integer.MAX_VALUE)
            throw new IllegalArgumentException("serialized size of key too large (>2GB)");

        byte[] tmp = new byte[(int) size];
        try
        {
            keySerializer.serialize(o, new ByteArrayOut(tmp));
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return new BytesSource.ByteArraySource(tmp);
    }

    public void put(K k, V v)
    {
        assertNotClosed();

        if (valueSerializer == null)
            throw new NullPointerException("no valueSerializer configured");

        BytesSource.ByteArraySource ks = keySource(k);
        long hash = ks.hash();
        long valueLen = valueSerializer.serializedSize(v);

        // Allocate and fill new hash entry.
        // Do this outside of the hash-partition lock to hold that lock no longer than necessary.
        long newHashEntryAdr = hashEntryAccess.createNewEntry(hash, ks, null, valueLen);
        if (newHashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                putFailCount.increment();
            remove(ks.hash(), ks);
            return;
        }

        try
        {
            HashEntryAccess.valueToHashEntry(newHashEntryAdr, valueSerializer, v, ks.size(), valueLen);
        }
        catch (IOException e)
        {
            dataMemory.free(newHashEntryAdr, true);
            throw new IOError(e);
        }

        putInternal(hash, ks, null, newHashEntryAdr);
    }

    boolean rehash()
    {
        assertNotClosed();

        if (!globalLock.compareAndSet(false, true))
            return false;
        try
        {
            rehashInt();

            return true;
        }
        finally
        {
            globalLock.set(false);
        }
    }

    public void cleanUp()
    {
        assertNotClosed();

        // TODO use a better LRU algorithm - e.g. increment a counter on each access and divide all counters
        // by 2 at each cleanup. Might not be the best solution, but definitely better that a stupid LRU based
        // on last access timestamp.

        if (!globalLock.compareAndSet(false, true))
            return;
        try
        {
            long freeCapacity = dataMemory.freeCapacity();
            if (freeCapacity > cleanUpTriggerMinFree)
                return;

            long entries = size();

            long perEntryMemory = (capacity - freeCapacity) / entries;

            long recycleGoal = cleanUpTriggerMinFree - freeCapacity;

            int entriesToRemove = (int) (recycleGoal / perEntryMemory);
            if (entriesToRemove < 4)
                entriesToRemove = 4;
            boolean rehashRequired = false;

            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Start cleanup to remove {} entries to recycle {} bytes " +
                             "(bytes-per-entry={}, entries={}, capacity={}, free={})",
                             entriesToRemove, recycleGoal,
                             perEntryMemory, entries, capacity, freeCapacity);

            //
            // iterate over all partitions and all entries to find the #entriesToRemove oldest entries
            //
            long[] candidateHash = new long[entriesToRemove];
            long[] candidateTS = new long[entriesToRemove];
            int candidateCount = 0;

            for (int partNo = 0; partNo < getHashTableSize(); partNo++)
            {
                long lock = hashPartitions.lockPartition(partNo, false);
                try
                {
                    int listLen = 0;
                    for (long hashEntryAdr = hashPartitions.getPartitionHead(partNo);
                         hashEntryAdr != 0L;
                         hashEntryAdr = HashEntryAccess.getNextEntry(hashEntryAdr), listLen++)
                    {
                        if (candidateCount < entriesToRemove)
                        {
                            candidateHash[candidateCount] = HashEntryAccess.getEntryHash(hashEntryAdr);
                            candidateTS[candidateCount] = HashEntryAccess.getEntryTimestamp(hashEntryAdr);
                            candidateCount++;
                        }
                        else
                        {
                            long ts = HashEntryAccess.getEntryTimestamp(hashEntryAdr);
                            for (int c = 0; c < candidateTS.length; c++)
                            {
                                long cts = candidateTS[c];
                                if (ts < cts)
                                {
                                    candidateHash[c] = HashEntryAccess.getEntryHash(hashEntryAdr);
                                    candidateTS[c] = ts;
                                    break;
                                }
                            }
                        }
                    }
                    if (listLen >= entriesPerPartitionTrigger)
                        rehashRequired = true;
                }
                finally
                {
                    hashPartitions.unlockPartition(lock, partNo, false);
                }
            }

            //
            // now we have the #fillIndex least used entries
            //
            long[] result = cleanupPerform(candidateHash, candidateTS, candidateCount);
            long capacityFreed = result[0];
            long entriesRemoved = result[1];

            size.add(-entriesRemoved);
            unlinkCount.add(entriesRemoved);

            evictionCount.add(1);
            evictedEntries.add(entriesRemoved);

            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Cleanup statistics: removed={} bytes-recycled={}, invalid={} " +
                             "(entries={}, capacity={}, free={})",
                             entriesRemoved, capacityFreed, candidateCount - entriesRemoved,
                             size(), capacity, freeCapacity());

            if (rehashRequired)
                rehashInt();
        }
        finally
        {
            globalLock.set(false);

            signals.cleanupTrigger.set(false);
            maybeTriggerCleanup();
        }
    }

    private long[] cleanupPerform(long[] candidateHash, long[] candidateTS, int candidateCount)
    {
        long[] recycleAdrs = new long[candidateHash.length];

        long[] result = new long[2];

        int hts = getHashTableSize();
        for (int partNo = 0; partNo < hts; partNo++)
        {
            long lock = 0L;
            long partHead = 0L;
            int recycleIdx = 0;
            try
            {
                for (int c = 0; c < candidateCount; c++)
                {
                    long h = candidateHash[c];

                    // skip if candidate is not for current partition
                    if (candidateTS[c] == 0L || hashPartitions.partitionForHash(h) != partNo)
                        continue;

                    // acquire long-run-lock for current partition, if not already locked
                    if (lock == 0L)
                    {
                        lock = hashPartitions.lockPartitionForLongRun(partNo);
                        partHead = hashPartitions.getPartitionHead(partNo);
                    }

                    // walk through partition linked-list and re-check for the candidate
                    for (long hashEntryAdr = partHead;
                         hashEntryAdr != 0L;
                         hashEntryAdr = HashEntryAccess.getNextEntry(hashEntryAdr))
                    {
                        // only remove the candidate if the last-access timestamp matches
                        if (HashEntryAccess.getEntryHash(hashEntryAdr) == h
                            // TODO decide whether to compare the entry touch timestamp again - may degrade cleanup "efficiency" but increase "accuracy"
//                            && candidateTS[c] == hashEntryAccess.getEntryTimestamp(hashEntryAdr)
                        )
                        {
                            partHead = hashEntryAccess.unlinkFromPartition(partHead, h, hashEntryAdr);

                            HashEntryAccess.lockEntryWrite(hashEntryAdr);

                            recycleAdrs[recycleIdx++] = hashEntryAdr;
                            candidateTS[c] = 0L;
                            break;
                        }
                    }

                    if (partHead == 0L)
                        break;
                }
            }
            finally
            {
                if (lock != 0L)
                {
                    hashPartitions.unlockPartitionForLongRun(lock, partNo);

                    //
                    // free the memory outside of partition lock because
                    // free() is expensive - synchronized in JNA and locked in JEMalloc
                    //
                    result[1] += recycleIdx;
                    for (int i = 0; i < recycleIdx; i++)
                    {
                        long recycleAdr = recycleAdrs[i];
                        if (recycleAdr != 0L)
                        {

                            result[0] += dataMemory.free(recycleAdr, false);
                            recycleAdrs[i] = 0L;
                        }
                    }
                }
            }
        }

        return result;
    }

    public long freeCapacity()
    {
        return dataMemory.freeCapacity();
    }

    public OHCacheStats extendedStats()
    {
        return new OHCacheStats(stats(), calcListLengths(false), size(),
                                capacity, dataMemory.freeCapacity(),
                                evictionCount.longValue(), rehashCount.longValue(),
                                putAddCount.longValue(), putReplaceCount.longValue(), putFailCount.longValue(), unlinkCount.longValue());
    }

    public CacheStats stats()
    {
        assertNotClosed();

        return new CacheStats(
                             hitCount.longValue(),
                             missCount.longValue(),
                             loadSuccessCount.longValue(),
                             loadExceptionCount.longValue(),
                             totalLoadTime.longValue(),
                             evictedEntries.longValue()
        );
    }

    public long size()
    {
        assertNotClosed();

        return size.longValue();
    }

    public void invalidateAll()
    {
        assertNotClosed();

        removeAllInt();
    }

    public void invalidateAll(Iterable<?> iterable)
    {
        for (Object o : iterable)
            invalidate(o);
    }

    public void putAll(Map<? extends K, ? extends V> map)
    {
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    public ImmutableMap<K, V> getAllPresent(Iterable<?> iterable)
    {
        assertNotClosed();

        ImmutableMap.Builder<K, V> r = ImmutableMap.builder();
        for (Object o : iterable)
        {
            V v = getIfPresent(o);
            if (v != null)
                r.put((K) o, v);
        }
        return r.build();
    }

    public V get(K k, Callable<? extends V> callable) throws ExecutionException
    {
        V v = getIfPresent(k);
        if (v == null)
        {
            long t0 = System.currentTimeMillis();
            try
            {
                v = callable.call();
                loadSuccessCount.increment();
            }
            catch (Exception e)
            {
                loadExceptionCount.increment();
                throw new ExecutionException(e);
            }
            finally
            {
                totalLoadTime.add(System.currentTimeMillis() - t0);
            }
            put(k, v);
        }
        return v;
    }

    public Iterator<K> hotN(int hotN)
    {
        assertNotClosed();

        if (keySerializer == null)
            throw new NullPointerException("no keySerializer configured");

        long minTS = Long.MAX_VALUE;
        long[] hotEntries = new long[hotN];
        long[] hotLocks = new long[hotN];
        long[] hotTS = new long[hotN];
        int hotFound = 0;

        int hts = getHashTableSize();
        for (int partNo = 0; partNo < hts; partNo++)
        {
            long partLock = hashPartitions.lockPartition(partNo, false);
            try
            {
                for (long hashEntryAdr = hashPartitions.getPartitionHead(partNo);
                     hashEntryAdr != 0L;
                     hashEntryAdr = HashEntryAccess.getNextEntry(hashEntryAdr))
                {
                    long ts = HashEntryAccess.getEntryTimestamp(hashEntryAdr);

                    if (hotFound < hotN)
                    {
                        hotLocks[hotFound] = HashEntryAccess.lockEntryRead(hashEntryAdr);
                        hotTS[hotFound] = ts;
                        hotEntries[hotFound++] = hashEntryAdr;
                        minTS = Math.min(ts, minTS);
                    }
                    else if (ts > minTS)
                    {
                        long entryLock = HashEntryAccess.lockEntryRead(hashEntryAdr);
                        for (int i = 0; i < hotTS.length; i++)
                        {
                            if (hotTS[i] == minTS)
                            {
                                HashEntryAccess.unlockEntryRead(hashEntryAdr, hotLocks[i]);
                                hotLocks[i] = entryLock;
                                hotTS[i] = ts;
                                hotEntries[i] = hashEntryAdr;
                                break;
                            }
                        }
                        minTS = Util.minOf(hotTS);
                    }
                }
            }
            finally
            {
                hashPartitions.unlockPartition(partLock, partNo, false);
            }
        }

        // TODO if necessary implement a functionality to materialize the keys when the iterator is accessed but keep in mind that the entries must be unlocked !

        List<K> keys = new ArrayList<>(hotFound);
        for (int i = 0; i < hotFound; i++)
            try
            {
                keys.add(keySerializer.deserialize(HashEntryAccess.readKeyFrom(hotEntries[i])));
            }
            catch (IOException e)
            {
                for (; i < hotFound; i++)
                    HashEntryAccess.unlockEntryRead(hotEntries[i], hotLocks[i]);
                throw new IOError(e);
            }
            finally
            {
                if (i<hotFound)
                    HashEntryAccess.unlockEntryRead(hotEntries[i], hotLocks[i]);
            }

        return keys.iterator();
    }

    public ConcurrentMap<K, V> asMap()
    {
        throw new UnsupportedOperationException();
    }

    private void rehashInt()
    {
        int hashTableSize = getHashTableSize();
        if (hashTableSize == Constants.MAX_TABLE_SIZE)
            return;
        int newHashTableSize = hashTableSize << 1;
        LOGGER.info("OHC hash table resize from {} to {} starts...", hashTableSize, newHashTableSize);

        long t0 = System.currentTimeMillis();

        // first lock all old partitions for long run operation
        for (int partNo = 0; partNo < hashTableSize; partNo++)
            hashPartitions.lockPartitionForLongRun(partNo);

        // Creates the new hash table. All hash partitions are already locked for "long run".
        // Also immediately switches to the new table - new hash partitions are unlocked as rehash progresses.
        HashPartitions.Table oldTable = hashPartitions.prepareHashTable(newHashTableSize, true);

        long entries = 0;

        for (int partNo = 0; partNo < hashTableSize; partNo++)
        {
            long curr0 = 0L;
            long curr1 = 0L;

            try
            {
                long next;
                for (long hashEntryAdr = Uns.getLongVolatile(oldTable.partitionAddressHead(partNo));
                     hashEntryAdr != 0L;
                     hashEntryAdr = next)
                {
                    next = HashEntryAccess.getNextEntry(hashEntryAdr);

                    entries++;

                    long hash = HashEntryAccess.getEntryHash(hashEntryAdr);
                    if ((hash & hashTableSize) == 0)
                    {
                        HashEntryAccess.setPreviousEntry(hashEntryAdr, curr0);
                        if (curr0 == 0L)
                            hashPartitions.setPartitionHead(partNo, hashEntryAdr);
                        else
                            HashEntryAccess.setNextEntry(curr0, hashEntryAdr);
                        curr0 = hashEntryAdr;
                    }
                    else
                    {
                        HashEntryAccess.setPreviousEntry(hashEntryAdr, curr1);
                        if (curr1 == 0L)
                            hashPartitions.setPartitionHead(partNo | hashTableSize, hashEntryAdr);
                        else
                            HashEntryAccess.setNextEntry(curr1, hashEntryAdr);
                        curr1 = hashEntryAdr;
                    }
                }
                if (curr0 != 0L)
                    HashEntryAccess.setNextEntry(curr0, 0L);
                else
                    hashPartitions.setPartitionHead(partNo, 0L);
                if (curr1 != 0L)
                    HashEntryAccess.setNextEntry(curr1, 0L);
                else
                    hashPartitions.setPartitionHead(partNo | hashTableSize, 0L);
            }
            finally
            {
                // mark the old lock as invalid to let waiters fail immediately
                Uns.unlockForFail(oldTable.partitionAddressLock(partNo), Uns.longRunStamp());

                hashPartitions.rehashProgress(partNo, partNo | hashTableSize);
            }
        }

        oldTable.release();

        rehashCount.increment();
        signals.rehashTrigger.set(false);

        long t = System.currentTimeMillis() - t0;
        LOGGER.info("OHC hash table resize from {} to {} took {}ms ({} entries)", hashTableSize, newHashTableSize, t, entries);
    }

    void maybeTriggerCleanup()
    {
        if (dataMemory.freeCapacity() < cleanUpTriggerMinFree)
            signals.triggerCleanup();
    }
}
