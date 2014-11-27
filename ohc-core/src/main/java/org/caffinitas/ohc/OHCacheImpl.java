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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.cache.CacheStats;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class OHCacheImpl<K, V> implements OHCache<K, V>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(OHCacheImpl.class);

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
            throw new RuntimeException();
        }
    }

    public static final int MIN_HASH_TABLE_SIZE = 32;
    private static final int MAXIMUM_INT = 1 << 30;
    public static final int ONE_GIGABYTE = 1024 * 1024 * 1024;

    private final long capacity;
    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;

    private final DataMemory dataMemory;
    private final HashEntryAccess hashEntryAccess;
    private final HashPartitions hashPartitions;
    private final int lruListLenTrigger;

    private volatile boolean closed;

    private final Thread scheduler;

    private boolean statisticsEnabled;

    private final LongAdder size = new LongAdder();
    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder loadSuccessCount = new LongAdder();
    private final LongAdder loadExceptionCount = new LongAdder();
    private final LongAdder totalLoadTime = new LongAdder();
    private final LongAdder evictionCount = new LongAdder();
    private final LongAdder rehashCount = new LongAdder();

    private final AtomicBoolean globalLock = new AtomicBoolean();

    private final Signals signals = new Signals();

    OHCacheImpl(OHCacheBuilder<K, V> builder)
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
            hts = roundUpToPowerOf2(hts);
            if (hts != builder.getHashTableSize())
                LOGGER.warn("Using hash table size {} instead of configured hash table size {} - adjust your configuration to be precise", hts, builder.getHashTableSize());
        }
        else
        {
            // auto-size hash table
            if (cap / 1024 > Integer.MAX_VALUE)
                throw new UnsupportedOperationException("Cannot calculate hash table size");
            hts = (int) (cap / 4096);
        }

        int lruListLenTrigger = builder.getLruListLenTrigger();
        if (lruListLenTrigger < 1)
            lruListLenTrigger = 1;
        this.lruListLenTrigger = lruListLenTrigger;

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

        this.statisticsEnabled = builder.isStatisticsEnabled();

        this.dataMemory = new DataMemory(capacity, cleanUpTriggerMinFree);

        try
        {
            this.hashPartitions = new HashPartitions(hts);
            this.hashEntryAccess = new HashEntryAccess(dataMemory, hashPartitions, lruListLenTrigger, signals);

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
            });

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

    static int roundUpToPowerOf2(int number)
    {
        return number >= MAXIMUM_INT
               ? MAXIMUM_INT
               : (number > 1) ? Integer.highestOneBit((number - 1) << 1) : 1;
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
            }
        }
        finally
        {
            // releasing memory immediately is dangerous since other threads may still access the data
            // Need to orderly clear the cache before releasing (this involves hash-partition and hash-entry locks,
            // which ensure that no other thread is accessing OHC)
            removeAllInt();
        }
    }

    private void removeAllInt()
    {
        for (int partNo = 0; partNo < getHashTableSize(); partNo++)
        {
            long lruHead;
            long lock = hashPartitions.lockPartition(partNo, true);
            try
            {
                lruHead = hashPartitions.getPartitionHead(partNo);
                hashPartitions.setPartitionHead(partNo, 0L);
            }
            finally
            {
                hashPartitions.unlockPartition(lock, partNo, true);
            }

            for (long hashEntryAdr = lruHead; hashEntryAdr != 0L; hashEntryAdr = hashEntryAccess.getNextEntry(hashEntryAdr))
            {
                size.add(-1);

                // need to lock the entry since another thread might still read from it
                hashEntryAccess.lockEntryWrite(hashEntryAdr);
                dataMemory.free(hashEntryAdr);
            }
        }
    }

    int[] calcListLengths(boolean longestOnly)
    {
        int[] ll = new int[longestOnly ? 1 : getHashTableSize()];
        for (int partNo = 0; partNo < ll.length; partNo++)
        {
            long lock = hashPartitions.lockPartition(partNo, false);
            try
            {
                int l = 0;
                for (long hashEntryAdr = hashPartitions.getPartitionHead(partNo); hashEntryAdr != 0L; hashEntryAdr = hashEntryAccess.getNextEntry(hashEntryAdr))
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
        return hashPartitions.getHashTableSize();
    }

    public long getMemUsed()
    {
        return capacity - freeCapacity();
    }

    public PutResult put(int hash, BytesSource keySource, BytesSource valueSource)
    {
        return put(hash, keySource, valueSource, null);
    }

    public PutResult put(int hash, BytesSource keySource, BytesSource valueSource, BytesSink oldValueSink)
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
        long newHashEntryAdr = hashEntryAccess.createNewEntryChain(hash, keySource, valueSource, -1L);
        if (newHashEntryAdr == 0L)
        {
            remove(hash, keySource);
            return PutResult.NO_MORE_FREE_CAPACITY;
        }

        return putInternal(hash, keySource, oldValueSink, newHashEntryAdr);
    }

    private PutResult putInternal(int hash, BytesSource keySource, BytesSink oldValueSink, long newHashEntryAdr)
    {
        long oldHashEntryAdr;

        maybeTriggerCleanup();

        // find + lock hash partition
        long lock = hashPartitions.lockPartition(hash, true);
        try
        {
            // find existing entry
            oldHashEntryAdr = hashEntryAccess.findHashEntry(hash, keySource);

            // remove existing entry
            if (oldHashEntryAdr != 0L)
                hashEntryAccess.removeFromPartitionLinkedList(hash, oldHashEntryAdr);

            // add new entry
            hashEntryAccess.addAsPartitionHead(hash, newHashEntryAdr);

            // We have to lock the old entry before we can actually free the allocated blocks.
            // There's no need for a corresponding unlock because we use CAS on a field for locking.
            hashEntryAccess.lockEntryWrite(oldHashEntryAdr);
        }
        finally
        {
            // release hash partition
            hashPartitions.unlockPartition(lock, hash, true);
        }

        // No old entry - just return.
        if (oldHashEntryAdr == 0L)
        {
            size.add(1);
            return PutResult.ADD;
        }

        try
        {
            // Write old value (if wanted).
            if (oldValueSink != null)
                hashEntryAccess.writeValueToSink(oldHashEntryAdr, oldValueSink);
        }
        finally
        {
            // release old value
            dataMemory.free(oldHashEntryAdr);
        }

        return PutResult.REPLACE;
    }

    public boolean get(int hash, BytesSource keySource, BytesSink valueSink)
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
                entryLock = hashEntryAccess.lockEntryRead(hashEntryAdr);
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

        hashEntryAccess.touchEntry(hashEntryAdr);

        // Write the value to the caller and unlock the entry.
        try
        {
            hashEntryAccess.writeValueToSink(hashEntryAdr, valueSink);
        }
        finally
        {
            hashEntryAccess.unlockEntryRead(hashEntryAdr, entryLock);
        }

        return true;
    }

    public boolean remove(int hash, BytesSource keySource)
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

            hashEntryAccess.removeFromPartitionLinkedList(hash, hashEntryAdr);

            // We have to lock the old entry before we can actually free the allocated blocks.
            // There's no need for a corresponding unlock because we use CAS on a field for locking.
            hashEntryAccess.lockEntryWrite(hashEntryAdr);
        }
        finally
        {
            // release hash partition
            hashPartitions.unlockPartition(lock, hash, true);
        }

        // free chain
        dataMemory.free(hashEntryAdr);
        // do NOT unlock - data block might be used elsewhere

        size.add(-1);

        return true;
    }

    public void invalidate(Object o)
    {
        BytesSource.ByteArraySource ks = keySource((K) o);

        remove(ks.hashCode(), ks);
    }

    public V getIfPresent(Object o)
    {
        assertNotClosed();

        if (valueSerializer == null)
            throw new NullPointerException("no valueSerializer configured");

        BytesSource.ByteArraySource ks = keySource((K) o);
        int hash = ks.hashCode();

        // find + lock hash partition
        long lock = hashPartitions.lockPartition(hash, false);
        long hashEntryAdr;
        long entryLock = 0L;
        try
        {
            // find hash entry
            hashEntryAdr = hashEntryAccess.findHashEntry(hash, ks);
            if (hashEntryAdr != 0L)
            {
                // to keep the hash-partition lock short, lock the entry here
                entryLock = hashEntryAccess.lockEntryRead(hashEntryAdr);
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

        hashEntryAccess.touchEntry(hashEntryAdr);

        try
        {
            return valueSerializer.deserialize(hashEntryAccess.readValueFrom(hashEntryAdr));
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finally
        {
            hashEntryAccess.unlockEntryRead(hashEntryAdr, entryLock);
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
        int hash = ks.hashCode();
        long valueLen = valueSerializer.serializedSize(v);

        // Allocate and fill new hash entry.
        // Do this outside of the hash-partition lock to hold that lock no longer than necessary.
        long newHashEntryAdr = hashEntryAccess.createNewEntryChain(hash, ks, null, valueLen);
        if (newHashEntryAdr == 0L)
        {
            remove(ks.hashCode(), ks);
            return;
        }

        try
        {
            hashEntryAccess.valueToHashEntry(newHashEntryAdr, valueSerializer, v, ks.size(), valueLen);
        }
        catch (IOException e)
        {
            dataMemory.free(newHashEntryAdr);
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
            // check if we need to rehash
            if (calcListLengths(true)[0] >= lruListLenTrigger)
                return false;

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

        if (!globalLock.compareAndSet(false, true))
            return;
        try
        {
            long freeCapacity = dataMemory.freeCapacity();
            if (freeCapacity > dataMemory.cleanUpTriggerMinFree)
                return;

            long entries = size();

            long perEntryMemory = (capacity - freeCapacity) / entries;

            int entriesToRemove = (int) ((dataMemory.cleanUpTriggerMinFree - freeCapacity) / perEntryMemory);
            int entriesToRemovePerPartition = entriesToRemove / getHashTableSize();
            if (entriesToRemovePerPartition < 1)
                entriesToRemovePerPartition = 1;
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Cleanup starts with free-space {}, entries={}, memory-per-entry={}, entries-to-remove={}",
                             freeCapacity,
                             entries,
                             perEntryMemory,
                             entriesToRemove
                );

            long capacityFreed = 0;
            int entriesRemoved = 0;

            boolean rehashRequired = false;

            for (int partNo = 0; partNo < getHashTableSize(); partNo++)
            {
                long startAt = 0L;

                long lock = hashPartitions.lockPartitionForLongRun(false, partNo);
                try
                {
                    long lastHashEntryAdr = 0L;
                    long lruHead = hashPartitions.getPartitionHead(partNo);
                    int listLen = 0;
                    for (long hashEntryAdr = lruHead; ; hashEntryAdr = hashEntryAccess.getNextEntry(hashEntryAdr), listLen++)
                    {
                        // at LRU tail
                        if (hashEntryAdr == 0L)
                            break;
                        lastHashEntryAdr = hashEntryAdr;
                    }

                    if (listLen >= lruListLenTrigger)
                        rehashRequired = true;

                    // hash partition is empty
                    if (lastHashEntryAdr == 0L)
                        continue;

                    long firstBefore = 0L;
                    int i = 0;
                    for (long hashEntryAdr = hashEntryAccess.getPreviousEntry(lastHashEntryAdr); i++ < entriesToRemovePerPartition; hashEntryAdr = hashEntryAccess.getPreviousEntry(hashEntryAdr))
                    {
                        // at LRU head
                        if (hashEntryAdr == 0L)
                            break;
                        firstBefore = hashEntryAdr;
                    }

                    // remove whole partition
                    if (firstBefore == 0L)
                    {
                        startAt = lruHead;
                        hashPartitions.setPartitionHead(partNo, 0L);
                    }
                    else
                    {
                        startAt = hashEntryAccess.getNextEntry(firstBefore);
                        hashEntryAccess.setNextEntry(firstBefore, 0L);
                        hashEntryAccess.setPreviousEntry(startAt, 0L);
                    }

                    // first hash-entry-address to remove in 'startAt' and unlinked from LRU list - can unlock the partition
                }
                finally
                {
                    hashPartitions.unlockPartitionForLongRun(false, lock, partNo);
                }

                // remove entries
                long next;
                for (long hashEntryAdr = startAt; hashEntryAdr != 0L; hashEntryAdr = next)
                {
                    next = hashEntryAccess.getNextEntry(hashEntryAdr);

                    entriesRemoved++;

                    // need to lock the entry since another thread might still read from it
                    hashEntryAccess.lockEntryWrite(hashEntryAdr);
                    capacityFreed += dataMemory.free(hashEntryAdr);
                }
            }

            evictionCount.add(entriesRemoved);

            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Cleanup statistics: removed entries={} bytes recycled={}", entriesRemoved, capacityFreed);

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

    public long freeCapacity()
    {
        return dataMemory.freeCapacity();
    }

    public OHCacheStats extendedStats()
    {
        return new OHCacheStats(stats(), calcListLengths(false), size(),
                                capacity, dataMemory.freeCapacity(), rehashCount.longValue());
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
                             evictionCount.longValue()
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

    public Iterator<K> hotN(int n)
    {
        assertNotClosed();

        if (keySerializer == null)
            throw new NullPointerException("no keySerializer configured");

        final int keysPerPartition = (n / getHashTableSize()) + 1;

        return new AbstractIterator<K>()
        {
            public HashEntryAccess.HashEntryCallback cb = new HashEntryAccess.HashEntryCallback()
            {
                void hashEntry(long hashEntryAdr)
                {
                    try
                    {
                        keys.add(keySerializer.deserialize(hashEntryAccess.readKeyFrom(hashEntryAdr)));
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }
            };
            private int partNo;
            private final List<K> keys = new ArrayList<>();
            private Iterator<K> subIter;

            protected K computeNext()
            {
                while (true)
                {
                    if (partNo == getHashTableSize())
                        return endOfData();

                    if (subIter != null && subIter.hasNext())
                        return subIter.next();
                    keys.clear();

                    assertNotClosed();

                    hashEntryAccess.hotN(partNo++, cb, keysPerPartition);
                    subIter = keys.iterator();
                }
            }
        };
    }

    public ConcurrentMap<K, V> asMap()
    {
        throw new UnsupportedOperationException();
    }

    private void rehashInt()
    {
        int hashTableSize = getHashTableSize();
        int newHashTableSize = hashTableSize << 1;
        if (newHashTableSize == MAXIMUM_INT)
            return;

        LOGGER.info("OHC hash table resize from {} to {} starts...", hashTableSize, newHashTableSize);

        hashPartitions.prepareRehash(newHashTableSize);
        long[] rehashLocks = hashPartitions.lockForRehash(newHashTableSize);

        int entries = 0;

        for (int partNo = 0; partNo < hashTableSize; partNo++)
        {
            long curr0 = 0L;
            long curr1 = 0L;

            long lock = hashPartitions.lockPartitionForLongRun(false, partNo);
            try
            {
                long next;
                for (long hashEntryAdr = hashPartitions.getPartitionHead(partNo); hashEntryAdr != 0L; hashEntryAdr = next)
                {
                    next = hashEntryAccess.getNextEntry(hashEntryAdr);

                    entries++;

                    int hash = hashEntryAccess.getEntryHash(hashEntryAdr);
                    if ((hash & hashTableSize) == hashTableSize)
                    {
                        hashEntryAccess.setPreviousEntry(hashEntryAdr, curr1);
                        if (curr1 == 0L)
                            hashPartitions.setPartitionHeadAlt(partNo | hashTableSize, hashEntryAdr);
                        else
                            hashEntryAccess.setNextEntry(curr1, hashEntryAdr);
                        curr1 = hashEntryAdr;
                    }
                    else
                    {
                        hashEntryAccess.setPreviousEntry(hashEntryAdr, curr0);
                        if (curr0 == 0L)
                            hashPartitions.setPartitionHeadAlt(partNo, hashEntryAdr);
                        else
                            hashEntryAccess.setNextEntry(curr0, hashEntryAdr);
                        curr0 = hashEntryAdr;
                    }
                }
                if (curr0 != 0L)
                    hashEntryAccess.setNextEntry(curr0, 0L);
                else
                    hashPartitions.setPartitionHeadAlt(partNo, 0L);
                if (curr1 != 0L)
                    hashEntryAccess.setNextEntry(curr1, 0L);
                else
                    hashPartitions.setPartitionHeadAlt(partNo | hashTableSize, 0L);
            }
            finally
            {
                hashPartitions.rehashProgress(lock, partNo, rehashLocks);
            }
        }

        hashPartitions.finishRehash();

        rehashCount.increment();
        signals.rehashTrigger.set(false);

        LOGGER.info("OHC hash table resized from {} to {} ({} entries)", hashTableSize, newHashTableSize, entries);
    }

    void maybeTriggerCleanup()
    {
        if (dataMemory.freeCapacity() < dataMemory.cleanUpTriggerMinFree)
            signals.triggerCleanup();
    }
}
