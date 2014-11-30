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
package org.caffinitas.ohc.segment;

import java.io.IOError;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
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
import org.caffinitas.ohc.segment.replacement.ReplacementStrategy;

public final class ShardedCacheImpl<K, V> implements OHCache<K, V>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(ShardedCacheImpl.class);

    public static final int ONE_GIGABYTE = 1024 * 1024 * 1024;

    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;

    private final OffHeapMap[] maps;
    private final long shardMask;
    private final int shardShift;
    private final DataMemory dataMemory;
    private final long capacity;
    private final long cleanUpTriggerMinFree;

    private final Thread maintenance;

    private boolean statisticsEnabled;
    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder loadSuccessCount = new LongAdder();
    private final LongAdder loadExceptionCount = new LongAdder();
    private final LongAdder totalLoadTime = new LongAdder();
    private final LongAdder evictedEntries = new LongAdder();
    private final LongAdder putFailCount = new LongAdder();
    private final LongAdder putAddCount = new LongAdder();
    private final LongAdder putReplaceCount = new LongAdder();
    private final LongAdder removeCount = new LongAdder();
    private final LongAdder rehashCount = new LongAdder();
    private final LongAdder cleanUpCount = new LongAdder();

    volatile boolean closed;

    public ShardedCacheImpl(OHCacheBuilder<K, V> builder)
    {
        // inquire current replacement strategy
        String rs = builder.getReplacementStrategy();
        if (rs == null)
            rs = "LRU";
        Class<? extends ReplacementStrategy> replacementStrategyClass;
        try
        {
            Class<ReplacementStrategy> intf = ReplacementStrategy.class;
            String cls = rs.indexOf('.') != -1
                         ? rs
                         : intf.getName().substring(0, intf.getName().lastIndexOf('.') + 1) + rs + intf.getSimpleName();
            replacementStrategyClass = (Class<? extends ReplacementStrategy>) Class.forName(cls);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        // off-heap allocation
        this.capacity = builder.getCapacity();
        this.dataMemory = new DataMemory(this.capacity);

        // build shards
        int shards = builder.getShardCount();
        if (shards <= 0)
            shards = Runtime.getRuntime().availableProcessors() * 2;
        shards = Util.roundUpToPowerOf2(shards);
        maps = new OffHeapMap[shards];
        for (int i = 0; i < shards; i++)
            try
            {
                maps[i] = new OffHeapMap(builder, dataMemory, replacementStrategyClass.newInstance());
            }
            catch (InstantiationException | IllegalAccessException e)
            {
                throw new RuntimeException(e);
            }

        // bit-mask for shard part of hash
        int bitNum = Util.bitNum(shards) - 1;
        this.shardShift = 64 - bitNum;
        this.shardMask = ((long) shards - 1) << shardShift;

        // calculate trigger for cleanup/eviction/replacement
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

        this.keySerializer = builder.getKeySerializer();
        this.valueSerializer = builder.getValueSerializer();

        maintenance = new Thread(new Runnable()
        {
            public void run()
            {
                while (true)
                {
                    try
                    {
                        Thread.sleep(100L);
                    }
                    catch (InterruptedException e)
                    {
                        break;
                    }

                    if (closed)
                        return;

                    maintenance();
                }
            }
        }, "OHC cleanup");
        maintenance.start();
    }

    void maintenance()
    {
        try
        {
            if (dataMemory.freeCapacity() < cleanUpTriggerMinFree)
                cleanUp();

            for (OffHeapMap map : maps)
                if (map.rehashTriggered())
                {
                    long lock = map.lock();
                    try
                    {
                        map.rehash();
                    }
                    finally
                    {
                        map.unlock(lock);
                    }

                    rehashCount.increment();
                }
        }
        catch (Throwable t)
        {
            LOGGER.error("Failure during triggered cleanup or rehash", t);
        }
    }

    //
    // map stuff
    //

    public V getIfPresent(Object key)
    {
        BytesSource.ByteArraySource keySource = keySource((K) key);
        long hash = keySource.hash();

        long hashEntryAdr;

        OffHeapMap map = shard(hash);
        long lock = map.lock();
        try
        {
            hashEntryAdr = map.getEntry(hash, keySource);

            if (hashEntryAdr != 0L)
                HashEntries.reference(hashEntryAdr);
        }
        finally
        {
            map.unlock(lock);
        }

        if (hashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                missCount.increment();
            return null;
        }

        if (statisticsEnabled)
            hitCount.increment();

        try
        {
            return valueSerializer.deserialize(HashEntries.readValueFrom(hashEntryAdr));
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finally
        {
            dereference(hashEntryAdr);
        }
    }

    public boolean get(long hash, BytesSource keySource, BytesSink valueSink)
    {
        long hashEntryAdr;

        OffHeapMap map = shard(hash);
        long lock = map.lock();
        try
        {
            hashEntryAdr = map.getEntry(hash, keySource);

            if (hashEntryAdr != 0L)
                HashEntries.reference(hashEntryAdr);
        }
        finally
        {
            map.unlock(lock);
        }

        if (hashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                missCount.increment();
            return false;
        }

        try
        {
            HashEntries.valueToSink(hashEntryAdr, valueSink);
            if (statisticsEnabled)
                hitCount.increment();
            return true;
        }
        finally
        {
            dereference(hashEntryAdr);
        }
    }

    public void put(K key, V value)
    {
        BytesSource.ByteArraySource keySource = keySource(key);
        long hash = keySource.hash();

        long valueLen = valueSerializer.serializedSize(value);

//        if (dataMemory.freeCapacity() <= Util.roundUpTo8(keySource.size()) + valueLen + Constants.ENTRY_OFF_DATA)
//            cleanUp();

        // Allocate and fill new hash entry.
        // Do this outside of the shard lock to hold that lock no longer than necessary.
        long newHashEntryAdr = HashEntries.createNew(dataMemory, hash, keySource, null, valueLen);
        if (newHashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                putFailCount.increment();
            remove(keySource.hash(), keySource);
            return;
        }

        try
        {
            valueSerializer.serialize(value, new HashEntryOutput(newHashEntryAdr, keySource.size(), valueLen));
        }
        catch (VirtualMachineError e)
        {
            dataMemory.free(newHashEntryAdr);
            throw e;
        }
        catch (Throwable e)
        {
            dataMemory.free(newHashEntryAdr);
            throw new IOError(e);
        }

        long oldHashEntryAdr;

        OffHeapMap map = shard(hash);
        long lock = map.lock();
        try
        {
            oldHashEntryAdr = map.replaceEntry(hash, keySource, newHashEntryAdr);
        }
        finally
        {
            map.unlock(lock);
        }

        if (oldHashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                putAddCount.increment();
            return;
        }

        dereference(oldHashEntryAdr);

        if (statisticsEnabled)
            putReplaceCount.increment();
    }

    public PutResult put(long hash, BytesSource keySource, BytesSource valueSource, BytesSink oldValueSink)
    {
        // Allocate and fill new hash entry.
        // Do this outside of the shard lock to hold that lock no longer than necessary.
        long newHashEntryAdr = HashEntries.createNew(dataMemory, hash, keySource, valueSource, -1L);
        if (newHashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                putFailCount.increment();
            remove(keySource.hash(), keySource);
            return PutResult.NO_MORE_FREE_CAPACITY;
        }

        long oldHashEntryAdr;

        OffHeapMap map = shard(hash);
        long lock = map.lock();
        try
        {
            oldHashEntryAdr = map.replaceEntry(hash, keySource, newHashEntryAdr);
        }
        finally
        {
            map.unlock(lock);
        }

        if (oldHashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                putAddCount.increment();
            return PutResult.ADD;
        }

        if (oldValueSink != null)
            HashEntries.valueToSink(oldHashEntryAdr, oldValueSink);

        dereference(oldHashEntryAdr);

        if (statisticsEnabled)
            putReplaceCount.increment();

        return PutResult.REPLACE;
    }

    public boolean remove(long hash, BytesSource keySource)
    {
        long hashEntryAdr;

        OffHeapMap map = shard(hash);
        long lock = map.lock();
        try
        {
            hashEntryAdr = map.removeEntry(hash, keySource);
        }
        finally
        {
            map.unlock(lock);
        }

        if (hashEntryAdr == 0L)
            return false;

        dereference(hashEntryAdr);

        if (statisticsEnabled)
            removeCount.increment();

        return true;
    }

    private void dereference(long hashEntryAdr)
    {
        if (HashEntries.dereference(hashEntryAdr))
            dataMemory.free(hashEntryAdr);
    }

    public void invalidate(Object key)
    {
        BytesSource.ByteArraySource keySource = keySource((K) key);
        long hash = keySource.hash();

        remove(hash, keySource);
    }

    private OffHeapMap shard(long hash)
    {
        int seg = (int) ((hash & shardMask) >>> shardShift);
        return maps[seg];
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

    //
    // maintenance
    //

    public Iterator<K> hotN(int n)
    {
        return null;
    }

    public void invalidateAll()
    {

    }

    public void cleanUp()
    {
        long freeCapacity = dataMemory.freeCapacity();
        if (freeCapacity > cleanUpTriggerMinFree)
            return;

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Clean up triggered on {} bytes free ({} below trigger {}) (capacity: {})",
                         freeCapacity, cleanUpTriggerMinFree - freeCapacity, cleanUpTriggerMinFree, capacity);

        long recycleGoal = cleanUpTriggerMinFree - freeCapacity;
        long perMapRecycleGoal = recycleGoal / maps.length;
        if (perMapRecycleGoal <= 0L)
            perMapRecycleGoal = 1L;
        long t0 = System.currentTimeMillis();

        long evicted = 0L;
        for (OffHeapMap map : maps)
        {
            long stamp = map.lock();
            try
            {
                evicted += map.cleanUp(perMapRecycleGoal);
            }
            finally
            {
                map.unlock(stamp);
            }
        }

        evictedEntries.add(evicted);
        cleanUpCount.increment();

        if (LOGGER.isDebugEnabled())
        {
            long t = System.currentTimeMillis() - t0;
            LOGGER.debug("Clean up finished after {}ms - now {} bytes free (capacity: {})", t, freeCapacity, capacity);
        }
    }

    //
    // state
    //

    private void assertNotClosed()
    {
        if (closed)
            throw new IllegalStateException("OHCache instance already closed");
    }

    public void close() throws IOException
    {
        if (closed)
            return;

        closed = true;

        if (maintenance != null)
        {
            try
            {
                maintenance.join(60000);
                maintenance.interrupt();
                if (maintenance.isAlive())
                    throw new RuntimeException("Background OHC maintenance did not terminate normally. This usually indicates a bug.");
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }

        invalidateAll();

        for (OffHeapMap map : maps)
            map.release();

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Closing OHC instance");
    }

    //
    // statistics and related stuff
    //

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

    }

    public OHCacheStats extendedStats()
    {
        long[] mapSizes = new long[maps.length];
        for (int i = 0; i < maps.length; i++)
            mapSizes[i] = maps[i].size();
        return new OHCacheStats(stats(),
                                mapSizes,
                                size(),
                                capacity,
                                freeCapacity(),
                                cleanUpCount.longValue(),
                                rehashCount.longValue(),
                                putAddCount.longValue(),
                                putReplaceCount.longValue(),
                                putFailCount.longValue(),
                                removeCount.longValue());
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

    public long getCapacity()
    {
        return capacity;
    }

    public long freeCapacity()
    {
        return dataMemory.freeCapacity();
    }

    public long size()
    {
        long size = 0L;
        for (OffHeapMap map : maps)
            size += map.size();
        return size;
    }

    //
    // convenience methods
    //

    public V get(K key, Callable<? extends V> valueLoader) throws ExecutionException
    {
        V v = getIfPresent(key);
        if (v == null)
        {
            long t0 = System.currentTimeMillis();
            try
            {
                v = valueLoader.call();
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
            put(key, v);
        }
        return v;
    }

    public ImmutableMap<K, V> getAllPresent(Iterable<?> keys)
    {
        return null;
    }

    public PutResult put(long hash, BytesSource keySource, BytesSource valueSource)
    {
        return put(hash, keySource, valueSource, null);
    }

    public void putAll(Map<? extends K, ? extends V> m)
    {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    public void invalidateAll(Iterable<?> iterable)
    {
        for (Object o : iterable)
            invalidate(o);
    }

    public long getMemUsed()
    {
        return getCapacity() - freeCapacity();
    }

    //
    // methods that don't make sense in this implementation
    //

    public ConcurrentMap<K, V> asMap()
    {
        throw new UnsupportedOperationException();
    }

    public int getHashTableSize()
    {
        return 0;
    }
}
