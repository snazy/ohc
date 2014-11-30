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

import static org.caffinitas.ohc.Constants.*;

public final class SegmentedCacheImpl<K, V> implements OHCache<K, V>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentedCacheImpl.class);

    public static final int ONE_GIGABYTE = 1024 * 1024 * 1024;

    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;

    private final OffHeapMap[] maps;
    private final long segmentMask;
    private final int segmentShift;
    private final long capacity;
    private final LongAdder freeCapacity = new LongAdder();
    private final long cleanUpTriggerMinFree;

    private final Thread maintenance;

    private boolean statisticsEnabled;
    private volatile long hitCount;
    private volatile long missCount;
    private volatile long loadSuccessCount;
    private volatile long loadExceptionCount;
    private volatile long totalLoadTime;
    private volatile long evictedEntries;
    private volatile long putFailCount;
    private volatile long putAddCount;
    private volatile long putReplaceCount;
    private volatile long removeCount;
    private volatile long cleanUpCount;

    public SegmentedCacheImpl(OHCacheBuilder<K, V> builder)
    {
        // off-heap allocation
        this.capacity = builder.getCapacity();
        this.freeCapacity.add(capacity);

        // build segments
        int segments = builder.getSegmentCount();
        if (segments <= 0)
            segments = Runtime.getRuntime().availableProcessors() * 2;
        segments = OffHeapMap.roundUpToPowerOf2(segments);
        maps = new OffHeapMap[segments];
        for (int i = 0; i < segments; i++)
            maps[i] = new OffHeapMap(builder, this);

        // bit-mask for segment part of hash
        int bitNum = bitNum(segments) - 1;
        this.segmentShift = 64 - bitNum;
        this.segmentMask = ((long) segments - 1) << segmentShift;

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

                    maintenance();
                }
            }
        }, "OHC cleanup");
        maintenance.start();
    }

    private static int bitNum(long val)
    {
        int bit = 0;
        for (; val != 0L; bit++)
            val >>>= 1;
        return bit;
    }

    void maintenance()
    {
        try
        {
            if (freeCapacity() < cleanUpTriggerMinFree)
                cleanUp();
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
        KeyBuffer keySource = keySource((K) key);

        long hashEntryAdr = segment(keySource.hash()).getEntry(keySource);

        if (hashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                missCount++;
            return null;
        }

        if (statisticsEnabled)
            hitCount++;

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

    public void put(K k, V v)
    {
        KeyBuffer key = keySource(k);
        long keyLen = key.size();
        long valueLen = valueSerializer.serializedSize(v);
        long hash = key.hash();

//        if (dataMemory.freeCapacity() <= Util.roundUpTo8(keySource.size()) + valueLen + Constants.ENTRY_OFF_DATA)
//            cleanUp();

        // Allocate and fill new hash entry.
        long newHashEntryAdr = allocate(keyLen, valueLen);
        if (newHashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                putFailCount++;

            removeInternal(key);

            return;
        }
        // initialize hash entry
        HashEntries.init(hash, keyLen, valueLen, newHashEntryAdr);
        HashEntries.toOffHeap(key, newHashEntryAdr, ENTRY_OFF_DATA);
        try
        {
            valueSerializer.serialize(v, new HashEntryOutput(newHashEntryAdr, key.size(), valueLen));
        }
        catch (VirtualMachineError e)
        {
            free(newHashEntryAdr);
            throw e;
        }
        catch (Throwable e)
        {
            free(newHashEntryAdr);
            throw new IOError(e);
        }

        long oldHashEntryAdr = segment(hash).replaceEntry(key, newHashEntryAdr);

        if (oldHashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                putAddCount++;
            return;
        }

        dereference(oldHashEntryAdr);

        if (statisticsEnabled)
            putReplaceCount++;
    }

    public void invalidate(Object k)
    {
        KeyBuffer key = keySource((K) k);

        if (!removeInternal(key)) return;

        if (statisticsEnabled)
            removeCount++;
    }

    private boolean removeInternal(KeyBuffer key)
    {
        long hashEntryAdr = segment(key.hash()).removeEntry(key);

        if (hashEntryAdr == 0L)
            return false;

        dereference(hashEntryAdr);
        return true;
    }

    private OffHeapMap segment(long hash)
    {
        int seg = (int) ((hash & segmentMask) >>> segmentShift);
        return maps[seg];
    }

    private KeyBuffer keySource(K o)
    {
        if (keySerializer == null)
            throw new NullPointerException("no keySerializer configured");
        int size = keySerializer.serializedSize(o);

        KeyBuffer key = new KeyBuffer(size);
        try
        {
            keySerializer.serialize(o, key);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return key.finish();
    }

    //
    // maintenance
    //

    public Iterator<K> hotN(int n)
    {
        // TODO implement
        return null;
    }

    public void invalidateAll()
    {
        for (OffHeapMap map : maps)
            map.clear();
    }

    public void cleanUp()
    {
        // TODO need something against concurrent cleanUp() runs

        long freeCapacity = freeCapacity();
        if (freeCapacity > cleanUpTriggerMinFree)
            return;

        long recycleGoal = cleanUpTriggerMinFree - freeCapacity;
        long perMapRecycleGoal = recycleGoal / maps.length;
        if (perMapRecycleGoal <= 0L)
            perMapRecycleGoal = 1L;

        long evicted = 0L;
        for (OffHeapMap map : maps)
            evicted += map.cleanUp(perMapRecycleGoal);

        evictedEntries += evicted;
        cleanUpCount++;
    }

    //
    // state
    //

    public void close() throws IOException
    {
        if (maintenance != null)
        {
            try
            {
                maintenance.interrupt();
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
        for (OffHeapMap map : maps)
            map.resetStatistics();
        cleanUpCount = 0;
        putAddCount = 0;
        putReplaceCount = 0;
        putFailCount = 0;
        removeCount = 0;
        hitCount = 0;
        missCount = 0;
        loadSuccessCount = 0;
        loadExceptionCount = 0;
        totalLoadTime = 0;
        evictedEntries = 0;
    }

    public OHCacheStats extendedStats()
    {
        long[] mapSizes = new long[maps.length];
        long rehashes = 0L;
        for (int i = 0; i < maps.length; i++)
        {
            OffHeapMap map = maps[i];
            rehashes += map.rehashes();
            mapSizes[i] = map.size();
        }
        return new OHCacheStats(stats(),
                                mapSizes,
                                size(),
                                capacity,
                                freeCapacity(),
                                cleanUpCount,
                                rehashes,
                                putAddCount,
                                putReplaceCount,
                                putFailCount,
                                removeCount);
    }

    public CacheStats stats()
    {
        return new CacheStats(
                             hitCount,
                             missCount,
                             loadSuccessCount,
                             loadExceptionCount,
                             totalLoadTime,
                             evictedEntries
        );
    }

    public long getCapacity()
    {
        return capacity;
    }

    public long freeCapacity()
    {
        return freeCapacity.longValue();
    }

    public long size()
    {
        long size = 0L;
        for (OffHeapMap map : maps)
            size += map.size();
        return size;
    }

    public int getHashTableSize()
    {
        return maps.length;
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
                loadSuccessCount++;
            }
            catch (Exception e)
            {
                loadExceptionCount++;
                throw new ExecutionException(e);
            }
            finally
            {
                totalLoadTime += System.currentTimeMillis() - t0;
            }
            put(key, v);
        }
        return v;
    }

    public ImmutableMap<K, V> getAllPresent(Iterable<?> keys)
    {
        // TODO implement
        return null;
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

    //
    // alloc/free
    //

    private long allocate(long keyLen, long valueLen)
    {
        if (keyLen < 0 || valueLen < 0)
            throw new IllegalArgumentException();

        // allocate memory for whole hash-entry block-chain
        long bytes = ENTRY_OFF_DATA + roundUpTo8(keyLen) + valueLen;

        freeCapacity.add(-bytes);
        if (freeCapacity.longValue() < 0L)
        {
            freeCapacity.add(bytes);
            return 0L;
        }

        long adr = Uns.allocate(bytes);
        if (adr != 0L)
        {
            Uns.putLongVolatile(adr, ENTRY_OFF_ALLOC_LEN, bytes);
            return adr;
        }

        freeCapacity.add(bytes);
        return 0L;
    }

    private void dereference(long hashEntryAdr)
    {
        if (HashEntries.dereference(hashEntryAdr))
            free(hashEntryAdr);
    }

    long free(long address)
    {
        if (address == 0L)
            throw new NullPointerException();

        long bytes = HashEntries.getAllocLen(address);
        if (bytes == 0L)
            throw new IllegalStateException();
        Uns.free(address);
        freeCapacity.add(bytes);
        return bytes;
    }
}
