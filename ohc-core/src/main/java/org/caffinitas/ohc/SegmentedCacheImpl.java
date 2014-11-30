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

import com.google.common.cache.CacheStats;
import com.google.common.collect.AbstractIterator;
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

    private boolean statisticsEnabled;
    private volatile long hitCount;
    private volatile long missCount;
    private volatile long loadSuccessCount;
    private volatile long loadExceptionCount;
    private volatile long totalLoadTime;
    private volatile long putFailCount;
    private volatile long putAddCount;
    private volatile long putReplaceCount;
    private volatile long removeCount;

    public SegmentedCacheImpl(OHCacheBuilder<K, V> builder)
    {
        long capacity = builder.getCapacity();

        // calculate trigger for cleanup/eviction/replacement
        double cuTrigger = builder.getCleanUpTriggerFree();
        long cleanUpTriggerFree;
        if (cuTrigger < 0d)
        {
            // auto-sizing

            // 12.5% if capacity less than 8GB
            // 10% if capacity less than 16 GB
            // 5% if capacity is higher than 16GB
            if (capacity < 8L * ONE_GIGABYTE)
                cleanUpTriggerFree = (long) (.125d * capacity);
            else if (capacity < 16L * ONE_GIGABYTE)
                cleanUpTriggerFree = (long) (.10d * capacity);
            else
                cleanUpTriggerFree = (long) (.05d * capacity);
        }
        else
        {
            if (cuTrigger >= 1d)
                throw new IllegalArgumentException("Invalid clean-up percentage trigger value " + String.format("%.2f", cuTrigger));
            cuTrigger *= capacity;
            cleanUpTriggerFree = (long) cuTrigger;
        }

        // calculate target for cleanup/eviction/replacement
        double cuTarget = builder.getCleanUpTargetFree();
        if (cuTarget < cuTrigger)
            cuTarget = cuTrigger;
        long cleanUpTargetFree = (long) (cuTarget * capacity);
        if (cuTarget < 0)
            cleanUpTargetFree = 2 * cleanUpTriggerFree;
        if (cuTarget > .9d || cleanUpTargetFree > capacity)
            cleanUpTargetFree = (long) (.9d * capacity);

        // build segments
        int segments = builder.getSegmentCount();
        if (segments <= 0)
            segments = Runtime.getRuntime().availableProcessors() * 2;
        segments = OffHeapMap.roundUpToPowerOf2(segments);
        maps = new OffHeapMap[segments];
        for (int i = 0; i < segments; i++)
            maps[i] = new OffHeapMap(builder,
                                     capacity / segments,
                                     cleanUpTriggerFree / segments,
                                     cleanUpTargetFree / segments);

        // bit-mask for segment part of hash
        int bitNum = bitNum(segments) - 1;
        this.segmentShift = 64 - bitNum;
        this.segmentMask = ((long) segments - 1) << segmentShift;

        this.statisticsEnabled = builder.isStatisticsEnabled();

        this.keySerializer = builder.getKeySerializer();
        this.valueSerializer = builder.getValueSerializer();
    }

    private static int bitNum(long val)
    {
        int bit = 0;
        for (; val != 0L; bit++)
            val >>>= 1;
        return bit;
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

        long bytes = allocLen(keyLen, valueLen);

        // Allocate and fill new hash entry.
        long hashEntryAdr = Uns.allocate(bytes);
        if (hashEntryAdr == 0L)
        {
            if (statisticsEnabled)
                putFailCount++;

            removeInternal(key);

            return;
        }
        // initialize hash entry
        HashEntries.init(hash, keyLen, valueLen, hashEntryAdr);
        HashEntries.toOffHeap(key, hashEntryAdr, ENTRY_OFF_DATA);
        try
        {
            valueSerializer.serialize(v, new HashEntryOutput(hashEntryAdr, key.size(), valueLen));
        }
        catch (VirtualMachineError e)
        {
            Uns.free(hashEntryAdr);
            throw e;
        }
        catch (Throwable e)
        {
            Uns.free(hashEntryAdr);
            throw new IOError(e);
        }

        if (segment(hash).replaceEntry(key, hashEntryAdr, bytes))
        {
            if (statisticsEnabled)
                putAddCount++;
            return;
        }

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
        return segment(key.hash()).removeEntry(key);
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
        // hotN implementation does only return a (good) approximation - not necessarily the exact hotN
        // since it iterates over the all segments and takes a fraction of 'n' from them.
        // This implementation may also return more results than expected just to keep it simple
        // (it does not really matter if you request 5000 keys and e.g. get 5015).

        final int perMap = n / maps.length + 1;

        return new AbstractIterator<K>()
        {
            int mapIndex;

            long[] hotPerMap;
            int subIndex;

            protected K computeNext()
            {
                while (true)
                {
                    if (hotPerMap != null && subIndex < hotPerMap.length)
                    {
                        long hashEntryAdr = hotPerMap[subIndex++];
                        if (hashEntryAdr != 0L)
                            try
                            {
                                return keySerializer.deserialize(HashEntries.readKeyFrom(hashEntryAdr));
                            }
                            catch (IOException e)
                            {
                                LOGGER.error("Key serializer failed to deserialize", e);
                                continue;
                            }
                            finally
                            {
                                dereference(hashEntryAdr);
                            }
                    }

                    if (mapIndex == maps.length)
                        return endOfData();

                    hotPerMap = maps[mapIndex++].hotN(perMap);
                    subIndex = 0;
                }
            }
        };
    }

    public void invalidateAll()
    {
        for (OffHeapMap map : maps)
            map.clear();
    }

    public void cleanUp()
    {
        for (OffHeapMap map : maps)
            map.cleanUp();
    }

    //
    // state
    //

    public void close() throws IOException
    {
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
        putAddCount = 0;
        putReplaceCount = 0;
        putFailCount = 0;
        removeCount = 0;
        hitCount = 0;
        missCount = 0;
        loadSuccessCount = 0;
        loadExceptionCount = 0;
        totalLoadTime = 0;
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
                                getCapacity(),
                                freeCapacity(),
                                cleanUpCount(),
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
                             evictedEntries()
        );
    }

    public long getCapacity()
    {
        long capacity = 0L;
        for (OffHeapMap map : maps)
            capacity += map.capacity();
        return capacity;
    }

    public long freeCapacity()
    {
        long capacity = 0L;
        for (OffHeapMap map : maps)
            capacity += map.freeCapacity();
        return capacity;
    }

    public long cleanUpCount()
    {
        long cleanUpCount = 0L;
        for (OffHeapMap map : maps)
            cleanUpCount += map.cleanUpCount();
        return cleanUpCount;
    }

    public long evictedEntries()
    {
        long evictedEntries = 0L;
        for (OffHeapMap map : maps)
            evictedEntries += map.evictedEntries();
        return evictedEntries;
    }

    public long size()
    {
        long size = 0L;
        for (OffHeapMap map : maps)
            size += map.size();
        return size;
    }

    public int getSegments()
    {
        return maps.length;
    }

    public double getLoadFactor()
    {
        return maps[0].loadFactor();
    }

    public int[] getHashTableSizes()
    {
        int[] r = new int[maps.length];
        for (int i = 0; i < maps.length; i++)
            r[i] = maps[i].hashTableSize();
        return r;
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

    private void dereference(long hashEntryAdr)
    {
        if (HashEntries.dereference(hashEntryAdr))
            free(hashEntryAdr);
    }

    long free(long hashEntryAdr)
    {
        if (hashEntryAdr == 0L)
            throw new NullPointerException();

        long bytes = HashEntries.getAllocLen(hashEntryAdr);
        if (bytes == 0L)
            throw new IllegalStateException();

        long hash = HashEntries.getHash(hashEntryAdr);

        Uns.free(hashEntryAdr);
        segment(hash).freed(bytes);
        return bytes;
    }
}
