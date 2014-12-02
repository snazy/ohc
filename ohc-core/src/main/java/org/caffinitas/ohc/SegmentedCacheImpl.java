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
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
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

    private final long maxEntrySize;

    private boolean statisticsEnabled;
    private volatile long loadSuccessCount;
    private volatile long loadExceptionCount;
    private volatile long totalLoadTime;
    private volatile long putFailCount;

    public SegmentedCacheImpl(OHCacheBuilder<K, V> builder)
    {
        long capacity = builder.getCapacity();
        if (capacity <= 0L)
            throw new IllegalArgumentException("capacity");

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

        // build segments
        int segments = builder.getSegmentCount();
        if (segments <= 0)
            segments = Runtime.getRuntime().availableProcessors() * 2;
        segments = OffHeapMap.roundUpToPowerOf2(segments);
        maps = new OffHeapMap[segments];
        for (int i = 0; i < segments; i++)
            maps[i] = new OffHeapMap(builder,
                                     capacity / segments,
                                     cleanUpTriggerFree / segments
            );

        // bit-mask for segment part of hash
        int bitNum = bitNum(segments) - 1;
        this.segmentShift = 64 - bitNum;
        this.segmentMask = ((long) segments - 1) << segmentShift;

        // calculate max entry size
        double mes = builder.getMaxEntrySize();
        long maxEntrySize;
        if (mes <= 0d || mes >= 1d)
            maxEntrySize = capacity / segments / 128;
        else
            maxEntrySize = (long) (mes * capacity / segments);
        this.maxEntrySize = maxEntrySize;

        this.statisticsEnabled = builder.isStatisticsEnabled();

        this.keySerializer = builder.getKeySerializer();
        if (keySerializer == null)
            throw new NullPointerException("keySerializer == null");
        this.valueSerializer = builder.getValueSerializer();
        if (valueSerializer == null)
            throw new NullPointerException("valueSerializer == null");
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
            return null;

        try
        {
            return valueSerializer.deserialize(new HashEntryValueInput(hashEntryAdr));
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

        long hashEntryAdr;
        if (bytes > maxEntrySize || (hashEntryAdr = Uns.allocate(bytes)) == 0L)
        {
            // entry too large to be inserted or OS is not able to provide enough memory
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
            valueSerializer.serialize(v, new HashEntryValueOutput(hashEntryAdr, key.size(), valueLen));
        }
        catch (Error e)
        {
            Uns.free(hashEntryAdr);
            throw e;
        }
        catch (Throwable e)
        {
            Uns.free(hashEntryAdr);
            throw new IOError(e);
        }

        segment(hash).putEntry(key, hashEntryAdr, bytes);
    }

    public void invalidate(Object k)
    {
        KeyBuffer key = keySource((K) k);

        removeInternal(key);
    }

    private void removeInternal(KeyBuffer key)
    {
        segment(key.hash()).removeEntry(key);
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
                                return keySerializer.deserialize(new HashEntryKeyInput(hashEntryAdr));
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
        putFailCount = 0;
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
                                putAddCount(),
                                putReplaceCount(),
                                putFailCount,
                                removeCount());
    }

    public CacheStats stats()
    {
        return new CacheStats(
                             hitCount(),
                             missCount(),
                             loadSuccessCount,
                             loadExceptionCount,
                             totalLoadTime,
                             evictedEntries()
        );
    }

    private long putAddCount()
    {
        long putAddCount = 0L;
        for (OffHeapMap map : maps)
            putAddCount += map.putAddCount();
        return putAddCount;
    }

    private long putReplaceCount()
    {
        long putReplaceCount = 0L;
        for (OffHeapMap map : maps)
            putReplaceCount += map.putReplaceCount();
        return putReplaceCount;
    }

    private long removeCount()
    {
        long removeCount = 0L;
        for (OffHeapMap map : maps)
            removeCount += map.removeCount();
        return removeCount;
    }

    private long hitCount()
    {
        long hitCount = 0L;
        for (OffHeapMap map : maps)
            hitCount += map.hitCount();
        return hitCount;
    }

    private long missCount()
    {
        long missCount = 0L;
        for (OffHeapMap map : maps)
            missCount += map.missCount();
        return missCount;
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
    // serialization (serialized data cannot be ported between different CPU architectures, if endianess differs)
    //

    public boolean deserializeEntry(SeekableByteChannel channel) throws IOException
    {
        // read hash, keyLen, valueLen
        byte[] hashKeyValueLen = new byte[3 * 8];
        ByteBuffer bb = ByteBuffer.allocate(3 * 8);
        readFully(channel, bb);

        long hash = Uns.getLongFromByteArray(hashKeyValueLen, 0);
        long keyLen = Uns.getLongFromByteArray(hashKeyValueLen, 8);
        long valueLen = Uns.getLongFromByteArray(hashKeyValueLen, 16);

        long kvLen = roundUpTo8(keyLen) + valueLen;
        long totalLen = kvLen + ENTRY_OFF_DATA;
        long hashEntryAdr;
        if (totalLen > maxEntrySize || (hashEntryAdr = Uns.allocate(totalLen)) == 0L)
        {
            channel.position(channel.position() + kvLen);
            return false;
        }

        HashEntries.init(hash, keyLen, valueLen, hashEntryAdr);

        // read key + value
        readFully(channel, HashEntries.directBufferFor(hashEntryAdr, ENTRY_OFF_DATA, kvLen));

        segment(hash).putEntry(hashEntryAdr, hash, keyLen, totalLen);

        return true;
    }

    public boolean serializeEntry(K key, WritableByteChannel channel) throws IOException
    {
        KeyBuffer keySource = keySource(key);

        long hashEntryAdr = segment(keySource.hash()).getEntry(keySource);

        return hashEntryAdr != 0L && serializeEntry(channel, hashEntryAdr);
    }

    public int serializeHotN(int n, WritableByteChannel channel) throws IOException
    {
        // hotN implementation does only return a (good) approximation - not necessarily the exact hotN
        // since it iterates over the all segments and takes a fraction of 'n' from them.
        // This implementation may also return more results than expected just to keep it simple
        // (it does not really matter if you request 5000 keys and e.g. get 5015).

        int perMap = n / maps.length + 1;
        int cnt = 0;

        for (OffHeapMap map : maps)
        {
            long[] hotPerMap = map.hotN(perMap);
            try
            {
                for (int i = 0; i < hotPerMap.length; i++)
                {
                    long hashEntryAdr = hotPerMap[i];
                    if (hashEntryAdr == 0L)
                        continue;

                    serializeEntry(channel, hashEntryAdr);
                    dereference(hashEntryAdr);
                    hotPerMap[i] = 0L;

                    cnt++;
                }
            }
            finally
            {
                for (long hashEntryAdr : hotPerMap)
                    if (hashEntryAdr != 0L)
                        dereference(hashEntryAdr);
            }
        }

        return cnt;
    }

    private boolean serializeEntry(WritableByteChannel channel, long hashEntryAdr) throws IOException
    {
        try
        {
            long keyLen = HashEntries.getKeyLen(hashEntryAdr);
            long valueLen = HashEntries.getValueLen(hashEntryAdr);

            // write hash, keyLen, valueLen + key + value
            writeFully(channel, HashEntries.directBufferFor(hashEntryAdr, ENTRY_OFF_DATA, 3 * 8L + roundUpTo8(keyLen) + valueLen));

            return true;
        }
        finally
        {
            dereference(hashEntryAdr);
        }
    }

    private void readFully(ReadableByteChannel channel, ByteBuffer buffer) throws IOException
    {
        while (buffer.remaining() > 0)
            channel.read(buffer);
    }

    private void writeFully(WritableByteChannel channel, ByteBuffer buffer) throws IOException
    {
        while (buffer.remaining() > 0)
            channel.write(buffer);
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
        ImmutableMap.Builder<K, V> b = ImmutableMap.builder();
        for (Object key : keys)
        {
            K k = (K) key;
            V v = getIfPresent(k);
            if (v != null)
                b.put(k, v);
        }
        return b.build();
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
