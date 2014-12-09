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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.AbstractIterator;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.UniformReservoir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.caffinitas.ohc.Util.*;

public final class SegmentedCacheImpl<K, V> implements OHCache<K, V>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentedCacheImpl.class);

    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;

    private final OffHeapMap[] maps;
    private final long segmentMask;
    private final int segmentShift;

    private final long maxEntrySize;

    private final long capacity;
    private final AtomicLong freeCapacity;

    private volatile long putFailCount;

    public SegmentedCacheImpl(OHCacheBuilder<K, V> builder)
    {
        long capacity = builder.getCapacity();
        if (capacity <= 0L)
            throw new IllegalArgumentException("capacity");

        this.capacity = capacity;
        freeCapacity = new AtomicLong(capacity);

        // build segments
        int segments = builder.getSegmentCount();
        if (segments <= 0)
            segments = Runtime.getRuntime().availableProcessors() * 2;
        segments = OffHeapMap.roundUpToPowerOf2(segments);
        maps = new OffHeapMap[segments];
        for (int i = 0; i < segments; i++)
        {
            try
            {
                maps[i] = new OffHeapMap(builder, freeCapacity);
            }
            catch (RuntimeException e)
            {
                while (i-- >= 0)
                    maps[i].release();
                throw e;
            }
        }

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

        this.keySerializer = builder.getKeySerializer();
        if (keySerializer == null)
            throw new NullPointerException("keySerializer == null");
        this.valueSerializer = builder.getValueSerializer();
        if (valueSerializer == null)
            throw new NullPointerException("valueSerializer == null");

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("OHC instance with {} segments and capacity of {} created.", segments, capacity);
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

    public V get(K key)
    {
        if (key == null)
            throw new NullPointerException();

        KeyBuffer keySource = keySource(key);

        OffHeapMap segment = segment(keySource.hash());
        long hashEntryAdr = segment.getEntry(keySource);

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
            segment.dereference(hashEntryAdr);
        }
    }

    public boolean containsKey(K key)
    {
        if (key == null)
            throw new NullPointerException();

        KeyBuffer keySource = keySource(key);

        return segment(keySource.hash()).containsEntry(keySource);
    }

    public void put(K k, V v)
    {
        put(k, v, false, null);
    }

    public boolean addOrReplace(K key, V old, V value)
    {
        return put(key, value, false, old);
    }

    public boolean putIfAbsent(K k, V v)
    {
        return put(k, v, true, null);
    }

    private boolean put(K k, V v, boolean ifAbsent, V old)
    {
        if (k == null || v == null)
            throw new NullPointerException();

        long keyLen = keySerializer.serializedSize(k);
        long valueLen = valueSerializer.serializedSize(v);

        long bytes = allocLen(keyLen, valueLen);

        long oldValueAdr = 0L;
        long oldValueLen = 0L;
        if (old != null)
        {
            oldValueLen = valueSerializer.serializedSize(old);
            oldValueAdr = Uns.allocate(oldValueLen);
            if (oldValueAdr == 0L)
                throw new RuntimeException("Unable to allocate " + oldValueLen + " bytes in off-heap");
            try
            {
                valueSerializer.serialize(old, new HashEntryValueOutput(oldValueAdr, oldValueLen));
            }
            catch (Error e)
            {
                Uns.free(oldValueAdr);
                throw e;
            }
            catch (Throwable e)
            {
                Uns.free(oldValueAdr);
                throw new IOError(e);
            }
        }

        try
        {

            long hashEntryAdr;
            if (bytes > maxEntrySize || (hashEntryAdr = Uns.allocate(bytes)) == 0L)
            {
                // entry too large to be inserted or OS is not able to provide enough memory
                putFailCount++;

                remove(k);

                return false;
            }

            HashEntryKeyOutput key = new HashEntryKeyOutput(hashEntryAdr, keyLen);
            try
            {
                keySerializer.serialize(k, key);
                valueSerializer.serialize(v, new HashEntryValueOutput(hashEntryAdr, keyLen, valueLen));
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
            key.finish();

            // initialize hash entry
            HashEntries.init(key.hash(), keyLen, valueLen, hashEntryAdr);

            long hash = key.hash();

            if (segment(hash).putEntry(hashEntryAdr, key.hash(), keyLen, bytes, ifAbsent, oldValueAdr, oldValueLen))
                return true;

            Uns.free(hashEntryAdr);
            return false;
        }
        finally
        {
            Uns.free(oldValueAdr);
        }
    }

    public void remove(K k)
    {
        if (k == null)
            throw new NullPointerException();

        KeyBuffer key = keySource(k);

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

    public Iterator<K> hotKeyIterator(int n)
    {
        // hotN implementation does only return a (good) approximation - not necessarily the exact hotN
        // since it iterates over the all segments and takes a fraction of 'n' from them.
        // This implementation may also return more results than expected just to keep it simple
        // (it does not really matter if you request 5000 keys and e.g. get 5015).

        final int perMap = n / maps.length + 1;

        return new AbstractIterator<K>()
        {
            int mapIndex;

            OffHeapMap segment;
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
                                segment.dereference(hashEntryAdr);
                            }
                    }

                    if (mapIndex == maps.length)
                        return endOfData();

                    segment = maps[mapIndex++];
                    hotPerMap = segment.hotN(perMap);
                    subIndex = 0;
                }
            }
        };
    }

    public void clear()
    {
        for (OffHeapMap map : maps)
            map.clear();
    }

    //
    // state
    //


    public void setCapacity(long capacity)
    {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException
    {
        clear();

        for (OffHeapMap map : maps)
            map.release();

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Closing OHC instance");
    }

    //
    // statistics and related stuff
    //

    public void resetStatistics()
    {
        for (OffHeapMap map : maps)
            map.resetStatistics();
        putFailCount = 0;
    }

    public OHCacheStats stats()
    {
        long rehashes = 0L;
        for (OffHeapMap map : maps)
            rehashes += map.rehashes();
        return new OHCacheStats(
                               hitCount(),
                               missCount(),
                               evictedEntries(),
                               perSegmentSizes(),
                               size(),
                               capacity(),
                               freeCapacity(),
                               rehashes,
                               putAddCount(),
                               putReplaceCount(),
                               putFailCount,
                               removeCount(),
                               Uns.getTotalAllocated());
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

    public long capacity()
    {
        return capacity;
    }

    public long freeCapacity()
    {
        return freeCapacity.get();
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

    public long weightedSize()
    {
        return size();
    }

    public int segments()
    {
        return maps.length;
    }

    public double loadFactor()
    {
        return maps[0].loadFactor();
    }

    public int[] hashTableSizes()
    {
        int[] r = new int[maps.length];
        for (int i = 0; i < maps.length; i++)
            r[i] = maps[i].hashTableSize();
        return r;
    }

    public long[] perSegmentSizes()
    {
        long[] r = new long[maps.length];
        for (int i = 0; i < maps.length; i++)
            r[i] = maps[i].size();
        return r;
    }

    public Histogram getBucketHistogram()
    {
        Histogram h = new Histogram(new UniformReservoir());
        for (OffHeapMap map : maps)
            map.updateBucketHistogram(h);
        return h;
    }

    //
    // serialization (serialized data cannot be ported between different CPU architectures, if endianess differs)
    //

    public boolean deserializeEntry(ReadableByteChannel channel) throws IOException
    {
        // read hash, keyLen, valueLen
        byte[] hashKeyValueLen = new byte[3 * 8];
        ByteBuffer bb = ByteBuffer.wrap(hashKeyValueLen);
        if (!readFully(channel, bb))
            return false;

        long hash = Uns.getLongFromByteArray(hashKeyValueLen, 0);
        long keyLen = Uns.getLongFromByteArray(hashKeyValueLen, 8);
        long valueLen = Uns.getLongFromByteArray(hashKeyValueLen, 16);

        long kvLen = roundUpTo8(keyLen) + valueLen;
        long totalLen = kvLen + ENTRY_OFF_DATA;
        long hashEntryAdr;
        if (totalLen > maxEntrySize || (hashEntryAdr = Uns.allocate(totalLen)) == 0L)
        {
            if (channel instanceof SeekableByteChannel)
            {
                SeekableByteChannel sc = (SeekableByteChannel) channel;
                sc.position(sc.position() + kvLen);
            }
            else
            {
                ByteBuffer tmp = ByteBuffer.allocate(8192);
                while (kvLen > 0L)
                {
                    tmp.clear();
                    if (kvLen < tmp.capacity())
                        tmp.limit((int) kvLen);
                    if (!readFully(channel, tmp))
                        return false;
                    kvLen -= tmp.limit();
                }
            }
            return false;
        }

        HashEntries.init(hash, keyLen, valueLen, hashEntryAdr);

        // read key + value
        if (!readFully(channel, Uns.directBufferFor(hashEntryAdr, ENTRY_OFF_DATA, kvLen)))
            return false;

        if (!segment(hash).putEntry(hashEntryAdr, hash, keyLen, totalLen, false, 0L, 0L))
            Uns.free(hashEntryAdr);

        return true;
    }

    public boolean serializeEntry(K key, WritableByteChannel channel) throws IOException
    {
        KeyBuffer keySource = keySource(key);

        OffHeapMap segment = segment(keySource.hash());
        long hashEntryAdr = segment.getEntry(keySource);

        return hashEntryAdr != 0L && serializeEntry(segment, channel, hashEntryAdr);
    }

    public int deserializeEntries(ReadableByteChannel channel) throws IOException
    {
        long headerAddress = Uns.allocateIOException(8);
        try
        {
            ByteBuffer header = Uns.directBufferFor(headerAddress, 0L, 8L);
            readFully(channel, header);
            header.flip();
            int magic = header.getInt();
            if (magic == HEADER_UNCOMPRESSED_WRONG)
                throw new IOException("File from instance with different CPU architecture cannot be loaded");
            if (magic != HEADER_UNCOMPRESSED)
                throw new IOException("Illegal file header");
            if (header.getInt() != 1)
                throw new IOException("Illegal file version");
        }
        finally
        {
            Uns.free(headerAddress);
        }

        int count = 0;
        while (deserializeEntry(channel))
            count++;
        return count;
    }

    public int serializeHotN(int n, WritableByteChannel channel) throws IOException
    {
        // hotN implementation does only return a (good) approximation - not necessarily the exact hotN
        // since it iterates over the all segments and takes a fraction of 'n' from them.
        // This implementation may also return more results than expected just to keep it simple
        // (it does not really matter if you request 5000 keys and e.g. get 5015).

        long headerAddress = Uns.allocateIOException(8);
        try
        {
            ByteBuffer headerBuffer = Uns.directBufferFor(headerAddress, 0L, 8L);
            headerBuffer.putInt(HEADER_UNCOMPRESSED);
            headerBuffer.putInt(1);
            headerBuffer.flip();
            writeFully(channel, headerBuffer);
        }
        finally
        {
            Uns.free(headerAddress);
        }

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

                    try
                    {
                        serializeEntry(map, channel, hashEntryAdr);
                    }
                    finally
                    {
                        hotPerMap[i] = 0L;
                    }

                    cnt++;
                }
            }
            finally
            {
                for (long hashEntryAdr : hotPerMap)
                    if (hashEntryAdr != 0L)
                        map.dereference(hashEntryAdr);
            }
        }

        return cnt;
    }

    private boolean serializeEntry(OffHeapMap segment, WritableByteChannel channel, long hashEntryAdr) throws IOException
    {
        try
        {
            long keyLen = HashEntries.getKeyLen(hashEntryAdr);
            long valueLen = HashEntries.getValueLen(hashEntryAdr);

            // write hash, keyLen, valueLen + key + value
            Util.writeFully(channel, Uns.directBufferFor(hashEntryAdr, ENTRY_OFF_HASH, 3 * 8L + roundUpTo8(keyLen) + valueLen));

            return true;
        }
        finally
        {
            segment.dereference(hashEntryAdr);
        }
    }

    //
    // convenience methods
    //

    public void putAll(Map<? extends K, ? extends V> m)
    {
        // TODO could be improved by grouping removes by segment - but increases heap pressure and complexity - decide later
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    public void removeAll(Iterable<K> iterable)
    {
        // TODO could be improved by grouping removes by segment - but increases heap pressure and complexity - decide later
        for (K o : iterable)
            remove(o);
    }

    public long memUsed()
    {
        return capacity() - freeCapacity();
    }

    //
    // key iterator
    //

    public Iterator<K> keyIterator()
    {
        return new Iterator<K>()
        {
            private int segmentIndex;
            private OffHeapMap segment;

            private int mapSegmentCount;
            private int mapSegmentIndex;

            private final List<Long> hashEntryAdrs = new ArrayList<>(1024);
            private int listIndex;

            private boolean eod;
            private K next;

            private long lastHashEntryAdr;

            public boolean hasNext()
            {
                if (eod)
                    return false;

                if (next == null)
                    next = computeNext();

                return next != null;
            }

            public K next()
            {
                if (eod)
                    throw new NoSuchElementException();

                if (next == null)
                    next = computeNext();
                K r = next;
                next = null;
                if (!eod)
                    return r;
                throw new NoSuchElementException();
            }

            public void remove()
            {
                if (eod)
                    throw new NoSuchElementException();

                segment.removeEntry(lastHashEntryAdr);
                segment.dereference(lastHashEntryAdr);
                lastHashEntryAdr = 0L;
            }

            private K computeNext()
            {
                if (lastHashEntryAdr != 0L)
                {
                    segment.dereference(lastHashEntryAdr);
                    lastHashEntryAdr = 0L;
                }

                while (true)
                {
                    if (mapSegmentIndex >= mapSegmentCount)
                    {
                        if (segmentIndex == maps.length)
                        {
                            eod = true;
                            return null;
                        }
                        segment = maps[segmentIndex++];
                        mapSegmentCount = segment.hashTableSize();
                        mapSegmentIndex = 0;
                    }

                    if (listIndex == hashEntryAdrs.size())
                    {
                        hashEntryAdrs.clear();
                        segment.getEntryAddresses(mapSegmentIndex, 1024, hashEntryAdrs);
                        mapSegmentIndex += 1024;
                        listIndex = 0;
                    }

                    if (listIndex < hashEntryAdrs.size())
                    {
                        long hashEntryAdr = hashEntryAdrs.get(listIndex++);
                        try
                        {
                            lastHashEntryAdr = hashEntryAdr;
                            return keySerializer.deserialize(new HashEntryKeyInput(hashEntryAdr));
                        }
                        catch (IOException e)
                        {
                            throw new IOError(e);
                        }
                    }
                }
            }
        };
    }
}
