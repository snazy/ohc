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
package org.caffinitas.ohc.chunked;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.caffinitas.ohc.CacheLoader;
import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.CloseableIterator;
import org.caffinitas.ohc.DirectValueAccess;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.OHCacheStats;
import org.caffinitas.ohc.histo.EstimatedHistogram;

/**
 * This is a {@link OHCache} implementation used to validate functionality of
 * {@link OHCacheChunkedImpl} - this implementation is <b>not</b> for production use!
 */
final class CheckOHCacheImpl<K, V> implements OHCache<K, V>
{
    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;
    private long capacity;

    private final CheckSegment[] maps;
    private final long maxEntrySize;
    private final int segmentShift;
    private final long segmentMask;
    private final float loadFactor;
    private final AtomicLong freeCapacity;
    private long putFailCount;
    private final Hasher hasher;

    CheckOHCacheImpl(OHCacheBuilder<K, V> builder)
    {
        capacity = builder.getCapacity();
        loadFactor = builder.getLoadFactor();
        freeCapacity = new AtomicLong(capacity);
        hasher = Hasher.create(builder.getHashAlgorighm());

        int segments = builder.getSegmentCount();
        int bitNum = Util.bitNum(segments) - 1;
        this.segmentShift = 64 - bitNum;
        this.segmentMask = ((long) segments - 1) << segmentShift;

        maps = new CheckSegment[segments];
        for (int i = 0; i < maps.length; i++)
            maps[i] = new CheckSegment(builder.getHashTableSize(), builder.getLoadFactor(), freeCapacity);

        keySerializer = builder.getKeySerializer();
        valueSerializer = builder.getValueSerializer();

        maxEntrySize = builder.getMaxEntrySize();
    }

    public boolean put(K key, V value)
    {
        KeyBuffer keyBuffer = keySource(key);
        byte[] data = value(value);

        if (maxEntrySize > 0L && CheckSegment.sizeOf(keyBuffer, data) > maxEntrySize)
        {
            remove(key);
            putFailCount++;
            return false;
        }

        CheckSegment segment = segment(keyBuffer.hash());
        return segment.put(keyBuffer, data, false, null);
    }

    public boolean addOrReplace(K key, V old, V value)
    {
        KeyBuffer keyBuffer = keySource(key);
        byte[] data = value(value);
        byte[] oldData = value(old);

        if (maxEntrySize > 0L && CheckSegment.sizeOf(keyBuffer, data) > maxEntrySize)
        {
            remove(key);
            putFailCount++;
            return false;
        }

        CheckSegment segment = segment(keyBuffer.hash());
        return segment.put(keyBuffer, data, false, oldData);
    }

    public boolean putIfAbsent(K k, V v)
    {
        KeyBuffer keyBuffer = keySource(k);
        byte[] data = value(v);

        if (maxEntrySize > 0L && CheckSegment.sizeOf(keyBuffer, data) > maxEntrySize)
        {
            remove(k);
            putFailCount++;
            return false;
        }

        CheckSegment segment = segment(keyBuffer.hash());
        return segment.put(keyBuffer, data, true, null);
    }

    public boolean putIfAbsent(K key, V value, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    public boolean addOrReplace(K key, V old, V value, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    public boolean put(K key, V value, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    public void putAll(Map<? extends K, ? extends V> m)
    {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    public boolean remove(K key)
    {
        KeyBuffer keyBuffer = keySource(key);
        CheckSegment segment = segment(keyBuffer.hash());
        return segment.remove(keyBuffer);
    }

    public void removeAll(Iterable<K> keys)
    {
        for (K key : keys)
            remove(key);
    }

    public void clear()
    {
        for (CheckSegment map : maps)
            map.clear();
    }

    public DirectValueAccess getDirect(K key)
    {
        throw new UnsupportedOperationException();
    }

    public DirectValueAccess getDirect(K key, boolean updateLRU)
    {
        throw new UnsupportedOperationException();
    }

    public V get(K key)
    {
        KeyBuffer keyBuffer = keySource(key);
        CheckSegment segment = segment(keyBuffer.hash());
        byte[] value = segment.get(keyBuffer);

        if (value == null)
            return null;

        return valueSerializer.deserialize(ByteBuffer.wrap(value));
    }

    public boolean containsKey(K key)
    {
        KeyBuffer keyBuffer = keySource(key);
        CheckSegment segment = segment(keyBuffer.hash());
        return segment.get(keyBuffer) != null;
    }

    public V getWithLoader(K key, CacheLoader<K, V> loader) throws InterruptedException, ExecutionException
    {
        throw new UnsupportedOperationException();
    }

    public V getWithLoader(K key, CacheLoader<K, V> loader, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        throw new UnsupportedOperationException();
    }

    public Future<V> getWithLoaderAsync(K key, CacheLoader<K, V> loader)
    {
        throw new UnsupportedOperationException();
    }

    public Future<V> getWithLoaderAsync(K key, CacheLoader<K, V> loader, long expireAt)
    {
        throw new UnsupportedOperationException();
    }

    public CloseableIterator<K> hotKeyIterator(int n)
    {
        return new AbstractHotKeyIter<K>(n)
        {
            protected K construct(KeyBuffer next)
            {
                return keySerializer.deserialize(next.buffer());
            }
        };
    }

    public CloseableIterator<ByteBuffer> hotKeyBufferIterator(int n)
    {
        return new AbstractHotKeyIter<ByteBuffer>(n)
        {
            protected ByteBuffer construct(KeyBuffer next)
            {
                return next.buffer();
            }
        };
    }

    public CloseableIterator<K> keyIterator()
    {
        return new AbstractKeyIter<K>()
        {
            protected K construct(KeyBuffer next)
            {
                return keySerializer.deserialize(next.buffer());
            }
        };
    }

    public CloseableIterator<ByteBuffer> keyBufferIterator()
    {
        return new AbstractKeyIter<ByteBuffer>()
        {
            protected ByteBuffer construct(KeyBuffer next)
            {
                return next.buffer();
            }
        };
    }

    abstract class AbstractHotKeyIter<E> extends AbstractKeyIter<E>
    {
        private final int perMap;

        protected AbstractHotKeyIter(int n)
        {
            // hotN implementation does only return a (good) approximation - not necessarily the exact hotN
            // since it iterates over the all segments and takes a fraction of 'n' from them.
            // This implementation may also return more results than expected just to keep it simple
            // (it does not really matter if you request 5000 keys and e.g. get 5015).

            this.perMap = n / maps.length + 1;
        }

        Iterator<KeyBuffer> keyBufferIterator()
        {
            return segment.hotN(perMap);
        }
    }

    abstract class AbstractKeyIter<E> implements CloseableIterator<E>
    {
        private int map;
        private Iterator<KeyBuffer> current = Collections.emptyIterator();
        private boolean eol;

        private KeyBuffer next;
        CheckSegment segment;
        private KeyBuffer last;
        private CheckSegment lastSegment;

        public void close()
        {
        }

        public boolean hasNext()
        {
            if (eol)
                return false;

            if (next == null)
                checkNext();

            return next != null;
        }

        boolean checkNext()
        {
            while (true)
            {
                if (!current.hasNext())
                {
                    if (map == maps.length)
                    {
                        eol = true;
                        return false;
                    }
                    segment = maps[map++];
                    current = keyBufferIterator();
                }

                if (current.hasNext())
                {
                    next = current.next();
                    last = next;
                    lastSegment = segment;
                    return true;
                }
            }
        }

        Iterator<KeyBuffer> keyBufferIterator()
        {
            return segment.keyIterator();
        }

        public E next()
        {
            if (eol)
                throw new NoSuchElementException();

            if (next == null)
                if (!checkNext())
                    throw new NoSuchElementException();

            E r = construct(next);
            next = null;
            return r;
        }

        abstract E construct(KeyBuffer next);

        public void remove()
        {
            if (last == null)
                throw new NoSuchElementException();
            lastSegment.remove(last);
        }
    }

    public boolean deserializeEntry(ReadableByteChannel channel)
    {
        throw new UnsupportedOperationException();
    }

    public boolean serializeEntry(K key, WritableByteChannel channel)
    {
        throw new UnsupportedOperationException();
    }

    public int deserializeEntries(ReadableByteChannel channel)
    {
        throw new UnsupportedOperationException();
    }

    public int serializeHotNEntries(int n, WritableByteChannel channel)
    {
        throw new UnsupportedOperationException();
    }

    public int serializeHotNKeys(int n, WritableByteChannel channel)
    {
        throw new UnsupportedOperationException();
    }

    public CloseableIterator<K> deserializeKeys(ReadableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public void resetStatistics()
    {
        for (CheckSegment map : maps)
            map.resetStatistics();
        putFailCount = 0;
    }

    public long size()
    {
        long r = 0;
        for (CheckSegment map : maps)
            r += map.size();
        return r;
    }

    public int[] hashTableSizes()
    {
        // no hash table size info
        return new int[maps.length];
    }

    public long[] perSegmentSizes()
    {
        long[] r = new long[maps.length];
        for (int i = 0; i < maps.length; i++)
            r[i] = maps[i].size();
        return r;
    }

    public EstimatedHistogram getBucketHistogram()
    {
        throw new UnsupportedOperationException();
    }

    public int segments()
    {
        return maps.length;
    }

    public long capacity()
    {
        return capacity;
    }

    public long memUsed()
    {
        return capacity - freeCapacity();
    }

    public long freeCapacity()
    {
        return freeCapacity.get();
    }

    public float loadFactor()
    {
        return loadFactor;
    }

    public OHCacheStats stats()
    {
        return new OHCacheStats(
                               hitCount(),
                               missCount(),
                               evictedEntries(),
                               0L,
                               perSegmentSizes(),
                               size(),
                               capacity(),
                               freeCapacity(),
                               -1L,
                               putAddCount(),
                               putReplaceCount(),
                               putFailCount,
                               removeCount(),
                               memUsed(),
                               0L
        );
    }

    private long evictedEntries()
    {
        long evictedEntries = 0L;
        for (CheckSegment map : maps)
            evictedEntries += map.evictedEntries;
        return evictedEntries;
    }

    private long putAddCount()
    {
        long putAddCount = 0L;
        for (CheckSegment map : maps)
            putAddCount += map.putAddCount;
        return putAddCount;
    }

    private long putReplaceCount()
    {
        long putReplaceCount = 0L;
        for (CheckSegment map : maps)
            putReplaceCount += map.putReplaceCount;
        return putReplaceCount;
    }

    private long removeCount()
    {
        long removeCount = 0L;
        for (CheckSegment map : maps)
            removeCount += map.removeCount;
        return removeCount;
    }

    private long hitCount()
    {
        long hitCount = 0L;
        for (CheckSegment map : maps)
            hitCount += map.hitCount;
        return hitCount;
    }

    private long missCount()
    {
        long missCount = 0L;
        for (CheckSegment map : maps)
            missCount += map.missCount;
        return missCount;
    }

    public void setCapacity(long capacity)
    {
        if (capacity < 0L)
            throw new IllegalArgumentException("New capacity " + capacity + " must not be smaller than current capacity");
        long diff = capacity - this.capacity;
        this.capacity = capacity;
        freeCapacity.addAndGet(diff);
    }

    public void close()
    {
        clear();
    }

    //
    //
    //

    private CheckSegment segment(long hash)
    {
        int seg = (int) ((hash & segmentMask) >>> segmentShift);
        return maps[seg];
    }

    private KeyBuffer keySource(K o)
    {
        int size = keySerializer.serializedSize(o);

        ByteBuffer keyBuffer = ByteBuffer.allocate(size);
        keySerializer.serialize(o, keyBuffer);
        assert(keyBuffer.position() == keyBuffer.capacity()) && (keyBuffer.capacity() == size);
        keyBuffer.flip();
        return new KeyBuffer(keyBuffer).finish(hasher);
    }

    private byte[] value(V value)
    {
        ByteBuffer buf = ByteBuffer.allocate(valueSerializer.serializedSize(value));
        valueSerializer.serialize(value, buf);
        return buf.array();
    }
}
