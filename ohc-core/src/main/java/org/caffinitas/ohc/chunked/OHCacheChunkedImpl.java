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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.caffinitas.ohc.CacheLoader;
import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.CloseableIterator;
import org.caffinitas.ohc.DirectValueAccess;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.OHCacheStats;
import org.caffinitas.ohc.histo.EstimatedHistogram;

public final class OHCacheChunkedImpl<K, V> implements OHCache<K, V>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OHCacheChunkedImpl.class);

    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;

    private final OffHeapChunkedMap[] maps;
    private final long segmentMask;
    private final int segmentShift;

    private final int maxEntrySize;
    private final int fixedKeySize;
    private final int fixedValueSize;
    private final int chunkSize;

    private long capacity;

    private volatile long putFailCount;

    private final Hasher hasher;

    public OHCacheChunkedImpl(OHCacheBuilder<K, V> builder)
    {
        long capacity = builder.getCapacity();
        if (capacity <= 0L)
            throw new IllegalArgumentException("capacity:" + capacity);

        if (Eviction.LRU != builder.getEviction())
            throw new IllegalArgumentException("Chunked implementation only available with LRU");

        int segments = builder.getSegmentCount();
        if (segments <= 0)
            segments = Math.min(16, Runtime.getRuntime().availableProcessors() * 2);
        segments = Ints.checkedCast(Util.roundUpToPowerOf2(segments, 1 << 30));

        this.chunkSize = builder.getChunkSize();
        if (chunkSize < 0 || chunkSize > capacity / segments / 2)
            throw new IllegalArgumentException("chunkSize:" + chunkSize);

        this.fixedKeySize = Math.max(builder.getFixedKeySize(), 0);
        this.fixedValueSize = Math.max(builder.getFixedValueSize(), 0);
        if ((fixedKeySize > 0 || fixedValueSize > 0) &&
            (fixedKeySize <= 0 || fixedValueSize <= 0))
            throw new IllegalArgumentException("fixedKeySize:" + fixedKeySize+",fixedValueSize:" + fixedValueSize);

        this.capacity = capacity;

        this.hasher = Hasher.create(builder.getHashAlgorighm());

        if (builder.getDefaultTTLmillis() > 0L)
            throw new IllegalArgumentException("chunked implementation does not support expiring entries");

        // build segments
        maps = new OffHeapChunkedMap[segments];
        for (int i = 0; i < segments; i++)
        {
            try
            {
                maps[i] = new OffHeapChunkedMap(builder, capacity / segments, chunkSize);
            }
            catch (RuntimeException e)
            {
                for (;i >= 0; i--)
                    if (maps[i] != null)
                        maps[i].release();
                throw e;
            }
        }

        // bit-mask for segment part of hash
        int bitNum = Util.bitNum(segments) - 1;
        this.segmentShift = 64 - bitNum;
        this.segmentMask = ((long) segments - 1) << segmentShift;

        // calculate max entry size
        long maxEntrySize = builder.getMaxEntrySize();
        if (maxEntrySize > capacity / segments)
            throw new IllegalArgumentException("Illegal max entry size " + maxEntrySize);
        else if (maxEntrySize <= 0)
            maxEntrySize = capacity / segments;
        this.maxEntrySize = (int) maxEntrySize;

        this.keySerializer = builder.getKeySerializer();
        if (keySerializer == null)
            throw new NullPointerException("keySerializer == null");
        this.valueSerializer = builder.getValueSerializer();
        if (valueSerializer == null)
            throw new NullPointerException("valueSerializer == null");

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("OHC chunked instance with {} segments and capacity of {} created.", segments, capacity);
    }

    private static UnsupportedOperationException unsupportedOp()
    {
        return new UnsupportedOperationException("Keeping external references to off-heap entries not supported");
    }

    //
    // map stuff
    //

    public DirectValueAccess getDirect(K key)
    {
        return getDirect(key, true);
    }

    public DirectValueAccess getDirect(K key, boolean updateLRU)
    {
        throw unsupportedOp();
    }

    public V get(K key)
    {
        if (key == null)
            throw new NullPointerException();

        KeyBuffer keySource = keySource(key);

        OffHeapChunkedMap seg = segment(keySource.hash());
        return (V) seg.getEntry(keySource, valueSerializer);
    }

    public boolean containsKey(K key)
    {
        if (key == null)
            throw new NullPointerException();

        KeyBuffer keySource = keySource(key);

        return segment(keySource.hash()).getEntry(keySource, null) == Boolean.TRUE;
    }

    public boolean put(K key, V value)
    {
        return putInternal(key, value, false, null, OHCache.NEVER_EXPIRE);
    }

    public boolean put(K key, V value, long expireAt)
    {
        return putInternal(key, value, false, null, expireAt);
    }

    public boolean addOrReplace(K key, V old, V value)
    {
        return putInternal(key, value, false, old, OHCache.NEVER_EXPIRE);
    }

    public boolean addOrReplace(K key, V old, V value, long expireAt)
    {
        return putInternal(key, value, false, old, expireAt);
    }

    public boolean putIfAbsent(K key, V value)
    {
        return putInternal(key, value, true, null, OHCache.NEVER_EXPIRE);
    }

    public boolean putIfAbsent(K key, V value, long expireAt)
    {
        return putInternal(key, value, true, null, expireAt);
    }

    private boolean putInternal(K k, V v, boolean ifAbsent, V old, long expireAt)
    {
        if (expireAt != OHCache.NEVER_EXPIRE)
            throw new IllegalArgumentException("chunked implementation does not support expiring entries");

        if (k == null || v == null)
            throw new NullPointerException();

        if (isFixedSize())
            return putInternalFixed(k, v, ifAbsent, old);

        return putInternalVariable(k, v, ifAbsent, old);
    }

    private boolean putInternalFixed(K k, V v, boolean ifAbsent, V old)
    {
        int keyLen = fixedKeySize;
        int valueLen = fixedValueSize;

        int bytes = Util.allocLen(keyLen, valueLen, isFixedSize());
        int entryBytes = bytes;

        int oldValueLen = 0;
        if (old != null)
        {
            oldValueLen = valueLen;
            bytes += oldValueLen;
        }

        ByteBuffer hashEntry = ByteBuffer.allocate(bytes);

        hashEntry.position(Util.entryOffData(isFixedSize()));
        keySerializer.serialize(k, hashEntry);
        fillUntil(hashEntry, Util.entryOffData(isFixedSize()) + keyLen);
        valueSerializer.serialize(v, hashEntry);
        fillUntil(hashEntry, Util.entryOffData(isFixedSize()) + keyLen + valueLen);

        if (old != null)
        {
            valueSerializer.serialize(old, hashEntry);
            fillUntil(hashEntry, Util.entryOffData(isFixedSize()) + keyLen + valueLen * 2);
        }

        hashEntry.position(Util.entryOffData(isFixedSize()));
        hashEntry.limit(Util.entryOffData(isFixedSize()) + keyLen);
        long hash = hasher.hash(hashEntry);

        hashEntry.position(0);
        hashEntry.limit(bytes);

        // initialize hash entry
        initEntry(hash, keyLen, valueLen, hashEntry);

        return segment(hash).putEntry(hashEntry, hash, keyLen, entryBytes, ifAbsent, oldValueLen);
    }

    private boolean putInternalVariable(K k, V v, boolean ifAbsent, V old)
    {
        int sz = Util.entryOffData(isFixedSize()) +
                 (isFixedSize()
                 ? fixedKeySize + fixedValueSize + (old != null ? fixedValueSize : 0)
                 : keySize(k) + valueSize(v) + (old != null ? valueSize(old) : 0));

        ByteBuffer hashEntry = ByteBuffer.allocate(sz);

        hashEntry.position(Util.entryOffData(isFixedSize()));
        keySerializer.serialize(k, hashEntry);
        int keyLen = hashEntry.position() - Util.entryOffData(isFixedSize());
        valueSerializer.serialize(v, hashEntry);
        int valueLen = hashEntry.position() - keyLen - Util.entryOffData(isFixedSize());
        int entryBytes = hashEntry.position();

        if (maxEntrySize > 0L && entryBytes > maxEntrySize)
        {
            remove(k);
            return false;
        }

        int oldValueLen = 0;
        if (old != null)
        {
            valueSerializer.serialize(old, hashEntry);
            oldValueLen = hashEntry.position() - entryBytes;
        }
        int bytes = hashEntry.position();

        hashEntry.position(Util.entryOffData(isFixedSize()));
        hashEntry.limit(Util.entryOffData(isFixedSize()) + keyLen);
        long hash = hasher.hash(hashEntry);

        hashEntry.position(0);
        hashEntry.limit(bytes);

        // initialize hash entry
        initEntry(hash, keyLen, valueLen, hashEntry);

        return segment(hash).putEntry(hashEntry, hash, keyLen, entryBytes, ifAbsent, oldValueLen);
    }

    private int valueSize(V v)
    {
        int sz = valueSerializer.serializedSize(v);
        if (sz <= 0)
            throw new IllegalArgumentException("Illegal value length " + sz);
        return sz;
    }

    private int keySize(K k)
    {
        int sz = keySerializer.serializedSize(k);
        if (sz <= 0)
            throw new IllegalArgumentException("Illegal key length " + sz);
        return sz;
    }

    private boolean isFixedSize()
    {
        return fixedKeySize > 0;
    }

    public boolean remove(K k)
    {
        if (k == null)
            throw new NullPointerException();

        KeyBuffer key = keySource(k);

        return segment(key.hash()).removeEntry(key);
    }

    public V getWithLoader(K key, CacheLoader<K, V> loader) throws InterruptedException, ExecutionException
    {
        throw unsupportedOp();
    }

    public V getWithLoader(K key, CacheLoader<K, V> loader, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        throw unsupportedOp();
    }

    public Future<V> getWithLoaderAsync(final K key, final CacheLoader<K, V> loader)
    {
        throw unsupportedOp();
    }

    public Future<V> getWithLoaderAsync(K key, CacheLoader<K, V> loader, long expireAt)
    {
        throw unsupportedOp();
    }

    private OffHeapChunkedMap segment(long hash)
    {
        int seg = (int) ((hash & segmentMask) >>> segmentShift);
        return maps[seg];
    }

    private KeyBuffer keySource(K o)
    {
        int sz = isFixedSize() ? fixedKeySize : keySize(o);
        ByteBuffer keyBuffer = ByteBuffer.allocate(sz);
        keySerializer.serialize(o, keyBuffer);
        keyBuffer.flip();
        return keySource(keyBuffer);
    }

    private KeyBuffer keySource(ByteBuffer keyBuffer)
    {
        return new KeyBuffer(keyBuffer).finish(hasher);
    }

    private void fillUntil(ByteBuffer keyBuffer, int until)
    {
        while (until - keyBuffer.position() >= 8)
            keyBuffer.putLong(0L);
        while (until - keyBuffer.position() >= 4)
            keyBuffer.putInt(0);
        while (until - keyBuffer.position() > 0)
            keyBuffer.put((byte) 0);
    }

    private void initEntry(long hash, int keyLen, int valueLen, ByteBuffer hashEntry)
    {
        hashEntry.putLong(Util.ENTRY_OFF_HASH, hash);
        hashEntry.putInt(Util.ENTRY_OFF_NEXT, 0);
        if (fixedKeySize > 0)
            return;
        hashEntry.putInt(Util.ENTRY_OFF_VALUE_LENGTH, valueLen);
        hashEntry.putInt(Util.ENTRY_OFF_KEY_LENGTH, keyLen);
        hashEntry.putInt(Util.ENTRY_OFF_RESERVED_LENGTH, valueLen);
    }

    //
    // maintenance
    //

    public void clear()
    {
        for (OffHeapChunkedMap map : maps)
            map.clear();
    }

    //
    // state
    //

    public void setCapacity(long capacity)
    {
        throw new UnsupportedOperationException("Changing capacity not supported");
    }

    public void close()
    {
        clear();

        for (OffHeapChunkedMap map : maps)
            map.release();
        Arrays.fill(maps, null);

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Closing OHC instance");
    }

    //
    // statistics and related stuff
    //

    public void resetStatistics()
    {
        for (OffHeapChunkedMap map : maps)
            map.resetStatistics();
        putFailCount = 0;
    }

    public OHCacheStats stats()
    {
        long rehashes = 0L;
        for (OffHeapChunkedMap map : maps)
            rehashes += map.rehashes();
        return new OHCacheStats(
                               hitCount(),
                               missCount(),
                               evictedEntries(),
                               expiredEntries(),
                               perSegmentSizes(),
                               size(),
                               capacity(),
                               freeCapacity(),
                               rehashes,
                               putAddCount(),
                               putReplaceCount(),
                               putFailCount,
                               removeCount(),
                               Uns.getTotalAllocated(),
                               0L);
    }

    private long putAddCount()
    {
        long putAddCount = 0L;
        for (OffHeapChunkedMap map : maps)
            putAddCount += map.putAddCount();
        return putAddCount;
    }

    private long putReplaceCount()
    {
        long putReplaceCount = 0L;
        for (OffHeapChunkedMap map : maps)
            putReplaceCount += map.putReplaceCount();
        return putReplaceCount;
    }

    private long removeCount()
    {
        long removeCount = 0L;
        for (OffHeapChunkedMap map : maps)
            removeCount += map.removeCount();
        return removeCount;
    }

    private long hitCount()
    {
        long hitCount = 0L;
        for (OffHeapChunkedMap map : maps)
            hitCount += map.hitCount();
        return hitCount;
    }

    private long missCount()
    {
        long missCount = 0L;
        for (OffHeapChunkedMap map : maps)
            missCount += map.missCount();
        return missCount;
    }

    public long capacity()
    {
        return capacity;
    }

    public long freeCapacity()
    {
        long freeCapacity = 0L;
        for (OffHeapChunkedMap map : maps)
            freeCapacity += map.freeCapacity();
        return freeCapacity;
    }

    private long evictedEntries()
    {
        long evictedEntries = 0L;
        for (OffHeapChunkedMap map : maps)
            evictedEntries += map.evictedEntries();
        return evictedEntries;
    }

    private long expiredEntries()
    {
        long expiredEntries = 0L;
        for (OffHeapChunkedMap map : maps)
            expiredEntries += map.expiredEntries();
        return expiredEntries;
    }

    public long size()
    {
        long size = 0L;
        for (OffHeapChunkedMap map : maps)
            size += map.size();
        return size;
    }

    public int segments()
    {
        return maps.length;
    }

    public float loadFactor()
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

    public EstimatedHistogram getBucketHistogram()
    {
        EstimatedHistogram hist = new EstimatedHistogram();
        for (OffHeapChunkedMap map : maps)
            map.updateBucketHistogram(hist);

        long[] offsets = hist.getBucketOffsets();
        long[] buckets = hist.getBuckets(false);

        for (int i = buckets.length - 1; i > 0; i--)
        {
            if (buckets[i] != 0L)
            {
                offsets = Arrays.copyOf(offsets, i + 2);
                buckets = Arrays.copyOf(buckets, i + 3);
                System.arraycopy(offsets, 0, offsets, 1, i + 1);
                System.arraycopy(buckets, 0, buckets, 1, i + 2);
                offsets[0] = 0L;
                buckets[0] = 0L;
                break;
            }
        }

        for (int i = 0; i < offsets.length; i++)
            offsets[i]--;

        return new EstimatedHistogram(offsets, buckets);
    }

    //
    // serialization (serialized data cannot be ported between different CPU architectures, if endianess differs)
    //

    public CloseableIterator<K> deserializeKeys(final ReadableByteChannel channel) throws IOException
    {
        throw unsupportedOp();
    }

    public boolean deserializeEntry(ReadableByteChannel channel) throws IOException
    {
        throw unsupportedOp();
    }

    public boolean serializeEntry(K key, WritableByteChannel channel) throws IOException
    {
        throw unsupportedOp();
    }

    public int deserializeEntries(ReadableByteChannel channel) throws IOException
    {
        throw unsupportedOp();
    }

    public int serializeHotNEntries(int n, WritableByteChannel channel) throws IOException
    {
        throw unsupportedOp();
    }

    public int serializeHotNKeys(int n, WritableByteChannel channel) throws IOException
    {
        throw unsupportedOp();
    }

    //
    // convenience methods
    //

    public void putAll(Map<? extends K, ? extends V> m)
    {
        // could be improved by grouping puts by segment - but increases heap pressure and complexity - decide later
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    public void removeAll(Iterable<K> iterable)
    {
        // could be improved by grouping removes by segment - but increases heap pressure and complexity - decide later
        for (K o : iterable)
            remove(o);
    }

    public long memUsed()
    {
        return capacity();
    }

    //
    // key iterators
    //

    // iterator-chain:
    // 1. segments (#-of-keys / #-of-segments = #-of-keys-per-segment)
    // 2. chunks (#-of-keys-per-segment / #-of-chunks = #-of-keys-per-chunk)
    //
    // Chunk iterator:
    // - snapshot content of chunk into a separate buffer
    // - iterate over that buffer

    private final class SegmentIterator implements CloseableIterator<ByteBuffer>
    {
        private final int keysPerChunk;

        private int seg;

        private Iterator<ByteBuffer> chunkIterator;

        private final ByteBuffer snaphotBuffer;

        private boolean eod;
        private ByteBuffer next;
        private ByteBuffer current;

        SegmentIterator(int nKeys)
        {
            keysPerChunk = nKeys / segments() / chunkSize + 1;
            snaphotBuffer = ByteBuffer.allocate(chunkSize + Util.CHUNK_OFF_DATA);
        }

        public void close() throws IOException
        {
            // noop
        }

        public boolean hasNext()
        {
            if (eod)
                return false;

            if (next == null)
                next = computeNext();

            return next != null;
        }

        public ByteBuffer next()
        {
            if (eod)
                throw new NoSuchElementException();

            ByteBuffer r;
            if (next == null)
            {
                r = computeNext();
            }
            else
            {
                r = next;
                next = null;
            }

            if (!eod && r != null)
            {
                current = r;
                return r.duplicate();
            }

            throw new NoSuchElementException();
        }

        public void remove()
        {
            ByteBuffer c = current;
            if (eod || c == null)
                throw new NoSuchElementException();
            current = null;

            // segment containing 'current' is at index 'seg - 1' since computeNext() increments 'seg'
            maps[seg - 1].removeEntry(keySource(c));
        }

        private ByteBuffer computeNext()
        {
            while (true)
            {
                if (chunkIterator != null && chunkIterator.hasNext())
                    return chunkIterator.next();

                if (seg == segments())
                {
                    eod = true;
                    current = null;
                    return null;
                }
                chunkIterator = maps[seg++].snapshotIterator(keysPerChunk, snaphotBuffer);
            }
        }
    }

    public CloseableIterator<K> hotKeyIterator(final int n)
    {
        return new CloseableIterator<K>()
        {
            private final CloseableIterator<ByteBuffer> wrapped = hotKeyBufferIterator(n);

            public void close() throws IOException
            {
                wrapped.close();
            }

            public boolean hasNext()
            {
                return wrapped.hasNext();
            }

            public K next()
            {
                ByteBuffer bb = wrapped.next();
                return keySerializer.deserialize(bb);
            }

            public void remove()
            {
                wrapped.remove();
            }
        };
    }

    public CloseableIterator<ByteBuffer> hotKeyBufferIterator(int n)
    {
        return new SegmentIterator(n);
    }

    public CloseableIterator<K> keyIterator()
    {
        return hotKeyIterator(Integer.MAX_VALUE);
    }

    public CloseableIterator<ByteBuffer> keyBufferIterator()
    {
        return hotKeyBufferIterator(Integer.MAX_VALUE);
    }

    public String toString()
    {
        return getClass().getSimpleName() +
               "(capacity=" + capacity() +
               " ,segments=" + maps.length +
               " ,chunkSize=" + chunkSize +
               " ,fixedKeySize=" + fixedKeySize +
               " ,fixedValueSize=" + fixedValueSize +
               " ,maxEntrySize=" + maxEntrySize;
    }
}
