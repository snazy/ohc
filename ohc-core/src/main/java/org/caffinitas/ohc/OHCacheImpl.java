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

import java.io.EOFException;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.caffinitas.ohc.histo.EstimatedHistogram;

import static org.caffinitas.ohc.Util.ENTRY_OFF_DATA;
import static org.caffinitas.ohc.Util.ENTRY_OFF_HASH;
import static org.caffinitas.ohc.Util.ENTRY_OFF_KEY_LENGTH;
import static org.caffinitas.ohc.Util.HEADER_ENTRIES;
import static org.caffinitas.ohc.Util.HEADER_ENTRIES_WRONG;
import static org.caffinitas.ohc.Util.HEADER_KEYS;
import static org.caffinitas.ohc.Util.HEADER_KEYS_WRONG;
import static org.caffinitas.ohc.Util.allocLen;
import static org.caffinitas.ohc.Util.readFully;
import static org.caffinitas.ohc.Util.roundUpTo8;
import static org.caffinitas.ohc.Util.writeFully;

public final class OHCacheImpl<K, V> implements OHCache<K, V>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OHCacheImpl.class);

    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;

    private final OffHeapMap[] maps;
    private final long segmentMask;
    private final int segmentShift;

    private final long maxEntrySize;

    private long capacity;
    private final AtomicLong freeCapacity;

    private volatile long putFailCount;

    public OHCacheImpl(OHCacheBuilder<K, V> builder)
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
        segments = (int) Util.roundUpToPowerOf2(segments, 1<<30);
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
        int bitNum = Util.bitNum(segments) - 1;
        this.segmentShift = 64 - bitNum;
        this.segmentMask = ((long) segments - 1) << segmentShift;

        // calculate max entry size
        long maxEntrySize = builder.getMaxEntrySize();
        if (maxEntrySize > capacity / segments)
            throw new IllegalArgumentException("Illegal max entry size " + maxEntrySize);
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

    //
    // map stuff
    //

    public V get(K key)
    {
        if (key == null)
            throw new NullPointerException();

        KeyBuffer keySource = keySource(key);

        OffHeapMap segment = segment(keySource.hash());
        long hashEntryAdr = segment.getEntry(keySource, true);

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
            HashEntries.dereference(hashEntryAdr);
        }
    }

    public boolean containsKey(K key)
    {
        if (key == null)
            throw new NullPointerException();

        KeyBuffer keySource = keySource(key);

        return segment(keySource.hash()).getEntry(keySource, false) != 0L;
    }

    public void put(K k, V v)
    {
        putInternal(k, v, false, null);
    }

    public boolean addOrReplace(K key, V old, V value)
    {
        return putInternal(key, value, false, old);
    }

    public boolean putIfAbsent(K k, V v)
    {
        return putInternal(k, v, true, null);
    }

    private boolean putInternal(K k, V v, boolean ifAbsent, V old)
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
            catch (RuntimeException | Error e)
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
            if ((maxEntrySize > 0L && bytes > maxEntrySize) || (hashEntryAdr = HashEntries.allocate(bytes)) == 0L)
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
            catch (RuntimeException | Error e)
            {
                HashEntries.free(hashEntryAdr, bytes);
                throw e;
            }
            catch (Throwable e)
            {
                HashEntries.free(hashEntryAdr, bytes);
                throw new IOError(e);
            }

            long hash = key.hash();

            // initialize hash entry
            HashEntries.init(hash, keyLen, valueLen, hashEntryAdr);

            if (segment(hash).putEntry(hashEntryAdr, hash, keyLen, bytes, ifAbsent, oldValueAdr, oldValueLen))
                return true;

            HashEntries.free(hashEntryAdr, bytes);
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

    public void clear()
    {
        for (OffHeapMap map : maps)
            map.clear();

        HashEntries.memBufferClear();
    }

    //
    // state
    //

    public void setCapacity(long capacity)
    {
        if (capacity < 0L)
            throw new IllegalArgumentException();
        long diff = capacity - this.capacity;
        this.capacity = capacity;
        freeCapacity.addAndGet(diff);
    }

    public void close()
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
        for (OffHeapMap map : maps)
            map.updateBucketHistogram(hist);

        long[] offsets = hist.getBucketOffsets();
        long[] buckets = hist.getBuckets(false);

        for (int i=buckets.length-1;i>0;i--)
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
        long headerAddress = Uns.allocateIOException(8);
        try
        {
            ByteBuffer header = Uns.directBufferFor(headerAddress, 0L, 8L);
            readFully(channel, header);
            header.flip();
            int magic = header.getInt();
            if (magic == HEADER_KEYS_WRONG)
                throw new IOException("File from instance with different CPU architecture cannot be loaded");
            if (magic == HEADER_ENTRIES)
                throw new IOException("File contains entries - expected keys");
            if (magic != HEADER_KEYS)
                throw new IOException("Illegal file header");
            if (header.getInt() != 1)
                throw new IOException("Illegal file version");
        }
        finally
        {
            Uns.free(headerAddress);
        }

        return new CloseableIterator<K>()
        {
            private K next;
            private boolean eod;

            private final byte[] keyLenBuf = new byte[8];
            private final ByteBuffer bb = ByteBuffer.wrap(keyLenBuf);

            private long bufAdr;
            private long bufLen;

            public void close()
            {
                Uns.free(bufAdr);
                bufAdr = 0L;
            }

            protected void finalize() throws Throwable
            {
                close();
                super.finalize();
            }

            public boolean hasNext()
            {
                if (eod)
                    return false;
                if (next == null)
                    checkNext();
                return next != null;
            }

            private void checkNext()
            {
                try
                {
                    bb.clear();
                    if (!readFully(channel, bb))
                    {
                        eod = true;
                        return;
                    }

                    long keyLen = Uns.getLongFromByteArray(keyLenBuf, 0);
                    long totalLen = ENTRY_OFF_DATA + keyLen;

                    if (bufLen < totalLen)
                    {
                        Uns.free(bufAdr);
                        bufAdr = 0L;

                        bufLen = Math.max(4096, Util.roundUpToPowerOf2(totalLen, 1 << 30));
                        bufAdr = Uns.allocateIOException(bufLen);
                    }

                    if (!readFully(channel, Uns.directBufferFor(bufAdr, ENTRY_OFF_DATA, keyLen)))
                    {
                        eod = true;
                        throw new EOFException();
                    }
                    HashEntries.init(0L, keyLen, 0L, bufAdr);
                    next = keySerializer.deserialize(new HashEntryKeyInput(bufAdr));
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }

            public K next()
            {
                if (eod)
                    throw new NoSuchElementException();

                K r = next;
                if (r == null)
                {
                    checkNext();
                    r = next;
                }
                if (r == null)
                    throw new NoSuchElementException();
                next = null;

                return r;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public boolean deserializeEntry(ReadableByteChannel channel) throws IOException
    {
        // read hash, keyLen, valueLen
        byte[] hashKeyValueLen = new byte[3 * 8];
        ByteBuffer bb = ByteBuffer.wrap(hashKeyValueLen);
        if (!readFully(channel, bb))
            return false;

        long hash = Uns.getLongFromByteArray(hashKeyValueLen, 0);
        long valueLen = Uns.getLongFromByteArray(hashKeyValueLen, 8);
        long keyLen = Uns.getLongFromByteArray(hashKeyValueLen, 16);

        long kvLen = roundUpTo8(keyLen) + valueLen;
        long totalLen = kvLen + ENTRY_OFF_DATA;
        long hashEntryAdr;
        if ((maxEntrySize > 0L && totalLen > maxEntrySize) || (hashEntryAdr = HashEntries.allocate(totalLen)) == 0L)
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
        if (!readFully(channel, Uns.directBufferFor(hashEntryAdr, ENTRY_OFF_DATA, kvLen)) ||
            !segment(hash).putEntry(hashEntryAdr, hash, keyLen, totalLen, false, 0L, 0L))
        {
            Uns.free(hashEntryAdr);
            return false;
        }

        return true;
    }

    public boolean serializeEntry(K key, WritableByteChannel channel) throws IOException
    {
        KeyBuffer keySource = keySource(key);

        OffHeapMap segment = segment(keySource.hash());
        long hashEntryAdr = segment.getEntry(keySource, true);

        return hashEntryAdr != 0L && serializeEntry(channel, hashEntryAdr);
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
            if (magic == HEADER_ENTRIES_WRONG)
                throw new IOException("File from instance with different CPU architecture cannot be loaded");
            if (magic == HEADER_KEYS)
                throw new IOException("File contains keys - expected entries");
            if (magic != HEADER_ENTRIES)
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

    public int serializeHotNEntries(int n, WritableByteChannel channel) throws IOException
    {
        return serializeHotN(n, channel, true);
    }

    public int serializeHotNKeys(int n, WritableByteChannel channel) throws IOException
    {
        return serializeHotN(n, channel, false);
    }

    private int serializeHotN(int n, WritableByteChannel channel, boolean entries) throws IOException
    {
        // hotN implementation does only return a (good) approximation - not necessarily the exact hotN
        // since it iterates over the all segments and takes a fraction of 'n' from them.
        // This implementation may also return more results than expected just to keep it simple
        // (it does not really matter if you request 5000 keys and e.g. get 5015).

        long headerAddress = Uns.allocateIOException(8);
        try
        {
            ByteBuffer headerBuffer = Uns.directBufferFor(headerAddress, 0L, 8L);
            headerBuffer.putInt(entries ? HEADER_ENTRIES : HEADER_KEYS);
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
                        if (entries)
                            serializeEntry(channel, hashEntryAdr);
                        else
                            serializeKey(channel, hashEntryAdr);
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
                        HashEntries.dereference(hashEntryAdr);
            }
        }

        return cnt;
    }

    private static boolean serializeEntry(WritableByteChannel channel, long hashEntryAdr) throws IOException
    {
        try
        {
            long keyLen = HashEntries.getKeyLen(hashEntryAdr);
            long valueLen = HashEntries.getValueLen(hashEntryAdr);

            long totalLen = 3 * 8L + roundUpTo8(keyLen) + valueLen;

            // write hash, keyLen, valueLen + key + value
            Util.writeFully(channel, Uns.directBufferFor(hashEntryAdr, ENTRY_OFF_HASH, totalLen));

            return true;
        }
        finally
        {
            HashEntries.dereference(hashEntryAdr);
        }
    }

    private static boolean serializeKey(WritableByteChannel channel, long hashEntryAdr) throws IOException
    {
        try
        {
            long keyLen = HashEntries.getKeyLen(hashEntryAdr);

            long totalLen = 8L + keyLen;

            // write hash, keyLen, valueLen + key + value
            Util.writeFully(channel, Uns.directBufferFor(hashEntryAdr, ENTRY_OFF_KEY_LENGTH, totalLen));

            return true;
        }
        finally
        {
            HashEntries.dereference(hashEntryAdr);
        }
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
        return capacity() - freeCapacity();
    }

    //
    // key iterators
    //

    public CloseableIterator<K> hotKeyIterator(int n)
    {
        return new AbstractHotKeyIterator<K>(n)
        {
            K buildResult(long hashEntryAdr)
            {
                try
                {
                    return keySerializer.deserialize(new HashEntryKeyInput(hashEntryAdr));
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
    }

    public CloseableIterator<ByteBuffer> hotKeyBufferIterator(int n)
    {
        return new AbstractHotKeyIterator<ByteBuffer>(n)
        {
            ByteBuffer buildResult(long hashEntryAdr)
            {
                return Uns.directBufferFor(hashEntryAdr, ENTRY_OFF_DATA, HashEntries.getKeyLen(hashEntryAdr));
            }
        };
    }

    public CloseableIterator<K> keyIterator()
    {
        return new AbstractKeyIterator<K>()
        {
            K buildResult(long hashEntryAdr)
            {
                try
                {
                    return keySerializer.deserialize(new HashEntryKeyInput(hashEntryAdr));
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
    }

    public CloseableIterator<ByteBuffer> keyBufferIterator()
    {
        return new AbstractKeyIterator<ByteBuffer>()
        {
            ByteBuffer buildResult(long hashEntryAdr)
            {
                return Uns.directBufferFor(hashEntryAdr, ENTRY_OFF_DATA, HashEntries.getKeyLen(hashEntryAdr));
            }
        };
    }

    private abstract class AbstractKeyIterator<R> implements CloseableIterator<R>
    {
        private int segmentIndex;
        private OffHeapMap segment;

        private int mapSegmentCount;
        private int mapSegmentIndex;

        private final List<Long> hashEntryAdrs = new ArrayList<>(1024);
        private int listIndex;

        private boolean eod;
        private R next;

        private OffHeapMap lastSegment;
        private long lastHashEntryAdr;

        public void close()
        {
            if (lastHashEntryAdr != 0L)
            {
                HashEntries.dereference(lastHashEntryAdr);
                lastHashEntryAdr = 0L;
                lastSegment = null;
            }
        }

        public boolean hasNext()
        {
            if (eod)
                return false;

            if (next == null)
                next = computeNext();

            return next != null;
        }

        public R next()
        {
            if (eod)
                throw new NoSuchElementException();

            if (next == null)
                next = computeNext();
            R r = next;
            next = null;
            if (!eod)
                return r;
            throw new NoSuchElementException();
        }

        public void remove()
        {
            if (eod)
                throw new NoSuchElementException();

            lastSegment.removeEntry(lastHashEntryAdr);
            close();
        }

        private R computeNext()
        {
            close();

            while (true)
            {
                if (listIndex < hashEntryAdrs.size())
                {
                    long hashEntryAdr = hashEntryAdrs.get(listIndex++);
                    lastSegment = segment;
                    lastHashEntryAdr = hashEntryAdr;
                    return buildResult(hashEntryAdr);
                }

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

            }
        }

        abstract R buildResult(long hashEntryAdr);
    }

    private abstract class AbstractHotKeyIterator<R> extends AbstractIterator<R> implements CloseableIterator<R>
    {
        private final int perMap;
        int mapIndex;

        OffHeapMap segment;
        long[] hotPerMap;
        int subIndex;

        OffHeapMap lastSegment;
        long lastHashEntryAdr;

        public AbstractHotKeyIterator(int n)
        {
            // hotN implementation does only return a (good) approximation - not necessarily the exact hotN
            // since it iterates over the all segments and takes a fraction of 'n' from them.
            // This implementation may also return more results than expected just to keep it simple
            // (it does not really matter if you request 5000 keys and e.g. get 5015).

            this.perMap = n / maps.length + 1;
        }

        public void close()
        {
            if (lastHashEntryAdr != 0L)
            {
                HashEntries.dereference(lastHashEntryAdr);
                lastHashEntryAdr = 0L;
                lastSegment = null;
            }
        }

        abstract R buildResult(long hashEntryAdr);

        protected R computeNext()
        {
            close();

            while (true)
            {
                if (hotPerMap != null && subIndex < hotPerMap.length)
                {
                    long hashEntryAdr = hotPerMap[subIndex++];
                    if (hashEntryAdr != 0L)
                    {
                        lastSegment = segment;
                        lastHashEntryAdr = hashEntryAdr;
                        return buildResult(hashEntryAdr);
                    }
                }

                if (mapIndex == maps.length)
                    return endOfData();

                segment = maps[mapIndex++];
                hotPerMap = segment.hotN(perMap);
                subIndex = 0;
            }
        }
    }
}
