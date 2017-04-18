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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.caffinitas.ohc.CacheLoader;
import org.caffinitas.ohc.CloseableIterator;
import org.caffinitas.ohc.DirectValueAccess;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.OHCacheStats;
import org.caffinitas.ohc.histo.EstimatedHistogram;
import org.testng.Assert;

/**
 * Test code that contains an instance of the production and check {@link OHCache}
 * implementations {@link OHCacheChunkedImpl} and
 * {@link CheckOHCacheImpl}.
 */
public class DoubleCheckCacheImpl<K, V> implements OHCache<K, V>
{
    public final OHCache<K, V> prod;
    public final OHCache<K, V> check;

    public DoubleCheckCacheImpl(OHCacheBuilder<K, V> builder)
    {
        this.prod = builder.build();
        this.check = new CheckOHCacheImpl<>(builder);
    }

    public boolean put(K key, V value)
    {
        boolean rProd = prod.put(key, value);
        boolean rCheck = check.put(key, value);
        Assert.assertEquals(rCheck, rProd, "for key='" + key + '\'');
        return rProd;
    }

    public boolean addOrReplace(K key, V old, V value)
    {
        boolean rProd = prod.addOrReplace(key, old, value);
        boolean rCheck = check.addOrReplace(key, old, value);
        Assert.assertEquals(rCheck, rProd, "for key='" + key + '\'');
        return rProd;
    }

    public boolean putIfAbsent(K k, V v)
    {
        boolean rProd = prod.putIfAbsent(k, v);
        boolean rCheck = check.putIfAbsent(k, v);
        Assert.assertEquals(rCheck, rProd, "for key='" + k + '\'');
        return rProd;
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
        prod.putAll(m);
        check.putAll(m);
    }

    public boolean remove(K key)
    {
        boolean rProd = prod.remove(key);
        boolean rCheck = check.remove(key);
        Assert.assertEquals(rCheck, rProd, "for key='" + key + '\'');
        return rProd;
    }

    public void removeAll(Iterable<K> keys)
    {
        prod.removeAll(keys);
        check.removeAll(keys);
    }

    public void clear()
    {
        prod.clear();
        check.clear();
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
        V rProd = prod.get(key);
        V rCheck = check.get(key);
        Assert.assertEquals(rCheck, rProd, "for key='" + key + '\'');
        return rProd;
    }

    public boolean containsKey(K key)
    {
        boolean rProd = prod.containsKey(key);
        boolean rCheck = check.containsKey(key);
        Assert.assertEquals(rCheck, rProd, "for key='" + key + '\'');
        return rProd;
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
        return new CheckIterator<>(
                                   prod.hotKeyIterator(n),
                                   check.hotKeyIterator(n),
                                   true
        );
    }

    public CloseableIterator<K> keyIterator()
    {
        return new CheckIterator<>(
                                   prod.keyIterator(),
                                   check.keyIterator(),
                                   true
        );
    }

    public CloseableIterator<ByteBuffer> hotKeyBufferIterator(int n)
    {
        return new CheckIterator<>(
                                   prod.hotKeyBufferIterator(n),
                                   check.hotKeyBufferIterator(n),
                                   false
        );
    }

    public CloseableIterator<ByteBuffer> keyBufferIterator()
    {
        return new CheckIterator<>(
                                   prod.keyBufferIterator(),
                                   check.keyBufferIterator(),
                                   false
        );
    }

    public boolean deserializeEntry(ReadableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public boolean serializeEntry(K key, WritableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public int deserializeEntries(ReadableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public int serializeHotNEntries(int n, WritableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public int serializeHotNKeys(int n, WritableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public CloseableIterator<K> deserializeKeys(ReadableByteChannel channel) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public void resetStatistics()
    {
        prod.resetStatistics();
        check.resetStatistics();
    }

    public long size()
    {
        long rProd = prod.size();
        long rCheck = check.size();
        Assert.assertEquals(rCheck, rProd);
        return rProd;
    }

    public int[] hashTableSizes()
    {
        return prod.hashTableSizes();
    }

    public long[] perSegmentSizes()
    {
        long[] rProd = prod.perSegmentSizes();
        long[] rCheck = check.perSegmentSizes();
        Assert.assertEquals(rCheck, rProd);
        return rProd;
    }

    public EstimatedHistogram getBucketHistogram()
    {
        return prod.getBucketHistogram();
    }

    public int segments()
    {
        int rProd = prod.segments();
        int rCheck = check.segments();
        Assert.assertEquals(rCheck, rProd);
        return rProd;
    }

    public long capacity()
    {
        long rProd = prod.capacity();
        long rCheck = check.capacity();
        Assert.assertEquals(rCheck, rProd);
        return rProd;
    }

    public long memUsed()
    {
        long rProd = prod.memUsed();
        long rCheck = check.memUsed();
        Assert.assertEquals(rCheck, rProd);
        return rProd;
    }

    public long freeCapacity()
    {
        long rProd = prod.freeCapacity();
        long rCheck = check.freeCapacity();
        Assert.assertEquals(rCheck, rProd, "capacity: " + capacity());
        return rProd;
    }

    public float loadFactor()
    {
        float rProd = prod.loadFactor();
        float rCheck = check.loadFactor();
        Assert.assertEquals(rCheck, rProd);
        return rProd;
    }

    public OHCacheStats stats()
    {
        OHCacheStats rProd = prod.stats();
        OHCacheStats rCheck = check.stats();
        Assert.assertEquals(rCheck, rProd);
        return rProd;
    }

    public void setCapacity(long capacity)
    {
        prod.setCapacity(capacity);
        check.setCapacity(capacity);
    }

    public void close() throws IOException
    {
        prod.close();
        check.close();
    }

    private class CheckIterator<T> implements CloseableIterator<T>
    {
        private final CloseableIterator<T> prodIter;
        private final CloseableIterator<T> checkIter;

        private final boolean canCompare;

        private final Set<T> prodReturned = new HashSet<>();
        private final Set<T> checkReturned = new HashSet<>();

        CheckIterator(CloseableIterator<T> prodIter, CloseableIterator<T> checkIter, boolean canCompare)
        {
            this.prodIter = prodIter;
            this.checkIter = checkIter;
            this.canCompare = canCompare;
        }

        public void close() throws IOException
        {
            prodIter.close();
            checkIter.close();

            Assert.assertEquals(prodReturned.size(), checkReturned.size());
            if (canCompare)
            {
                for (T t : prodReturned)
                    Assert.assertTrue(check.containsKey((K) t), "check does not contain key " + t);
                for (T t : checkReturned)
                    Assert.assertTrue(prod.containsKey((K) t), "prod does not contain key " + t);
            }
        }

        public boolean hasNext()
        {
            boolean rProd = prodIter.hasNext();
            boolean rCheck = checkIter.hasNext();
            Assert.assertEquals(rCheck, rProd);
            return rProd;
        }

        public T next()
        {
            T rProd = prodIter.next();
            T rCheck = checkIter.next();
            prodReturned.add(rProd);
            checkReturned.add(rCheck);
            return rProd;
        }

        public void remove()
        {
            prodIter.remove();
            checkIter.remove();
        }
    }
}
