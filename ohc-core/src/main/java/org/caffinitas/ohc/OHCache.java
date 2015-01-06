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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

import org.caffinitas.ohc.histo.EstimatedHistogram;

public interface OHCache<K, V> extends Closeable
{

    /**
     * Adds the key/value.
     * If the entry size of key/value exceeds the configured maximum entry length, any previously existing entry
     * for the key is removed.
     */
    void put(K key, V value);

    /**
     * Adds key/value if either the key is not present or the existing value matches parameter {@code old}.
     * If the entry size of key/value exceeds the configured maximum entry length, the old value is removed.
     */
    boolean addOrReplace(K key, V old, V value);

    /**
     * Adds the key/value if the key is not present.
     * If the entry size of key/value exceeds the configured maximum entry length, any previously existing entry
     * for the key is removed.
     */
    boolean putIfAbsent(K k, V v);

    void putAll(Map<? extends K, ? extends V> m);

    void remove(K key);

    void removeAll(Iterable<K> keys);

    void clear();

    V get(K key);

    boolean containsKey(K key);

    // iterators

    /**
     * Builds an iterator over the N most recently used keys returning deserialized objects.
     * You must call {@code close()} on the returned iterator.
     */
    CloseableIterator<K> hotKeyIterator(int n);

    /**
     * Builds an iterator over all keys returning deserialized objects.
     * You must call {@code close()} on the returned iterator.
     */
    CloseableIterator<K> keyIterator();

    /**
     * Builds an iterator over all keys returning direct byte buffers.
     * Do not use a returned {@code ByteBuffer} after calling any method on the iterator.
     * You must call {@code close()} on the returned iterator.
     */
    CloseableIterator<ByteBuffer> hotKeyBufferIterator(int n);

    /**
     * Builds an iterator over all keys returning direct byte buffers.
     * Do not use a returned {@code ByteBuffer} after calling any method on the iterator.
     * You must call {@code close()} on the returned iterator.
     */
    CloseableIterator<ByteBuffer> keyBufferIterator();

    // serialization

    boolean deserializeEntry(ReadableByteChannel channel) throws IOException;

    boolean serializeEntry(K key, WritableByteChannel channel) throws IOException;

    int deserializeEntries(ReadableByteChannel channel) throws IOException;

    int serializeHotNEntries(int n, WritableByteChannel channel) throws IOException;

    int serializeHotNKeys(int n, WritableByteChannel channel) throws IOException;

    CloseableIterator<K> deserializeKeys(ReadableByteChannel channel) throws IOException;

    // statistics / information

    void resetStatistics();

    long size();

    int[] hashTableSizes();

    long[] perSegmentSizes();

    EstimatedHistogram getBucketHistogram();

    int segments();

    long capacity();

    long memUsed();

    long freeCapacity();

    float loadFactor();

    OHCacheStats stats();

    /**
     * Modify the cache's capacity.
     * Lowering the capacity will not immediately remove any entry nor will it immediately free allocated (off heap) memory.
     * <p>
     * Future operations will even allocate in flight, temporary memory - i.e. setting capacity to 0 does not
     * disable the cache, it will continue to work but cannot add more data.
     * </p>
     */
    public void setCapacity(long capacity);
}
