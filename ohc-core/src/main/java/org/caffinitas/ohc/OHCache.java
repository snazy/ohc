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
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.codahale.metrics.Histogram;

public interface OHCache<K, V> extends Closeable
{

    void put(K key, V value);

    boolean addOrReplace(K key, V old, V value);

    boolean putIfAbsent(K k, V v);

    void putAll(Map<? extends K, ? extends V> m);

    void remove(K key);

    void removeAll(Iterable<K> keys);

    void clear();

    V get(K key);

    boolean containsKey(K key);

    // iterators

    Iterator<K> hotKeyIterator(int n);

    Iterator<K> keyIterator();

    // serialization

    boolean deserializeEntry(ReadableByteChannel channel) throws IOException;

    boolean serializeEntry(K key, WritableByteChannel channel) throws IOException;

    int deserializeEntries(ReadableByteChannel channel) throws IOException;

    int serializeHotN(int n, WritableByteChannel channel) throws IOException;

    // statistics / information

    void resetStatistics();

    long size();

    int[] hashTableSizes();

    long[] perSegmentSizes();

    Histogram getBucketHistogram();

    int segments();

    long capacity();

    long memUsed();

    long freeCapacity();

    double loadFactor();

    OHCacheStats stats();

    // C* ICache

    public void setCapacity(long capacity);

    public long weightedSize();
}
