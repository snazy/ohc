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
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.Map;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;

public interface OHCache<K, V> extends Closeable
{
    V getIfPresent(K key);

    boolean contains(K key);

    void put(K key, V value);

    void putAll(Map<? extends K, ? extends V> m);

    void remove(K key);

    void invalidateAll(Iterable<K> keys);

    void invalidateAll();

    long size();

    void cleanUp();

    boolean isStatisticsEnabled();

    void setStatisticsEnabled(boolean statisticsEnabled);

    void resetStatistics();

    int[] getHashTableSizes();

    int getSegments();

    long getCapacity();

    long getMemUsed();

    long getFreeCapacity();

    Iterator<K> hotN(int n);

    OHCacheStats stats();

    double getLoadFactor();

    Iterator<K> keyIterator();

    boolean deserializeEntry(SeekableByteChannel channel) throws IOException;

    boolean serializeEntry(K key, WritableByteChannel channel) throws IOException;

    int serializeHotN(int n, WritableByteChannel channel) throws IOException;
}
