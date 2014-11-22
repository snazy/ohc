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
package org.caffinitas.ohc.benchmark;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.cache.SerializingCache;
import org.apache.cassandra.io.ISerializer;

@SuppressWarnings("unchecked")
public class SerializationCacheWrapper<K, V> implements Cache<K, V> {
    SerializingCache<K, V> cache; 
    public SerializationCacheWrapper(long capacity, ISerializer<V> seializer) {
        cache = SerializingCache.create(capacity, seializer);
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public V getIfPresent(Object key) {
        return cache.get((K) key);
    }

    @Override
    public V get(K key, Callable<? extends V> valueLoader) throws ExecutionException {
        return null;
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        
    }

    @Override
    public void invalidate(Object key) {
        cache.remove((K) key); 
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        cache.clear();
    }

    @Override
    public void invalidateAll() {
        cache.clear();
    }

    @Override
    public CacheStats stats() {
        return null;
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        return null;
    }

    @Override
    public void cleanUp() {
        cache.clear();
    }

    @Override
    public long size() {
        return cache.size();
    }
}
