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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.caffinitas.ohc.histo.EstimatedHistogram;

public interface OHCache<K, V> extends Closeable
{
    long USE_DEFAULT_EXPIRE_AT = -1L;
    long NEVER_EXPIRE = Long.MAX_VALUE;

    /**
     * Same as {@link #put(Object, Object, long)} but uses the configured default TTL, if any.
     * @param key      key of the entry to be added. Must not be {@code null}.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @return {@code true}, if the entry has been added, {@code false} otherwise
     */
    boolean put(K key, V value);

    /**
     * Adds the key/value.
     * If the entry size of key/value exceeds the configured maximum entry length, any previously existing entry
     * for the key is removed.
     * @param key      key of the entry to be added. Must not be {@code null}.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @param expireAt timestamp in milliseconds since "epoch" (like {@link System#currentTimeMillis() System.currentTimeMillis()})
     *                 when the entry shall expire. Pass {@link #USE_DEFAULT_EXPIRE_AT} for the configured default
     *                 time-to-live or {@link #NEVER_EXPIRE} to let it never expire.
     * @return {@code true}, if the entry has been added, {@code false} otherwise
     */
    boolean put(K key, V value, long expireAt);

    /**
     * Same as {@link #addOrReplace(Object, Object, Object, long)} but uses the configured default TTL, if any.
     *
     * @param key      key of the entry to be added or replaced. Must not be {@code null}.
     * @param old      if the entry exists, it's serialized value is compared to the serialized value of {@code old}
     *                 and only replaced, if it matches.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @return {@code true} on success or {@code false} if the existing value does not matcuh {@code old}
     */
    boolean addOrReplace(K key, V old, V value);

    /**
     * Adds key/value if either the key is not present or the existing value matches parameter {@code old}.
     * If the entry size of key/value exceeds the configured maximum entry length, the old value is removed.
     *
     * @param key      key of the entry to be added or replaced. Must not be {@code null}.
     * @param old      if the entry exists, it's serialized value is compared to the serialized value of {@code old}
     *                 and only replaced, if it matches.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @param expireAt timestamp in milliseconds since "epoch" (like {@link System#currentTimeMillis() System.currentTimeMillis()})
     *                 when the entry shall expire. Pass {@link #USE_DEFAULT_EXPIRE_AT} for the configured default
     *                 time-to-live or {@link #NEVER_EXPIRE} to let it never expire.
     * @return {@code true} on success or {@code false} if the existing value does not matcuh {@code old}
     */
    boolean addOrReplace(K key, V old, V value, long expireAt);

    /**
     * Same as {@link #putIfAbsent(Object, Object, long)} but uses the configured default TTL, if any.
     *
     * @param key      key of the entry to be added. Must not be {@code null}.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @return {@code true} on success or {@code false} if the key is already present.
     */
    boolean putIfAbsent(K key, V value);

    /**
     * Adds the key/value if the key is not present.
     * If the entry size of key/value exceeds the configured maximum entry length, any previously existing entry
     * for the key is removed.
     *
     * @param key      key of the entry to be added. Must not be {@code null}.
     * @param value    value of the entry to be added. Must not be {@code null}.
     * @param expireAt timestamp in milliseconds since "epoch" (like {@link System#currentTimeMillis() System.currentTimeMillis()})
     *                 when the entry shall expire. Pass {@link #USE_DEFAULT_EXPIRE_AT} for the configured default
     *                 time-to-live or {@link #NEVER_EXPIRE} to let it never expire.
     * @return {@code true} on success or {@code false} if the key is already present.
     */
    boolean putIfAbsent(K key, V value, long expireAt);

    /**
     * This is effectively a shortcut to add all entries in the given map {@code m}.
     *
     * @param m entries to be added
     */
    void putAll(Map<? extends K, ? extends V> m);

    /**
     * Remove a single entry for the given key.
     *
     * @param key key of the entry to be removed. Must not be {@code null}.
     * @return {@code true}, if the entry has been removed, {@code false} otherwise
     */
    boolean remove(K key);

    /**
     * This is effectively a shortcut to remove the entries for all keys given in the iterable {@code keys}.
     *
     * @param keys keys to be removed
     */
    void removeAll(Iterable<K> keys);

    /**
     * Removes all entries from the cache.
     */
    void clear();

    /**
     * Get the value for a given key.
     *
     * @param key      key of the entry to be retrieved. Must not be {@code null}.
     * @return either the non-{@code null} value or {@code null} if no entry for the requested key exists
     */
    V get(K key);

    /**
     * Checks whether an entry for a given key exists.
     * Usually, this is more efficient than testing for {@code null} via {@link #get(Object)}.
     *
     * @param key      key of the entry to be retrieved. Must not be {@code null}.
     * @return either {@code true} if an entry for the given key exists or {@code false} if no entry for the requested key exists
     */
    boolean containsKey(K key);

    /**
     * Returns a closeable byte buffer.
     * You must close the returned {@link DirectValueAccess} instance after use.
     * After closing, you must not call any of the methods of the {@link java.nio.ByteBuffer}
     * returned by {@link DirectValueAccess#buffer()}.
     *
     * @return reference-counted byte buffer or {@code null} if key does not exist.
     */
    DirectValueAccess getDirect(K key);

    /**
     * Like {@link OHCache#getDirect(Object)}, but allows skipping the update of LRU stats when {@code updateLRU}
     * is {@code false}.
     *
     * @return reference-counted byte buffer or {@code null} if key does not exist.
     */
    DirectValueAccess getDirect(K key, boolean updateLRU);


    // cache loader support

    /**
     * Shortcut to call {@link #getWithLoader(Object, CacheLoader, long, TimeUnit)} using the default entry
     * time-to-live. Note that the cache has to be configured with an {@link java.util.concurrent.Executor} to
     * schedule the load process.
     * <p>
     * Note that the future may indicate a failure via it's {@link Future#get() get} methods if the
     * {@link CacheLoader#load(Object)} implementation threw an error or a runtime exception occured due to
     * not enough memory for example.
     * </p>
     *
     * @param key    key of the value to load
     * @param loader loader implementation to use
     * @return a future to process the load request asynchronously.
     */
    Future<V> getWithLoaderAsync(K key, CacheLoader<K, V> loader);

    /**
     * Shortcut to call {@link #getWithLoader(Object, CacheLoader, long, TimeUnit)} using the default entry
     * time-to-live. Note that the cache has to be configured with an {@link java.util.concurrent.Executor} to
     * schedule the load process.
     * <p>
     * Note that the future may indicate a failure via it's {@link Future#get() get} methods if the
     * {@link CacheLoader#load(Object)} implementation threw an error or a runtime exception occured due to
     * not enough memory for example.
     * </p>
     *
     * @param key    key of the value to load
     * @param loader loader implementation to use
     * @return a future to process the load request asynchronously.
     */
    Future<V> getWithLoaderAsync(K key, CacheLoader<K, V> loader, long expireAt);

    /**
     * Effectively calls {@link #getWithLoaderAsync(Object, CacheLoader) getWithLoaderAsync(key, loader)}{@code .}{@link Future#get() get()}.
     *
     * @param key    key of the value to load
     * @param loader loader implementation to use
     * @return the loaded value or {@code null}
     * @throws InterruptedException as from the contract of {@link Future#get()}
     * @throws ExecutionException   thrown if the {@link CacheLoader#load(Object)} implementation threw an error or a
     *                              runtime exception occured due to not enough memory for example.
     */
    V getWithLoader(K key, CacheLoader<K, V> loader) throws InterruptedException, ExecutionException;

    /**
     * Effectively calls {@link #getWithLoaderAsync(Object, CacheLoader) getWithLoaderAsync(key, loader)}{@code .}{@link Future#get(long, TimeUnit) get(timeout, unit)}.
     *
     * @param key     key of the value to load
     * @param loader  loader implementation to use
     * @param timeout timeout value to be passed to {@link Future#get(long, TimeUnit)}
     * @param unit    timeout unit to be passed to {@link Future#get(long, TimeUnit)}
     * @return the loaded value or {@code null}
     * @throws InterruptedException as from the contract of {@link Future#get()}
     * @throws ExecutionException   thrown if the {@link CacheLoader#load(Object)} implementation threw an error or a
     *                              runtime exception occured due to not enough memory for example.
     * @throws TimeoutException     as from the contract of {@link Future#get(long, TimeUnit)}
     */
    V getWithLoader(K key, CacheLoader<K, V> loader, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;

    // iterators

    /**
     * Builds an iterator over the N most recently used keys returning deserialized objects.
     * You must call {@code close()} on the returned iterator.
     * <p>
     *     Note: During a rehash, the implementation might return keys twice or not at all.
     * </p>
     */
    CloseableIterator<K> hotKeyIterator(int n);

    /**
     * Builds an iterator over all keys returning deserialized objects.
     * You must call {@code close()} on the returned iterator.
     * <p>
     *     Note: During a rehash, the implementation might return keys twice or not at all.
     * </p>
     */
    CloseableIterator<K> keyIterator();

    /**
     * Builds an iterator over all keys returning direct byte buffers.
     * Do not use a returned {@code ByteBuffer} after calling any method on the iterator.
     * You must call {@code close()} on the returned iterator.
     * <p>
     *     Note: During a rehash, the implementation might return keys twice or not at all.
     * </p>
     */
    CloseableIterator<ByteBuffer> hotKeyBufferIterator(int n);

    /**
     * Builds an iterator over all keys returning direct byte buffers.
     * Do not use a returned {@code ByteBuffer} after calling any method on the iterator.
     * You must call {@code close()} on the returned iterator.
     * <p>
     *     Note: During a rehash, the implementation might return keys twice or not at all.
     * </p>
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
    void setCapacity(long capacity);
}
