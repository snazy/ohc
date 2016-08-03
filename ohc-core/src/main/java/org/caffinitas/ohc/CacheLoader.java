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

/**
 * Implementation to load values for cache entries using {@link OHCache#getWithLoader(Object, CacheLoader) getWithLoader}
 * or  {@link OHCache#getWithLoaderAsync(Object, CacheLoader)} getWithLoaderAsynd}.
 *
 * @param <K> type of the cache key
 * @param <V> type of the cache value
 */
public interface CacheLoader<K, V>
{
    /**
     * Cache loaders implement this method and return a non-{@code null} value on success.
     * {@code null} can be returned to indicate that the no value for the requested key could be found.
     * <p>
     * Permanent failures in loading a <em>specific</em> key can be indicated by throwing a {@link PermanentLoadException}.
     * Other exceptions indicate a temporary failure.
     * </p>
     *
     * @param key key for the value to load. Always non-{@code null}
     * @return non-{@code null} value on success or {@code null} if it could not find a value.
     * @throws PermanentLoadException can be thrown by the loader implementation to indicate that
     *                                is will <em>never</em> succeed in finding a value for the requested key.
     * @throws Exception              any exception other than {@link PermanentLoadException} can be thrown by the
     *                                loader implementation to indicate a <em>temporary</em> failure in loading the value
     */
    V load(K key) throws PermanentLoadException, Exception;
}
