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

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ScheduledExecutorService;

import org.caffinitas.ohc.linked.OHCacheImpl;

/**
 * Configures and builds OHC instance.
 * <table summary="Configuration parameters">
 *     <tr>
 *         <th>Field</th>
 *         <th>Meaning</th>
 *         <th>Default</th>
 *     </tr>
 *     <tr>
 *         <td>{@code keySerializer}</td>
 *         <td>Serializer implementation used for keys</td>
 *         <td>Must be configured</td>
 *     </tr>
 *     <tr>
 *         <td>{@code valueSerializer}</td>
 *         <td>Serializer implementation used for values</td>
 *         <td>Must be configured</td>
 *     </tr>
 *     <tr>
 *         <td>{@code executorService}</td>
 *         <td>Executor service required for get operations using a cache loader. E.g. {@link org.caffinitas.ohc.OHCache#getWithLoaderAsync(Object, CacheLoader)}</td>
 *         <td>(Not configured by default meaning get operations with cache loader not supported by default)</td>
 *     </tr>
 *     <tr>
 *         <td>{@code segmentCount}</td>
 *         <td>Number of segments</td>
 *         <td>2 * number of CPUs ({@code java.lang.Runtime.availableProcessors()})</td>
 *     </tr>
 *     <tr>
 *         <td>{@code hashTableSize}</td>
 *         <td>Initial size of each segment's hash table</td>
 *         <td>{@code 8192}</td>
 *     </tr>
 *     <tr>
 *         <td>{@code loadFactor}</td>
 *         <td>Hash table load factor. I.e. determines when rehashing occurs.</td>
 *         <td>{@code .75f}</td>
 *     </tr>
 *     <tr>
 *         <td>{@code capacity}</td>
 *         <td>Capacity of the cache in bytes</td>
 *         <td>16 MB * number of CPUs ({@code java.lang.Runtime.availableProcessors()}), minimum 64 MB</td>
 *     </tr>
 *     <tr>
 *         <td>{@code maxEntrySize}</td>
 *         <td>Maximum size of a hash entry (including header, serialized key + serialized value)</td>
 *         <td>(not set, defaults to capacity)</td>
 *     </tr>
 *     <tr>
 *         <td>{@code bucketLength}</td>
 *         <td>(For tables implementation only) Number of entries per bucket.</td>
 *         <td>{@code 8}</td>
 *     </tr>
 *     <tr>
 *         <td>{@code throwOOME}</td>
 *         <td>Throw {@code OutOfMemoryError} if off-heap allocation fails</td>
 *         <td>{@code false}</td>
 *     </tr>
 *     <tr>
 *         <td>{@code hashAlgorighm}</td>
 *         <td>Hash algorithm to use internally. Valid options are: {@code XX} for xx-hash, {@code MURMUR3} or {@code CRC32}
 *         Note: this setting does may only help to improve throughput in rare situations - i.e. if the key is
 *         very long and you've proven that it really improves performace</td>
 *         <td>{@code MURMUR3}</td>
 *     </tr>
 *     <tr>
 *         <td>{@code unlocked}</td>
 *         <td>If set to {@code true}, implementations will not perform any locking. The calling code has to take
 *         care of synchronized access. In order to create an instance for a thread-per-core implementation,
 *         set {@code segmentCount=1}, too.</td>
 *         <td>{@code false}</td>
 *     </tr>
 *     <tr>
 *         <td>{@code defaultTTLmillis}</td>
 *         <td>If set to a value {@code > 0}, implementations supporting TTLs will tag all entries with
 *         the given TTL in <b>milliseconds</b>.</td>
 *         <td>{@code 0}</td>
 *     </tr>
 *     <tr>
 *         <td>{@code timeoutsSlots}</td>
 *         <td>The number of timeouts slots for each segment - compare with hashed wheel timer.</td>
 *         <td>{@code 64}</td>
 *     </tr>
 *     <tr>
 *         <td>{@code timeoutsPrecision}</td>
 *         <td>The amount of time in milliseconds for each timeouts-slot.</td>
 *         <td>{@code 128}</td>
 *     </tr>
 * </table>
 * <p>
 *     You may also use system properties prefixed with {@code org.caffinitas.org.} to other defaults.
 *     E.g. the system property {@code org.caffinitas.org.segmentCount} configures the default of the number of segments.
 * </p>
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
public class OHCacheBuilder<K, V>
{
    private int segmentCount;
    private int hashTableSize = 8192;
    private int bucketLength = 8;
    private long capacity;
    private CacheSerializer<K> keySerializer;
    private CacheSerializer<V> valueSerializer;
    private float loadFactor = .75f;
    private long maxEntrySize;
    private Class<? extends OHCache> type = OHCacheImpl.class;
    private ScheduledExecutorService executorService;
    private boolean throwOOME;
    private HashAlgorithm hashAlgorighm = HashAlgorithm.MURMUR3;
    private boolean unlocked;
    private long defaultTTLmillis;
    private int timeoutsSlots;
    private int timeoutsPrecision;

    private OHCacheBuilder()
    {
        int cpus = Runtime.getRuntime().availableProcessors();

        segmentCount = roundUpToPowerOf2(cpus * 2, 1 << 30);
        capacity = Math.min(cpus * 16, 64) * 1024 * 1024;

        segmentCount = fromSystemProperties("segmentCount", segmentCount);
        hashTableSize = fromSystemProperties("hashTableSize", hashTableSize);
        bucketLength = fromSystemProperties("bucketLength", bucketLength);
        capacity = fromSystemProperties("capacity", capacity);
        loadFactor = fromSystemProperties("loadFactor", loadFactor);
        maxEntrySize = fromSystemProperties("maxEntrySize", maxEntrySize);
        throwOOME = fromSystemProperties("throwOOME", throwOOME);
        hashAlgorighm = HashAlgorithm.valueOf(fromSystemProperties("hashAlgorighm", hashAlgorighm.name()));
        unlocked = fromSystemProperties("unlocked", unlocked);
        defaultTTLmillis = fromSystemProperties("defaultTTLmillis", defaultTTLmillis);
        timeoutsSlots = fromSystemProperties("timeoutsSlots", timeoutsSlots);
        timeoutsPrecision = fromSystemProperties("timeoutsPrecision", timeoutsPrecision);
        String t = fromSystemProperties("type", null);
        if (t != null)
            try
            {
                type = (Class<? extends OHCache>) Class.forName(t);
            }
            catch (ClassNotFoundException x)
            {
                try
                {
                    type = (Class<? extends OHCache>) Class.forName("org.caffinitas.ohc." + t + ".OHCacheImpl");
                }
                catch (ClassNotFoundException e)
                {
                    throw new RuntimeException(e);
                }
            }
    }

    public static final String SYSTEM_PROPERTY_PREFIX = "org.caffinitas.ohc.";

    private static float fromSystemProperties(String name, float defaultValue)
    {
        try
        {
            return Float.parseFloat(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Float.toString(defaultValue)));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static long fromSystemProperties(String name, long defaultValue)
    {
        try
        {
            return Long.parseLong(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Long.toString(defaultValue)));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static int fromSystemProperties(String name, int defaultValue)
    {
        try
        {
            return Integer.parseInt(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Integer.toString(defaultValue)));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static boolean fromSystemProperties(String name, boolean defaultValue)
    {
        try
        {
            return Boolean.parseBoolean(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Boolean.toString(defaultValue)));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to parse system property " + SYSTEM_PROPERTY_PREFIX + name, e);
        }
    }

    private static String fromSystemProperties(String name, String defaultValue)
    {
        return System.getProperty(SYSTEM_PROPERTY_PREFIX + name, defaultValue);
    }

    static int roundUpToPowerOf2(int number, int max)
    {
        return number >= max
               ? max
               : (number > 1) ? Integer.highestOneBit((number - 1) << 1) : 1;
    }

    public static <K, V> OHCacheBuilder<K, V> newBuilder()
    {
        return new OHCacheBuilder<>();
    }

    public OHCache<K, V> build()
    {
        try
        {
            return type.getDeclaredConstructor(OHCacheBuilder.class).newInstance(this);
        }
        catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Class<? extends OHCache> getType()
    {
        return type;
    }

    public OHCacheBuilder<K, V> type(Class<? extends OHCache> type)
    {
        this.type = type;
        return this;
    }

    public int getHashTableSize()
    {
        return hashTableSize;
    }

    public OHCacheBuilder<K, V> hashTableSize(int hashTableSize)
    {
        this.hashTableSize = hashTableSize;
        return this;
    }

    public int getBucketLength()
    {
        return bucketLength;
    }

    public OHCacheBuilder<K, V> bucketLength(int bucketLength)
    {
        this.bucketLength = bucketLength;
        return this;
    }

    public long getCapacity()
    {
        return capacity;
    }

    public OHCacheBuilder<K, V> capacity(long capacity)
    {
        this.capacity = capacity;
        return this;
    }

    public CacheSerializer<K> getKeySerializer()
    {
        return keySerializer;
    }

    public OHCacheBuilder<K, V> keySerializer(CacheSerializer<K> keySerializer)
    {
        this.keySerializer = keySerializer;
        return this;
    }

    public CacheSerializer<V> getValueSerializer()
    {
        return valueSerializer;
    }

    public OHCacheBuilder<K, V> valueSerializer(CacheSerializer<V> valueSerializer)
    {
        this.valueSerializer = valueSerializer;
        return this;
    }

    public int getSegmentCount()
    {
        return segmentCount;
    }

    public OHCacheBuilder<K, V> segmentCount(int segmentCount)
    {
        this.segmentCount = segmentCount;
        return this;
    }

    public float getLoadFactor()
    {
        return loadFactor;
    }

    public OHCacheBuilder<K, V> loadFactor(float loadFactor)
    {
        this.loadFactor = loadFactor;
        return this;
    }

    public long getMaxEntrySize()
    {
        return maxEntrySize;
    }

    public OHCacheBuilder<K, V> maxEntrySize(long maxEntrySize)
    {
        this.maxEntrySize = maxEntrySize;
        return this;
    }

    public ScheduledExecutorService getExecutorService()
    {
        return executorService;
    }

    public OHCacheBuilder<K, V> executorService(ScheduledExecutorService executorService)
    {
        this.executorService = executorService;
        return this;
    }

    public HashAlgorithm getHashAlgorighm()
    {
        return hashAlgorighm;
    }

    public OHCacheBuilder<K, V> hashMode(HashAlgorithm hashMode)
    {
        this.hashAlgorighm = hashMode;
        return this;
    }

    public boolean isThrowOOME()
    {
        return throwOOME;
    }

    public OHCacheBuilder<K, V> throwOOME(boolean throwOOME)
    {
        this.throwOOME = throwOOME;
        return this;
    }

    public boolean isUnlocked()
    {
        return unlocked;
    }

    public OHCacheBuilder<K, V> unlocked(boolean unlocked)
    {
        this.unlocked = unlocked;
        return this;
    }

    public long getDefaultTTLmillis()
    {
        return defaultTTLmillis;
    }

    public OHCacheBuilder<K, V> defaultTTLmillis(long defaultTTLmillis)
    {
        this.defaultTTLmillis = defaultTTLmillis;
        return this;
    }

    public int getTimeoutsSlots()
    {
        return timeoutsSlots;
    }

    public OHCacheBuilder<K, V> timeoutsSlots(int timeoutsSlots)
    {
        this.timeoutsSlots = timeoutsSlots;
        return this;
    }

    public int getTimeoutsPrecision()
    {
        return timeoutsPrecision;
    }

    public OHCacheBuilder<K, V> timeoutsPrecision(int timeoutsPrecision)
    {
        this.timeoutsPrecision = timeoutsPrecision;
        return this;
    }
}
