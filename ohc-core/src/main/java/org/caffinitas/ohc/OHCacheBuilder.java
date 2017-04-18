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

import java.util.concurrent.ScheduledExecutorService;

import org.caffinitas.ohc.chunked.OHCacheChunkedImpl;
import org.caffinitas.ohc.linked.OHCacheLinkedImpl;

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
 *         <td>{@code chunkSize}</td>
 *         <td>If set and positive, the <i>chunked</i> implementation will be used and each segment
 *         will be divided into this amount of chunks.</td>
 *         <td>{@code 0} - i.e. <i>linked</i> implementation will be used</td>
 *     </tr>
 *     <tr>
 *         <td>{@code fixedEntrySize}</td>
 *         <td>If set and positive, the <i>chunked</i> implementation with fixed sized entries
 *         will be used. The parameter {@code chunkSize} must be set for fixed-sized entries.</td>
 *         <td>{@code 0} - i.e. <i>linked</i> implementation will be used,
 *         if {@code chunkSize} is also {@code 0}</td>
 *     </tr>
 *     <tr>
 *         <td>{@code maxEntrySize}</td>
 *         <td>Maximum size of a hash entry (including header, serialized key + serialized value)</td>
 *         <td>(not set, defaults to capacity divided by number of segments)</td>
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
 *     <tr>
 *         <td>{@code ticker}</td>
 *         <td>Indirection for current time - used for unit tests.</td>
 *         <td>Default ticker using {@code System.nanoTime()} and {@code System.currentTimeMillis()}</td>
 *     </tr>
 *     <tr>
 *         <td>{@code capacity}</td>
 *         <td>Expected number of elements in the cache</td>
 *         <td>No default value, recommended to provide a default value.</td>
 *     </tr>
 *     <tr>
 *         <td>{@code eviction}</td>
 *         <td>Choose the eviction algorithm to use. Available are:
 *         <ul>
 *             <li>{@link Eviction#LRU LRU}: Plain LRU - least used entry is subject to eviction</li>
 *             <li>{@link Eviction#W_TINY_LFU W-WinyLFU}: Enable use of Window Tiny-LFU. The size of the
 *             frequency sketch ("admission filter") is set to the value of {@code hashTableSize}.
 *             See <a href="http://highscalability.com/blog/2016/1/25/design-of-a-modern-cache.html">this article</a>
 *             for a description.</li>
 *             <li>{@link Eviction#NONE None}: No entries will be evicted - this effectively provides a
 *             capacity-bounded off-heap map.</li>
 *         </ul>
 *         </td>
 *         <td>{@code LRU}</td>
 *     </tr>
 *     <tr>
 *         <td>{@code frequencySketchSize}</td>
 *         <td>Size of the frequency sketch used by {@link Eviction#W_TINY_LFU W-WinyLFU}</td>
 *         <td>Defaults to {@code hashTableSize}.</td>
 *     </tr>
 *     <tr>
 *         <td>{@code edenSize}</td>
 *         <td>Size of the eden generation used by {@link Eviction#W_TINY_LFU W-WinyLFU} relative to a segment's size</td>
 *         <td>{@code 0.2}</td>
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
    private long capacity;
    private int chunkSize;
    private CacheSerializer<K> keySerializer;
    private CacheSerializer<V> valueSerializer;
    private float loadFactor = .75f;
    private int fixedKeySize;
    private int fixedValueSize;
    private long maxEntrySize;
    private ScheduledExecutorService executorService;
    private boolean throwOOME;
    private HashAlgorithm hashAlgorighm = HashAlgorithm.MURMUR3;
    private boolean unlocked;
    private long defaultTTLmillis;
    private boolean timeouts;
    private int timeoutsSlots;
    private int timeoutsPrecision;
    private Ticker ticker = Ticker.DEFAULT;
    private Eviction eviction = Eviction.LRU;
    private int frequencySketchSize;
    private double edenSize = 0.2d;

    private OHCacheBuilder()
    {
        int cpus = Runtime.getRuntime().availableProcessors();

        segmentCount = roundUpToPowerOf2(cpus * 2, 1 << 30);
        capacity = Math.min(cpus * 16, 64) * 1024 * 1024;

        segmentCount = fromSystemProperties("segmentCount", segmentCount);
        hashTableSize = fromSystemProperties("hashTableSize", hashTableSize);
        capacity = fromSystemProperties("capacity", capacity);
        chunkSize = fromSystemProperties("chunkSize", chunkSize);
        loadFactor = fromSystemProperties("loadFactor", loadFactor);
        maxEntrySize = fromSystemProperties("maxEntrySize", maxEntrySize);
        throwOOME = fromSystemProperties("throwOOME", throwOOME);
        hashAlgorighm = HashAlgorithm.valueOf(fromSystemProperties("hashAlgorighm", hashAlgorighm.name()));
        unlocked = fromSystemProperties("unlocked", unlocked);
        defaultTTLmillis = fromSystemProperties("defaultTTLmillis", defaultTTLmillis);
        timeouts = fromSystemProperties("timeouts", timeouts);
        timeoutsSlots = fromSystemProperties("timeoutsSlots", timeoutsSlots);
        timeoutsPrecision = fromSystemProperties("timeoutsPrecision", timeoutsPrecision);
        eviction = fromSystemProperties("eviction", eviction, Eviction.class);
        frequencySketchSize = fromSystemProperties("frequencySketchSize", frequencySketchSize);
        edenSize = fromSystemProperties("edenSize", edenSize);
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

    private static double fromSystemProperties(String name, double defaultValue)
    {
        try
        {
            return Double.parseDouble(System.getProperty(SYSTEM_PROPERTY_PREFIX + name, Double.toString(defaultValue)));
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

    private static <E extends Enum> E fromSystemProperties(String name, E defaultValue, Class<E> type)
    {
        String value = fromSystemProperties(name, defaultValue.name());
        return (E) Enum.valueOf(type, value.toUpperCase());
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
        if (fixedKeySize > 0 || fixedValueSize > 0|| chunkSize > 0)
            return new OHCacheChunkedImpl<>(this);
        return new OHCacheLinkedImpl<>(this);
    }

    public int getHashTableSize()
    {
        return hashTableSize;
    }

    public OHCacheBuilder<K, V> hashTableSize(int hashTableSize)
    {
        if (hashTableSize < -1)
            throw new IllegalArgumentException("hashTableSize:" + hashTableSize);
        this.hashTableSize = hashTableSize;
        return this;
    }

    public int getChunkSize()
    {
        return chunkSize;
    }

    public OHCacheBuilder<K, V> chunkSize(int chunkSize)
    {
        if (chunkSize < -1)
            throw new IllegalArgumentException("chunkSize:" + chunkSize);
        this.chunkSize = chunkSize;
        return this;
    }

    public long getCapacity()
    {
        return capacity;
    }

    public OHCacheBuilder<K, V> capacity(long capacity)
    {
        if (capacity <= 0)
            throw new IllegalArgumentException("capacity:" + capacity);
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
        if (segmentCount < -1)
            throw new IllegalArgumentException("segmentCount:" + segmentCount);
        this.segmentCount = segmentCount;
        return this;
    }

    public float getLoadFactor()
    {
        return loadFactor;
    }

    public OHCacheBuilder<K, V> loadFactor(float loadFactor)
    {
        if (loadFactor <= 0f)
            throw new IllegalArgumentException("loadFactor:" + loadFactor);
        this.loadFactor = loadFactor;
        return this;
    }

    public long getMaxEntrySize()
    {
        return maxEntrySize;
    }

    public OHCacheBuilder<K, V> maxEntrySize(long maxEntrySize)
    {
        if (maxEntrySize < 0)
            throw new IllegalArgumentException("maxEntrySize:" + maxEntrySize);
        this.maxEntrySize = maxEntrySize;
        return this;
    }

    public int getFixedKeySize()
    {
        return fixedKeySize;
    }

    public int getFixedValueSize()
    {
        return fixedValueSize;
    }

    public OHCacheBuilder<K, V> fixedEntrySize(int fixedKeySize, int fixedValueSize)
    {
        if ((fixedKeySize > 0 || fixedValueSize > 0) &&
            (fixedKeySize <= 0 || fixedValueSize <= 0))
            throw new IllegalArgumentException("fixedKeySize:" + fixedKeySize+",fixedValueSize:" + fixedValueSize);
        this.fixedKeySize = fixedKeySize;
        this.fixedValueSize = fixedValueSize;
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
        if (hashMode == null)
            throw new NullPointerException("hashMode");
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

    public boolean isTimeouts()
    {
        return timeouts;
    }

    public OHCacheBuilder<K, V> timeouts(boolean timeouts)
    {
        this.timeouts = timeouts;
        return this;
    }

    public int getTimeoutsSlots()
    {
        return timeoutsSlots;
    }

    public OHCacheBuilder<K, V> timeoutsSlots(int timeoutsSlots)
    {
        if (timeoutsSlots > 0)
            this.timeouts = true;
        this.timeoutsSlots = timeoutsSlots;
        return this;
    }

    public int getTimeoutsPrecision()
    {
        return timeoutsPrecision;
    }

    public OHCacheBuilder<K, V> timeoutsPrecision(int timeoutsPrecision)
    {
        if (timeoutsPrecision > 0)
            this.timeouts = true;
        this.timeoutsPrecision = timeoutsPrecision;
        return this;
    }

    public Ticker getTicker()
    {
        return ticker;
    }

    public OHCacheBuilder<K, V> ticker(Ticker ticker)
    {
        this.ticker = ticker;
        return this;
    }

    public Eviction getEviction()
    {
        return eviction;
    }

    public OHCacheBuilder<K, V> eviction(Eviction eviction)
    {
        this.eviction = eviction;
        return this;
    }

    public int getFrequencySketchSize()
    {
        return frequencySketchSize;
    }

    public OHCacheBuilder<K, V> frequencySketchSize(int frequencySketchSize)
    {
        this.frequencySketchSize = frequencySketchSize;
        return this;
    }

    public double getEdenSize()
    {
        return edenSize;
    }

    public OHCacheBuilder<K, V> edenSize(double edenSize)
    {
        this.edenSize = edenSize;
        return this;
    }
}
