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

import java.io.IOError;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class OHCacheImpl<K, V> implements OHCache<K, V>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(OHCacheImpl.class);

    static
    {
        try
        {
            Field f = AtomicLong.class.getDeclaredField("VM_SUPPORTS_LONG_CAS");
            f.setAccessible(true);
            if (!(Boolean) f.get(null))
                throw new IllegalStateException("Off Heap Cache implementation requires a JVM that supports CAS on long fields");
        }
        catch (Exception e)
        {
            throw new RuntimeException();
        }
    }

    public static final int MAX_HASH_TABLE_SIZE = 4194304;
    public static final int MIN_HASH_TABLE_SIZE = 32;
    public static final int MAX_BLOCK_SIZE = 262144;
    public static final int MIN_BLOCK_SIZE = 512;

    private final int blockSize;
    private final int hashTableSize;
    private final long capacity;
    private final double cleanUpTrigger;
    private final CacheSerializer<K> keySerializer;
    private final CacheSerializer<V> valueSerializer;

    // off-heap address offset
    private final long rootAddress;

    private final FreeBlocks freeBlocks;
    private final HashEntryAccess hashEntryAccess;
    private final HashPartitionAccess hashPartitionAccess;

    private volatile boolean closed;

    private final AtomicBoolean cleanupLock = new AtomicBoolean();
    private final ScheduledExecutorService executorService;

    OHCacheImpl(OHCacheBuilder<K, V> builder)
    {
        if (builder.getBlockSize() < MIN_BLOCK_SIZE)
            throw new IllegalArgumentException("Block size must not be less than " + MIN_BLOCK_SIZE + " is (" + builder.getBlockSize() + ')');
        if (builder.getBlockSize() > MAX_BLOCK_SIZE)
            throw new IllegalArgumentException("Block size must not be greater than " + MAX_BLOCK_SIZE + " is (" + builder.getBlockSize() + ')');
        int bs;
        for (bs = MIN_BLOCK_SIZE; bs < MAX_BLOCK_SIZE; bs <<= 1)
            if (bs >= builder.getBlockSize())
                break;
        if (bs != builder.getBlockSize())
            LOGGER.warn("Using block size {} instead of configured block size {} - adjust your configuration to be precise", bs, builder.getBlockSize());
        blockSize = bs;

        long minSize = 8 * 1024 * 1024; // very small

        long cap = builder.getCapacity();
        cap /= bs;
        cap *= bs;
        if (cap < minSize)
            throw new IllegalArgumentException("Total size must not be less than " + minSize + " is (" + builder.getCapacity() + ')');
        if (cap != builder.getCapacity())
            LOGGER.warn("Using capacity {} instead of configured capacity {} - adjust your configuration to be precise", cap, builder.getCapacity());
        capacity = cap;

        int hts = builder.getHashTableSize();
        if (hts > 0)
        {
            if (hts < MIN_HASH_TABLE_SIZE)
                throw new IllegalArgumentException("Block size must not be less than " + MIN_HASH_TABLE_SIZE + " is (" + hts + ')');
            if (hts > MAX_HASH_TABLE_SIZE)
                throw new IllegalArgumentException("Block size must not be greater than " + MAX_HASH_TABLE_SIZE + " is (" + hts + ')');
            for (hts = MIN_HASH_TABLE_SIZE; hts < MAX_HASH_TABLE_SIZE; hts <<= 1)
                if (hts >= builder.getHashTableSize())
                    break;
            if (hts != builder.getHashTableSize())
                LOGGER.warn("Using hash table size {} instead of configured hash table size {} - adjust your configuration to be precise", hts, builder.getHashTableSize());
        }
        else
        {
            // auto-size hash table
            int blockCount = (int) (cap / bs);
            hts = blockCount / 16;
        }
        hashTableSize = hts;

        int lruListWarnTrigger = builder.getLruListWarnTrigger();
        if (lruListWarnTrigger < 1)
            lruListWarnTrigger = 1;

        long hashTableMem = HashPartitionAccess.sizeForEntries(hashTableSize);

        this.rootAddress = Uns.allocate(capacity + hashTableMem);

        long blocksAddress = this.rootAddress + hashTableMem;

        this.freeBlocks = new FreeBlocks(blocksAddress, blocksAddress + capacity, blockSize);
        this.hashPartitionAccess = new HashPartitionAccess(hashTableSize, rootAddress);
        this.hashEntryAccess = new HashEntryAccess(blockSize, freeBlocks, hashPartitionAccess, lruListWarnTrigger);

        this.keySerializer = builder.getKeySerializer();
        this.valueSerializer = builder.getValueSerializer();

        double cut = builder.getCleanUpTrigger();
        if (cut < 0d || cut > 1d)
            throw new IllegalArgumentException("Invalid clean-up percentage trigger value " + String.format("%.2f", cut));
        this.cleanUpTrigger = cut;

        long cleanupCheckInterval = builder.getCleanupCheckInterval();

        if ((cut > 0d && cleanupCheckInterval <= 0L)
            || (cut <= 0d && cleanupCheckInterval > 0L))
            throw new IllegalArgumentException("Incompatible settings - cleanup-check-interval " +
                                               cleanupCheckInterval + " vs cleanup-trigger " + String.format("%.2f", cut));

        ScheduledExecutorService es = null;
        if (cleanupCheckInterval > 0)
        {
            es = Executors.newScheduledThreadPool(1);
            es.scheduleWithFixedDelay(new Runnable()
            {
                public void run()
                {
                    cleanUp();
                }
            }, cleanupCheckInterval, cleanupCheckInterval, TimeUnit.MILLISECONDS);
        }
        executorService = es;

        LOGGER.info("Initialized OHC with capacity={}, hash-table-size={}, block-size={}", cap, hts, bs);
    }

    public void close()
    {
        closed = true;

        executorService.shutdown();
        try
        {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }

        // TODO releasing memory immediately is dangerous since other threads may still access the data
        // Need to orderly clear the cache before releasing (this involves hash-partition and hash-entry locks,
        // which ensure that no other thread is accessing OHC)
        Uns.free(rootAddress);
    }

    private void assertNotClosed()
    {
        if (closed)
            throw new IllegalStateException("OHCache instance already closed");
    }

    public int getBlockSize()
    {
        assertNotClosed();
        return blockSize;
    }

    public long getCapacity()
    {
        assertNotClosed();
        return capacity;
    }

    public int getHashTableSize()
    {
        assertNotClosed();
        return hashTableSize;
    }

    public int calcFreeBlockCount()
    {
        assertNotClosed();
        return freeBlocks.calcFreeBlockCount();
    }

    public long getMemUsed()
    {
        return capacity - calcFreeBlockCount() * blockSize;
    }

    public PutResult put(int hash, BytesSource keySource, BytesSource valueSource)
    {
        return put(hash, keySource, valueSource, null);
    }

    public PutResult put(int hash, BytesSource keySource, BytesSource valueSource, BytesSink oldValueSink)
    {
        if (keySource == null)
            throw new NullPointerException();
        if (valueSource == null)
            throw new NullPointerException();
        if (keySource.size() <= 0)
            throw new ArrayIndexOutOfBoundsException();
        if (valueSource.size() < 0)
            throw new ArrayIndexOutOfBoundsException();

        // Allocate and fill new hash entry.
        // Do this outside of the hash-partition lock to hold that lock no longer than necessary.
        long newHashEntryAdr = hashEntryAccess.createNewEntryChain(hash, keySource, valueSource, -1L);
        if (newHashEntryAdr == 0L)
            return PutResult.NO_MORE_SPACE;

        return putInternal(hash, keySource, oldValueSink, newHashEntryAdr);
    }

    private PutResult putInternal(int hash, BytesSource keySource, BytesSink oldValueSink, long newHashEntryAdr)
    {
        long oldHashEntryAdr;

        // find + lock hash partition
        long partitionAdr = hashPartitionAccess.lockPartitionForHash(hash);
        try
        {
            // find existing entry
            oldHashEntryAdr = hashEntryAccess.findHashEntry(partitionAdr, hash, keySource);

            // remove existing entry
            if (oldHashEntryAdr != 0L)
                hashEntryAccess.removeFromPartitionLRU(partitionAdr, oldHashEntryAdr);

            // add new entry
            hashEntryAccess.addAsPartitionLRUHead(partitionAdr, newHashEntryAdr);

            // We have to lock the old entry before we can actually free the allocated blocks.
            // There's no need for a corresponding unlock because we use CAS on a field for locking.
            hashEntryAccess.lockEntry(oldHashEntryAdr);
        }
        finally
        {
            // release hash partition
            hashPartitionAccess.unlockPartition(partitionAdr);
        }

        // No old entry - just return.
        if (oldHashEntryAdr == 0L)
            return PutResult.ADD;

        try
        {
            // Write old value (if wanted).
            if (oldValueSink != null)
                hashEntryAccess.writeValueToSink(oldHashEntryAdr, oldValueSink);
        }
        finally
        {
            // release old value
            freeBlocks.freeChain(oldHashEntryAdr);
        }

        return PutResult.REPLACE;
    }

    public boolean get(int hash, BytesSource keySource, BytesSink valueSink)
    {
        if (keySource == null)
            throw new NullPointerException();
        if (valueSink == null)
            throw new NullPointerException();
        if (keySource.size() <= 0)
            throw new ArrayIndexOutOfBoundsException();

        long hashEntryAdr = getInternal(hash, keySource);

        if (hashEntryAdr == 0L)
            return false;

        // Write the value to the caller and unlock the entry.
        try
        {
            hashEntryAccess.writeValueToSink(hashEntryAdr, valueSink);
        }
        finally
        {
            hashEntryAccess.unlockEntry(hashEntryAdr);
        }

        return true;
    }

    private long getInternal(int hash, BytesSource keySource)
    {
        // find + lock hash partition
        long partitionAdr = hashPartitionAccess.lockPartitionForHash(hash);
        long hashEntryAdr;
        try
        {
            // find hash entry
            hashEntryAdr = hashEntryAccess.findHashEntry(partitionAdr, hash, keySource);
            if (hashEntryAdr != 0L)
            {
                hashEntryAccess.updatePartitionLRU(partitionAdr, hashEntryAdr);

                // to keep the hash-partition lock short, lock the entry here
                hashEntryAccess.lockEntry(hashEntryAdr);
            }
        }
        finally
        {
            // release hash partition
            hashPartitionAccess.unlockPartition(partitionAdr);
        }

        return hashEntryAdr;
    }

    public boolean remove(int hash, BytesSource keySource)
    {
        if (keySource == null)
            throw new NullPointerException();
        if (keySource.size() <= 0)
            throw new ArrayIndexOutOfBoundsException();

        // find + lock hash partition
        long hashEntryAdr;
        long partitionAdr = hashPartitionAccess.lockPartitionForHash(hash);
        try
        {
            // find hash entry
            hashEntryAdr = hashEntryAccess.findHashEntry(partitionAdr, hash, keySource);
            if (hashEntryAdr == 0L)
                return false;

            hashEntryAccess.removeFromPartitionLRU(partitionAdr, hashEntryAdr);

            // We have to lock the old entry before we can actually free the allocated blocks.
            // There's no need for a corresponding unlock because we use CAS on a field for locking.
            hashEntryAccess.lockEntry(hashEntryAdr);
        }
        finally
        {
            // release hash partition
            hashPartitionAccess.unlockPartition(partitionAdr);
        }

        // free chain
        freeBlocks.freeChain(hashEntryAdr);

        return true;
    }

    public long getFreeBlockSpins()
    {
        return freeBlocks.getFreeBlockSpins();
    }

    public long getLockPartitionSpins()
    {
        return hashPartitionAccess.getLockPartitionSpins();
    }

    public Iterator<K> hotN(int n)
    {
        throw new UnsupportedOperationException();
    }

    // com.google.common.cache.Cache implementation

    public void invalidate(Object o)
    {
        remove(o.hashCode(), keySource((K) o));
    }

    public V getIfPresent(Object o)
    {
        if (valueSerializer == null)
            throw new NullPointerException("no valueSerializer configured");

        int hash = o.hashCode();

        long hashEntryAdr = getInternal(hash, keySource((K) o));
        if (hashEntryAdr == 0L)
            return null;

        try
        {
            return valueSerializer.deserialize(hashEntryAccess.readFrom(hashEntryAdr));
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finally
        {
            hashEntryAccess.unlockEntry(hashEntryAdr);
        }
    }

    private BytesSource keySource(K o)
    {
        if (keySerializer == null)
            throw new NullPointerException("no keySerializer configured");
        long size = keySerializer.serializedSize(o);
        if (size < 0)
            throw new IllegalArgumentException();
        if (size >= Integer.MAX_VALUE)
            throw new IllegalArgumentException("serialized size of key too large (>2GB)");

        byte[] tmp = new byte[(int) size];
        try
        {
            keySerializer.serialize(o, new ByteArrayOut(tmp));
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return new BytesSource.ByteArraySource(tmp);
    }

    public void put(K k, V v)
    {
        if (valueSerializer == null)
            throw new NullPointerException("no valueSerializer configured");

        int hash = k.hashCode();

        BytesSource keySource = keySource(k);

        long valueLen = valueSerializer.serializedSize(v);

        // Allocate and fill new hash entry.
        // Do this outside of the hash-partition lock to hold that lock no longer than necessary.
        long newHashEntryAdr = hashEntryAccess.createNewEntryChain(hash, keySource, null, valueLen);
        if (newHashEntryAdr == 0L)
            return;

        try
        {
            hashEntryAccess.valueToHashEntry(newHashEntryAdr, valueSerializer, v);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        putInternal(hash, keySource, null, newHashEntryAdr);
    }

    public void cleanUp()
    {
        if (!cleanupLock.compareAndSet(false, true))
            return;
        try
        {
            int totalBlockCount = (int) (capacity / blockSize);
            int freeBlockCount = calcFreeBlockCount();
            double fsPerc = ((double) freeBlockCount) / totalBlockCount;

            if (fsPerc > cleanUpTrigger)
                return;

            long entries = size();

            double blocksPerEntry = totalBlockCount - freeBlockCount;
            blocksPerEntry /= entries;

            int expectedFreeBlockCount = (int) (cleanUpTrigger * totalBlockCount);
            int entriesToRemove = (int) ((expectedFreeBlockCount - freeBlockCount) * blocksPerEntry);
            int entriesToRemovePerPartition = (int) ((expectedFreeBlockCount - freeBlockCount) * blocksPerEntry / hashTableSize);
            if (entriesToRemovePerPartition < 1)
                entriesToRemovePerPartition = 1;
            LOGGER.info("OHC cleanup starts with free-space percentage {}, entries={}, blocks-per-entry={}, entries-to-remove={}",
                        String.format("%.2f", fsPerc),
                        entries,
                        String.format("%.2f", blocksPerEntry),
                        entriesToRemove
            );

            int blocksFreed = 0;
            int entriesRemoved = 0;

            for (int h = 0; h < hashTableSize; h++)
            {
                long startAt = 0L;

                long partAdr = hashPartitionAccess.lockPartitionForHash(h);
                try
                {
                    long lastHashEntryAdr = 0L;
                    long lruHead = hashPartitionAccess.getLRUHead(partAdr);
                    for (long hashEntryAdr = lruHead; ; hashEntryAdr = hashEntryAccess.getLRUNext(hashEntryAdr))
                    {
                        // at LRU tail
                        if (hashEntryAdr == 0L)
                            break;
                        lastHashEntryAdr = hashEntryAdr;
                    }

                    // hash partition is empty
                    if (lastHashEntryAdr == 0L)
                        continue;

                    long firstBefore = 0L;
                    int i = 0;
                    for (long hashEntryAdr = hashEntryAccess.getLRUPrevious(lastHashEntryAdr); i++ < entriesToRemovePerPartition; hashEntryAdr = hashEntryAccess.getLRUPrevious(hashEntryAdr))
                    {
                        // at LRU head
                        if (hashEntryAdr == 0L)
                            break;
                        firstBefore = hashEntryAdr;
                    }

                    // remove whole partition
                    if (firstBefore == 0L)
                    {
                        startAt = lruHead;
                        hashPartitionAccess.setLRUHead(partAdr, 0L);
                    }
                    else
                    {
                        startAt = hashEntryAccess.getLRUNext(firstBefore);
                        hashEntryAccess.setLRUNext(firstBefore, 0L);
                        hashEntryAccess.setLRUPrevious(startAt, 0L);
                    }

                    // first hash-entry-address to remove in 'startAt' and unlinked from LRU list - can unlock the partition
                }
                finally
                {
                    hashPartitionAccess.unlockPartition(partAdr);
                }

                // remove entries
                long next;
                for (long hashEntryAdr = startAt; hashEntryAdr != 0L; hashEntryAdr = next)
                {
                    next = hashEntryAccess.getLRUNext(hashEntryAdr);

                    entriesRemoved ++;

                    hashEntryAccess.lockEntry(hashEntryAdr);
                    try
                    {
                        blocksFreed += freeBlocks.freeChain(hashEntryAdr);
                    }
                    finally
                    {
                        hashEntryAccess.unlockEntry(hashEntryAdr);
                    }
                }
            }

            LOGGER.info("Cleanup statistics: removed entries={} blocks recycled={}", entriesRemoved, blocksFreed);

        }
        finally
        {
            cleanupLock.set(false);
        }
    }

    public double getFreeSpacePercentage()
    {
        long totalBlockCount = capacity / blockSize;
        int freeBlockCount = calcFreeBlockCount();
        return ((double) freeBlockCount) / totalBlockCount;
    }

    public ConcurrentMap<K, V> asMap()
    {
        throw new UnsupportedOperationException();
    }

    public CacheStats stats()
    {
        throw new UnsupportedOperationException();
    }

    public long size()
    {
        long sz = 0L;
        for (int p = 0; p < hashTableSize; p++)
        {

            long partitionAdr = hashPartitionAccess.lockPartitionForHash(p);
            try
            {
                for (long hashEntryAdr = hashPartitionAccess.getLRUHead(partitionAdr); hashEntryAdr != 0L; hashEntryAdr = hashEntryAccess.getLRUNext(hashEntryAdr))
                    sz++;
            }
            finally
            {
                hashPartitionAccess.unlockPartition(partitionAdr);
            }
        }
        return sz;
    }

    public void invalidateAll()
    {
        throw new UnsupportedOperationException();
    }

    public void invalidateAll(Iterable<?> iterable)
    {
        for (Object o : iterable)
            invalidate(o);
    }

    public void putAll(Map<? extends K, ? extends V> map)
    {
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    public ImmutableMap<K, V> getAllPresent(Iterable<?> iterable)
    {
        throw new UnsupportedOperationException();
    }

    public V get(K k, Callable<? extends V> callable) throws ExecutionException
    {
        V v = getIfPresent(k);
        if (v == null)
        {
            try
            {
                v = callable.call();
            }
            catch (Exception e)
            {
                throw new ExecutionException(e);
            }
            put(k, v);
        }
        return v;
    }

    static final class ByteArrayOut extends OutputStream
    {
        private final byte[] buffer;
        private int pos;

        public ByteArrayOut(byte[] buffer)
        {
            this.buffer = buffer;
        }

        public void write(int b) throws IOException
        {
            buffer[pos++] = (byte) b;
        }

        public void write(byte[] b, int off, int len) throws IOException
        {
            System.arraycopy(b, off, buffer, pos, len);
            pos += len;
        }
    }
}
