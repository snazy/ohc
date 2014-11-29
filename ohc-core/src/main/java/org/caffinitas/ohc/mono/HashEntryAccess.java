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
package org.caffinitas.ohc.mono;

import java.io.DataInput;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.caffinitas.ohc.api.BytesSink;
import org.caffinitas.ohc.api.BytesSource;
import org.caffinitas.ohc.api.CacheSerializer;
import org.caffinitas.ohc.internal.Util;

/**
 * Encapsulates access to hash entries.
 */
final class HashEntryAccess implements Constants
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HashEntryAccess.class);

    private final DataMemory dataMemory;
    private final HashPartitions hashPartitions;

    private final int lruListLenTrigger;
    private final Signals signals;

    HashEntryAccess(DataMemory dataMemory, HashPartitions hashPartitions, int lruListLenTrigger, Signals signals)
    {
        this.dataMemory = dataMemory;
        this.hashPartitions = hashPartitions;
        this.lruListLenTrigger = lruListLenTrigger;
        this.signals = signals;
    }

    long createNewEntry(long hash, BytesSource keySource, BytesSource valueSource, long valueLen)
    {
        long keyLen = keySource.size();
        if (valueSource != null)
            valueLen = valueSource.size();
        if (keyLen < 0 || valueLen < 0)
            throw new IllegalArgumentException();

        // allocate memory for whole hash-entry block-chain
        long valueOff = Util.roundUpTo8(keyLen);
        long hashEntryAdr = dataMemory.allocate(valueOff + valueLen);
        if (hashEntryAdr == 0L)
            return 0L;

        // initialize hash entry fields
        initHashEntry(hash, keyLen, valueLen, hashEntryAdr);

        // serialize key
        sourceToOffHeap(keySource, hashEntryAdr, ENTRY_OFF_DATA);

        if (valueSource != null)
            // serialize value
            sourceToOffHeap(valueSource, hashEntryAdr, ENTRY_OFF_DATA + valueOff);

        return hashEntryAdr;
    }

    private static void sourceToOffHeap(BytesSource source, long hashEntryAdr, long blkOff)
    {
        long len = source.size();
        if (source.hasArray())
        {
            // keySource provides array access (can use Unsafe.copyMemory)
            byte[] arr = source.array();
            int arrOff = source.arrayOffset();
            Uns.copyMemory(arr, arrOff, hashEntryAdr + blkOff, len);
        }
        else
        {
            // keySource has no array
            for (int p = 0; p < len; )
            {
                byte b = source.getByte(p++);
                Uns.putByte(hashEntryAdr + blkOff++, b);
            }
        }
    }

    private static void initHashEntry(long hash, long keyLen, long valueLen, long hashEntryAdr)
    {
        touchEntry(hashEntryAdr);
        // (long that contains the length of the malloc'd block set in DataMemory.allocate)
        Uns.putLongVolatile(hashEntryAdr + ENTRY_OFF_HASH, hash);
        setPreviousEntry(hashEntryAdr, 0L);
        setNextEntry(hashEntryAdr, 0L);
        Uns.putLongVolatile(hashEntryAdr + ENTRY_OFF_KEY_LENGTH, keyLen);
        Uns.putLongVolatile(hashEntryAdr + ENTRY_OFF_VALUE_LENGTH, valueLen);
        Uns.initStamped(hashEntryAdr + ENTRY_OFF_LOCK, false);
    }

    long findHashEntry(long hash, BytesSource keySource)
    {
        long firstHashEntryAdr = hashPartitions.getPartitionHead(hash);
        boolean first = true;
        int loops = 0;
        for (long hashEntryAdr = firstHashEntryAdr; hashEntryAdr != 0L; hashEntryAdr = getNextEntry(hashEntryAdr), loops++)
        {
            if (!first && firstHashEntryAdr == hashEntryAdr)
                throw new InternalError("endless loop for hash " + hash);
            first = false;

            long hashEntryHash = getEntryHash(hashEntryAdr);
            if (hashEntryHash != hash)
                continue;

            long serKeyLen = getHashKeyLen(hashEntryAdr);
            if (serKeyLen != keySource.size())
                continue;

            if (!compareKey(hashEntryAdr, keySource, serKeyLen))
                continue;

            if (loops >= lruListLenTrigger)
                entryListLongWarn(loops);

            return hashEntryAdr;
        }

        if (loops >= lruListLenTrigger)
            entryListLongWarn(loops);

        return 0L;
    }

    private void entryListLongWarn(int loops)
    {
        if (signals.triggerRehash())
            LOGGER.warn("Degraded OHC performance! Partition linked list very long - rehash triggered " +
                        "({} links required to find hash entry)",
                        loops);
    }

    private static boolean compareKey(long hashEntryAdr, BytesSource keySource, long serKeyLen)
    {
        if (hashEntryAdr == 0L)
            return false;

        long blkOff = ENTRY_OFF_DATA;
        int p = 0;

        // array optimized version
        if (keySource.hasArray())
        {
            byte[] arr = keySource.array();
            int arrOff = keySource.arrayOffset();
            for (; p <= serKeyLen - 8; p += 8, blkOff += 8)
            {
                long lSer = Uns.getLong(hashEntryAdr + blkOff);
                long lKey = Uns.getLongFromByteArray(arr, arrOff + p);
                if (lSer != lKey)
                    return false;
            }
        }

        // last-resort byte-by-byte compare
        for (; p < serKeyLen; p++)
        {
            byte bSer = Uns.getByte(hashEntryAdr + blkOff++);
            byte bKey = keySource.getByte(p);

            if (bSer != bKey)
                return false;
        }

        return true;
    }

    static void writeValueToSink(long hashEntryAdr, BytesSink valueSink)
    {
        if (hashEntryAdr == 0L)
            return;

        long serKeyLen = getHashKeyLen(hashEntryAdr);
        if (serKeyLen < 0L)
            throw new InternalError();
        long valueLen = Uns.getLongVolatile(hashEntryAdr + ENTRY_OFF_VALUE_LENGTH);
        if (valueLen < 0L)
            throw new InternalError();
        long blkOff;

        serKeyLen = Util.roundUpTo8(serKeyLen);

        // skip key
        blkOff = ENTRY_OFF_DATA + serKeyLen;

        if (valueLen > Integer.MAX_VALUE)
            throw new IllegalStateException("integer overflow");
        valueSink.setSize((int) valueLen);

        if (valueSink.hasArray())
        {
            // valueSink provides array access (can use Unsafe.copyMemory)
            byte[] arr = valueSink.array();
            int arrOff = valueSink.arrayOffset();
            Uns.copyMemory(hashEntryAdr + blkOff, arr, arrOff, valueLen);
        }
        else
        {
            // last-resort byte-by-byte copy
            for (int p = 0; p < valueLen; p++)
            {
                byte b = Uns.getByte(hashEntryAdr + blkOff++);
                valueSink.putByte(p, b);
            }
        }
    }

    void addEntryToPartition(long hash, long hashEntryAdr)
    {
        if (hashEntryAdr == 0L)
            return;

        long partHead = hashPartitions.getPartitionHead(hash);

        if (partHead != 0L)
        {
            setNextEntry(hashEntryAdr, partHead);
            setPreviousEntry(partHead, hashEntryAdr);
        }
        else
            setNextEntry(hashEntryAdr, 0L);
        setPreviousEntry(hashEntryAdr, 0L);

        hashPartitions.setPartitionHead(hash, hashEntryAdr);
    }

    void unlinkFromPartition(long hash, long hashEntryAdr)
    {
        if (hashEntryAdr == 0L)
            return;

        long partHead = hashPartitions.getPartitionHead(hash);

        long entryPrev = getPreviousEntry(hashEntryAdr);
        long entryNext = getNextEntry(hashEntryAdr);

        if (entryNext != 0L)
            setPreviousEntry(entryNext, entryPrev);

        if (entryPrev != 0L)
            setNextEntry(entryPrev, entryNext);

        if (partHead == hashEntryAdr)
            hashPartitions.setPartitionHead(hash, entryNext);
    }

    long unlinkFromPartition(long partHead, long hash, long hashEntryAdr)
    {
        if (partHead == 0L)
            throw new NullPointerException();
        if (hashEntryAdr == 0L)
            return 0L;

        long entryPrev = getPreviousEntry(hashEntryAdr);
        long entryNext = getNextEntry(hashEntryAdr);

        if (entryNext != 0L)
            setPreviousEntry(entryNext, entryPrev);

        if (entryPrev != 0L)
            setNextEntry(entryPrev, entryNext);

        if (partHead == hashEntryAdr)
        {
            hashPartitions.setPartitionHead(hash, entryNext);
            return entryNext;
        }

        return partHead;
    }

    static long lockEntryRead(long hashEntryAdr)
    {
        if (hashEntryAdr == 0L)
            return 0L;

        long stamp = Uns.lockStampedRead(hashEntryAdr + ENTRY_OFF_LOCK);
        if (stamp == Uns.INVALID_LOCK)
            throw new IllegalStateException();
        return stamp;
    }

    static long lockEntryWrite(long hashEntryAdr)
    {
        if (hashEntryAdr == 0L)
            return 0L;

        long stamp = Uns.lockStampedWrite(hashEntryAdr + ENTRY_OFF_LOCK);
        if (stamp == Uns.INVALID_LOCK)
            throw new IllegalStateException();
        return stamp;
    }

    static void unlockEntryRead(long hashEntryAdr, long stamp)
    {
        if (hashEntryAdr != 0L)
            Uns.unlockStampedRead(hashEntryAdr + ENTRY_OFF_LOCK, stamp);
    }

    static long getEntryTimestamp(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLong(hashEntryAdr + ENTRY_OFF_TIMESTAMP) : 0L;
    }

    static void touchEntry(long hashEntryAdr)
    {
        if (hashEntryAdr != 0L)
            Uns.putLong(hashEntryAdr + ENTRY_OFF_TIMESTAMP, System.currentTimeMillis());
    }

    static long getEntryHash(long hashEntryAdr)
    {
        return Uns.getLongVolatile(hashEntryAdr + ENTRY_OFF_HASH);
    }

    static long getNextEntry(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLongVolatile(hashEntryAdr + ENTRY_OFF_NEXT) : 0L;
    }

    static void setNextEntry(long hashEntryAdr, long nextAdr)
    {
        if (hashEntryAdr == nextAdr)
            throw new IllegalArgumentException();
        if (hashEntryAdr != 0L)
            Uns.putLongVolatile(hashEntryAdr + ENTRY_OFF_NEXT, nextAdr);
    }

    static long getPreviousEntry(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLongVolatile(hashEntryAdr + ENTRY_OFF_PREVIOUS) : 0L;
    }

    static void setPreviousEntry(long hashEntryAdr, long previousAdr)
    {
        if (hashEntryAdr == previousAdr)
            throw new IllegalArgumentException();
        if (hashEntryAdr != 0L)
            Uns.putLongVolatile(hashEntryAdr + ENTRY_OFF_PREVIOUS, previousAdr);
    }

    static long getHashKeyLen(long hashEntryAdr)
    {
        return Uns.getLongVolatile(hashEntryAdr + ENTRY_OFF_KEY_LENGTH);
    }

    static long getValueLen(long hashEntryAdr)
    {
        return Uns.getLongVolatile(hashEntryAdr + ENTRY_OFF_VALUE_LENGTH);
    }

    static DataInput readKeyFrom(long hashEntryAdr)
    {
        return newHashEntryInput(hashEntryAdr, false);
    }

    static DataInput readValueFrom(long hashEntryAdr)
    {
        return newHashEntryInput(hashEntryAdr, true);
    }

    static <V> void valueToHashEntry(long hashEntryAdr, CacheSerializer<V> valueSerializer, V v, long keyLen, long valueLen) throws IOException
    {
        HashEntryOutput entryOutput = new HashEntryOutput(hashEntryAdr, keyLen, valueLen);
        valueSerializer.serialize(v, entryOutput);
    }

    private static HashEntryInput newHashEntryInput(long hashEntryAdr, boolean value)
    {
        return new HashEntryInput(hashEntryAdr, value, getHashKeyLen(hashEntryAdr), getValueLen(hashEntryAdr));
    }
}
