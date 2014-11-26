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

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    long createNewEntryChain(int hash, BytesSource keySource, BytesSource valueSource, long valueLen)
    {
        long keyLen = keySource.size();
        if (valueSource != null)
            valueLen = valueSource.size();
        if (valueLen < 0)
            throw new IllegalArgumentException();

        long total = roundUp(keyLen) + valueLen;

        // allocate memory for whole hash-entry block-chain
        long hashEntryAdr = dataMemory.allocate(total);
        if (hashEntryAdr == 0L)
            return 0L;

        // initialize hash entry fields
        initHashEntry(hash, keyLen, valueLen, hashEntryAdr);

        // write key + value to data
        long blkOff = ENTRY_OFF_DATA;

        // write key to data
        long len = keySource.size();
        if (keySource.hasArray())
        {
            // keySource provides array access (can use Unsafe.copyMemory)
            byte[] arr = keySource.array();
            int arrOff = keySource.arrayOffset();
            Uns.copyMemory(arr, arrOff, hashEntryAdr + blkOff, len);
        }
        else
        {
            // keySource has no array
            for (int p = 0; p < len; p++)
            {
                byte b = keySource.getByte(p);
                Uns.putByte(hashEntryAdr + blkOff++, b);
            }
        }

        // round up to next 8 byte boundary (8 byte copy operations on 8 byte boundaries are faster)
        blkOff = roundUp(blkOff);

        if (valueSource != null)
        {
            // write value to data
            //
            // Although the following code is similar to the one used for the key above, it would require us to
            // allocate objects to be able to use the same code for both - so...
            //
            len = valueSource.size();
            if (valueSource.hasArray())
            {
                // valueSource provides array access (can use Unsafe.copyMemory)
                byte[] arr = valueSource.array();
                int arrOff = valueSource.arrayOffset();
                Uns.copyMemory(arr, arrOff, hashEntryAdr + blkOff, len);
            }
            else
            {
                // valueSource has no array
                for (int p = 0; p < len; p++)
                {
                    byte b = valueSource.getByte(p);
                    Uns.putByte(hashEntryAdr + blkOff++, b);
                }
            }
        }

        return hashEntryAdr;
    }

    void initHashEntry(int hash, long keyLen, long valueLen, long hashEntryAdr)
    {
        touchEntry(hashEntryAdr);
        Uns.putLongVolatile(hashEntryAdr + ENTRY_OFF_HASH, hash);
        setPreviousEntry(hashEntryAdr, 0L);
        setNextEntry(hashEntryAdr, 0L);
        Uns.initStamped(hashEntryAdr + ENTRY_OFF_LOCK);
        Uns.putLongVolatile(hashEntryAdr + ENTRY_OFF_KEY_LENGTH, keyLen);
        Uns.putLongVolatile(hashEntryAdr + ENTRY_OFF_VALUE_LENGTH, valueLen);
    }

    long findHashEntry(int hash, BytesSource keySource)
    {
        long firstHashEntryAdr = hashPartitions.getPartitionHead(hash);
        boolean first = true;
        int loops = 0;
        for (long hashEntryAdr = firstHashEntryAdr; hashEntryAdr != 0L; hashEntryAdr = getNextEntry(hashEntryAdr), loops++)
        {
            if (!first && firstHashEntryAdr == hashEntryAdr)
                throw new InternalError("endless loop for hash " + hash);
            first = false;

            int hashEntryHash = getEntryHash(hashEntryAdr);
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
            LOGGER.warn("Degraded OHC performance! Partition linked list very long - check OHC hash table size " +
                        "({} links required to find hash entry) " +
                        "(this message will reappear in 10 seconds if the problem persists)",
                        loops);
    }

    private boolean compareKey(long hashEntryAdr, BytesSource keySource, long serKeyLen)
    {
        if (hashEntryAdr == 0L)
            return false;

        long blkOff = ENTRY_OFF_DATA;

        // array optimized version
        if (keySource.hasArray())
        {
            byte[] arr = keySource.array();
            int arrOff = keySource.arrayOffset();
            for (int p = 0; p < serKeyLen; )
            {
                if (serKeyLen - p >= 8)
                {
                    long lSer = Uns.getLong(hashEntryAdr + blkOff);
                    long lKey = Uns.getLongFromByteArray(arr, arrOff + p);
                    if (lSer != lKey)
                        return false;
                    blkOff += 8;
                    p += 8;
                }
                else
                {
                    byte bSer = Uns.getByte(hashEntryAdr + blkOff++);
                    byte bKey = keySource.getByte(p++);

                    if (bSer != bKey)
                        return false;
                }
            }

            return true;
        }

        // last-resort byte-by-byte compare
        for (int p = 0; p < serKeyLen; p++)
        {
            byte bSer = Uns.getByte(hashEntryAdr + blkOff++);
            byte bKey = keySource.getByte(p);

            if (bSer != bKey)
                return false;
        }

        return true;
    }

    void writeValueToSink(long hashEntryAdr, BytesSink valueSink)
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

        serKeyLen = roundUp(serKeyLen);

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

    static long roundUp(long val)
    {
        long rem = val & 7;
        if (rem != 0)
            val += 8L - rem;
        return val;
    }

    void addAsPartitionHead(int hash, long hashEntryAdr)
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

    void removeFromPartitionLinkedList(int hash, long hashEntryAdr)
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

        // we can leave the LRU next+previous pointers in hashEntryAdr alone
    }

    long lockEntryRead(long hashEntryAdr)
    {
        if (hashEntryAdr == 0L)
            return 0L;
        return Uns.lockStampedRead(hashEntryAdr + ENTRY_OFF_LOCK);
    }

    long lockEntryWrite(long hashEntryAdr)
    {
        if (hashEntryAdr == 0L)
            return 0L;
        return Uns.lockStampedWrite(hashEntryAdr + ENTRY_OFF_LOCK);
    }

    void unlockEntryRead(long hashEntryAdr, long stamp)
    {
        if (hashEntryAdr != 0L)
            Uns.unlockStampedRead(hashEntryAdr + ENTRY_OFF_LOCK, stamp);
    }

    void unlockEntryWrite(long hashEntryAdr, long stamp)
    {
        if (hashEntryAdr != 0L)
            Uns.unlockStampedWrite(hashEntryAdr + ENTRY_OFF_LOCK, stamp);
    }

    long getEntryTimestamp(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLong(hashEntryAdr + ENTRY_OFF_TIMESTAMP) : 0L;
    }

    void touchEntry(long hashEntryAdr)
    {
        if (hashEntryAdr != 0L)
            Uns.putLong(hashEntryAdr + ENTRY_OFF_TIMESTAMP, System.currentTimeMillis());
    }

    int getEntryHash(long hashEntryAdr)
    {
        return (int) Uns.getLongVolatile(hashEntryAdr + ENTRY_OFF_HASH);
    }

    long getNextEntry(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLongVolatile(hashEntryAdr + ENTRY_OFF_NEXT) : 0L;
    }

    void setNextEntry(long hashEntryAdr, long nextAdr)
    {
        if (hashEntryAdr == nextAdr)
            throw new IllegalArgumentException();
        if (hashEntryAdr != 0L)
            Uns.putLongVolatile(hashEntryAdr + ENTRY_OFF_NEXT, nextAdr);
    }

    long getPreviousEntry(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLongVolatile(hashEntryAdr + ENTRY_OFF_PREVIOUS) : 0L;
    }

    void setPreviousEntry(long hashEntryAdr, long previousAdr)
    {
        if (hashEntryAdr == previousAdr)
            throw new IllegalArgumentException();
        if (hashEntryAdr != 0L)
            Uns.putLongVolatile(hashEntryAdr + ENTRY_OFF_PREVIOUS, previousAdr);
    }

    long getHashKeyLen(long hashEntryAdr)
    {
        return Uns.getLongVolatile(hashEntryAdr + ENTRY_OFF_KEY_LENGTH);
    }

    long getValueLen(long hashEntryAdr)
    {
        return Uns.getLongVolatile(hashEntryAdr + ENTRY_OFF_VALUE_LENGTH);
    }

    DataInput readKeyFrom(long hashEntryAdr)
    {
        return new HashEntryInput(hashEntryAdr, false);
    }

    DataInput readValueFrom(long hashEntryAdr)
    {
        return new HashEntryInput(hashEntryAdr, true);
    }

    <V> void valueToHashEntry(long hashEntryAdr, CacheSerializer<V> valueSerializer, V v, long keyLen, long valueLen) throws IOException
    {
        HashEntryOutput entryOutput = new HashEntryOutput(hashEntryAdr, keyLen, valueLen);
        valueSerializer.serialize(v, entryOutput);
    }

    void hotN(int hash, HashEntryCallback heCb, int numKeys)
    {
        long lock = hashPartitions.lockPartition(hash, false);
        try
        {
            for (long hashEntryAdr = hashPartitions.getPartitionHead(hash); numKeys-- > 0 && hashEntryAdr != 0L; hashEntryAdr = getNextEntry(hashEntryAdr))
                heCb.hashEntry(hashEntryAdr);
        }
        finally
        {
            hashPartitions.unlockPartition(lock, hash, false);
        }
    }

    final class HashEntryOutput extends AbstractDataOutput
    {
        private long blkAdr;
        private long blkOff;
        private final long blkEnd;

        HashEntryOutput(long hashEntryAdr, long keyLen, long valueLen)
        {
            if (hashEntryAdr == 0L)
                throw new NullPointerException();

            if (valueLen > Integer.MAX_VALUE)
                throw new IllegalStateException("integer overflow");

            this.blkAdr = hashEntryAdr;
            this.blkOff = ENTRY_OFF_DATA + roundUp(keyLen);
            this.blkEnd = this.blkOff + valueLen;
        }

        public void write(byte[] b, int off, int len) throws IOException
        {
            if (b == null)
                throw new NullPointerException();
            if (off < 0 || off + len > b.length || len < 0)
                throw new ArrayIndexOutOfBoundsException();

            if (len > avail())
                len = (int) avail();
            if (len <= 0)
                throw new EOFException();

            Uns.copyMemory(b, off, blkAdr + blkOff, len);
            blkOff += len;
        }

        private void assertAvail(int req) throws IOException
        {
            if (avail() < req)
                throw new EOFException();
        }

        private long avail()
        {
            return blkEnd-blkOff;
        }

        public void write(int b) throws IOException
        {
            assertAvail(1);

            Uns.putByte(blkAdr + blkOff++, (byte) b);
        }

        public void writeShort(int v) throws IOException
        {
            assertAvail(2);

            Uns.putShort(blkAdr + blkOff, (short) v);
            blkOff += 2;
        }

        public void writeChar(int v) throws IOException
        {
            assertAvail(2);

            Uns.putChar(blkAdr + blkOff, (char) v);
            blkOff += 2;
        }

        public void writeInt(int v) throws IOException
        {
            assertAvail(4);

            Uns.putInt(blkAdr + blkOff, v);
            blkOff += 4;
        }

        public void writeLong(long v) throws IOException
        {
            assertAvail(8);

            Uns.putLong(blkAdr + blkOff, v);
            blkOff += 8;
        }

        public void writeFloat(float v) throws IOException
        {
            assertAvail(4);

            Uns.putFloat(blkAdr + blkOff, v);
            blkOff += 4;
        }

        public void writeDouble(double v) throws IOException
        {
            assertAvail(8);

            Uns.putDouble(blkAdr + blkOff, v);
            blkOff += 8;
        }
    }

    final class HashEntryInput extends AbstractDataInput
    {
        private long blkAdr;
        private long blkOff;
        private final long blkEnd;

        HashEntryInput(long hashEntryAdr, boolean value)
        {
            if (hashEntryAdr == 0L)
                throw new NullPointerException();

            long serKeyLen = getHashKeyLen(hashEntryAdr);
            if (serKeyLen < 0L)
                throw new InternalError();
            long valueLen = getValueLen(hashEntryAdr);
            if (valueLen < 0L)
                throw new InternalError();
            long blkOff = ENTRY_OFF_DATA;

            if (value)
            {
                serKeyLen = roundUp(serKeyLen);

                // skip key
                blkOff += serKeyLen;
            }

            if (valueLen > Integer.MAX_VALUE)
                throw new IllegalStateException("integer overflow");

            this.blkAdr = hashEntryAdr;
            this.blkOff = blkOff;
            this.blkEnd = blkOff + (value ? valueLen : serKeyLen);
        }

        private long avail()
        {
            return blkEnd-blkOff;
        }

        private void assertAvail(int req) throws IOException
        {
            if (avail() < req)
                throw new EOFException();
        }

        public int available()
        {
            long av = avail();
            return av > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)av;
        }

        public void readFully(byte[] b, int off, int len) throws IOException
        {
            if (b == null)
                throw new NullPointerException();
            if (off < 0 || off + len > b.length || len < 0)
                throw new ArrayIndexOutOfBoundsException();

            assertAvail(len);

            Uns.copyMemory(blkAdr + blkOff, b, off, len);
        }

        public byte readByte() throws IOException
        {
            assertAvail(1);

            return Uns.getByte(blkAdr + blkOff++);
        }

        public int readUnsignedShort() throws IOException
        {
            assertAvail(2);

            int r = Uns.getShort(blkAdr + blkOff) & 0xffff;
            blkOff += 2;
            return r;
        }

        public short readShort() throws IOException
        {
            assertAvail(2);

            short r = Uns.getShort(blkAdr + blkOff);
            blkOff += 2;
            return r;
        }

        public char readChar() throws IOException
        {
            assertAvail(2);

            char r = Uns.getChar(blkAdr + blkOff);
            blkOff += 2;
            return r;
        }

        public int readInt() throws IOException
        {
            assertAvail(4);

            int r = Uns.getInt(blkAdr + blkOff);
            blkOff += 4;
            return r;
        }

        public long readLong() throws IOException
        {
            assertAvail(8);

            long r = Uns.getLong(blkAdr + blkOff);
            blkOff += 8;
            return r;
        }

        public float readFloat() throws IOException
        {
            assertAvail(4);

            float r = Uns.getFloat(blkAdr + blkOff);
            blkOff += 4;
            return r;
        }

        public double readDouble() throws IOException
        {
            assertAvail(8);

            double r = Uns.getDouble(blkAdr + blkOff);
            blkOff += 8;
            return r;
        }
    }

    static abstract class HashEntryCallback
    {
        abstract void hashEntry(long hashEntryAdr);
    }
}
