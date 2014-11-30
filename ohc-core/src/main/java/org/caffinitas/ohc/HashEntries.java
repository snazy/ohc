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
import static org.caffinitas.ohc.Constants.*;

/**
 * Encapsulates access to hash entries.
 */
public final class HashEntries
{
    static void toOffHeap(BytesSource source, long hashEntryAdr, long blkOff)
    {
        long len = source.size();
        if (source.hasArray())
        {
            // keySource provides array access (can use Unsafe.copyMemory)
            byte[] arr = source.array();
            int arrOff = source.arrayOffset();
            Uns.copyMemory(arr, arrOff, hashEntryAdr, blkOff, len);
        }
        else
        {
            // keySource has no array
            for (int p = 0; p < len; )
            {
                byte b = source.getByte(p++);
                Uns.putByte(hashEntryAdr, blkOff++, b);
            }
        }
    }

    static void init(long hash, long keyLen, long valueLen, long hashEntryAdr)
    {
        Uns.putLongVolatile(hashEntryAdr, ENTRY_OFF_HASH, hash);
        setNext(hashEntryAdr, 0L);
        setPrevious(hashEntryAdr, 0L);
        Uns.putLongVolatile(hashEntryAdr, ENTRY_OFF_KEY_LENGTH, keyLen);
        Uns.putLongVolatile(hashEntryAdr, ENTRY_OFF_VALUE_LENGTH, valueLen);
        Uns.putLongVolatile(hashEntryAdr, ENTRY_OFF_REFCOUNT, 1L);
    }

    static void valueToSink(long hashEntryAdr, BytesSink valueSink)
    {
        if (hashEntryAdr == 0L)
            return;

        // skip key
        long blkOff = ENTRY_OFF_DATA + roundUpTo8(getKeyLen(hashEntryAdr));

        long valueLen = getValueLen(hashEntryAdr);

        valueSink.setSize(valueLen);

        if (valueSink.hasArray())
        {
            // valueSink provides array access (can use Unsafe.copyMemory)
            byte[] arr = valueSink.array();
            int arrOff = valueSink.arrayOffset();
            Uns.copyMemory(hashEntryAdr, blkOff, arr, arrOff, valueLen);
        }
        else
        {
            // last-resort byte-by-byte copy
            for (int p = 0; p < valueLen; p++)
            {
                byte b = Uns.getByte(hashEntryAdr, blkOff++);
                valueSink.putByte(p, b);
            }
        }
    }

    static boolean compareKey(long hashEntryAdr, BytesSource keySource, long serKeyLen)
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
                if (Uns.getLong(hashEntryAdr, blkOff) != Uns.getLongFromByteArray(arr, arrOff + p))
                    return false;
        }

        // last-resort byte-by-byte compare
        for (; p < serKeyLen; p++)
            if (Uns.getByte(hashEntryAdr, blkOff++) != keySource.getByte(p))
                return false;

        return true;
    }

    public static long getLRUNext(long hashEntryAdr)
    {
        return Uns.getLongVolatile(hashEntryAdr, ENTRY_OFF_LRU_NEXT);
    }

    public static void setLRUNext(long hashEntryAdr, long replacement)
    {
        Uns.putLongVolatile(hashEntryAdr, ENTRY_OFF_LRU_NEXT, replacement);
    }

    public static long getLRUPrev(long hashEntryAdr)
    {
        return Uns.getLongVolatile(hashEntryAdr, ENTRY_OFF_LRU_PREV);
    }

    public static void setLRUPrev(long hashEntryAdr, long replacement)
    {
        Uns.putLongVolatile(hashEntryAdr, ENTRY_OFF_LRU_PREV, replacement);
    }

    static long getHash(long hashEntryAdr)
    {
        return Uns.getLongVolatile(hashEntryAdr, ENTRY_OFF_HASH);
    }

    static long getNext(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLongVolatile(hashEntryAdr, ENTRY_OFF_NEXT) : 0L;
    }

    static void setNext(long hashEntryAdr, long nextAdr)
    {
        if (hashEntryAdr == nextAdr)
            throw new IllegalArgumentException();
        if (hashEntryAdr != 0L)
            Uns.putLongVolatile(hashEntryAdr, ENTRY_OFF_NEXT, nextAdr);
    }

    static long getPrevious(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLongVolatile(hashEntryAdr, ENTRY_OFF_PREVIOUS) : 0L;
    }

    static void setPrevious(long hashEntryAdr, long prevAdr)
    {
        if (hashEntryAdr == prevAdr)
            throw new IllegalArgumentException();
        if (hashEntryAdr != 0L)
            Uns.putLongVolatile(hashEntryAdr, ENTRY_OFF_PREVIOUS, prevAdr);
    }

    static long getKeyLen(long hashEntryAdr)
    {
        return Uns.getLongVolatile(hashEntryAdr, ENTRY_OFF_KEY_LENGTH);
    }

    static long getValueLen(long hashEntryAdr)
    {
        return Uns.getLongVolatile(hashEntryAdr, ENTRY_OFF_VALUE_LENGTH);
    }

    static long getAllocLen(long address)
    {
        return Uns.getLongVolatile(address, ENTRY_OFF_ALLOC_LEN);
    }

    static DataInput readKeyFrom(long hashEntryAdr)
    {
        return newInput(hashEntryAdr, false);
    }

    static DataInput readValueFrom(long hashEntryAdr)
    {
        return newInput(hashEntryAdr, true);
    }

    private static HashEntryInput newInput(long hashEntryAdr, boolean value)
    {
        return new HashEntryInput(hashEntryAdr, value, getKeyLen(hashEntryAdr), getValueLen(hashEntryAdr));
    }

    static void reference(long hashEntryAdr)
    {
        Uns.increment(hashEntryAdr, ENTRY_OFF_REFCOUNT);
    }

    static boolean dereference(long hashEntryAdr)
    {
        return Uns.decrement(hashEntryAdr, ENTRY_OFF_REFCOUNT);
    }
}
