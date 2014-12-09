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

import static org.caffinitas.ohc.Util.*;

/**
 * Encapsulates access to hash entries.
 */
public final class HashEntries
{
    static void init(long hash, long keyLen, long valueLen, long hashEntryAdr)
    {
        Uns.putLong(hashEntryAdr, ENTRY_OFF_HASH, hash);
        setNext(hashEntryAdr, 0L);
        Uns.putLong(hashEntryAdr, ENTRY_OFF_KEY_LENGTH, keyLen);
        Uns.putLong(hashEntryAdr, ENTRY_OFF_VALUE_LENGTH, valueLen);
        Uns.putLong(hashEntryAdr, ENTRY_OFF_REFCOUNT, 1L);
    }

    static boolean compareKey(long hashEntryAdr, KeyBuffer key, long serKeyLen)
    {
        if (hashEntryAdr == 0L)
            return false;

        long blkOff = ENTRY_OFF_DATA;
        int p = 0;
        byte[] arr = key.array();
        for (; p <= serKeyLen - 8; p += 8, blkOff += 8)
            if (Uns.getLong(hashEntryAdr, blkOff) != Uns.getLongFromByteArray(arr, p))
                return false;
        for (; p < serKeyLen; p ++, blkOff ++)
            if (Uns.getByte(hashEntryAdr, blkOff) != arr[p])
                return false;

        return true;
    }

    static boolean compareKey(long hashEntryAdr, long newHashEntryAdr, long serKeyLen)
    {
        if (hashEntryAdr == 0L)
            return false;

        long blkOff = ENTRY_OFF_DATA;
        int p = 0;
        for (; p <= serKeyLen - 8; p += 8, blkOff += 8)
            if (Uns.getLong(hashEntryAdr, blkOff) != Uns.getLong(newHashEntryAdr, blkOff))
                return false;
        for (; p < serKeyLen; p ++, blkOff ++)
            if (Uns.getByte(hashEntryAdr, blkOff) != Uns.getByte(newHashEntryAdr, blkOff))
                return false;

        return true;
    }

    public static long getLRUNext(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, ENTRY_OFF_LRU_NEXT);
    }

    public static void setLRUNext(long hashEntryAdr, long replacement)
    {
        Uns.putLong(hashEntryAdr, ENTRY_OFF_LRU_NEXT, replacement);
    }

    public static long getLRUPrev(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, ENTRY_OFF_LRU_PREV);
    }

    public static void setLRUPrev(long hashEntryAdr, long replacement)
    {
        Uns.putLong(hashEntryAdr, ENTRY_OFF_LRU_PREV, replacement);
    }

    static long getHash(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, ENTRY_OFF_HASH);
    }

    static long getNext(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLong(hashEntryAdr, ENTRY_OFF_NEXT) : 0L;
    }

    static void setNext(long hashEntryAdr, long nextAdr)
    {
        if (hashEntryAdr == nextAdr)
            throw new IllegalArgumentException();
        if (hashEntryAdr != 0L)
            Uns.putLong(hashEntryAdr, ENTRY_OFF_NEXT, nextAdr);
    }

    static long getKeyLen(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, ENTRY_OFF_KEY_LENGTH);
    }

    static long getValueLen(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, ENTRY_OFF_VALUE_LENGTH);
    }

    static long getAllocLen(long address)
    {
        return allocLen(getKeyLen(address), getValueLen(address));
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
