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
package org.caffinitas.ohc.tables;

/**
 * Encapsulates access to hash entries.
 */
final class HashEntries
{
    static void init(long hash, long keyLen, long valueLen, long hashEntryAdr)
    {
        Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_HASH, hash);
        Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_KEY_LENGTH, keyLen);
        Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_VALUE_LENGTH, valueLen);
        Uns.putInt(hashEntryAdr, Util.ENTRY_OFF_REFCOUNT, 1);
        Uns.putInt(hashEntryAdr, Util.ENTRY_OFF_SENTINEL, 0);
    }

    static boolean compareKey(long hashEntryAdr, org.caffinitas.ohc.tables.KeyBuffer key, long serKeyLen)
    {
        if (hashEntryAdr == 0L)
            return false;

        long blkOff = Util.ENTRY_OFF_DATA;
        int p = 0;
        byte[] arr = key.array();
        for (; p <= serKeyLen - 8; p += 8, blkOff += 8)
            if (Uns.getLong(hashEntryAdr, blkOff) != Uns.getLongFromByteArray(arr, p))
                return false;
        for (; p <= serKeyLen - 4; p += 4, blkOff += 4)
            if (Uns.getInt(hashEntryAdr, blkOff) != Uns.getIntFromByteArray(arr, p))
                return false;
        for (; p <= serKeyLen - 2; p += 2, blkOff += 2)
            if (Uns.getShort(hashEntryAdr, blkOff) != Uns.getShortFromByteArray(arr, p))
                return false;
        for (; p < serKeyLen; p++, blkOff++)
            if (Uns.getByte(hashEntryAdr, blkOff) != arr[p])
                return false;

        return true;
    }

    static boolean compare(long hashEntryAdr, long offset, long otherHashEntryAdr, long otherOffset, long len)
    {
        if (hashEntryAdr == 0L)
            return false;

        int p = 0;
        for (; p <= len - 8; p += 8, offset += 8, otherOffset += 8)
            if (Uns.getLong(hashEntryAdr, offset) != Uns.getLong(otherHashEntryAdr, otherOffset))
                return false;
        for (; p <= len - 4; p += 4, offset += 4, otherOffset += 4)
            if (Uns.getInt(hashEntryAdr, offset) != Uns.getInt(otherHashEntryAdr, otherOffset))
                return false;
        for (; p <= len - 2; p += 2, offset += 2, otherOffset += 2)
            if (Uns.getShort(hashEntryAdr, offset) != Uns.getShort(otherHashEntryAdr, otherOffset))
                return false;
        for (; p < len; p++, offset++, otherOffset++)
            if (Uns.getByte(hashEntryAdr, offset) != Uns.getByte(otherHashEntryAdr, otherOffset))
                return false;

        return true;
    }

    static int getLRUIndex(long hashEntryAdr)
    {
        return Uns.getInt(hashEntryAdr, Util.ENTRY_OFF_LRU_INDEX);
    }

    static void setLRUIndex(long hashEntryAdr, int lruIndex)
    {
        Uns.putInt(hashEntryAdr, Util.ENTRY_OFF_LRU_INDEX, lruIndex);
    }

    static long getHash(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, Util.ENTRY_OFF_HASH);
    }

    static long getKeyLen(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, Util.ENTRY_OFF_KEY_LENGTH);
    }

    static long getValueLen(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, Util.ENTRY_OFF_VALUE_LENGTH);
    }

    static long getAllocLen(long address)
    {
        return Util.allocLen(getKeyLen(address), getValueLen(address));
    }

    /**
     * Increments the hash-entry's ENTRY_OFF_REFCOUNT value.
     * Take care calling this method <b>while</b> holding a lock.
     */
    static void reference(long hashEntryAdr)
    {
        Uns.increment(hashEntryAdr, Util.ENTRY_OFF_REFCOUNT);
    }

    /**
     * Decrements the hash-entry's ENTRY_OFF_REFCOUNT value and free's the allocated memory if the
     * reference counter reaches {@code 0}. Take care calling this method <b>without</b> holding a lock,
     * because freeing memory is an expensive operation.
     */
    static boolean dereference(long hashEntryAdr)
    {
        if (Uns.decrement(hashEntryAdr, Util.ENTRY_OFF_REFCOUNT))
        {
            Uns.free(hashEntryAdr);
            return true;
        }
        return false;
    }
}
