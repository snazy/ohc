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
package org.caffinitas.ohc.linked;

/**
 * Encapsulates access to hash entries.
 */
final class HashEntries
{
    static void init(long hash, int keyLen, int valueLen, long hashEntryAdr, int sentinel, long expireAt)
    {
        Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_EXPIRE_AT, expireAt);
        Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_HASH, hash);
        setNext(hashEntryAdr, 0L);
        Uns.putInt(hashEntryAdr, Util.ENTRY_OFF_KEY_LENGTH, keyLen);
        Uns.putInt(hashEntryAdr, Util.ENTRY_OFF_VALUE_LENGTH, valueLen);
        Uns.putInt(hashEntryAdr, Util.ENTRY_OFF_REFCOUNT, 1);
        Uns.putInt(hashEntryAdr, Util.ENTRY_OFF_GENERATION, Util.GEN_EDEN);
        Uns.putInt(hashEntryAdr, Util.ENTRY_OFF_SENTINEL, sentinel);
    }

    static boolean compare(long hashEntryAdr, long offset, long otherHashEntryAdr, long otherOffset, long len)
    {
        if (hashEntryAdr == 0L)
            return false;

        if (hashEntryAdr == otherHashEntryAdr)
        {
            assert offset == otherOffset;
            return true;
        }

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

    static long getLRUNext(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, Util.ENTRY_OFF_LRU_NEXT);
    }

    static void setLRUNext(long hashEntryAdr, long replacement)
    {
        Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_LRU_NEXT, replacement);
    }

    static long getAndSetLRUNext(long hashEntryAdr, long replacement)
    {
        return Uns.getAndPutLong(hashEntryAdr, Util.ENTRY_OFF_LRU_NEXT, replacement);
    }

    static long getLRUPrev(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, Util.ENTRY_OFF_LRU_PREV);
    }

    static void setLRUPrev(long hashEntryAdr, long replacement)
    {
        Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_LRU_PREV, replacement);
    }

    static long getAndSetLRUPrev(long hashEntryAdr, long replacement)
    {
        return Uns.getAndPutLong(hashEntryAdr, Util.ENTRY_OFF_LRU_PREV, replacement);
    }

    static long getHash(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, Util.ENTRY_OFF_HASH);
    }

    static long getNext(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getLong(hashEntryAdr, Util.ENTRY_OFF_NEXT) : 0L;
    }

    static int getSentinel(long hashEntryAdr)
    {
        return hashEntryAdr != 0L ? Uns.getInt(hashEntryAdr, Util.ENTRY_OFF_SENTINEL) : 0;
    }

    static void setSentinel(long hashEntryAdr, int sentinelState)
    {
        if (hashEntryAdr != 0L)
            Uns.putInt(hashEntryAdr, Util.ENTRY_OFF_SENTINEL, sentinelState);
    }

    static void setNext(long hashEntryAdr, long nextAdr)
    {
        if (hashEntryAdr == nextAdr)
            throw new IllegalArgumentException();
        if (hashEntryAdr != 0L)
            Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_NEXT, nextAdr);
    }

    static int getKeyLen(long hashEntryAdr)
    {
        return Uns.getInt(hashEntryAdr, Util.ENTRY_OFF_KEY_LENGTH);
    }

    static int getValueLen(long hashEntryAdr)
    {
        return Uns.getInt(hashEntryAdr, Util.ENTRY_OFF_VALUE_LENGTH);
    }

    static long getExpireAt(long hashEntryAdr)
    {
        return Uns.getLong(hashEntryAdr, Util.ENTRY_OFF_EXPIRE_AT);
    }

    static void setExpireAt(long hashEntryAdr, long expireAt)
    {
        Uns.putLong(hashEntryAdr, Util.ENTRY_OFF_EXPIRE_AT, expireAt);
    }

    static int getGeneration(long hashEntryAdr)
    {
        return Uns.getInt(hashEntryAdr, Util.ENTRY_OFF_GENERATION);
    }

    static void setGeneration(long hashEntryAdr, int generation)
    {
        Uns.putInt(hashEntryAdr, Util.ENTRY_OFF_GENERATION, generation);
    }

    static long getAllocLen(long hashEntryAdr)
    {
        return Util.allocLen(getKeyLen(hashEntryAdr), getValueLen(hashEntryAdr));
    }

    static void reference(long hashEntryAdr)
    {
        Uns.increment(hashEntryAdr, Util.ENTRY_OFF_REFCOUNT);
    }

    static boolean dereference(long hashEntryAdr)
    {
        if (hashEntryAdr != 0L && Uns.decrement(hashEntryAdr, Util.ENTRY_OFF_REFCOUNT))
        {
            Uns.free(hashEntryAdr);
            return true;
        }
        return false;
    }
}
