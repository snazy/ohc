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

import com.google.common.primitives.Ints;

import org.caffinitas.ohc.Ticker;

/**
 * Manages cache entry time-to-live in off-heap memory.
 * <p>
 * TTL entries are organized in {@code slots} slots. The <i>slot</i> is chosen using
 * the entry's {@code expireAt} absolute timestamp value - rounded by the provided
 * {@code precision}.
 * Technically the index of a slot is calculated using the formula:
 * {@code slotIndex = (expireAt >> precisionShift) & slotBitmask}.
 * </p>
 * <p>
 * Each slot maintains an unsorted list of entries. Each entry consists of the
 * {@code hashEntryAdr} of the cache's entry and the {@code expireAt} absolute
 * timestamp. The size of a slot's list is unbounded, it is resized if necessary -
 * both increasing and decreasing.
 * </p>
 */
final class Timeouts
{
    private final long slotBitmask;

    private final int precisionShift;
    private final int slotCount;
    private final Slot[] slots;

    private final Ticker ticker;

    Timeouts(Ticker ticker, int slots, long precision)
    {
        if (slots == 0)
            slots = 64;
        if (precision == 0)
            precision = 128;

        if (slots < 1)
            throw new IllegalArgumentException("timeouts-slots <= 0");
        if (precision < 1)
            throw new IllegalArgumentException("precision <= 0");

        this.ticker = ticker;

        this.slotCount = Ints.checkedCast(Util.roundUpToPowerOf2(Math.max(slots, 16), 1 << 30));

        int slotShift = 64 - Util.bitNum(slotCount) - 1;
        this.slotBitmask = ((long) slotCount - 1) << slotShift;

        this.slots = new Slot[slotCount];
        for (int i = 0; i < slotCount; i++)
            this.slots[i] = new Slot();

        precision = Util.roundUpToPowerOf2(Math.max(precision, 1), 1 << 30);
        precisionShift = 64 - Util.bitNum(precision) - 1;
    }

    /**
     * Add a cache entry.
     *
     * @param hashEntryAdr address of the cache entry
     * @param expireAt     absolute expiration timestamp
     */
    void add(long hashEntryAdr, long expireAt)
    {
        // just ignore the fact that expireAt can be less than current time

        int slotNum = slot(expireAt);
        slots[slotNum].add(hashEntryAdr, expireAt);
    }

    /**
     * Remote a cache entry.
     *
     * @param hashEntryAdr address of the cache entry
     * @param expireAt     absolute expiration timestamp
     */
    void remove(long hashEntryAdr, long expireAt)
    {
        int slot = slot(expireAt);
        slots[slot].remove(hashEntryAdr);
    }

    int used()
    {
        int used = 0;
        for (Slot slot : slots)
            used += slot.used;
        return used;
    }

    /**
     * Remove expired entries.
     *
     * @param expireHandler implementation that will be called for each expired entry.
     */
    int removeExpired(TimeoutHandler expireHandler)
    {
        // ensure the clock never goes backwards
        long t = ticker.currentTimeMillis();

        int expired = 0;
        for (int i = 0; i < slotCount; i++)
        {
            expired += slots[i].removeExpired(t, expireHandler);
        }
        return expired;
    }

    /**
     * Releases all allocated off-heap memory.
     */
    void release()
    {
        for (Slot slot : slots)
            slot.release();
    }

    private int slot(long expireAt)
    {
        expireAt >>= precisionShift;
        return (int) (expireAt & slotBitmask);
    }

    interface TimeoutHandler
    {
        /**
         * Called by {@link #removeExpired(TimeoutHandler)} for each expired entry.
         * The implementation must <b>not</b> call {@link #remove(long, long)} since
         * removal of the entry in this structure has already taken place.
         *
         * @param hashEntryAdr cache entry address that has expired
         */
        void expired(long hashEntryAdr);
    }

    private final class Slot
    {
        // minimum number of entries in a slot
        private static final int MIN_LEN = 16;

        private static final int ENTRY_SIZE = 8 + 8;
        // Each entry in a slot consists of two 8-byte values:
        // 1. pointer to hashEntryAdr
        // 2. expireAt

        private long addr;
        private int allocLen;
        private int len;
        private int used;
        private int min0;

        void add(long hashEntryAdr, long expireAt)
        {
            for (int i = min0; i < allocLen; i++)
            {
                int off = i * ENTRY_SIZE;
                long adr = Uns.getLong(addr, off);
                if (adr == 0)
                {
                    // reuse empty entry
                    if (i + 1 > len)
                        len = i + 1;
                    min0 = i + 1;

                    postAdd(hashEntryAdr, expireAt, off);

                    return;
                }
            }

            resize();

            int off = len * ENTRY_SIZE;
            len = min0 = len + 1;

            postAdd(hashEntryAdr, expireAt, off);
        }

        private void postAdd(long hashEntryAdr, long expireAt, int off)
        {
            Uns.putLong(addr, off, hashEntryAdr);
            Uns.putLong(addr, off + 8, expireAt);
            used++;
        }

        private void resize()
        {
            int newAllocLen = Math.max(MIN_LEN, allocLen * 2);
            long newAddr = Uns.allocate(newAllocLen * ENTRY_SIZE, true);
            if (addr != 0L)
            {
                Uns.copyMemory(addr, 0, newAddr, 0, len * ENTRY_SIZE);
                Uns.setMemory(newAddr, len * ENTRY_SIZE, (newAllocLen - len) * ENTRY_SIZE, (byte) 0);
            }
            else
                Uns.setMemory(newAddr, 0, newAllocLen * ENTRY_SIZE, (byte) 0);
            Uns.free(addr);
            addr = newAddr;
            allocLen = newAllocLen;
        }

        void remove(long hashEntryAdr)
        {
            for (int i = 0; i < len; i++)
            {
                int off = i * ENTRY_SIZE;
                long adr = Uns.getLong(addr, off);
                if (adr == hashEntryAdr)
                {
                    // reuse empty entry
                    clearEntry(i, off);
                }
            }

            maybeCompact();
        }

        int removeExpired(long now, TimeoutHandler expireHandler)
        {
            int expired = 0;
            for (int i = 0; i < len; i++)
            {
                int off = i * ENTRY_SIZE;
                long hashEntryAdr = Uns.getLong(addr, off);
                if (hashEntryAdr != 0L)
                {
                    long expireAt = Uns.getLong(addr, off + 8);
                    if (now >= expireAt)
                    {
                        clearEntry(i, off);

                        expireHandler.expired(hashEntryAdr);

                        expired++;
                    }
                }
            }

            maybeCompact();

            return expired;
        }

        private void clearEntry(int idx, int off)
        {
            if (idx < min0)
                min0 = idx;

            if (idx == len - 1)
                len--;
            Uns.putLong(addr, off, 0L);
            Uns.putLong(addr, off + 8, 0L);
            used--;
        }

        private void maybeCompact()
        {
            // compact slot, if too many unused entries in slot
            // i.e. if only half of the possible entries are used, but keep 16 at least entries
            if (used * 2 <= allocLen)
            {
                int newAllocLen = Math.max(MIN_LEN, allocLen / 2);
                if (newAllocLen < allocLen)
                    compact(newAllocLen);
            }
        }

        private void compact(int newAllocLen)
        {
            long newAddr = Uns.allocate(newAllocLen * ENTRY_SIZE, true);
            int tOff = 0;
            int sOff = 0;
            for (int i = 0; i < len; i++, sOff += ENTRY_SIZE)
            {
                long hashEntryAdr = Uns.getLong(addr, sOff);
                if (hashEntryAdr != 0L)
                {
                    long expireAt = Uns.getLong(addr, sOff + 8);
                    Uns.putLong(newAddr, tOff, hashEntryAdr);
                    Uns.putLong(newAddr, tOff + 8, expireAt);
                    tOff += ENTRY_SIZE;
                }
            }
            Uns.setMemory(newAddr, used * ENTRY_SIZE, (newAllocLen - used) * ENTRY_SIZE, (byte) 0);
            Uns.free(addr);
            len = used;
            min0 = used;
            addr = newAddr;
            allocLen = newAllocLen;
        }

        void release()
        {
            Uns.free(addr);
            addr = 0L;
        }
    }
}
