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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * On-heap test-only counterpart of {@link OffHeapLinkedMap} for {@link CheckOHCacheImpl}.
 */
final class CheckSegment
{
    private final Map<KeyBuffer, byte[]> map;
    private final LinkedList<KeyBuffer> lru = new LinkedList<>();
    private final AtomicLong freeCapacity;

    long hitCount;
    long missCount;
    long putAddCount;
    long putReplaceCount;
    long removeCount;
    long evictedEntries;

    public CheckSegment(int initialCapacity, float loadFactor, AtomicLong freeCapacity)
    {
        this.map = new HashMap<>(initialCapacity, loadFactor);
        this.freeCapacity = freeCapacity;
    }

    synchronized void clear()
    {
        for (Map.Entry<KeyBuffer, byte[]> entry : map.entrySet())
            freeCapacity.addAndGet(sizeOf(entry.getKey(), entry.getValue()));
        map.clear();
        lru.clear();
    }

    synchronized byte[] get(KeyBuffer keyBuffer)
    {
        byte[] r = map.get(keyBuffer);
        if (r == null)
        {
            missCount++;
            return null;
        }

        lru.remove(keyBuffer);
        lru.addFirst(keyBuffer);
        hitCount++;

        return r;
    }

    synchronized boolean put(KeyBuffer keyBuffer, byte[] data, boolean ifAbsent, byte[] old)
    {
        long sz = sizeOf(keyBuffer, data);
        while (freeCapacity.get() < sz)
            if (!evictOne())
            {
                remove(keyBuffer);
                return false;
            }

        byte[] existing = map.get(keyBuffer);
        if (ifAbsent || old != null)
        {
            if (ifAbsent && existing != null)
                return false;
            if (old != null && existing != null && !Arrays.equals(old, existing))
                return false;
        }

        map.put(keyBuffer, data);
        lru.remove(keyBuffer);
        lru.addFirst(keyBuffer);

        if (existing != null)
        {
            freeCapacity.addAndGet(sizeOf(keyBuffer, existing));
            putReplaceCount++;
        }
        else
            putAddCount++;

        freeCapacity.addAndGet(-sz);

        return true;
    }

    synchronized void remove(KeyBuffer keyBuffer)
    {
        byte[] old = map.remove(keyBuffer);
        if (old != null)
        {
            lru.remove(keyBuffer);
            removeCount++;
            freeCapacity.addAndGet(sizeOf(keyBuffer, old));
        }
    }

    synchronized long size()
    {
        return map.size();
    }

    synchronized Iterator<KeyBuffer> hotN(int n)
    {
        List<KeyBuffer> lst = new ArrayList<>(n);
        for (Iterator<KeyBuffer> iter = lru.iterator(); iter.hasNext() && n-- > 0; )
            lst.add(iter.next());
        return lst.iterator();
    }

    synchronized Iterator<KeyBuffer> keyIterator()
    {
        return new ArrayList<>(lru).iterator();
    }

    //

    private boolean evictOne()
    {
        KeyBuffer last = lru.pollLast();
        if (last == null)
            return false;
        byte[] old = map.remove(last);
        freeCapacity.addAndGet(sizeOf(last, old));
        evictedEntries++;
        return true;
    }

    static long sizeOf(KeyBuffer key, byte[] value)
    {
        // calculate the same value as the original impl would do
        return Util.ENTRY_OFF_DATA + Util.roundUpTo8(key.array().length) + value.length;
    }

    void resetStatistics()
    {
        evictedEntries = 0L;
        hitCount = 0L;
        missCount = 0L;
        putAddCount = 0L;
        putReplaceCount = 0L;
        removeCount = 0L;
    }

}
