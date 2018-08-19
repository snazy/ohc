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

import java.util.Arrays;

public final class OHCacheStats
{
    private final long hitCount;
    private final long missCount;
    private final long evictionCount;
    private final long expireCount;
    private final long[] segmentSizes;
    private final long capacity;
    private final long free;
    private final long size;
    private final long rehashCount;
    private final long putAddCount;
    private final long putReplaceCount;
    private final long putFailCount;
    private final long removeCount;
    private final long totalAllocated;
    private final long lruCompactions;

    public OHCacheStats(long hitCount, long missCount, long evictionCount, long expireCount,
                        long[] segmentSizes, long size, long capacity, long free, long rehashCount,
                        long putAddCount, long putReplaceCount, long putFailCount, long removeCount,
                        long totalAllocated, long lruCompactions)
    {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.evictionCount = evictionCount;
        this.expireCount = expireCount;
        this.segmentSizes = segmentSizes;
        this.size = size;
        this.capacity = capacity;
        this.free = free;
        this.rehashCount = rehashCount;
        this.putAddCount = putAddCount;
        this.putReplaceCount = putReplaceCount;
        this.putFailCount = putFailCount;
        this.removeCount = removeCount;
        this.totalAllocated = totalAllocated;
        this.lruCompactions = lruCompactions;
    }

    public long getCapacity()
    {
        return capacity;
    }

    public long getFree()
    {
        return free;
    }

    public long getRehashCount()
    {
        return rehashCount;
    }

    public long getHitCount()
    {
        return hitCount;
    }

    public long getMissCount()
    {
        return missCount;
    }

    public long getEvictionCount()
    {
        return evictionCount;
    }

    public long getExpireCount()
    {
        return expireCount;
    }

    public long[] getSegmentSizes()
    {
        return segmentSizes;
    }

    public long getSize()
    {
        return size;
    }

    public long getPutAddCount()
    {
        return putAddCount;
    }

    public long getPutReplaceCount()
    {
        return putReplaceCount;
    }

    public long getPutFailCount()
    {
        return putFailCount;
    }

    public long getRemoveCount()
    {
        return removeCount;
    }

    public double getAverageSegmentSize()
    {
        return avgOf(segmentSizes);
    }

    public long getMinSegmentSize()
    {
        return minOf(segmentSizes);
    }

    public long getMaxSegmentSize()
    {
        return maxOf(segmentSizes);
    }

    public long getTotalAllocated()
    {
        return totalAllocated;
    }

    public long getLruCompactions()
    {
        return lruCompactions;
    }

    @Override
    public String toString() {
        return "OHCacheStats{" +
                "hitCount=" + hitCount +
                ", missCount=" + missCount +
                ", evictionCount=" + evictionCount +
                ", expireCount=" + expireCount +
                ", size=" + size +
                ", capacity=" + capacity +
                ", free=" + free +
                ", rehashCount=" + rehashCount +
                ", put(add/replace/fail)=" + Long.toString(putAddCount)+'/'+putReplaceCount+'/'+putFailCount +
                ", removeCount=" + removeCount +
                ", segmentSizes(#/min/max/avg)=" + String.format("%d/%d/%d/%.2f", segmentSizes.length, getMinSegmentSize(), getMaxSegmentSize(), getAverageSegmentSize()) +
                ", totalAllocated=" + totalAllocated +
                ", lruCompactions=" + lruCompactions +
                '}';
    }

    private static long maxOf(long[] arr)
    {
        long r = 0;
        for (long l : arr)
            if (l > r)
                r = l;
        return r;
    }

    private static long minOf(long[] arr)
    {
        long r = Long.MAX_VALUE;
        for (long l : arr)
            if (l < r)
                r = l;
        return r;
    }

    private static double avgOf(long[] arr)
    {
        double r = 0d;
        for (long l : arr)
            r += l;
        return r / arr.length;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OHCacheStats that = (OHCacheStats) o;

        if (capacity != that.capacity) return false;
        if (evictionCount != that.evictionCount) return false;
        if (free != that.free) return false;
        if (hitCount != that.hitCount) return false;
        if (missCount != that.missCount) return false;
        if (putAddCount != that.putAddCount) return false;
        if (putFailCount != that.putFailCount) return false;
        if (putReplaceCount != that.putReplaceCount) return false;
//        if (rehashCount != that.rehashCount) return false;
        if (removeCount != that.removeCount) return false;
        if (size != that.size) return false;
//        if (totalAllocated != that.totalAllocated) return false;
        if (!Arrays.equals(segmentSizes, that.segmentSizes)) return false;

        return true;
    }

    public int hashCode()
    {
        int result = (int) (hitCount ^ (hitCount >>> 32));
        result = 31 * result + (int) (missCount ^ (missCount >>> 32));
        result = 31 * result + (int) (evictionCount ^ (evictionCount >>> 32));
        result = 31 * result + Arrays.hashCode(segmentSizes);
        result = 31 * result + (int) (capacity ^ (capacity >>> 32));
        result = 31 * result + (int) (free ^ (free >>> 32));
        result = 31 * result + (int) (size ^ (size >>> 32));
//        result = 31 * result + (int) (rehashCount ^ (rehashCount >>> 32));
        result = 31 * result + (int) (putAddCount ^ (putAddCount >>> 32));
        result = 31 * result + (int) (putReplaceCount ^ (putReplaceCount >>> 32));
        result = 31 * result + (int) (putFailCount ^ (putFailCount >>> 32));
        result = 31 * result + (int) (removeCount ^ (removeCount >>> 32));
//        result = 31 * result + (int) (totalAllocated ^ (totalAllocated >>> 32));
        return result;
    }
}
