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

import com.google.common.base.Objects;
import com.google.common.cache.CacheStats;

public final class OHCacheStats
{
    private final CacheStats cacheStats;
    private final int[] hashPartitionLengths;
    private final long capacity;
    private final long free;
    private final long size;
    private final long rehashCount;

    public OHCacheStats(CacheStats cacheStats, int[] hashPartitionLengths, long size, long capacity, long free, long rehashCount)
    {
        this.cacheStats = cacheStats;
        this.hashPartitionLengths = hashPartitionLengths;
        this.size = size;
        this.capacity = capacity;
        this.free = free;
        this.rehashCount = rehashCount;
    }

    public long getRehashCount()
    {
        return rehashCount;
    }

    public CacheStats getCacheStats()
    {
        return cacheStats;
    }

    public int[] getHashPartitionLengths()
    {
        return hashPartitionLengths;
    }

    public long getSize()
    {
        return size;
    }

    public double averageHashPartitionLength()
    {
        return avgOf(hashPartitionLengths);
    }

    public int minHashPartitionLength()
    {
        return minOf(hashPartitionLengths);
    }

    public int maxHashPartitionLength()
    {
        return maxOf(hashPartitionLengths);
    }

    private static double avgOf(int[] arr)
    {
        double r = 0d;
        for (int l : arr)
            r += l;
        return r / arr.length;
    }

    private static int minOf(int[] arr)
    {
        int r = Integer.MAX_VALUE;
        for (int l : arr)
            if (l < r)
                r = l;
        return r;
    }

    private static int maxOf(int[] arr)
    {
        int r = 0;
        for (int l : arr)
            if (l > r)
                r = l;
        return r;
    }

    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("cacheStats", cacheStats)
                      .add("size", size)
                      .add("capacity", capacity)
                      .add("free", free)
                      .add("rehashCount", rehashCount)
                      .add("hashPartitionLengths(#/min/max/avg)", String.format("%d/%d/%d/%.2f", hashPartitionLengths.length, minHashPartitionLength(), maxHashPartitionLength(), averageHashPartitionLength()))
                      .toString();
    }
}
