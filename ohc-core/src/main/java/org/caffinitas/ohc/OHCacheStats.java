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
    private final int[] freeListLengths;
    private final int[] hashPartitionLengths;
    private final double blocksPerEntry;
    private final int blockSize;
    private final long capacity;
    private final long size;

    public OHCacheStats(CacheStats cacheStats, int[] freeListLengths, int[] hashPartitionLengths, long size, int blockSize, long capacity)
    {
        this.cacheStats = cacheStats;
        this.freeListLengths = freeListLengths;
        this.hashPartitionLengths = hashPartitionLengths;
        this.size = size;

        double blocksPerEntry = capacity / blockSize - sumOf(freeListLengths);
        blocksPerEntry /= size;

        this.blocksPerEntry = blocksPerEntry;

        this.blockSize = blockSize;
        this.capacity = capacity;
    }

    public long getFreeBlockCount()
    {
        return sumOf(freeListLengths);
    }

    public CacheStats getCacheStats()
    {
        return cacheStats;
    }

    public int[] getFreeListLengths()
    {
        return freeListLengths;
    }

    public int[] getHashPartitionLengths()
    {
        return hashPartitionLengths;
    }

    public long getSize()
    {
        return size;
    }

    public double averageFreeListLength()
    {
        return avgOf(freeListLengths);
    }

    public double averageHashPartitionLength()
    {
        return avgOf(hashPartitionLengths);
    }

    public int minFreeListLength()
    {
        return minOf(freeListLengths);
    }

    public int minHashPartitionLength()
    {
        return minOf(hashPartitionLengths);
    }

    public int maxFreeListLength()
    {
        return maxOf(freeListLengths);
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

    private static long sumOf(int[] arr)
    {
        long r = 0;
        for (int l : arr)
            r += l;
        return r;
    }

    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("cacheStats", cacheStats)
                      .add("size", size)
                      .add("freeLists(#/min/max/avg)", String.format("%d/%d/%d/%.2f", freeListLengths.length, minFreeListLength(), maxFreeListLength(), averageFreeListLength()))
                      .add("hashPartitionLengths(#/min/max/avg)", String.format("%d/%d/%d/%.2f", hashPartitionLengths.length, minHashPartitionLength(), maxHashPartitionLength(), averageHashPartitionLength()))
                      .add("blocksPerEntry", String.format("%.2f", blocksPerEntry))
                      .add("freeSpace", getFreeBlockCount() * blockSize)
                      .add("capacity", capacity)
                      .add("blocks(free/total)", String.format("%d/%d", getFreeBlockCount(), (capacity / blockSize)))
                      .toString();
    }
}
