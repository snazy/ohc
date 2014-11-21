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

public class OHCacheBuilder
{
    private int blockSize = 2048;
    private int hashTableSize = 256;
    private long totalCapacity = 64L*1024L*1024L;
//    private boolean profilingMetrics;

    private OHCacheBuilder()
    {
    }

    public static OHCacheBuilder newBuilder()
    {
        return new OHCacheBuilder();
    }

    public int getBlockSize()
    {
        return blockSize;
    }

    public OHCacheBuilder setBlockSize(int blockSize)
    {
        this.blockSize = blockSize;
        return this;
    }

    public int getHashTableSize()
    {
        return hashTableSize;
    }

    public OHCacheBuilder setHashTableSize(int hashTableSize)
    {
        this.hashTableSize = hashTableSize;
        return this;
    }

    public long getTotalCapacity()
    {
        return totalCapacity;
    }

    public OHCacheBuilder setTotalCapacity(long totalCapacity)
    {
        this.totalCapacity = totalCapacity;
        return this;
    }

//    public boolean isProfilingMetrics()
//    {
//        return profilingMetrics;
//    }
//
//    public void setProfilingMetrics(boolean profilingMetrics)
//    {
//        this.profilingMetrics = profilingMetrics;
//    }

    public OHCache build()
    {
        return new OHCacheImpl(this);
    }
}
