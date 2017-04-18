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
package org.caffinitas.ohc.jmh;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.HashAlgorithm;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode({ /*Mode.AverageTime, */Mode.Throughput })
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 3, time = 5)
@Threads(4)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
public class OHCBenchmark
{
    private OHCache<Integer, byte[]> cache;

    @Param({ "256"/*, "2048", "65536"*/ })
    private int valueSz = 256;
    @Param("1073741824")
    private long capacity = 1024 * 1024 * 1024;
    @Param("-1")
    private int segCnt = -1;
    @Param("-1")
    private int hashTableSz = -1;
    @Param("10000000")
    private int keys = 10_000_000;
    @Param({ "MURMUR3", "CRC32", "XX" })
    private HashAlgorithm hashAlg = HashAlgorithm.MURMUR3;
    @Param({ "-1", "65536"/*, "131072"*/ })
    private int chunkSz = -1;
    @Param("-1")
    private int fixedKeyLen = -1;
    @Param("-1")
    private int fixedValLen = -1;
    @Param({"LRU", "W_TINY_LFU"})
    private Eviction eviction = Eviction.LRU;

    private byte[] value;

    @State(Scope.Thread)
    public static class PutState
    {
        public int key = ThreadLocalRandom.current().nextInt(1000);
    }

    @State(Scope.Thread)
    public static class GetState
    {
        public int key = ThreadLocalRandom.current().nextInt(1000);
        public int run;
    }

    @Setup
    public void setup() throws ClassNotFoundException
    {
        cache = OHCacheBuilder.<Integer, byte[]>newBuilder()
                              .capacity(capacity)
                              .segmentCount(segCnt)
                              .hashTableSize(hashTableSz)
                              .keySerializer(Utils.intSerializer)
                              .valueSerializer(Utils.byteArraySerializer)
                              .chunkSize(chunkSz)
                              .fixedEntrySize(fixedKeyLen, fixedValLen)
                              .hashMode(hashAlg)
                              .eviction(eviction)
                              .build();

        value = new byte[valueSz];

        for (int i = 0; i < keys; i++)
            cache.put(i, value);
    }

    @TearDown
    public void tearDown() throws IOException
    {
        cache.close();
    }

    @Benchmark
    @Threads(value = 4)
    public void getNonExisting()
    {
        cache.get(0);
    }

    @Benchmark
    @Threads(value = 4)
    public void containsNonExisting()
    {
        cache.containsKey(0);
    }

    @Benchmark
    @Threads(value = 1)
    public void putSingleThreaded(PutState state)
    {
        cache.put(state.key++, value);
        if (state.key > keys)
            state.key = 1;
    }

    @Benchmark
    @Threads(value = 4)
    public void putMultiThreaded(PutState state)
    {
        cache.put(state.key++, value);
        if (state.key > keys)
            state.key = 1;
    }

    @Benchmark
    @Threads(value = 1)
    public void getSingleThreaded(GetState state)
    {
        cache.get(state.key++);
        if (state.key > keys)
            state.key = 1;
    }

    @Benchmark
    @Threads(value = 4)
    public void getMultiThreaded(GetState state)
    {
        cache.get(state.key++);

        state.run ++;

        if (state.run < 4)
        {
            if (state.key > keys / 5)
                state.key = 1;
        }

        state.run = 0;

        if (state.key > keys)
            state.key = 1;
    }
}
