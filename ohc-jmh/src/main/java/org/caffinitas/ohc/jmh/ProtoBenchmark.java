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
@Measurement(iterations = 3)
@Threads(4)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
public class ProtoBenchmark
{
    private OHCache<Integer, byte[]> cache;

    @Param({ "256"/*, "2048", "65536"*/ })
    private int valueSize = 2048;
    @Param("1073741824")
    private long capacity = 1024 * 1024 * 1024;
    @Param("-1")
    private int segmentCount = -1;
    @Param("-1")
    private int hashTableSize = -1;
    @Param("1000000")
    private int keys = 1000000;
    @Param({ "MURMUR3"/*, "CRC32", "XX"*/ })
    private HashAlgorithm hashAlgorithm;
    @Param({ "-1", "65536"/*, "131072"*/ })
    private int chunkSize = -1;
    @Param("-1")
    private int fixedKeyLen = -1;
    @Param("-1")
    private int fixedValueLen = -1;
    @Param("false")
    private boolean unlocked = false;

    @State(Scope.Thread)
    public static class PutState
    {
        public int key = ThreadLocalRandom.current().nextInt(1000);
    }

    @State(Scope.Thread)
    public static class GetState
    {
        public int key = ThreadLocalRandom.current().nextInt(1000);
    }

    @Setup
    public void setup() throws ClassNotFoundException
    {
        cache = OHCacheBuilder.<Integer, byte[]>newBuilder()
                              .capacity(capacity)
                              .segmentCount(segmentCount)
                              .hashTableSize(hashTableSize)
                              .keySerializer(Utils.intSerializer)
                              .valueSerializer(Utils.byteArraySerializer)
                              .chunkSize(chunkSize)
                              .fixedEntrySize(fixedKeyLen, fixedValueLen)
                              .unlocked(unlocked)
                              .build();

        for (int i = 0; i < keys; i++)
            cache.put(i, new byte[valueSize]);
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
        cache.put(state.key++, new byte[valueSize]);
        if (state.key > keys)
            state.key = 1;
    }

    @Benchmark
    @Threads(value = 4)
    public void putMultiThreaded(PutState state)
    {
        cache.put(state.key++, new byte[valueSize]);
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
        if (state.key > keys)
            state.key = 1;
    }
}
