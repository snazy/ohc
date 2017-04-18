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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.caffinitas.ohc.alloc.IAllocator;
import org.caffinitas.ohc.alloc.JNANativeAllocator;
import org.caffinitas.ohc.alloc.UnsafeAllocator;
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
public class AllocatorBenchmark
{
    @Param({ "128", /*"256", "512", "1024", "1536", "2048", "4096", */"8192" })
    private int size = 128;
    @Param({ "Unsafe", "JNA" })
    private String allocatorType = "JNA";

    private IAllocator allocator;

    @State(Scope.Thread)
    public static class Rand extends FasterRandom
    {}

    @State(Scope.Benchmark)
    public static class Allocations
    {
        final AtomicLong[] adrs = new AtomicLong[1024];
        {
            for (int i = 0; i < adrs.length; i++)
                adrs[i] = new AtomicLong();
        }

        void allocate(IAllocator allocator, Rand rand, int size)
        {
            int idx = rand.nextInt(adrs.length);

            AtomicLong adr = adrs[idx];

            while (true)
            {
                long a = adr.get();
                if (a == 0L)
                {
                    break;
                }
                if (adr.compareAndSet(a, 0L))
                {
                    allocator.free(a);
                    break;
                }
            }

            long a = allocator.allocate(size);
            if (!adr.compareAndSet(0L, a))
            {
                allocator.free(a);
            }
        }

        void freeAll(IAllocator allocator)
        {
            for (AtomicLong al : adrs)
            {
                long adr = al.get();
                if (adr != 0L)
                    allocator.free(adr);
            }
        }
    }

    @Setup
    public void createAllocator()
    {
        switch (allocatorType)
        {
            case "Unsafe": allocator = new UnsafeAllocator(); break;
            case "JNA": allocator = new JNANativeAllocator(); break;
        }
    }

    @TearDown
    public void tearDown(Allocations allocations) throws IOException
    {
        allocations.freeAll(allocator);
    }

    @Benchmark
    @Threads(value = 1)
    public void allocateSingleThreaded(Rand rand, Allocations state)
    {
        state.allocate(allocator, rand, size);
    }

    @Benchmark
    @Threads(value = 4)
    public void allocateMultiThreaded(Rand rand, Allocations state)
    {
        state.allocate(allocator, rand, size);
    }
}
