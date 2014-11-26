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
package org.caffinitas.ohc.benchmark;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.Lists;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;

import org.caffinitas.ohc.DataManagement;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.benchmark.TestRunnables.CacheGetTest;
import org.caffinitas.ohc.benchmark.TestRunnables.CacheInsertTest;
import org.caffinitas.ohc.benchmark.TestRunnables.CacheRemoveTest;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.caffinitas.ohc.benchmark.AbstractTestRunnable.printMessage;

import static java.lang.Thread.sleep;

public class BenchmarkOHC
{
    public static final ExecutorService exec = Executors.newCachedThreadPool();
    private static Cache<String, String> cache;

    public static final String TYPE = "c";
    public static final String THREADS = "t";
    public static final String SIZE = "s";
    public static final String ITERATIONS = "i";
    public static final String PREFIX_SIZE = "p";
    public static final String HASH_TABLE_SIZE = "z";
    public static final String BLOCK_SIZE = "b";

    public static void main(String[] args) throws Exception
    {
        CommandLine cmd = parseArguments(args);
        String type = cmd.getOptionValue(TYPE, "ohc");
        String valuePrefix = randomString(Integer.parseInt(cmd.getOptionValue(PREFIX_SIZE, "" + 1)));
        long iterations = Long.parseLong(cmd.getOptionValue(ITERATIONS, "" + (10 * 1000 * 1000)));
        long threads = Integer.parseInt(cmd.getOptionValue(THREADS, "" + 10));
        long size = Long.parseLong(cmd.getOptionValue(SIZE, "" + (1 * 1024 * 1024 * 1024)));
        int hashTableSize = Integer.parseInt(cmd.getOptionValue(HASH_TABLE_SIZE, "" + 64));
        int blockSize = Integer.parseInt(cmd.getOptionValue(BLOCK_SIZE, "" + 1024));

        if (type.equals("native"))
        {
            Weigher<String, String> weigher = new Weigher<String, String>()
            {
                public int weigh(String key, String value)
                {
                    return key.length() + value.length();
                }
            };
            cache = CacheBuilder.newBuilder()
                                .concurrencyLevel(64)
                                .maximumWeight(size)
                                .weigher(weigher)
                                .build();
            printMessage("Intializing java default cache...");
        }
        else if (type.equals("offheap"))
        {
            ISerializer<String> serializer = new ISerializer<String>()
            {
                public void serialize(String t, DataOutputPlus out) throws IOException
                {
                    byte[] bytes = t.getBytes(Charsets.UTF_8);
                    out.writeInt(bytes.length);
                    out.write(bytes);
                }

                public String deserialize(DataInput in) throws IOException
                {
                    byte[] bytes = new byte[in.readInt()];
                    in.readFully(bytes);
                    return new String(bytes, Charsets.UTF_8);
                }

                public long serializedSize(String t, TypeSizes type)
                {
                    return t.getBytes(Charsets.UTF_8).length + 4;
                }
            };
            cache = new SerializationCacheWrapper<>(size, serializer);
        }
        else
        {
            // default
            cache = OHCacheBuilder.<String, String>newBuilder().keySerializer(BenchmarkUtils.serializer)
                                  .valueSerializer(BenchmarkUtils.serializer)
                                  .hashTableSize(hashTableSize)
                                  .blockSize(blockSize)
                                  .capacity(size)
                                  .dataManagement(DataManagement.FLOATING)
                                  .build();
            printMessage("Intializing OHC cache...");
        }

        // JIT and warm up...
        for (int i = 0; i < 100000; i++)
            cache.put("key-" + i, "" + i);
        // clean-up
        for (int i = 0; i < 100000; i++)
            cache.invalidate("key-" + i);

        sleep(1000);
        printMessage("Warm-up complete, starting the test...");

        long slice = iterations / threads;
        List<Future<Void>> futures = Lists.newArrayList();

        long start = 0;
        for (int i = 1; i <= threads; i++)
        {
            futures.add(exec.submit(new CacheInsertTest(cache, start, slice * i, valuePrefix)));
            start = slice * i;
        }
        BenchmarkUtils.waitOnFuture(futures);
        printMessage("");
        logMemoryUse(cache);

        start = 0;
        for (int i = 1; i <= threads; i++)
        {
            futures.add(exec.submit(new CacheGetTest(cache, start, slice * i, valuePrefix)));
            start = slice * i;
        }
        BenchmarkUtils.waitOnFuture(futures);
        printMessage("");
        logMemoryUse(cache);

        start = 0;
        for (int i = 1; i <= threads; i++)
        {
            futures.add(exec.submit(new CacheRemoveTest(cache, start, slice * i, valuePrefix)));
            start = slice * i;
        }
        BenchmarkUtils.waitOnFuture(futures);
        printMessage("");
        logMemoryUse(cache);

        System.exit(0);
    }

    private static CommandLine parseArguments(String[] args) throws ParseException
    {
        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        options.addOption(TYPE, true, "cache type");
        options.addOption(THREADS, true, "threads for execution");
        options.addOption(SIZE, true, "size of the cache");
        options.addOption(ITERATIONS, true, "iterations per thread");
        options.addOption("h", false, "help, print this command");
        options.addOption(PREFIX_SIZE, true, "prefix size in the value");
        options.addOption(HASH_TABLE_SIZE, true, "hash table size");
        options.addOption(BLOCK_SIZE, true, "data block size");

        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h"))
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("BenchMarkLruc", options);
            System.exit(-1);
        }

        return cmd;
    }

    private static String randomString(int prefixSize)
    {
        String prefix = "";
        for (int i = 0; i < prefixSize; i++)
        {
            prefix += "1";
        }
        return prefix;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void logMemoryUse(Cache<?, ?> cache) throws Exception
    {
        if (cache instanceof OHCache)
        {
            sleep(100);
            printMessage("Memory consumed: %s / %s, size %d",
                         FileUtils.byteCountToDisplaySize(((OHCache) cache).getMemUsed()),
                         FileUtils.byteCountToDisplaySize(((OHCache) cache).getCapacity()),
                         cache.size());
            Iterator<String> iterator = ((OHCache<String, String>) cache).hotN(5);
            printMessage("5 Hot entries are...");
            for (int i = 0; i < 20 && iterator.hasNext(); i++)
                System.out.print(iterator.next() + ", ");
            if (iterator.hasNext())
                System.out.print(", ...");
            printMessage("");
        }
        printMessage("VM total:%s", FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory()));
        printMessage("VM free:%s", FileUtils.byteCountToDisplaySize(Runtime.getRuntime().freeMemory()));
        if (cache instanceof OHCache)
            printMessage("Cache stats:%s", ((OHCache) cache).extendedStats());
        else
            printMessage("Cache stats:%s", cache.stats());
    }
}
