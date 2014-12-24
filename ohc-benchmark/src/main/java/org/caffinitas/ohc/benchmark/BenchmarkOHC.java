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

import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.benchmark.distribution.DistributionFactory;
import org.caffinitas.ohc.benchmark.distribution.OptionDistribution;

import static java.lang.Thread.sleep;

public final class BenchmarkOHC
{
    public static final String THREADS = "t";
    public static final String CAPACITY = "cap";
    public static final String DURATION = "d";
    public static final String SEGMENT_COUNT = "sc";
    public static final String LOAD_FACTOR = "lf";
    public static final String HASH_TABLE_SIZE = "z";
    public static final String WARM_UP = "wu";
    public static final String READ_WRITE_RATIO = "r";
    public static final String READ_KEY_DIST = "rkd";
    public static final String WRITE_KEY_DIST = "wkd";
    public static final String VALUE_SIZE_DIST = "vs";
    public static final String DRIVERS = "dr";
    public static final String TYPE = "type";

    public static final String DEFAULT_VALUE_SIZE_DIST = "fixed(512)";
    public static final String DEFAULT_KEY_DIST = "uniform(1..10000)";
    public static final int ONE_MB = 1024 * 1024;

    public static void main(String[] args) throws Exception
    {
        try
        {
            CommandLine cmd = parseArguments(args);

            String[] warmUp = cmd.getOptionValue(WARM_UP, "15,5").split(",");
            int warmUpSecs = Integer.parseInt(warmUp[0]);
            int coldSleepSecs = Integer.parseInt(warmUp[1]);

            int duration = Integer.parseInt(cmd.getOptionValue(DURATION, "60"));
            int cores = Runtime.getRuntime().availableProcessors();
            if (cores >= 8)
                cores -= 2;
            else if (cores > 2)
                cores--;
            int threads = Integer.parseInt(cmd.getOptionValue(THREADS, Integer.toString(cores)));
            long capacity = Long.parseLong(cmd.getOptionValue(CAPACITY, "" + (1024 * 1024 * 1024)));
            int hashTableSize = Integer.parseInt(cmd.getOptionValue(HASH_TABLE_SIZE, "0"));
            int segmentCount = Integer.parseInt(cmd.getOptionValue(SEGMENT_COUNT, "0"));
            float loadFactor = Float.parseFloat(cmd.getOptionValue(LOAD_FACTOR, "0"));

            double readWriteRatio = Double.parseDouble(cmd.getOptionValue(READ_WRITE_RATIO, ".5"));

            int driverCount = Integer.parseInt(cmd.getOptionValue(DRIVERS, Integer.toString(Runtime.getRuntime().availableProcessors() / 4)));
            if (driverCount < 1)
                driverCount = 1;

            String type = cmd.getOptionValue(TYPE, "linked");

            int threadsPerDriver = threads / driverCount;

            Driver[] drivers = new Driver[driverCount];
            int remainingThreads = threads;
            Random rnd = new Random();
            DistributionFactory readKeyDist = OptionDistribution.get(cmd.getOptionValue(READ_KEY_DIST, DEFAULT_KEY_DIST));
            DistributionFactory writeKeyDist = OptionDistribution.get(cmd.getOptionValue(WRITE_KEY_DIST, DEFAULT_KEY_DIST));
            DistributionFactory valueSizeDist = OptionDistribution.get(cmd.getOptionValue(VALUE_SIZE_DIST, DEFAULT_VALUE_SIZE_DIST));
            for (int i = 0; i < driverCount; i++)
            {
                int driverThreads = Math.min(threadsPerDriver, remainingThreads);
                remainingThreads -= driverThreads;

                drivers[i] = new Driver(i, readKeyDist.get(), writeKeyDist.get(), valueSizeDist.get(),
                                        readWriteRatio, driverThreads, rnd.nextLong());
            }

            printMessage("Starting benchmark with%n" +
                         "   threads     : %d%n" +
                         "   drivers     : %d%n" +
                         "   warm-up-secs: %d%n" +
                         "   idle-secs   : %d%n" +
                         "   runtime-secs: %d%n",
                         threads, driverCount, warmUpSecs, coldSleepSecs, duration);

            printMessage("Initializing OHC cache...");
            Shared.cache = OHCacheBuilder.<Long, byte[]>newBuilder()
                                         .type((Class<? extends OHCache>) Class.forName("org.caffinitas.ohc."+type+".OHCacheImpl"))
                                         .keySerializer(BenchmarkUtils.longSerializer)
                                         .valueSerializer(BenchmarkUtils.serializer)
                                         .hashTableSize(hashTableSize)
                                         .loadFactor(loadFactor)
                                         .segmentCount(segmentCount)
                                         .capacity(capacity)
                                         .build();

            printMessage("Cache configuration: hash-table-size: %d%n" +
                         "                     load-factor    : %.3f%n" +
                         "                     segments       : %d%n" +
                         "                     capacity       : %d%n",
                         Shared.cache.hashTableSizes()[0],
                         Shared.cache.loadFactor(),
                         Shared.cache.segments(),
                         Shared.cache.capacity());

            ThreadPoolExecutor main = new ThreadPoolExecutor(driverCount, driverCount, coldSleepSecs + 1, TimeUnit.SECONDS,
                                                             new LinkedBlockingQueue<Runnable>(), new ThreadFactory()
            {
                volatile int threadNo;

                public Thread newThread(Runnable r)
                {
                    return new Thread(r, "driver-main-" + threadNo++);
                }
            });
            main.prestartAllCoreThreads();

            // warm up

            if (warmUpSecs > 0)
            {
                printMessage("Start warm-up...");
                runFor(warmUpSecs, main, drivers);
                printMessage("");
                logMemoryUse();
            }

            // cold sleep

            if (coldSleepSecs > 0)
            {
                printMessage("Warm up complete, sleep for %d seconds...", coldSleepSecs);
                Thread.sleep(coldSleepSecs * 1000L);
            }

            // benchmark

            printMessage("Start benchmark...");
            runFor(duration, main, drivers);
            printMessage("");
            logMemoryUse();

            // finish

            for (Driver driver : drivers)
                driver.shutdown();
            for (Driver driver : drivers)
                driver.terminate();

            System.exit(0);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(1);
        }
    }

    private static void runFor(int duration, ExecutorService main, Driver[] drivers) throws InterruptedException, ExecutionException
    {

        printMessage("%s: Running for %d seconds...", new Date(), duration);

        // clear all statistics, timers, etc
        Shared.clearStats();

        long endAt = System.currentTimeMillis() + duration * 1000L;

        for (Driver driver : drivers)
        {
            driver.endAt = endAt;
            driver.future = main.submit(driver);
        }

        long statsInterval = 10000L;
        long nextStats = System.currentTimeMillis() + statsInterval;

        while (System.currentTimeMillis() < endAt)
        {
            if (Shared.fatal.get())
            {
                System.err.println("Unhandled exception caught - exiting");
                System.exit(1);
            }

            if (nextStats <= System.currentTimeMillis())
            {
                Shared.printStats("At " + new Date());
                nextStats += statsInterval;
            }

            Thread.sleep(100);
        }

        printMessage("%s: Time over ... waiting for tasks to complete...", new Date());

        for (Driver driver : drivers)
            driver.future.get();

        Shared.printStats("Final");
    }

    private static CommandLine parseArguments(String[] args) throws ParseException
    {
        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        options.addOption("h", false, "help, print this command");

        options.addOption(THREADS, true, "threads for execution");
        options.addOption(WARM_UP, true, "warm up - <work-secs>,<sleep-secs>");
        options.addOption(DURATION, true, "benchmark duration in seconds");
        options.addOption(READ_WRITE_RATIO, true, "read-write ration (as a double 0..1 representing the chance for a read)");

        options.addOption(CAPACITY, true, "size of the cache");

        options.addOption(HASH_TABLE_SIZE, true, "hash table size");
        options.addOption(LOAD_FACTOR, true, "hash table load factor");
        options.addOption(SEGMENT_COUNT, true, "number of segments (number of individual off-heap-maps)");

        options.addOption(VALUE_SIZE_DIST, true, "value sizes - default: " + DEFAULT_VALUE_SIZE_DIST);
        options.addOption(READ_KEY_DIST, true, "hot key use distribution - default: " + DEFAULT_KEY_DIST);
        options.addOption(WRITE_KEY_DIST, true, "hot key use distribution - default: " + DEFAULT_KEY_DIST);

        options.addOption(DRIVERS, true, "number of drivers - default: # of cores divided by 4");

        options.addOption(TYPE, true, "implementation type - default: linked - option: tables");

        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h"))
        {
            HelpFormatter formatter = new HelpFormatter();
            String help = "";
            for (String s : OptionDistribution.help())
                help = help + '\n' + s;
            formatter.printHelp(160, "BenchmarkOHC", null, options, help);
            System.exit(-1);
        }

        return cmd;
    }

    private static void logMemoryUse() throws Exception
    {
        OHCache<Long, byte[]> cache = Shared.cache;
        sleep(100);
        printMessage("Memory consumed: %s / %s, size %d%n" +
                     "          stats: %s",
                     byteCountToDisplaySize(cache.memUsed()),
                     byteCountToDisplaySize(cache.capacity()),
                     cache.size(),
                     cache.stats());
        printMessage("");
        printMessage("VM total:%s", byteCountToDisplaySize(Runtime.getRuntime().totalMemory()));
        printMessage("VM free:%s", byteCountToDisplaySize(Runtime.getRuntime().freeMemory()));
        printMessage("Cache stats:%s", cache.stats());
    }

    private static String byteCountToDisplaySize(long l)
    {
        if (l > ONE_MB)
            return Long.toString(l / ONE_MB) + " MB";
        if (l > 1024)
            return Long.toString(l / 1024) + " kB";
        return Long.toString(l);
    }

    public static void printMessage(String format, Object... objects)
    {
        System.out.println(String.format(format, objects));
    }
}
