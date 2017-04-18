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

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
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

import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.HashAlgorithm;
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
    public static final String KEY_LEN = "kl";
    public static final String READ_WRITE_RATIO = "r";
    public static final String READ_KEY_DIST = "rkd";
    public static final String WRITE_KEY_DIST = "wkd";
    public static final String VALUE_SIZE_DIST = "vs";
    public static final String BUCKET_HISTOGRAM = "bh";
    public static final String CHUNK_SIZE = "cs";
    public static final String FIXED_KEY_SIZE = "fks";
    public static final String FIXED_VALUE_SIZE = "fvs";
    public static final String MAX_ENTRY_SIZE = "mes";
    public static final String UNLOCKED = "ul";
    public static final String EVICTION = "e";
    public static final String HASH_MODE = "hm";
    public static final String CSV = "csv";

    public static final String DEFAULT_VALUE_SIZE_DIST = "fixed(512)";
    public static final String DEFAULT_KEY_DIST = "uniform(1..10000)";
    public static final int ONE_MB = 1024 * 1024;

    public static void main(String[] args) throws Exception
    {
        Locale.setDefault(Locale.ENGLISH);
        Locale.setDefault(Locale.Category.FORMAT, Locale.ENGLISH);

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
            int keyLen = Integer.parseInt(cmd.getOptionValue(KEY_LEN, "0"));
            int chunkSize = Integer.parseInt(cmd.getOptionValue(CHUNK_SIZE, "-1"));
            int fixedKeySize = Integer.parseInt(cmd.getOptionValue(FIXED_KEY_SIZE, "-1"));
            int fixedValueSize = Integer.parseInt(cmd.getOptionValue(FIXED_VALUE_SIZE, "-1"));
            int maxEntrySize = Integer.parseInt(cmd.getOptionValue(MAX_ENTRY_SIZE, "-1"));
            boolean unlocked = Boolean.parseBoolean(cmd.getOptionValue(UNLOCKED, "false"));
            HashAlgorithm hashMode = HashAlgorithm.valueOf(cmd.getOptionValue(HASH_MODE, "MURMUR3"));
            Eviction eviction = Eviction.valueOf(cmd.getOptionValue(EVICTION, "lru"));

            boolean bucketHistogram = Boolean.parseBoolean(cmd.getOptionValue(BUCKET_HISTOGRAM, "false"));

            double readWriteRatio = Double.parseDouble(cmd.getOptionValue(READ_WRITE_RATIO, ".5"));

            Driver[] drivers = new Driver[threads];
            Random rnd = new Random();
            String readKeyDistStr = cmd.getOptionValue(READ_KEY_DIST, DEFAULT_KEY_DIST);
            String writeKeyDistStr = cmd.getOptionValue(WRITE_KEY_DIST, DEFAULT_KEY_DIST);
            String valueSizeDistStr = cmd.getOptionValue(VALUE_SIZE_DIST, DEFAULT_VALUE_SIZE_DIST);
            DistributionFactory readKeyDist = OptionDistribution.get(readKeyDistStr);
            DistributionFactory writeKeyDist = OptionDistribution.get(writeKeyDistStr);
            DistributionFactory valueSizeDist = OptionDistribution.get(valueSizeDistStr);
            for (int i = 0; i < threads; i++)
            {
                drivers[i] = new Driver(readKeyDist.get(), writeKeyDist.get(), valueSizeDist.get(),
                                        readWriteRatio, rnd.nextLong());
            }

            printMessage("Initializing OHC cache...");
            OHCacheBuilder<Long, byte[]> builder = OHCacheBuilder.<Long, byte[]>newBuilder()
                                                   .keySerializer(keyLen <= 0 ? BenchmarkUtils.longSerializer : new BenchmarkUtils.KeySerializer(keyLen))
                                                   .valueSerializer(BenchmarkUtils.serializer)
                                                   .capacity(capacity);
            if (cmd.hasOption(LOAD_FACTOR))
                builder.loadFactor(loadFactor);
            if (cmd.hasOption(SEGMENT_COUNT))
                builder.segmentCount(segmentCount);
            if (cmd.hasOption(HASH_TABLE_SIZE))
                builder.hashTableSize(hashTableSize);
            if (cmd.hasOption(CHUNK_SIZE))
                builder.chunkSize(chunkSize);
            if (cmd.hasOption(MAX_ENTRY_SIZE))
                builder.maxEntrySize(maxEntrySize);
            if (cmd.hasOption(UNLOCKED))
                builder.unlocked(unlocked);
            if (cmd.hasOption(HASH_MODE))
                builder.hashMode(hashMode);
            if (cmd.hasOption(FIXED_KEY_SIZE))
                builder.fixedEntrySize(fixedKeySize, fixedValueSize);
            if (cmd.hasOption(EVICTION))
                builder.eviction(eviction);

            Shared.cache = builder.build();

            printMessage("Cache configuration: instance       : %s%n" +
                         "                     hash-table-size: %d%n" +
                         "                     load-factor    : %.3f%n" +
                         "                     segments       : %d%n" +
                         "                     capacity       : %d%n",
                         Shared.cache,
                         Shared.cache.hashTableSizes()[0],
                         Shared.cache.loadFactor(),
                         Shared.cache.segments(),
                         Shared.cache.capacity());

            String csvFileName = cmd.getOptionValue(CSV, null);
            PrintStream csv = null;
            if (csvFileName != null)
            {
                File csvFile = new File(csvFileName);
                csv = new PrintStream(new FileOutputStream(csvFile));
                csv.println("# OHC benchmark - http://github.com/snazy/ohc");
                csv.println("# ");
                csv.printf("# started on %s (%s)%n", InetAddress.getLocalHost().getHostName(), InetAddress.getLocalHost().getHostAddress());
                csv.println("# ");
                csv.printf("# Warum-up/sleep seconds:   %d / %d%n", warmUpSecs, coldSleepSecs);
                csv.printf("# Duration:                 %d seconds%n", duration);
                csv.printf("# Threads:                  %d%n", threads);
                csv.printf("# Capacity:                 %d bytes%n", capacity);
                csv.printf("# Read/Write Ratio:         %f%n", readWriteRatio);
                csv.printf("# Eviction:                 %s%n", eviction);
                csv.printf("# Segment Count:            %d%n", segmentCount);
                csv.printf("# Hash table size:          %d%n", hashTableSize);
                csv.printf("# Load Factor:              %f%n", loadFactor);
                csv.printf("# Additional key len:       %d%n", keyLen);
                csv.printf("# Fixed key/value size:     %d/%d%n", fixedKeySize, fixedValueSize);
                csv.printf("# Max entry size:           %d%n", maxEntrySize);
                csv.printf("# Chunk size:               %d%n", chunkSize);
                csv.printf("# Read key distribution:    '%s'%n", readKeyDistStr);
                csv.printf("# Write key distribution:   '%s'%n", writeKeyDistStr);
                csv.printf("# Value size distribution:  '%s'%n", valueSizeDistStr);
                csv.printf("# Type: %s%n", Shared.cache.getClass().getName());
                csv.println("# ");
                csv.printf("# started at %s%n", new Date());
                Properties props = System.getProperties();
                csv.printf("# java.version:             %s%n", props.get("java.version"));
                for (Map.Entry<Object, Object> e : props.entrySet())
                {
                    String k = (String) e.getKey();
                    if (k.startsWith("org.caffinitas.ohc."))
                        csv.printf("# %s: %s%n", k, e.getValue());
                }
                csv.printf("# number of cores: %d%n", Runtime.getRuntime().availableProcessors());
                csv.println("# ");
                csv.println("\"runtime\";" +
                            "\"r_count\";" +
                            "\"r_oneMinuteRate\";\"r_fiveMinuteRate\";\"r_fifteenMinuteRate\";\"r_meanRate\";" +
                            "\"r_snapMin\";\"r_snapMax\";\"r_snapMean\";\"r_snapStdDev\";" +
                            "\"r_snap75\";\"r_snap95\";\"r_snap98\";\"r_snap99\";\"r_snap999\";\"r_snapMedian\";" +
                            "\"w_count\";" +
                            "\"w_oneMinuteRate\";\"w_fiveMinuteRate\";\"w_fifteenMinuteRate\";\"w_meanRate\";" +
                            "\"w_snapMin\";\"w_snapMax\";\"w_snapMean\";\"w_snapStdDev\";" +
                            "\"w_snap75\";\"w_snap95\";\"w_snap98\";\"w_snap99\";\"w_snap999\";\"w_snapMedian\"");
            }

            printMessage("Starting benchmark with%n" +
                         "   threads     : %d%n" +
                         "   warm-up-secs: %d%n" +
                         "   idle-secs   : %d%n" +
                         "   runtime-secs: %d%n",
                         threads, warmUpSecs, coldSleepSecs, duration);

            ThreadPoolExecutor main = new ThreadPoolExecutor(threads, threads, coldSleepSecs + 1, TimeUnit.SECONDS,
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
                runFor(warmUpSecs, main, drivers, bucketHistogram, csv);
                printMessage("");
                logMemoryUse();

                if (csv != null)
                    csv.println("# warm up complete");
            }
            // cold sleep

            if (coldSleepSecs > 0)
            {
                printMessage("Warm up complete, sleep for %d seconds...", coldSleepSecs);
                Thread.sleep(coldSleepSecs * 1000L);
            }

            // benchmark

            printMessage("Start benchmark...");
            runFor(duration, main, drivers, bucketHistogram, csv);
            printMessage("");
            logMemoryUse();

            if (csv != null)
                csv.println("# benchmark complete");

            // finish

            if (csv != null)
                csv.close();

            System.exit(0);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(1);
        }
    }

    private static void runFor(int duration, ExecutorService main, Driver[] drivers, boolean bucketHistogram, PrintStream csv)
    throws InterruptedException, ExecutionException
    {

        printMessage("%s: Running for %d seconds...", new Date(), duration);

        // clear all statistics, timers, etc
        Shared.clearStats();
        for (Driver driver : drivers)
            driver.clearStats();

        long endAt = System.currentTimeMillis() + duration * 1000L;

        for (Driver driver : drivers)
        {
            driver.endAt = endAt;
            driver.future = main.submit(driver);
        }

        long mergeInterval = 500L;
        long statsInterval = 10000L;
        long nextMerge = System.currentTimeMillis() + mergeInterval;
        long nextStats = System.currentTimeMillis() + statsInterval;

        while (System.currentTimeMillis() < endAt)
        {
            if (Shared.fatal.get())
            {
                System.err.println("Unhandled exception caught - exiting");
                System.exit(1);
            }

            if (nextMerge <= System.currentTimeMillis())
                mergeTimers(drivers);

            if (nextStats <= System.currentTimeMillis())
            {
                Shared.printStats("At " + new Date(), bucketHistogram, csv);
                nextStats += statsInterval;
            }

            Thread.sleep(100);
        }

        printMessage("%s: Time over ... waiting for tasks to complete...", new Date());
        mergeTimers(drivers);

        for (Driver driver : drivers)
            driver.future.get();

        mergeTimers(drivers);
        Shared.printStats("Final", bucketHistogram, csv);
    }

    private static void mergeTimers(Driver[] drivers)
    {
        for (Driver driver : drivers)
        {
            for (int i = 0; i < Shared.timers.length; i++)
            {
                driver.timers[i].mergeTo(Shared.timers[i]);
            }
        }
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

        options.addOption(KEY_LEN, true, "key length (additional) - default: 0");
        options.addOption(VALUE_SIZE_DIST, true, "value sizes - default: " + DEFAULT_VALUE_SIZE_DIST);
        options.addOption(READ_KEY_DIST, true, "hot key use distribution - default: " + DEFAULT_KEY_DIST);
        options.addOption(WRITE_KEY_DIST, true, "hot key use distribution - default: " + DEFAULT_KEY_DIST);

        options.addOption(CHUNK_SIZE, true, "chunk size");

        options.addOption(FIXED_KEY_SIZE, true, "fixed key size");
        options.addOption(FIXED_VALUE_SIZE, true, "fixed value size");
        options.addOption(MAX_ENTRY_SIZE, true, "max entry size");

        options.addOption(UNLOCKED, true, "unlocked - do ONLY use ONE thread");
        options.addOption(HASH_MODE, true, "hash mode to use - either MURMUR3 or CRC");
        options.addOption(EVICTION, true, "enable W-TinyLFU (linked implementation only)");

        options.addOption(BUCKET_HISTOGRAM, true, "enable bucket histogram. Default: false");

        options.addOption(CSV, true, "CSV stats output file");

        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h"))
        {
            HelpFormatter formatter = new HelpFormatter();
            String help = "";
            for (String s : OptionDistribution.help())
                help = help + '\n' + s;
            formatter.printHelp(160, "BenchmarkOHC", null, options, help);
            System.exit(0);
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
