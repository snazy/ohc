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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.cache.Cache;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;
import org.caffinitas.ohc.DataManagement;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.benchmark.distribution.Distribution;
import org.caffinitas.ohc.benchmark.distribution.DistributionFactory;
import org.caffinitas.ohc.benchmark.distribution.FasterRandom;
import org.caffinitas.ohc.benchmark.distribution.OptionDistribution;

import static java.lang.Thread.sleep;

public class BenchmarkOHC
{
    public static final String THREADS = "t";
    public static final String SIZE = "s";
    public static final String DURATION = "d";
    public static final String HASH_TABLE_SIZE = "z";
    public static final String BLOCK_SIZE = "b";
    public static final String WARM_UP = "wu";
    public static final String READ_WRITE_RATIO = "r";
    public static final String READ_KEY_DIST = "rkd";
    public static final String WRITE_KEY_DIST = "wkd";
    public static final String VALUE_SIZE_DIST = "vs";

    public static final String DEFAULT_VALUE_SIZE_DIST = "fixed(512)";
    public static final String DEFAULT_KEY_DIST = "uniform(1..10000)";
    static OHCache<Long, byte[]> cache;
    static AtomicBoolean fatal = new AtomicBoolean();
    static ThreadMXBean threadMXBean;
    static final Timer readTimer = Metrics.newTimer(ReadTask.class, "reads");
    static final Timer writeTimer = Metrics.newTimer(WriteTask.class, "writes");

    public static void main(String[] args) throws Exception
    {
        try
        {
            threadMXBean = ManagementFactory.getPlatformMXBean(ThreadMXBean.class);

            CommandLine cmd = parseArguments(args);

            String[] warmUp = cmd.getOptionValue(WARM_UP, "15,5").split(",");
            int warmUpSecs = Integer.parseInt(warmUp[0]);
            int coldSleepSecs = Integer.parseInt(warmUp[1]);

            int duration = Integer.parseInt(cmd.getOptionValue(DURATION, "60"));
            int threads = Integer.parseInt(cmd.getOptionValue(THREADS, "10"));
            long size = Long.parseLong(cmd.getOptionValue(SIZE, "" + (1024 * 1024 * 1024)));
            int hashTableSize = Integer.parseInt(cmd.getOptionValue(HASH_TABLE_SIZE, "64"));
            int blockSize = Integer.parseInt(cmd.getOptionValue(BLOCK_SIZE, "1024"));

            double readWriteRatio = Double.parseDouble(cmd.getOptionValue(READ_WRITE_RATIO, ".5"));
            Distribution readKeyDist = parseDistribution(cmd.getOptionValue(READ_KEY_DIST, DEFAULT_KEY_DIST));
            Distribution writeKeyDist = parseDistribution(cmd.getOptionValue(WRITE_KEY_DIST, DEFAULT_KEY_DIST));
            Distribution valueSizeDist = parseDistribution(cmd.getOptionValue(VALUE_SIZE_DIST, DEFAULT_VALUE_SIZE_DIST));

            printMessage("Initializing OHC cache...");
            cache = OHCacheBuilder.<Long, byte[]>newBuilder()
                                  .keySerializer(BenchmarkUtils.longSerializer)
                                  .valueSerializer(BenchmarkUtils.serializer)
                                  .hashTableSize(hashTableSize)
                                  .blockSize(blockSize)
                                  .capacity(size)
                                  .dataManagement(DataManagement.FLOATING)
                                  .build();

            printMessage("Cache configuration: hash-table-size: %d%n" +
                         "                     block-size     : %d%n" +
                         "                     capacity       : %d%n" +
                         "                     data-management: %s%n",
                         cache.getHashTableSize(),
                         cache.getBlockSize(),
                         cache.getCapacity(),
                         cache.getDataManagement());

            LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(10000);
            ThreadPoolExecutor exec = new ThreadPoolExecutor(threads, threads,
                                                             0L, TimeUnit.MILLISECONDS,
                                                             queue);
            exec.prestartAllCoreThreads();

            // warm up

            printMessage("Start warm-up...");
            runFor(exec, warmUpSecs, readWriteRatio, readKeyDist, writeKeyDist, valueSizeDist);
            printMessage("");
            logMemoryUse(cache);

            // cold sleep

            printMessage("Warm up complete, sleep for %d seconds...", coldSleepSecs);
            Thread.sleep(coldSleepSecs * 1000L);

            // benchmark

            printMessage("Start benchmark...");
            runFor(exec, duration, readWriteRatio, readKeyDist, writeKeyDist, valueSizeDist);
            printMessage("");
            logMemoryUse(cache);

            // finish

            exec.shutdown();
            if (!exec.awaitTermination(60, TimeUnit.SECONDS))
                throw new RuntimeException("Executor thread pool did not terminate...");

            System.exit(0);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(1);
        }
    }

    static long ntime()
    {
        return threadMXBean.getCurrentThreadCpuTime();
//        return System.nanoTime();
    }

    private static void runFor(ThreadPoolExecutor exec, int duration,
                               double readWriteRatio,
                               Distribution readKeyDist, Distribution writeKeyDist,
                               Distribution valueSizeDist) throws InterruptedException
    {

        FasterRandom rnd = new FasterRandom();
        rnd.setSeed(new Random().nextLong());

        printMessage("Running for %d seconds...", duration);

        long writeTrigger = (long) (readWriteRatio * Long.MAX_VALUE);

        readTimer.clear();
        writeTimer.clear();

        long endAt = System.currentTimeMillis() + duration * 1000L;
        long statsInterval = 10000L;
        long nextStats = System.currentTimeMillis() + statsInterval;

        while (System.currentTimeMillis() < endAt)
        {
            boolean read = rnd.nextLong() >>> 1 <= writeTrigger;

            if (read)
                submit(exec, new ReadTask(readKeyDist.next()));
            else
                submit(exec, new WriteTask(writeKeyDist.next(), (int) valueSizeDist.next()));

            if (fatal.get())
            {
                System.err.println("Unhandled exception caught - exiting");
                System.exit(1);
            }

            if (nextStats <= System.currentTimeMillis())
            {
                dumpStats(readTimer, "Reads");
                dumpStats(writeTimer, "Writes");
                nextStats += statsInterval;
            }
        }

        printMessage("Time over ... waiting for tasks to complete...");
        while (exec.getActiveCount() > 0)
        {
            Thread.sleep(10);
        }
    }

    private static void dumpStats(Timer timer, String header)
    {
        Snapshot snap = timer.getSnapshot();
        System.out.printf("%-10s: one/five/fifteen/mean:  %.0f/%.0f/%.0f/%.0f%n" +
                          "            count:                  %10d %n" +
                          "            min/max/mean/stddev:    %8.5f/%8.5f/%8.5f/%8.5f [%s]%n" +
                          "            75/95/98/99/999/median: %8.5f/%8.5f/%8.5f/%8.5f/%8.5f/%8.5f [%s]%n",
                          header,
                          //
                          timer.oneMinuteRate(),
                          timer.fiveMinuteRate(),
                          timer.fifteenMinuteRate(),
                          timer.meanRate(),
                          //
                          timer.count(),
                          //
                          timer.min(),
                          timer.max(),
                          timer.mean(),
                          timer.stdDev(),
                          timer.durationUnit(),
                          snap.get75thPercentile(),
                          snap.get95thPercentile(),
                          snap.get98thPercentile(),
                          snap.get99thPercentile(),
                          snap.get999thPercentile(),
                          snap.getMedian(),
                          timer.durationUnit());
    }

    private static void submit(ThreadPoolExecutor exec, Runnable task) throws InterruptedException
    {
        while (true)
            try
            {
                exec.execute(task);
                return;
            }
            catch (RejectedExecutionException e)
            {
                Thread.sleep(10);
            }
    }

    private static Distribution parseDistribution(String optionValue)
    {
        DistributionFactory df = OptionDistribution.get(optionValue);
        return df.get();
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

        options.addOption(SIZE, true, "size of the cache");

        options.addOption(HASH_TABLE_SIZE, true, "hash table size");
        options.addOption(BLOCK_SIZE, true, "data block size");

        options.addOption(VALUE_SIZE_DIST, true, "value sizes - default: " + DEFAULT_VALUE_SIZE_DIST);
        options.addOption(READ_KEY_DIST, true, "hot key use distribution - default: " + DEFAULT_KEY_DIST);
        options.addOption(WRITE_KEY_DIST, true, "hot key use distribution - default: " + DEFAULT_KEY_DIST);

        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h"))
        {
            HelpFormatter formatter = new HelpFormatter();
            String help = "";
            for (String s : OptionDistribution.help())
                help = help + "\n" + s;
            formatter.printHelp(160, "BenchmarkOHC", null, options, help);
            System.exit(-1);
        }

        return cmd;
    }

    private static void logMemoryUse(Cache<?, ?> cache) throws Exception
    {
        if (cache instanceof OHCache)
        {
            sleep(100);
            printMessage("Memory consumed: %s / %s, size %d%n" +
                         "          stats: %s",
                         FileUtils.byteCountToDisplaySize(((OHCache) cache).getMemUsed()),
                         FileUtils.byteCountToDisplaySize(((OHCache) cache).getCapacity()),
                         cache.size(),
                         ((OHCache) cache).extendedStats());
            printMessage("");
        }
        printMessage("VM total:%s", FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory()));
        printMessage("VM free:%s", FileUtils.byteCountToDisplaySize(Runtime.getRuntime().freeMemory()));
        if (cache instanceof OHCache)
            printMessage("Cache stats:%s", ((OHCache) cache).extendedStats());
        else
            printMessage("Cache stats:%s", cache.stats());
    }

    private static class ReadTask implements Runnable
    {
        private final long key;

        public ReadTask(long key)
        {
            this.key = key;
        }

        public void run()
        {
            try
            {
                long t0 = ntime();
                cache.getIfPresent(key);
                long t = ntime() - t0;
                readTimer.update(t, TimeUnit.NANOSECONDS);
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                fatal.set(true);
            }
        }
    }

    private static class WriteTask implements Runnable
    {
        private final long key;
        private final int valueLen;

        public WriteTask(long key, int valueLen)
        {
            this.key = key;
            this.valueLen = valueLen;
        }

        public void run()
        {
            try
            {
                long t0 = ntime();
                cache.put(key, new byte[valueLen]);
                long t = ntime() - t0;
                writeTimer.update(t, TimeUnit.NANOSECONDS);
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                fatal.set(true);
            }
        }
    }

    public static void printMessage(String format, Object... objects)
    {
        System.out.println(String.format(format, objects));
    }
}
