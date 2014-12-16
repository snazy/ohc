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
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.benchmark.distribution.Distribution;
import org.caffinitas.ohc.benchmark.distribution.FasterRandom;
import org.caffinitas.ohc.benchmark.distribution.OptionDistribution;

import static java.lang.Thread.sleep;

public class BenchmarkOHC
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

    public static final String DEFAULT_VALUE_SIZE_DIST = "fixed(512)";
    public static final String DEFAULT_KEY_DIST = "uniform(1..10000)";
    public static final int ONE_MB = 1024 * 1024;
    static OHCache<Long, byte[]> cache;
    static AtomicBoolean fatal = new AtomicBoolean();
    static ThreadMXBean threadMXBean;
    static final Timer readTimer = Metrics.newTimer(ReadTask.class, "reads");
    static final Timer writeTimer = Metrics.newTimer(WriteTask.class, "writes");

    static final class GCStats
    {
        final AtomicLong count = new AtomicLong();
        final AtomicLong duration = new AtomicLong();
        int cores;
    }

    static final ConcurrentHashMap<String, GCStats> gcStats = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception
    {
        try
        {
            threadMXBean = ManagementFactory.getPlatformMXBean(ThreadMXBean.class);
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            NotificationListener gcListener = new NotificationListener()
            {
                public void handleNotification(Notification notification, Object handback)
                {
                    CompositeDataSupport userData = (CompositeDataSupport) notification.getUserData();
                    String gcName = (String) userData.get("gcName");
                    CompositeDataSupport gcInfo = (CompositeDataSupport) userData.get("gcInfo");
                    Long duration = (Long) gcInfo.get("duration");
                    GCStats stats = gcStats.get(gcName);
                    if (stats == null)
                    {
                        GCStats ex = gcStats.putIfAbsent(gcName, stats = new GCStats());
                        if (ex != null)
                            stats = ex;
                    }
                    stats.count.incrementAndGet();
                    stats.duration.addAndGet(duration);
                    Number gcThreadCount = (Number) gcInfo.get("GcThreadCount");
                    if (gcThreadCount != null && gcThreadCount.intValue() > stats.cores)
                        stats.cores = gcThreadCount.intValue();
                }
            };
            for (ObjectInstance inst : mbeanServer.queryMBeans(ObjectName.getInstance("java.lang:type=GarbageCollector,name=*"), null))
                mbeanServer.addNotificationListener(inst.getObjectName(), gcListener, null, null);

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
            double loadFactor = Double.parseDouble(cmd.getOptionValue(LOAD_FACTOR, "0"));

            double readWriteRatio = Double.parseDouble(cmd.getOptionValue(READ_WRITE_RATIO, ".5"));
            Distribution readKeyDist = parseDistribution(cmd.getOptionValue(READ_KEY_DIST, DEFAULT_KEY_DIST));
            Distribution writeKeyDist = parseDistribution(cmd.getOptionValue(WRITE_KEY_DIST, DEFAULT_KEY_DIST));
            Distribution valueSizeDist = parseDistribution(cmd.getOptionValue(VALUE_SIZE_DIST, DEFAULT_VALUE_SIZE_DIST));

            printMessage("Starting benchmark with%n" +
                         "   threads     : %d%n" +
                         "   warm-up-secs: %d%n" +
                         "   idle-secs   : %d%n" +
                         "   runtime-secs: %d%n",
                         threads, warmUpSecs, coldSleepSecs, duration);

            printMessage("Initializing OHC cache...");
            cache = OHCacheBuilder.<Long, byte[]>newBuilder()
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
                         cache.hashTableSizes()[0],
                         cache.loadFactor(),
                         cache.segments(),
                         cache.capacity());

            LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(5000);
            ThreadPoolExecutor exec = new ThreadPoolExecutor(threads, threads,
                                                             0L, TimeUnit.MILLISECONDS,
                                                             queue);
            exec.prestartAllCoreThreads();

            // warm up

            if (warmUpSecs > 0)
            {
                printMessage("Start warm-up...");
                runFor(exec, warmUpSecs, readWriteRatio, readKeyDist, writeKeyDist, valueSizeDist);
                printMessage("");
                logMemoryUse(cache);
            }

            // cold sleep

            if (coldSleepSecs > 0)
            {
                printMessage("Warm up complete, sleep for %d seconds...", coldSleepSecs);
                Thread.sleep(coldSleepSecs * 1000L);
            }

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
        // java.lang.management.ThreadMXBean.getCurrentThreadCpuTime() performs better
        // (at least on OSX with single 8-core CPU). Seems that there's less contention/synchronization
        // overhead.

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

        printMessage("%s: Running for %d seconds...", new Date(), duration);

        long writeTrigger = (long) (readWriteRatio * Long.MAX_VALUE);

        // clear all statistics, timers, etc
        readTimer.clear();
        writeTimer.clear();
        gcStats.clear();
        cache.resetStatistics();

        long endAt = System.currentTimeMillis() + duration * 1000L;
        long statsInterval = 10000L;
        long nextStats = System.currentTimeMillis() + statsInterval;

        while (System.currentTimeMillis() < endAt)
        {
            // TODO also add a rate-limited version instead of "full speed" for both

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
                printStats("At " + new Date());
                nextStats += statsInterval;
            }
        }

        printMessage("%s: Time over ... waiting for %d tasks to complete...", new Date(), exec.getActiveCount());
        while (exec.getActiveCount() > 0)
            Thread.sleep(10);
        printStats("Final");
    }

    private static void printStats(String title)
    {
        printMessage("%s%n     %s entries, %d/%d free, %s", title, cache.size(), cache.freeCapacity(), cache.capacity(), cache.stats());
        for (Map.Entry<String, GCStats> gcStat : gcStats.entrySet())
        {
            GCStats gs = gcStat.getValue();
            long count = gs.count.longValue();
            long duration = gs.duration.longValue();
            double runtimeAvg = ((double) duration) / count;
            printMessage("     GC  %-15s : count: %8d    duration: %8dms (avg:%6.2fms)    cores: %d",
                         gcStat.getKey(),
                         count, duration, runtimeAvg, gs.cores);
        }
        dumpStats(readTimer, "Reads");
        dumpStats(writeTimer, "Writes");
    }

    private static void dumpStats(Timer timer, String header)
    {
        Snapshot snap = timer.getSnapshot();
        System.out.printf("     %-10s: one/five/fifteen/mean:  %.0f/%.0f/%.0f/%.0f%n" +
                          "                 count:                  %10d %n" +
                          "                 min/max/mean/stddev:    %8.5f/%8.5f/%8.5f/%8.5f [%s]%n" +
                          "                 75/95/98/99/999/median: %8.5f/%8.5f/%8.5f/%8.5f/%8.5f/%8.5f [%s]%n",
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
        return OptionDistribution.get(optionValue).get();
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

    private static void logMemoryUse(OHCache<?, ?> cache) throws Exception
    {
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
                cache.get(key);
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
