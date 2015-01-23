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

import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;

import com.codahale.metrics.Snapshot;
import org.caffinitas.ohc.OHCache;

final class Shared
{
    private static final double NANOS_PER_MILLI = 1000000d;
    static OHCache<Long, byte[]> cache;

    static final AtomicBoolean fatal = new AtomicBoolean();
    static MergeableTimer[] timers = new MergeableTimer[]{ new MergeableTimer(), new MergeableTimer() };
    static final ConcurrentHashMap<String, GCStats> gcStats = new ConcurrentHashMap<>();

    static
    {
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
        try
        {
            for (ObjectInstance inst : mbeanServer.queryMBeans(ObjectName.getInstance("java.lang:type=GarbageCollector,name=*"), null))
                mbeanServer.addNotificationListener(inst.getObjectName(), gcListener, null, null);
        }
        catch (MalformedObjectNameException | InstanceNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    static void clearStats()
    {
        timers[0] = new MergeableTimer();
        timers[1] = new MergeableTimer();
        gcStats.clear();
        cache.resetStatistics();
    }

    static final class GCStats
    {
        final AtomicLong count = new AtomicLong();
        final AtomicLong duration = new AtomicLong();
        int cores;
    }

    static void printStats(String title, boolean bucketHistogram, PrintStream csv)
    {
        if (bucketHistogram)
            System.out.printf("%s%n     %s%n" +
                              "   Histogram:%n%s%n", title, cache.stats(), cache.getBucketHistogram());
        else
            System.out.printf("%s%n     %s%n", title, cache.stats());
        for (Map.Entry<String, GCStats> gcStat : gcStats.entrySet())
        {
            GCStats gs = gcStat.getValue();
            long count = gs.count.longValue();
            long duration = gs.duration.longValue();
            double runtimeAvg = ((double) duration) / count;
            System.out.printf("     GC  %-15s : count: %8d    duration: %8dms (avg:%6.2fms)    cores: %d%n",
                              gcStat.getKey(),
                              count, duration, runtimeAvg, gs.cores);
        }

        if (csv != null)
            csv.print(timers[0].runtime());

        dumpStats(timers[0], "Reads", csv);
        dumpStats(timers[1], "Writes", csv);

        if (csv != null)
            csv.println();
    }

    private static void dumpStats(MergeableTimer timer, String header, PrintStream csv)
    {
        Snapshot snap = timer.histogram.getSnapshot();
        double snapMean = snap.getMean() / NANOS_PER_MILLI;
        double snapStdDev = snap.getStdDev() / NANOS_PER_MILLI;
        double snap75 = snap.get75thPercentile() / NANOS_PER_MILLI;
        double snap95 = snap.get95thPercentile() / NANOS_PER_MILLI;
        double snap98 = snap.get98thPercentile() / NANOS_PER_MILLI;
        double snap99 = snap.get99thPercentile() / NANOS_PER_MILLI;
        double snap999 = snap.get999thPercentile() / NANOS_PER_MILLI;
        double snapMedian = snap.getMedian() / NANOS_PER_MILLI;
        double snapMax = (double) snap.getMax() / NANOS_PER_MILLI;
        double snapMin = (double) snap.getMin() / NANOS_PER_MILLI;
        double meanRate = timer.meter.getMeanRate();
        long count = timer.meter.getCount();
        double oneMinuteRate = timer.meter.getOneMinuteRate();
        double fiveMinuteRate = timer.meter.getFiveMinuteRate();
        double fifteenMinuteRate = timer.meter.getFifteenMinuteRate();
        System.out.printf("     %-10s: one/five/fifteen/mean:  %.0f/%.0f/%.0f/%.0f%n" +
                          "                 count:                  %10d %n" +
                          "                 min/max/mean/stddev:    %8.5f/%8.5f/%8.5f/%8.5f%n" +
                          "                 75/95/98/99/999/median: %8.5f/%8.5f/%8.5f/%8.5f/%8.5f/%8.5f%n",
                          header,
                          //
                          oneMinuteRate, fiveMinuteRate, fifteenMinuteRate, meanRate,
                          //
                          count,
                          //
                          snapMin, snapMax, snapMean, snapStdDev,
                          snap75, snap95, snap98, snap99, snap999, snapMedian);

        if (csv != null)
            csv.printf(";%d;%.0f;%.0f;%.0f;%.0f;%.5f;%.5f;%.5f;%.5f;%.5f;%.5f;%.5f;%.5f;%.5f;%.5f",
                       count,
                       oneMinuteRate, fiveMinuteRate, fifteenMinuteRate, meanRate,
                       snapMin, snapMax, snapMean, snapStdDev,
                       snap75, snap95, snap98, snap99, snap999, snapMedian);
    }
}
