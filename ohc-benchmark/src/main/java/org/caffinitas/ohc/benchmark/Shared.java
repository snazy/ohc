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

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;
import org.caffinitas.ohc.OHCache;

final class Shared
{
    static OHCache<Long, byte[]> cache;

    static final AtomicBoolean fatal = new AtomicBoolean();
    static final ThreadMXBean threadMXBean  = ManagementFactory.getPlatformMXBean(ThreadMXBean.class);
    static final Timer readTimer = Metrics.newTimer(ReadTask.class, "reads");
    static final Timer writeTimer = Metrics.newTimer(WriteTask.class, "writes");
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

    static long ntime()
    {
        // java.lang.management.ThreadMXBean.getCurrentThreadCpuTime() performs better
        // (at least on OSX with single 8-core CPU). Seems that there's less contention/synchronization
        // overhead.

        return threadMXBean.getCurrentThreadCpuTime();
//        return System.nanoTime();
    }

    static void clearStats()
    {
        readTimer.clear();
        writeTimer.clear();
        gcStats.clear();
        cache.resetStatistics();
    }

    static final class GCStats
    {
        final AtomicLong count = new AtomicLong();
        final AtomicLong duration = new AtomicLong();
        int cores;
    }

    static void printStats(String title)
    {
        System.out.println(String.format("%s%n     %s", title, cache.stats()));
        for (Map.Entry<String, GCStats> gcStat : gcStats.entrySet())
        {
            GCStats gs = gcStat.getValue();
            long count = gs.count.longValue();
            long duration = gs.duration.longValue();
            double runtimeAvg = ((double) duration) / count;
            System.out.println(String.format("     GC  %-15s : count: %8d    duration: %8dms (avg:%6.2fms)    cores: %d",
                                             gcStat.getKey(),
                                             count, duration, runtimeAvg, gs.cores));
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
}
