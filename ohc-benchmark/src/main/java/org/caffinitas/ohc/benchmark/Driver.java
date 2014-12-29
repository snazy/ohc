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

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Timer;
import org.caffinitas.ohc.benchmark.distribution.Distribution;
import org.caffinitas.ohc.benchmark.distribution.FasterRandom;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.SpmcArrayQueue;

final class Driver implements Runnable
{
    final Distribution readKeyDist;
    final Distribution writeKeyDist;
    final Distribution valueSizeDist;
    final double readWriteRatio;

    final ThreadPoolExecutor exec;
    final int threads;
    final FasterRandom rnd;

    final SpmcArrayQueue<Task> tasks;
    final MpscArrayQueue<Timing> times;

    long endAt;
    Future<?> future;
    boolean stop;

    Driver(final int driverNo, Distribution readKeyDist, Distribution writeKeyDist, Distribution valueSizeDist, double readWriteRatio, int threads, long seed)
    {
        this.threads = threads;

        tasks = new SpmcArrayQueue<>(200 * threads);
        times = new MpscArrayQueue<>(400 * threads);

        exec = new ThreadPoolExecutor(threads, threads,
                                      1, TimeUnit.SECONDS,
                                      new LinkedBlockingQueue<Runnable>(), new ThreadFactory()
        {
            volatile int threadNo;

            public Thread newThread(Runnable r)
            {
                return new Thread(r, "driver-pool-" + driverNo + '-' + threadNo++);
            }
        });
        exec.prestartAllCoreThreads();

        this.readKeyDist = readKeyDist;
        this.writeKeyDist = writeKeyDist;
        this.valueSizeDist = valueSizeDist;
        this.readWriteRatio = readWriteRatio;

        rnd = new FasterRandom();
        rnd.setSeed(seed);
    }

    public void run()
    {
        try
        {
            long writeTrigger = (long) (readWriteRatio * Long.MAX_VALUE);

            stop = false;

            for (int i = 0; i < threads; i++)
                exec.submit(new TaskRunner());

            while (System.currentTimeMillis() < endAt)
            {
                // TODO also add a rate-limited version instead of "full speed" for both

                boolean read = rnd.nextLong() >>> 1 <= writeTrigger;

                Task task = read
                            ? new ReadTask(readKeyDist.next())
                            : new WriteTask(writeKeyDist.next(), (int) valueSizeDist.next());
                while (!tasks.offer(task))
                    Thread.sleep(1);
                if (tasks.size() >= Math.min(100, threads * 25))
                    flushTimers();

                if (Shared.fatal.get())
                {
                    System.err.println("Unhandled exception caught - exiting");
                    stop = true;
                    return;
                }
            }
            flushTimers();

            stop = true;

            while (exec.getActiveCount() > 0)
                Thread.sleep(10);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            Shared.fatal.set(true);
            stop = true;
            return;
        }
    }

    void flushTimers()
    {
        Timing timing;
        while ((timing = times.poll()) != null)
            timing.timer.update(timing.time, TimeUnit.NANOSECONDS);
    }

    void shutdown()
    {
        exec.shutdown();
    }

    void terminate() throws InterruptedException
    {
        exec.awaitTermination(60, TimeUnit.SECONDS);
    }

    final class TaskRunner implements Runnable
    {
        public void run()
        {
            try
            {
                while (!stop)
                {
                    Task task = tasks.poll();
                    if (task == null)
                        Thread.sleep(1);
                    else
                        times.add(new Timing(task.run(), task.timer()));
                }
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                Shared.fatal.set(true);
            }
        }
    }

    static final class Timing
    {
        final long time;
        final Timer timer;

        Timing(long time, Timer timer)
        {
            this.time = time;
            this.timer = timer;
        }
    }
}
