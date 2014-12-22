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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.caffinitas.ohc.benchmark.distribution.Distribution;
import org.caffinitas.ohc.benchmark.distribution.FasterRandom;

final class Driver implements Runnable
{
    final Distribution readKeyDist;
    final Distribution writeKeyDist;
    final Distribution valueSizeDist;
    final double readWriteRatio;

    final LinkedBlockingQueue<Runnable> queue;
    final ThreadPoolExecutor exec;
    final FasterRandom rnd;

    long endAt;
    Future<?> future;

    Driver(Distribution readKeyDist, Distribution writeKeyDist, Distribution valueSizeDist, double readWriteRatio, int threads, long seed)
    {
        queue = new LinkedBlockingQueue<>(5000);
        exec = new ThreadPoolExecutor(threads, threads,
                                                         0L, TimeUnit.MILLISECONDS,
                                                         queue);
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

            while (System.currentTimeMillis() < endAt)
            {
                // TODO also add a rate-limited version instead of "full speed" for both

                boolean read = rnd.nextLong() >>> 1 <= writeTrigger;

                if (read)
                    submit(queue, new ReadTask(readKeyDist.next()));
                else
                    submit(queue, new WriteTask(writeKeyDist.next(), (int) valueSizeDist.next()));

                if (Shared.fatal.get())
                {
                    System.err.println("Unhandled exception caught - exiting");
                    return;
                }
            }

            while (exec.getActiveCount() > 0)
                Thread.sleep(10);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            Shared.fatal.set(true);
            return;
        }
    }

    private static void submit(BlockingQueue<Runnable> queue, Runnable task) throws InterruptedException
    {
        while (true)
            if (queue.offer(task, 10, TimeUnit.MILLISECONDS))
                return;
    }

    void shutdown()
    {
        exec.shutdown();
    }

    void terminate() throws InterruptedException
    {
        exec.awaitTermination(60, TimeUnit.SECONDS);
    }
}
