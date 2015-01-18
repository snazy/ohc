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

import org.caffinitas.ohc.benchmark.distribution.Distribution;
import org.caffinitas.ohc.benchmark.distribution.FasterRandom;

final class Driver implements Runnable
{
    final Distribution readKeyDist;
    final Distribution writeKeyDist;
    final Distribution valueSizeDist;
    final double readWriteRatio;

    final FasterRandom rnd;

    long endAt;
    Future<?> future;
    boolean stop;

    MergeableTimerSource[] timers = new MergeableTimerSource[]{
                                                              new MergeableTimerSource(),
                                                              new MergeableTimerSource() };

    Driver(Distribution readKeyDist, Distribution writeKeyDist, Distribution valueSizeDist, double readWriteRatio, long seed)
    {
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

            while (System.currentTimeMillis() < endAt)
            {
                // TODO also add a rate-limited version instead of "full speed" for both

                boolean read = rnd.nextLong() >>> 1 <= writeTrigger;

                Task task = read
                            ? new ReadTask(readKeyDist.next())
                            : new WriteTask(writeKeyDist.next(), (int) valueSizeDist.next());
                timers[task.timer()].time(task);

                if (Shared.fatal.get())
                {
                    System.err.println("Unhandled exception caught - exiting");
                    stop = true;
                    return;
                }
            }

            stop = true;
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            Shared.fatal.set(true);
            stop = true;
        }
    }

    public void clearStats()
    {
        for (MergeableTimerSource timer : timers)
            timer.clear();
    }
}
