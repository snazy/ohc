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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.UniformReservoir;

public final class MergeableTimerSource
{
    private final Clock clock;
    private final AtomicLong count;
    private final AtomicReference<Histogram> histogram;

    public MergeableTimerSource()
    {
        this.clock = Clock.defaultClock();
        this.count = new AtomicLong();
        this.histogram = new AtomicReference<>(new Histogram(new UniformReservoir()));
    }

    public <T> T time(Callable<T> event) throws Exception
    {
        final long startTime = clock.getTick();
        try
        {
            return event.call();
        }
        finally
        {
            update(clock.getTick() - startTime);
        }
    }

    public void mergeTo(MergeableTimer timer)
    {
        Histogram hist = this.histogram.getAndSet(new Histogram(new UniformReservoir()));
        for (long l : hist.getSnapshot().getValues())
            timer.histogram.update(l);
        timer.meter.mark(count.getAndSet(0L));
    }

    private void update(long duration)
    {
        if (duration >= 0)
        {
            histogram.get().update(duration);
            count.incrementAndGet();
        }
    }

    public void clear()
    {
        this.histogram.set(new Histogram(new UniformReservoir()));
        this.count.set(0L);
    }
}
