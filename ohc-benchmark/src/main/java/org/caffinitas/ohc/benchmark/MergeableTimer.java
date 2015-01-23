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

import com.codahale.metrics.Clock;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.UniformReservoir;

public class MergeableTimer
{
    final Meter meter;
    final Histogram histogram;

    final long started = System.currentTimeMillis();

    public MergeableTimer()
    {
        this.meter = new Meter(Clock.defaultClock());
        this.histogram = new Histogram(new UniformReservoir());
    }

    long runtime()
    {
        return System.currentTimeMillis() - started;
    }
}
