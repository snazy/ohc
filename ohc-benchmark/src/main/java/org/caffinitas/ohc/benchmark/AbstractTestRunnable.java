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
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;

public abstract class AbstractTestRunnable implements Callable<Void> {
    final long to;
    final long from;
    final String value_prefix;
    final Cache<String, String> cache;

    public AbstractTestRunnable(Cache<String, String> cache, long from, long to, String value_prefix) {
        this.from = from;
        this.to = to;
        this.cache = cache;
        this.value_prefix = value_prefix;
    }

    public Void call() throws Exception {
        long start = System.nanoTime();
        for (long i = from; i < to; i++) {
            operation("key-" + i, value_prefix + "-" + i);
            if (i!= from && i % 10000000 == 0)
                // printMessage("Now serving %d, after %ds", i, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start));
                System.out.print(".");
        }
        long time_taken = System.nanoTime() - start;
        message(time_taken, ((double) TimeUnit.NANOSECONDS.toMicros(time_taken))/(to - from));
        return null;
    }

    abstract void operation(String key, String value);
    abstract void message(long time_taken, double d);

    public static void printMessage(String format, Object... objects) {
        System.out.println(String.format(format, objects));
    }
}
