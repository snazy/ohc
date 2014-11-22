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

import com.google.common.cache.Cache;

public class TestRunnables {
    public static class CacheInsertTest extends AbstractTestRunnable {
        public CacheInsertTest(Cache<String, String> cache, long from, long to, String value_prefix) {
            super(cache, from, to, value_prefix);
        }

        @Override
        void operation(String key, String value) {
            cache.put(key, value);
        }

        @Override
        void message(long time_taken, double d) {
            printMessage("\nInsertion Completion time: %d, avg: %.2f micro", time_taken, d);
        }
    }

    public static class CacheGetTest extends AbstractTestRunnable {
        private volatile int notFoundCount;
        private volatile int invalidValueCount;

        public CacheGetTest(Cache<String, String> cache, long from, long to, String value_prefix) {
            super(cache, from, to, value_prefix);
        }

        @Override
        void operation(String key, String expected) {
            String value = cache.getIfPresent(key);
            if (value == null) {
                notFoundCount++;
            }
            else if (!value.equals(expected)) {
                invalidValueCount++;
            }
        }

        @Override
        void message(long time_taken, double d) {
            printMessage("\nGet Completion time: %d, avg: %.2f micro, invalid-value: %d, not-found: %d", time_taken, d, invalidValueCount, notFoundCount);
        }
    }

    public static class CacheRemoveTest extends AbstractTestRunnable {
        public CacheRemoveTest(Cache<String, String> cache, long from, long to, String value_prefix) {
            super(cache, from, to, value_prefix);
        }

        @Override
        void operation(String key, String existing) {
            cache.invalidate(key);
        }

        @Override
        void message(long time_taken, double d) {
            printMessage("\nCleanup Completion time: %d, avg: %.2f micro", time_taken, d);
        }
    }
}
