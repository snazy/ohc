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
package org.caffinitas.ohc;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

abstract class DataMemory
{
    final long cleanUpTriggerMinFree;
    final LongAdder freeCapacity = new LongAdder();

    protected DataMemory(long capacity, long cleanUpTriggerMinFree)
    {
        this.freeCapacity.add(capacity);
        this.cleanUpTriggerMinFree = cleanUpTriggerMinFree;
    }

    abstract long allocate(long bytes);
    abstract long free(long address);
    long freeCapacity()
    {
        return freeCapacity.longValue();
    }

    abstract long blockSize();
    abstract void close();

    abstract long getFreeListSpins();
    abstract int[] calcFreeBlockCounts();

    abstract DataManagement getDataManagement();

}
