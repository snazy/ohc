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

import java.util.concurrent.atomic.LongAdder;

final class DataMemory implements Constants
{
    final long cleanUpTriggerMinFree;
    final LongAdder freeCapacity = new LongAdder();

    DataMemory(long capacity, long cleanUpTriggerMinFree)
    {
        this.freeCapacity.add(capacity);
        this.cleanUpTriggerMinFree = cleanUpTriggerMinFree;

        // just try to allocate the memory once to ensure that the configured capacity is available (at least during init)
        Uns.free(Uns.allocate(capacity));
    }

    long allocate(long bytes)
    {
        bytes += ENTRY_OFF_DATA;

        freeCapacity.add(-bytes);
        long newFree = freeCapacity.longValue();
        if (newFree < 0L)
        {
            freeCapacity.add(bytes);
            return 0L;
        }

        long adr = Uns.allocate(bytes);
        if (adr != 0L)
        {
            Uns.putLongVolatile(adr + ENTRY_OFF_DATA_LENGTH, bytes);
            return adr;
        }

        freeCapacity.add(bytes);
        return 0L;
    }

    long free(long address)
    {

        // TODO queue 'free' requests and execute them async - seems to take very long (at least JEMalloc) !

        long bytes = Uns.getLongVolatile(address + ENTRY_OFF_DATA_LENGTH);
        Uns.free(address);
        freeCapacity.add(bytes);
        return bytes;
    }

    long freeCapacity()
    {
        return freeCapacity.longValue();
    }
}
