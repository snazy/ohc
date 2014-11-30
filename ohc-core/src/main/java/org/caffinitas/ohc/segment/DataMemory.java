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
package org.caffinitas.ohc.segment;

import java.util.concurrent.atomic.LongAdder;

import org.caffinitas.ohc.api.OutOfOffHeapMemoryException;

final class DataMemory implements Constants
{
    final LongAdder freeCapacity = new LongAdder();

    DataMemory(long capacity)
    {
        this.freeCapacity.add(capacity);

        // just try to allocate the memory once to ensure that the configured capacity is available (at least during init)
        long tstAdr = Uns.allocate(capacity);
        if (tstAdr == 0L)
            throw new OutOfOffHeapMemoryException(capacity);
        Uns.free(tstAdr);
    }

    long allocate(long bytes)
    {
        bytes += ENTRY_OFF_DATA;

        freeCapacity.add(-bytes);
        if (freeCapacity.longValue() < 0L)
        {
            freeCapacity.add(bytes);
            return 0L;
        }

        long adr = Uns.allocate(bytes);
        if (adr != 0L)
        {
            Uns.putLongVolatile(adr, ENTRY_OFF_DATA_LENGTH, bytes);
            return adr;
        }

        freeCapacity.add(bytes);
        return 0L;
    }

    long free(long address)
    {
        if (address == 0L)
            throw new NullPointerException();

        long bytes = getEntryBytes(address);
        if (bytes == 0L)
            throw new IllegalStateException();
        Uns.free(address);
        freeCapacity.add(bytes);
        return bytes;
    }

    static long getEntryBytes(long address)
    {
        return Uns.getLongVolatile(address, ENTRY_OFF_DATA_LENGTH);
    }

    long freeCapacity()
    {
        return freeCapacity.longValue();
    }
}
