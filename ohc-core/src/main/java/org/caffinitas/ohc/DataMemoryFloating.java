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

final class DataMemoryFloating extends DataMemory implements Constants
{
    DataMemoryFloating(long capacity, long cleanUpTriggerMinFree)
    {
        super(capacity, cleanUpTriggerMinFree);

        // just try to allocate the memory once to ensure that the configured capacity is available (at least during init)
        Uns.free(Uns.allocate(capacity));
    }

    void close()
    {
        // nop
    }

    DataManagement getDataManagement()
    {
        return DataManagement.FLOATING;
    }

    long blockSize()
    {
        return Long.MAX_VALUE;
    }

    long allocate(long bytes)
    {
        bytes += ENTRY_OFF_DATA_IN_FIRST;

        freeCapacity.add(-bytes);
        long newFree = freeCapacity.longValue();
        if (newFree < 0L)
        {
            freeCapacity.add(bytes);
            return 0L;
        }

        long adr = Uns.allocate(bytes);
        if (adr != 0L)
            Uns.putLongVolatile(adr + ENTRY_OFF_DATA_LENGTH, bytes);
        return adr;
    }

    long free(long address)
    {
        long bytes = Uns.getLongVolatile(address + ENTRY_OFF_DATA_LENGTH);
        Uns.free(address);
        freeCapacity.add(bytes);
        return bytes;
    }

    long getFreeListSpins()
    {
        return 0;
    }

    int[] calcFreeBlockCounts()
    {
        return new int[0];
    }
}
