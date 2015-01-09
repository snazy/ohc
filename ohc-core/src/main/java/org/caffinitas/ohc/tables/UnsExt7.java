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
package org.caffinitas.ohc.tables;

import sun.misc.Unsafe;

final class UnsExt7 extends UnsExt
{
    UnsExt7(Unsafe unsafe)
    {
        super(unsafe);
    }

    long getAndPutLong(long address, long offset, long value)
    {
        long r = unsafe.getLong(null, address + offset);
        unsafe.putLong(null, address + offset, value);
        return r;
    }

    long getAndAddLong(long address, long offset, long value)
    {
        address += offset;
        long v;
        while (true)
        {
            v = unsafe.getLongVolatile(null, address);
            if (unsafe.compareAndSwapLong(null, address, v, v + value))
                return v;
        }
    }

    int getAndPutInt(long address, long offset, int value)
    {
        int r = unsafe.getInt(null, address + offset);
        unsafe.putInt(null, address + offset, value);
        return r;
    }

    int getAndAddInt(long address, long offset, int value)
    {
        address += offset;
        int v;
        while (true)
        {
            v = unsafe.getIntVolatile(null, address);
            if (unsafe.compareAndSwapInt(null, address, v, v + value))
                return v;
        }
    }
}
