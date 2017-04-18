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
package org.caffinitas.ohc.alloc;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

public final class UnsafeAllocator implements IAllocator
{
    static final Unsafe unsafe;

    static
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    public long allocate(long size)
    {
        try
        {
            return unsafe.allocateMemory(size);
        }
        catch (OutOfMemoryError oom)
        {
            return 0L;
        }
    }

    public void free(long peer)
    {
        unsafe.freeMemory(peer);
    }

    public long getTotalAllocated()
    {
        return -1L;
    }
}
