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

import java.lang.reflect.Field;

import sun.misc.Unsafe;

public class CasLock
{

    private static final Unsafe unsafe;

    private static final long lockOff;

    static
    {
        try
        {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);

            lockOff = unsafe.objectFieldOffset(CasLock.class.getDeclaredField("lock"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private long lock;

    public void lock()
    {
        long tid = Thread.currentThread().getId();
        while (true)
        {
            if (unsafe.compareAndSwapLong(this, lockOff, 0L, tid))
                return;
            unsafe.park(false, 10000);
        }
    }

    public void unlock()
    {
        long tid = Thread.currentThread().getId();
        while (true)
        {
            if (unsafe.compareAndSwapLong(this, lockOff, tid, 0L))
                return;
            unsafe.park(false, 100);
        }
    }
}
