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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class Signals
{
    private final Lock lock;
    private final Condition condition;
    final AtomicBoolean cleanupTrigger = new AtomicBoolean();
    final AtomicBoolean rehashTrigger = new AtomicBoolean();

    Signals()
    {
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }

    boolean waitFor()
    {
        lock.lock();
        try
        {
            if (!cleanupTrigger.get() && !rehashTrigger.get())
                condition.await();
            return true;
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return false;
        }
        finally
        {
            lock.unlock();
        }
    }

    void triggerCleanup()
    {
        if (cleanupTrigger.compareAndSet(false, true))
            signalHousekeeping();
    }

    boolean triggerRehash()
    {
        if (rehashTrigger.compareAndSet(false, true))
        {
            signalHousekeeping();
            return true;
        }
        return false;
    }

    void signalHousekeeping()
    {
        lock.lock();
        try
        {
            condition.signal();
        }
        finally
        {
            lock.unlock();
        }
    }
}
