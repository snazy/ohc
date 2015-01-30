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
package org.caffinitas.ohc.linked;

import java.nio.ByteBuffer;

import org.caffinitas.ohc.DirectValueAccess;

class DirectValueAccessImpl implements DirectValueAccess
{
    private final long hashEntryAdr;
    volatile boolean closed;
    private final ByteBuffer buffer;

    DirectValueAccessImpl(long hashEntryAdr)
    {
        this(hashEntryAdr, HashEntries.getKeyLen(hashEntryAdr), HashEntries.getValueLen(hashEntryAdr));
    }

    DirectValueAccessImpl(long hashEntryAdr, long keyLen, long valueLen)
    {
        this.hashEntryAdr = hashEntryAdr;
        this.buffer = Uns.directBufferFor(hashEntryAdr, Util.ENTRY_OFF_DATA + Util.roundUpTo8(keyLen), valueLen);
    }

    public ByteBuffer buffer()
    {
        if (closed)
            throw new IllegalStateException("already closed");
        return buffer;
    }

    public void abort()
    {
        deref();
    }

    public void close()
    {
        deref();
    }

    private void deref()
    {
        if (!closed)
        {
            closed = true;
            HashEntries.dereference(hashEntryAdr);
        }
    }
}
