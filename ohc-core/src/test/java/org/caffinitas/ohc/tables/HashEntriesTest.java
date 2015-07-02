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

import java.nio.ByteBuffer;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class HashEntriesTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    static final long MIN_ALLOC_LEN = 128;

    @Test
    public void testInit() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        boolean ok = false;
        try
        {
            HashEntries.init(0x98765432abcddeafL, 5L, 10L, adr);

            assertEquals(Uns.getLong(adr, Util.ENTRY_OFF_HASH), 0x98765432abcddeafL);
            assertEquals(Uns.getLong(adr, Util.ENTRY_OFF_KEY_LENGTH), 5L);
            assertEquals(Uns.getLong(adr, Util.ENTRY_OFF_VALUE_LENGTH), 10L);

            assertEquals(HashEntries.getHash(adr), 0x98765432abcddeafL);
            assertEquals(HashEntries.getKeyLen(adr), 5L);
            assertEquals(HashEntries.getValueLen(adr), 10L);
            assertTrue(HashEntries.dereference(adr));
            ok = true;
        }
        finally
        {
            if (!ok)
                Uns.free(adr);
        }
    }

    @Test
    public void testCompareKey() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {
            ByteBuffer keyBuffer = Util.allocateByteBuffer(11);
            keyBuffer.putInt(0x98765432);
            keyBuffer.putInt(0xabcdabba);
            keyBuffer.put((byte)(0x44 & 0xff));
            keyBuffer.put((byte)(0x55 & 0xff));
            keyBuffer.put((byte)(0x88 & 0xff));
            KeyBuffer key = new KeyBuffer(keyBuffer.array()).finish();

            Uns.setMemory(adr, Util.ENTRY_OFF_DATA, 11, (byte) 0);

            assertFalse(HashEntries.compareKey(adr, key, 11));

            Uns.copyMemory(key.array(), 0, adr, Util.ENTRY_OFF_DATA, 11);

            assertTrue(HashEntries.compareKey(adr, key, 11));
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testCompare() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {
            long adr2 = Uns.allocate(MIN_ALLOC_LEN);
            try
            {

                Uns.setMemory(adr, 5, 11, (byte) 0);
                Uns.setMemory(adr2, 5, 11, (byte) 1);

                assertFalse(HashEntries.compare(adr, 5, adr2, 5, 11));

                assertTrue(HashEntries.compare(adr, 5, adr, 5, 11));
                assertTrue(HashEntries.compare(adr2, 5, adr2, 5, 11));

                Uns.setMemory(adr, 5, 11, (byte) 1);

                assertTrue(HashEntries.compare(adr, 5, adr2, 5, 11));
            }
            finally
            {
                Uns.free(adr2);
            }
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testGetSetLRUIndex() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {
            Uns.setMemory(adr, 0, MIN_ALLOC_LEN, (byte) 0);
            HashEntries.init(0x98765432abcddeafL, 5L, 10L, adr);

            Uns.putLong(adr, Util.ENTRY_OFF_LRU_INDEX, 0x98765432);
            assertEquals(HashEntries.getLRUIndex(adr), 0x98765432);

            HashEntries.setLRUIndex(adr, 0xfafefcfb);
            assertEquals(Uns.getLong(adr, Util.ENTRY_OFF_LRU_INDEX), 0xfafefcfb);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testGetHash() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {
            Uns.setMemory(adr, 0, MIN_ALLOC_LEN, (byte) 0);
            HashEntries.init(0x98765432abcddeafL, 5L, 10L, adr);

            assertEquals(HashEntries.getHash(adr), 0x98765432abcddeafL);

            assertEquals(Uns.getLong(adr, Util.ENTRY_OFF_HASH), 0x98765432abcddeafL);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testGetAllocLen() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {

            HashEntries.init(0x98765432abcddeafL, 0L, 10L, adr);
            assertEquals(HashEntries.getAllocLen(adr), Util.ENTRY_OFF_DATA + 10L);

            HashEntries.init(0x98765432abcddeafL, 5L, 10L, adr);
            assertEquals(HashEntries.getAllocLen(adr), Util.ENTRY_OFF_DATA + 8L + 10L);

            HashEntries.init(0x98765432abcddeafL, 8L, 10L, adr);
            assertEquals(HashEntries.getAllocLen(adr), Util.ENTRY_OFF_DATA + 8L + 10L);

            HashEntries.init(0x98765432abcddeafL, 9L, 10L, adr);
            assertEquals(HashEntries.getAllocLen(adr), Util.ENTRY_OFF_DATA + 16L + 10L);

            HashEntries.init(0x98765432abcddeafL, 15L, 10L, adr);
            assertEquals(HashEntries.getAllocLen(adr), Util.ENTRY_OFF_DATA + 16L + 10L);

            HashEntries.init(0x98765432abcddeafL, 16L, 10L, adr);
            assertEquals(HashEntries.getAllocLen(adr), Util.ENTRY_OFF_DATA + 16L + 10L);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testReferenceDereference() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        boolean ok = false;
        try
        {
            HashEntries.init(0x98765432abcddeafL, 0L, 10L, adr);

            HashEntries.reference(adr); // to 2
            HashEntries.reference(adr); // to 3
            HashEntries.reference(adr); // to 4
            assertEquals(Uns.getInt(adr, Util.ENTRY_OFF_REFCOUNT), 4);
            assertFalse(HashEntries.dereference(adr)); // to 3
            assertFalse(HashEntries.dereference(adr)); // to 2
            assertFalse(HashEntries.dereference(adr)); // to 1
            assertTrue(HashEntries.dereference(adr)); // to 0
            ok = true;
        }
        finally
        {
            if (!ok)
                Uns.free(adr);
        }
    }

    @Test
    public void testDereferenceFail() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        boolean ok = false;
        try
        {
            HashEntries.init(0x98765432abcddeafL, 0L, 10L, adr);

            assertTrue(HashEntries.dereference(adr)); // to 0
            ok = true;

            // must NOT dereference another time - memory has been free()'d !!!!
            //HashEntries.dereference(adr);
        }
        finally
        {
            if (!ok)
                Uns.free(adr);
        }
    }
}
