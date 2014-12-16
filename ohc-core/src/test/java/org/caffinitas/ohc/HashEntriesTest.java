package org.caffinitas.ohc;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class HashEntriesTest
{
    static final long MIN_ALLOC_LEN = 128;

    @Test
    public void testInit() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
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
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testCompareKey() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {
            KeyBuffer key = new KeyBuffer(11);
            key.writeInt(0x98765432);
            key.writeInt(0xabcdabba);
            key.write(0x44);
            key.write(0x55);
            key.write(0x88);
            key.finish();

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
    public void testGetSetLRUNext() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {
            Uns.setMemory(adr, 0, MIN_ALLOC_LEN, (byte) 0);
            HashEntries.init(0x98765432abcddeafL, 5L, 10L, adr);

            Uns.putLong(adr, Util.ENTRY_OFF_LRU_NEXT, 0x98765432abdffeedL);
            assertEquals(HashEntries.getLRUNext(adr), 0x98765432abdffeedL);

            HashEntries.setLRUNext(adr, 0xfafefcfb23242526L);
            assertEquals(Uns.getLong(adr, Util.ENTRY_OFF_LRU_NEXT), 0xfafefcfb23242526L);
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test
    public void testGetSetLRUPrev() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {
            Uns.setMemory(adr, 0, MIN_ALLOC_LEN, (byte) 0);
            HashEntries.init(0x98765432abcddeafL, 5L, 10L, adr);

            Uns.putLong(adr, Util.ENTRY_OFF_LRU_PREV, 0x98765432abdffeedL);
            assertEquals(HashEntries.getLRUPrev(adr), 0x98765432abdffeedL);

            HashEntries.setLRUPrev(adr, 0xfafefcfb23242526L);
            assertEquals(Uns.getLong(adr, Util.ENTRY_OFF_LRU_PREV), 0xfafefcfb23242526L);
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
    public void testGetSetNext() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {
            Uns.setMemory(adr, 0, MIN_ALLOC_LEN, (byte) 0);
            HashEntries.init(0x98765432abcddeafL, 5L, 10L, adr);

            Uns.putLong(adr, Util.ENTRY_OFF_NEXT, 0x98765432abdffeedL);
            assertEquals(HashEntries.getNext(adr), 0x98765432abdffeedL);

            HashEntries.setNext(adr, 0xfafefcfb23242526L);
            assertEquals(Uns.getLong(adr, Util.ENTRY_OFF_NEXT), 0xfafefcfb23242526L);
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
        try
        {
            HashEntries.init(0x98765432abcddeafL, 0L, 10L, adr);

            HashEntries.reference(adr); // to 2
            HashEntries.reference(adr); // to 3
            HashEntries.reference(adr); // to 4
            assertEquals(Uns.getLong(adr, Util.ENTRY_OFF_REFCOUNT), 4);
            assertFalse(HashEntries.dereference(adr)); // to 3
            assertFalse(HashEntries.dereference(adr)); // to 2
            assertFalse(HashEntries.dereference(adr)); // to 1
            assertTrue(HashEntries.dereference(adr)); // to 0
        }
        finally
        {
            Uns.free(adr);
        }
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDereferenceFail() throws Exception
    {
        long adr = Uns.allocate(MIN_ALLOC_LEN);
        try
        {
            HashEntries.init(0x98765432abcddeafL, 0L, 10L, adr);

            assertTrue(HashEntries.dereference(adr)); // to 0

            HashEntries.dereference(adr);
        }
        finally
        {
            Uns.free(adr);
        }
    }
}