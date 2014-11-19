package org.caffinitas.ohc;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OHCacheTest
{
    @Test
    public void basic() throws IOException
    {
        try (OHCache cache = OHCacheBuilder.newBuilder()
                                      .build())
        {
            Assert.assertEquals(cache.calcFreeBlockCount(), cache.getTotalCapacity() / cache.getBlockSize() - cache.getHashTableSize());

            cache.put(0, new BytesSource()
            {
                public int size()
                {
                    return 3210;
                }

                public byte getByte(int pos)
                {
                    return 0;
                }
            }, new BytesSource()
            {
                public int size()
                {
                    return 7654;
                }

                public byte getByte(int pos)
                {
                    return 0;
                }
            });
        }
    }
}
