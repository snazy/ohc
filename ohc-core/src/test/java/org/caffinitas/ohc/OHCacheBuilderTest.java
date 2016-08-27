package org.caffinitas.ohc;

import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.nio.ByteBuffer;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class OHCacheBuilderTest
{
    @AfterMethod
    public void clearProperties()
    {
        for (Object k : new HashSet(System.getProperties().keySet()))
        {
            String key = (String)k;
            if (key.startsWith("org.caffinitas.ohc."))
                System.getProperties().remove(key);
        }
    }

    @Test
    public void testHashTableSize() throws Exception
    {
        OHCacheBuilder<String, String> builder = OHCacheBuilder.newBuilder();
        Assert.assertEquals(builder.getHashTableSize(), 8192);
        builder.hashTableSize(12345);
        Assert.assertEquals(builder.getHashTableSize(), 12345);

        System.setProperty("org.caffinitas.ohc.hashTableSize", "98765");
        builder = OHCacheBuilder.newBuilder();
        Assert.assertEquals(builder.getHashTableSize(), 98765);
    }

    @Test
    public void testChunkSize() throws Exception
    {
        OHCacheBuilder<String, String> builder = OHCacheBuilder.newBuilder();
        Assert.assertEquals(builder.getChunkSize(), 0);
        builder.chunkSize(12345);
        Assert.assertEquals(builder.getChunkSize(), 12345);

        System.setProperty("org.caffinitas.ohc.chunkSize", "98765");
        builder = OHCacheBuilder.newBuilder();
        Assert.assertEquals(builder.getChunkSize(), 98765);
    }

    @Test
    public void testCapacity() throws Exception
    {
        int cpus = Runtime.getRuntime().availableProcessors();

        OHCacheBuilder<String, String> builder = OHCacheBuilder.newBuilder();
        Assert.assertEquals(builder.getCapacity(), Math.min(cpus * 16, 64) * 1024 * 1024);
        builder.capacity(12345);
        Assert.assertEquals(builder.getCapacity(), 12345);

        System.setProperty("org.caffinitas.ohc.capacity", "98765");
        builder = OHCacheBuilder.newBuilder();
        Assert.assertEquals(builder.getCapacity(), 98765);
    }

    @Test
    public void testSegmentCount() throws Exception
    {
        int cpus = Runtime.getRuntime().availableProcessors();
        int segments = cpus * 2;
        while (Integer.bitCount(segments) != 1)
            segments++;

        OHCacheBuilder<String, String> builder = OHCacheBuilder.newBuilder();
        Assert.assertEquals(builder.getSegmentCount(), segments);
        builder.segmentCount(12345);
        Assert.assertEquals(builder.getSegmentCount(), 12345);

        System.setProperty("org.caffinitas.ohc.segmentCount", "98765");
        builder = OHCacheBuilder.newBuilder();
        Assert.assertEquals(builder.getSegmentCount(), 98765);
    }

    @Test
    public void testLoadFactor() throws Exception
    {
        OHCacheBuilder<String, String> builder = OHCacheBuilder.newBuilder();
        Assert.assertEquals(builder.getLoadFactor(), .75f);
        builder.loadFactor(12345);
        Assert.assertEquals(builder.getLoadFactor(), 12345.0f);

        System.setProperty("org.caffinitas.ohc.loadFactor", "98765");
        builder = OHCacheBuilder.newBuilder();
        Assert.assertEquals(builder.getLoadFactor(), 98765.0f);
    }

    @Test
    public void testMaxEntrySize() throws Exception
    {
        OHCacheBuilder<String, String> builder = OHCacheBuilder.newBuilder();
        Assert.assertEquals(builder.getMaxEntrySize(), 0L);
        builder.maxEntrySize(12345);
        Assert.assertEquals(builder.getMaxEntrySize(), 12345);

        System.setProperty("org.caffinitas.ohc.maxEntrySize", "98765");
        builder = OHCacheBuilder.newBuilder();
        Assert.assertEquals(builder.getMaxEntrySize(), 98765);
    }

    @Test
    public void testThrowOOME() throws Exception
    {
        OHCacheBuilder<String, String> builder = OHCacheBuilder.newBuilder();
        Assert.assertFalse(builder.isThrowOOME());
        builder.throwOOME(true);
        Assert.assertTrue(builder.isThrowOOME());

        System.setProperty("org.caffinitas.ohc.throwOOME", "true");
        builder = OHCacheBuilder.newBuilder();
        Assert.assertTrue(builder.isThrowOOME());
    }

    @Test
    public void testExecutorService() throws Exception
    {
        OHCacheBuilder<String, String> builder = OHCacheBuilder.newBuilder();
        Assert.assertNull(builder.getExecutorService());

        ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
        builder.executorService(es);
        es.shutdown();
        Assert.assertSame(builder.getExecutorService(), es);
    }

    @Test
    public void testKeySerializer() throws Exception
    {
        OHCacheBuilder<String, String> builder = OHCacheBuilder.newBuilder();
        Assert.assertNull(builder.getKeySerializer());

        CacheSerializer<String> inst = new CacheSerializer<String>()
        {
            public void serialize(String s, ByteBuffer out)
            {

            }

            public String deserialize(ByteBuffer in)
            {
                return null;
            }

            public int serializedSize(String s)
            {
                return 0;
            }
        };
        builder.keySerializer(inst);
        Assert.assertSame(builder.getKeySerializer(), inst);
    }

    @Test
    public void testValueSerializer() throws Exception
    {
        OHCacheBuilder<String, String> builder = OHCacheBuilder.newBuilder();
        Assert.assertNull(builder.getValueSerializer());

        CacheSerializer<String> inst = new CacheSerializer<String>()
        {
            public void serialize(String s, ByteBuffer out)
            {

            }

            public String deserialize(ByteBuffer in)
            {
                return null;
            }

            public int serializedSize(String s)
            {
                return 0;
            }
        };
        builder.valueSerializer(inst);
        Assert.assertSame(builder.getValueSerializer(), inst);
    }
}
