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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OHCTest
{

    public static final long ONE_MB = 1024 * 1024;
    public static final CacheSerializer<String> stringSerializer = new CacheSerializer<String>()
    {
        public void serialize(String s, DataOutput out) throws IOException
        {
            out.writeUTF(s);
        }

        public String deserialize(DataInput in) throws IOException
        {
            return in.readUTF();
        }

        public int serializedSize(String s)
        {
            return writeUTFLen(s);
        }

        private int writeUTFLen(String str)
        {
            int strlen = str.length();
            int utflen = 0;
            int c;

            for (int i = 0; i < strlen; i++)
            {
                c = str.charAt(i);
                if ((c >= 0x0001) && (c <= 0x007F))
                    utflen++;
                else if (c > 0x07FF)
                    utflen += 3;
                else
                    utflen += 2;
            }

            if (utflen > 65535)
                throw new RuntimeException("encoded string too long: " + utflen + " bytes");

            return utflen + 2;
        }
    };
    public static final CacheSerializer<Integer> complexSerializer = new CacheSerializer<Integer>()
    {
        public void serialize(Integer s, DataOutput out) throws IOException
        {
            out.writeBoolean(true);
            out.writeByte(1);
            out.writeChar('A');
            out.writeDouble(42.42424242d);
            out.writeFloat(11.111f);
            out.writeInt(s);
            out.writeLong(Long.MAX_VALUE);
            out.writeShort(0x7654);
        }

        public Integer deserialize(DataInput in) throws IOException
        {
            Assert.assertEquals(in.readBoolean(), true);
            Assert.assertEquals(in.readByte(), (byte) 1);
            Assert.assertEquals(in.readChar(), 'A');
            Assert.assertEquals(in.readDouble(), 42.42424242d);
            Assert.assertEquals(in.readFloat(), 11.111f);
            int r = in.readInt();
            Assert.assertEquals(in.readLong(), Long.MAX_VALUE);
            Assert.assertEquals(in.readShort(), 0x7654);
            return r;
        }

        public int serializedSize(Integer s)
        {
            return 30;
        }
    };

    private static String big;
    private static String bigRandom;
    static int manyCount = 100000;

    static
    {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++)
            sb.append("the quick brown fox jumps over the lazy dog");
        big = sb.toString();

        Random r = new Random();
        sb.setLength(0);
        for (int i = 0; i < 30000; i++)
            sb.append((char) (r.nextInt(99) + 31));
        bigRandom = sb.toString();
    }

    @Test
    public void basic() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .build())
        {
            Assert.assertEquals(cache.getFreeCapacity(), cache.getCapacity());

            cache.put(11, "hello world \u00e4\u00f6\u00fc\u00df");

            Assert.assertTrue(cache.getFreeCapacity() < cache.getCapacity());

            String v = cache.getIfPresent(11);
            Assert.assertEquals(v, "hello world \u00e4\u00f6\u00fc\u00df");

            cache.remove(11);

            Assert.assertEquals(cache.getFreeCapacity(), cache.getCapacity());

            fill(cache);

            check(cache);
        }
    }

    @Test(dependsOnMethods = "basic")
    public void manyValues() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .capacity(32L * 1024 * 1024)
                                                           .hashTableSize(64)
                                                           .build())
        {
            Assert.assertEquals(cache.getFreeCapacity(), cache.getCapacity());

            fillMany(cache);

            OHCacheStats stats = cache.stats();
            Assert.assertEquals(stats.getPutAddCount(), manyCount);
            Assert.assertEquals(stats.getSize(), manyCount);

            for (int i = 0; i < manyCount; i++)
                Assert.assertEquals(cache.getIfPresent(i), Integer.toHexString(i));

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), manyCount);
            Assert.assertEquals(stats.getSize(), manyCount);

            for (int i = 0; i < manyCount; i++)
                cache.put(i, Integer.toOctalString(i));

            stats = cache.stats();
            Assert.assertEquals(stats.getPutReplaceCount(), manyCount);
            Assert.assertEquals(stats.getSize(), manyCount);

            for (int i = 0; i < manyCount; i++)
                Assert.assertEquals(cache.getIfPresent(i), Integer.toOctalString(i));

            stats = cache.stats();
            Assert.assertEquals(stats.getHitCount(), manyCount * 2);
            Assert.assertEquals(stats.getSize(), manyCount);

            for (int i = 0; i < manyCount; i++)
                cache.remove(i);

            stats = cache.stats();
            Assert.assertEquals(stats.getRemoveCount(), manyCount);
            Assert.assertEquals(stats.getSize(), 0);
            Assert.assertEquals(stats.getFree(), stats.getCapacity());
        }
    }

    @Test(dependsOnMethods = "basic")
    public void keyIterator() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .capacity(32L * 1024 * 1024)
                                                           .build())
        {
            long capacity = cache.getCapacity();
            Assert.assertEquals(cache.getFreeCapacity(), capacity);

            fill(cache);

            Set<Integer> returned = new TreeSet<>();
            Iterator<Integer> iter = cache.keyIterator();
            for (int i = 0; i < 5; i++)
            {
                Assert.assertTrue(iter.hasNext());
                returned.add(iter.next());
            }
            Assert.assertFalse(iter.hasNext());
            Assert.assertEquals(returned.size(), 5);

            Assert.assertTrue(returned.contains(1));
            Assert.assertTrue(returned.contains(2));
            Assert.assertTrue(returned.contains(3));
            Assert.assertTrue(returned.contains(4));
            Assert.assertTrue(returned.contains(5));

            returned.clear();

            iter = cache.keyIterator();
            for (int i = 0; i < 5; i++)
                returned.add(iter.next());
            Assert.assertFalse(iter.hasNext());
            Assert.assertEquals(returned.size(), 5);

            Assert.assertTrue(returned.contains(1));
            Assert.assertTrue(returned.contains(2));
            Assert.assertTrue(returned.contains(3));
            Assert.assertTrue(returned.contains(4));
            Assert.assertTrue(returned.contains(5));

            iter = cache.keyIterator();
            for (int i = 0; i < 5; i++)
            {
                iter.next();
                iter.remove();
            }

            Assert.assertEquals(cache.getFreeCapacity(), capacity);

            Assert.assertEquals(0, cache.size());
            Assert.assertNull(cache.getIfPresent(1));
            Assert.assertNull(cache.getIfPresent(2));
            Assert.assertNull(cache.getIfPresent(3));
            Assert.assertNull(cache.getIfPresent(4));
            Assert.assertNull(cache.getIfPresent(5));
        }
    }

    @Test(dependsOnMethods = "manyValues")
    public void directIO() throws IOException, InterruptedException
    {
        File f = File.createTempFile("OHCBasicTestDirectIO-", ".bin");
        f.deleteOnExit();

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .build())
        {
            fillMany(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                cache.serializeHotN(manyCount, ch);
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .build())
        {
            int count;

            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                count = cache.deserializeEntries(ch);
            }

            checkManyForSerialized(cache, count);
        }
    }

    @Test(dependsOnMethods = "directIO")
    public void directIOBig() throws IOException, InterruptedException
    {
        File f = File.createTempFile("OHCBasicTestDirectIOBig-", ".bin");
        f.deleteOnExit();

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            fillBig(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                cache.serializeHotN(100, ch);
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            int count;
            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                count = cache.deserializeEntries(ch);
            }

            Assert.assertEquals(5, count);

            checkBig(cache);
        }
    }

    @Test(dependsOnMethods = "directIO")
    public void directIOBigRandom() throws IOException, InterruptedException
    {
        File f = File.createTempFile("OHCBasicTestDirectIOBigRandom-", ".bin");
        f.deleteOnExit();

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            fillBigRandom(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                cache.serializeHotN(5, ch);
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            int count;
            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                count = cache.deserializeEntries(ch);
            }

            Assert.assertEquals(5, count);

            checkBigRandom(cache);
        }
    }

    private void fillMany(OHCache<Integer, String> cache)
    {
        for (int i = 0; i < manyCount; i++)
            cache.put(i, Integer.toHexString(i));
    }

    @Test(dependsOnMethods = "directIO")
    public void compressedDirectIO() throws IOException, InterruptedException
    {
        File f = File.createTempFile("OHCBasicTestDirectIO-", ".bin");
        f.deleteOnExit();

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .build())
        {
            fillMany(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                try (CompressingOutputChannel cch = new CompressingOutputChannel(ch, 8192))
                {
                    cache.serializeHotN(manyCount, cch);
                }
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .build())
        {
            int count;
            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                try (DecompressingInputChannel dch = new DecompressingInputChannel(ch))
                {
                    count = cache.deserializeEntries(dch);
                }
            }

            checkManyForSerialized(cache, count);
        }
    }

    private void checkManyForSerialized(OHCache<Integer, String> cache, int count)
    {
        Assert.assertTrue(count > manyCount * 9 / 10, "count=" + count); // allow some variation

        int found = 0;
        for (int i = 0; i < manyCount; i++)
        {
            String v = cache.getIfPresent(i);
            if (v != null)
            {
                Assert.assertEquals(v, Integer.toHexString(i));
                found++;
            }
        }

        Assert.assertEquals(found, count);
    }

    @Test(dependsOnMethods = "compressedDirectIO")
    public void compressedDirectIOBig() throws IOException, InterruptedException
    {
        File f = File.createTempFile("OHCBasicTestCompressedDirectIOBig-", ".bin");
        f.deleteOnExit();

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            fillBig(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                try (CompressingOutputChannel cch = new CompressingOutputChannel(ch, 8192))
                {
                    cache.serializeHotN(100, cch);
                }
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            int count;
            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                try (DecompressingInputChannel dch = new DecompressingInputChannel(ch))
                {
                    count = cache.deserializeEntries(dch);
                }
            }

            Assert.assertEquals(5, count);

            checkBig(cache);
        }
    }

    @Test(dependsOnMethods = "compressedDirectIO")
    public void compressedDirectIOBigRandom() throws IOException, InterruptedException
    {
        File f = File.createTempFile("OHCBasicTestCompressedDirectIOBigRandom-", ".bin");
        f.deleteOnExit();

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            fillBigRandom(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                try (CompressingOutputChannel cch = new CompressingOutputChannel(ch, 8192))
                {
                    cache.serializeHotN(100, cch);
                }
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            int count;
            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                try (DecompressingInputChannel dch = new DecompressingInputChannel(ch))
                {
                    count = cache.deserializeEntries(dch);
                }
            }

            Assert.assertEquals(5, count);

            checkBigRandom(cache);
        }
    }

    @Test(dependsOnMethods = "basic")
    public void hotN() throws IOException, InterruptedException
    {
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .build())
        {
            fill(cache);

            Assert.assertNotNull(cache.hotN(1).next());
        }
    }

    @Test(dependsOnMethods = "manyValues")
    public void cleanUpTest() throws IOException, InterruptedException
    {
        char[] chars = new char[900];
        for (int i = 0; i < chars.length; i++)
            chars[i] = (char) ('A' + i % 26);
        String v = new String(chars);

        // Build cache with 64MB capacity and trigger on less than 8 MB free capacity
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .segmentCount(1)
                                                           .capacity(32 * ONE_MB)
                                                           .cleanUpTriggerFree(.125d)
                                                           .build())
        {
            int i;
            for (i = 0; cache.getFreeCapacity() > 4 * ONE_MB + 1000; i++)
            {
                cache.put(i, v);
                if ((i % 10000) == 0)
                    Assert.assertEquals(cache.stats().getCleanupCount(), 0L, "oops - cleanup triggered - fix the unit test!");
            }

            Assert.assertEquals(cache.stats().getCleanupCount(), 0L, "oops - cleanup triggered - fix the unit test!");

            cache.put(i, v);

            Assert.assertEquals(cache.stats().getCleanupCount(), 1L, "cleanup did not run");
            Assert.assertEquals(cache.stats().getEvictionCount(), 1L, "cleanup did not run");
        }
    }

    @Test(dependsOnMethods = "basic")
    public void putTooLarge() throws IOException, InterruptedException
    {
        char[] c940 = new char[8192];
        for (int i = 0; i < c940.length; i++)
            c940[i] = (char) ('A' + i % 26);
        String v = new String(c940);

        // Build cache with 64MB capacity and trigger on less than 8 MB free capacity
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(complexSerializer)
                                                           .valueSerializer(stringSerializer)
                                                           .segmentCount(1)
                                                           .capacity(ONE_MB)
                                             .maxEntrySize((double) ONE_MB / 128) // == 8kB
                                             .cleanUpTriggerFree(.125d)
                                             .build())
        {
            cache.put(88, v);

            Assert.assertNull(cache.getIfPresent(88));
            Assert.assertEquals(cache.getFreeCapacity(), cache.getCapacity());
            Assert.assertEquals(cache.stats().getCleanupCount(), 0L, "cleanup did run");
        }
    }

    private void fillBigRandom(OHCache<Integer, String> cache)
    {
        cache.put(1, "one " + bigRandom);
        cache.put(2, "two " + bigRandom);
        cache.put(3, "three " + bigRandom);
        cache.put(4, "four " + bigRandom);
        cache.put(5, "five " + bigRandom);
    }

    private void checkBigRandom(OHCache<Integer, String> cache)
    {
        Assert.assertEquals(cache.getIfPresent(1), "one " + bigRandom);
        Assert.assertEquals(cache.getIfPresent(2), "two " + bigRandom);
        Assert.assertEquals(cache.getIfPresent(3), "three " + bigRandom);
        Assert.assertEquals(cache.getIfPresent(4), "four " + bigRandom);
        Assert.assertEquals(cache.getIfPresent(5), "five " + bigRandom);
    }

    private void fillBig(OHCache<Integer, String> cache)
    {
        cache.put(1, "one " + big);
        cache.put(2, "two " + big);
        cache.put(3, "three " + big);
        cache.put(4, "four " + big);
        cache.put(5, "five " + big);
    }

    private void checkBig(OHCache<Integer, String> cache)
    {
        Assert.assertEquals(cache.getIfPresent(1), "one " + big);
        Assert.assertEquals(cache.getIfPresent(2), "two " + big);
        Assert.assertEquals(cache.getIfPresent(3), "three " + big);
        Assert.assertEquals(cache.getIfPresent(4), "four " + big);
        Assert.assertEquals(cache.getIfPresent(5), "five " + big);
    }

    private void fill(OHCache<Integer, String> cache)
    {
        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");
        cache.put(4, "four");
        cache.put(5, "five");
    }

    private void check(OHCache<Integer, String> cache)
    {
        Assert.assertEquals(cache.getIfPresent(1), "one");
        Assert.assertEquals(cache.getIfPresent(2), "two");
        Assert.assertEquals(cache.getIfPresent(3), "three");
        Assert.assertEquals(cache.getIfPresent(4), "four");
        Assert.assertEquals(cache.getIfPresent(5), "five");
    }
}