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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import org.caffinitas.ohc.CloseableIterator;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class SerializationTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    @Test
    public void testDirectIO() throws IOException, InterruptedException
    {
        File f = File.createTempFile("EntrySerializationTest-directIO-", ".bin");
        f.deleteOnExit();

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(TestUtils.intSerializer)
                                                           .valueSerializer(TestUtils.stringSerializer)
                                                           .build())
        {
            TestUtils.fillMany(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                cache.serializeHotNEntries(TestUtils.manyCount, ch);
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(TestUtils.intSerializer)
                                                           .valueSerializer(TestUtils.stringSerializer)
                                                           .build())
        {
            int count;

            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                count = cache.deserializeEntries(ch);
            }

            TestUtils.checkManyForSerializedEntries(cache, count);
        }
    }

    @Test(dependsOnMethods = "testDirectIO")
    public void testDirectIOBig() throws IOException, InterruptedException
    {
        File f = File.createTempFile("EntrySerializationTest-directIOBig-", ".bin");
        f.deleteOnExit();

        int serialized;
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(TestUtils.intSerializer)
                                                           .valueSerializer(TestUtils.stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            TestUtils.fillBig5(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                serialized = cache.serializeHotNEntries(100, ch);
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(TestUtils.intSerializer)
                                                           .valueSerializer(TestUtils.stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            int count;
            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                count = cache.deserializeEntries(ch);
            }

            Assert.assertEquals(count, serialized);

            TestUtils.checkBig5(cache);
        }
    }

    @Test(dependsOnMethods = "testDirectIO")
    public void testDirectIOBigRandom() throws IOException, InterruptedException
    {
        File f = File.createTempFile("EntrySerializationTest-directIOBigRandom-", ".bin");
        f.deleteOnExit();

        int serialized;
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(TestUtils.intSerializer)
                                                           .valueSerializer(TestUtils.stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            TestUtils.fillBigRandom5(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                serialized = cache.serializeHotNEntries(5, ch);
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(TestUtils.intSerializer)
                                                           .valueSerializer(TestUtils.stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            int count;
            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                count = cache.deserializeEntries(ch);
            }

            Assert.assertEquals(count, serialized);

            TestUtils.checkBigRandom5(cache, serialized);
        }
    }

    @Test(dependsOnMethods = "testDirectIO")
    public void testCompressedDirectIO() throws IOException, InterruptedException
    {
        File f = File.createTempFile("EntrySerializationTest-compressedDirectIO-", ".bin");
        f.deleteOnExit();

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(TestUtils.intSerializer)
                                                           .valueSerializer(TestUtils.stringSerializer)
                                                           .build())
        {
            TestUtils.fillMany(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                try (CompressingOutputChannel cch = new CompressingOutputChannel(ch, 8192))
                {
                    cache.serializeHotNEntries(TestUtils.manyCount, cch);
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
                                                           .keySerializer(TestUtils.intSerializer)
                                                           .valueSerializer(TestUtils.stringSerializer)
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

            TestUtils.checkManyForSerializedEntries(cache, count);
        }
    }

    @Test(dependsOnMethods = "testCompressedDirectIO")
    public void testCompressedDirectIOBig() throws IOException, InterruptedException
    {
        File f = File.createTempFile("EntrySerializationTest-compressedDirectIOBig-", ".bin");
        f.deleteOnExit();

        int serialized;
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(TestUtils.intSerializer)
                                                           .valueSerializer(TestUtils.stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            TestUtils.fillBig5(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                try (CompressingOutputChannel cch = new CompressingOutputChannel(ch, 8192))
                {
                    serialized = cache.serializeHotNEntries(100, cch);
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
                                                           .keySerializer(TestUtils.intSerializer)
                                                           .valueSerializer(TestUtils.stringSerializer)
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

            Assert.assertEquals(count, serialized);

            TestUtils.checkBig5(cache);
        }
    }

    @Test(dependsOnMethods = "testCompressedDirectIO")
    public void testCompressedDirectIOBigRandom() throws IOException, InterruptedException
    {
        File f = File.createTempFile("EntrySerializationTest-compressedDirectIOBigRandom-", ".bin");
        f.deleteOnExit();

        int serialized;
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(TestUtils.intSerializer)
                                                           .valueSerializer(TestUtils.stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            TestUtils.fillBigRandom5(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                try (CompressingOutputChannel cch = new CompressingOutputChannel(ch, 8192))
                {
                    serialized = cache.serializeHotNEntries(100, cch);
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
                                                           .keySerializer(TestUtils.intSerializer)
                                                           .valueSerializer(TestUtils.stringSerializer)
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

            Assert.assertEquals(count, serialized);

            TestUtils.checkBigRandom5(cache, serialized);
        }
    }

    @Test
    public void testTooBigEntryOnDeserialize() throws IOException, InterruptedException
    {
        File f = File.createTempFile("EntrySerializationTest-tooBigEntryOnPut-", ".bin");
        f.deleteOnExit();

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                            .keySerializer(TestUtils.intSerializer)
                                                            .valueSerializer(TestUtils.stringSerializer)
                                                            .capacity(512L * 1024 * 1024)
                                                            .build())
        {
            cache.put(1, "hello");
            cache.put(2, "world");
            cache.put(3, new String(new byte[100]));
            cache.put(4, "foo");

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                cache.serializeEntry(1, ch);
                cache.serializeEntry(2, ch);
                cache.serializeEntry(3, ch);
                cache.serializeEntry(4, ch);
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                            .keySerializer(TestUtils.intSerializer)
                                                            .valueSerializer(TestUtils.stringSerializer)
                                                            .capacity(512L * 1024 * 1024)
                                                            .maxEntrySize(TestUtils.intSerializer.serializedSize(1) + Util.ENTRY_OFF_DATA + Util.roundUpTo8(9))
                                                            .build())
        {
            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                Assert.assertTrue(cache.deserializeEntry(ch));
                Assert.assertTrue(cache.deserializeEntry(ch));
                Assert.assertFalse(cache.deserializeEntry(ch));
                Assert.assertTrue(cache.deserializeEntry(ch));
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }
    }

    @Test
    public void testKeySerialization() throws IOException, InterruptedException
    {
        File f = File.createTempFile("EntrySerializationTest-keySerialization-", ".bin");
        f.deleteOnExit();

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                            .keySerializer(TestUtils.intSerializer)
                                                            .valueSerializer(TestUtils.stringSerializer)
                                                            .build())
        {
            TestUtils.fillMany(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                cache.serializeHotNKeys(TestUtils.manyCount, ch);
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                            .keySerializer(TestUtils.intSerializer)
                                                            .valueSerializer(TestUtils.stringSerializer)
                                                            .build())
        {
            int count = 0;

            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                try (CloseableIterator<Integer> keyIter = cache.deserializeKeys(ch))
                {
                    while (keyIter.hasNext())
                    {
                        Integer key = keyIter.next();
                        Assert.assertNotNull(key);
                        count++;
                    }
                }
            }

            TestUtils.checkManyForSerializedKeys(cache, count);
        }
    }
}
