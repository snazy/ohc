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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import org.testng.Assert;
import org.testng.annotations.Test;

public class EntrySerializationTest
{

    @Test
    public void directIO() throws IOException, InterruptedException
    {
        File f = File.createTempFile("OHCBasicTestDirectIO-", ".bin");
        f.deleteOnExit();

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(Utils.complexSerializer)
                                                           .valueSerializer(Utils.stringSerializer)
                                                           .build())
        {
            Utils.fillMany(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                cache.serializeHotNEntries(Utils.manyCount, ch);
            }
            catch (Throwable t)
            {
                // just here since the surrounding try-with-resource might silently consume this exception
                t.printStackTrace();
                throw new Error(t);
            }
        }
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(Utils.complexSerializer)
                                                           .valueSerializer(Utils.stringSerializer)
                                                           .build())
        {
            int count;

            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                count = cache.deserializeEntries(ch);
            }

            Utils.checkManyForSerialized(cache, count);
        }
    }

    @Test(dependsOnMethods = "directIO")
    public void directIOBig() throws IOException, InterruptedException
    {
        File f = File.createTempFile("OHCBasicTestDirectIOBig-", ".bin");
        f.deleteOnExit();

        int serialized;
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(Utils.complexSerializer)
                                                           .valueSerializer(Utils.stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            Utils.fillBig5(cache);

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
                                                           .keySerializer(Utils.complexSerializer)
                                                           .valueSerializer(Utils.stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            int count;
            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                count = cache.deserializeEntries(ch);
            }

            Assert.assertEquals(count, serialized);

            Utils.checkBig5(cache);
        }
    }

    @Test(dependsOnMethods = "directIO")
    public void directIOBigRandom() throws IOException, InterruptedException
    {
        File f = File.createTempFile("OHCBasicTestDirectIOBigRandom-", ".bin");
        f.deleteOnExit();

        int serialized;
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(Utils.complexSerializer)
                                                           .valueSerializer(Utils.stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            Utils.fillBigRandom5(cache);

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
                                                           .keySerializer(Utils.complexSerializer)
                                                           .valueSerializer(Utils.stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            int count;
            try (BufferedReadableByteChannel ch = new BufferedReadableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                count = cache.deserializeEntries(ch);
            }

            Assert.assertEquals(count, serialized);

            Utils.checkBigRandom5(cache, serialized);
        }
    }

    @Test(dependsOnMethods = "directIO")
    public void compressedDirectIO() throws IOException, InterruptedException
    {
        File f = File.createTempFile("OHCBasicTestDirectIO-", ".bin");
        f.deleteOnExit();

        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(Utils.complexSerializer)
                                                           .valueSerializer(Utils.stringSerializer)
                                                           .build())
        {
            Utils.fillMany(cache);

            try (BufferedWritableByteChannel ch = new BufferedWritableByteChannel(FileChannel.open(f.toPath(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 8192))
            {
                try (CompressingOutputChannel cch = new CompressingOutputChannel(ch, 8192))
                {
                    cache.serializeHotNEntries(Utils.manyCount, cch);
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
                                                           .keySerializer(Utils.complexSerializer)
                                                           .valueSerializer(Utils.stringSerializer)
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

            Utils.checkManyForSerialized(cache, count);
        }
    }

    @Test(dependsOnMethods = "compressedDirectIO")
    public void compressedDirectIOBig() throws IOException, InterruptedException
    {
        File f = File.createTempFile("OHCBasicTestCompressedDirectIOBig-", ".bin");
        f.deleteOnExit();

        int serialized;
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(Utils.complexSerializer)
                                                           .valueSerializer(Utils.stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            Utils.fillBig5(cache);

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
                                                           .keySerializer(Utils.complexSerializer)
                                                           .valueSerializer(Utils.stringSerializer)
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

            Utils.checkBig5(cache);
        }
    }

    @Test(dependsOnMethods = "compressedDirectIO")
    public void compressedDirectIOBigRandom() throws IOException, InterruptedException
    {
        File f = File.createTempFile("OHCBasicTestCompressedDirectIOBigRandom-", ".bin");
        f.deleteOnExit();

        int serialized;
        try (OHCache<Integer, String> cache = OHCacheBuilder.<Integer, String>newBuilder()
                                                           .keySerializer(Utils.complexSerializer)
                                                           .valueSerializer(Utils.stringSerializer)
                                                           .capacity(512L * 1024 * 1024)
                                                           .build())
        {
            Utils.fillBigRandom5(cache);

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
                                                           .keySerializer(Utils.complexSerializer)
                                                           .valueSerializer(Utils.stringSerializer)
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

            Utils.checkBigRandom5(cache, serialized);
        }
    }

    @Test
    public void testTooBigEntryOnPut()
    {
        Assert.fail();
        // TODO check off-heap mem is not leaked
    }

    @Test
    public void testTooBigEntryOnReplace()
    {
        Assert.fail();
        // TODO check off-heap mem is not leaked
    }

    @Test
    public void testTooBigEntryOnPutIfAbsent()
    {
        Assert.fail();
        // TODO check off-heap mem is not leaked
    }

    @Test
    public void testTooBigEntryOnDeserialize()
    {
        Assert.fail();
        // TODO check off-heap mem is not leaked
    }

    @Test
    public void testFailingKeySerializerOnPut()
    {
        Assert.fail();
        // TODO check off-heap mem is not leaked
    }

    @Test
    public void testFailingKeySerializerOnGet()
    {
        Assert.fail();
        // TODO check off-heap mem is not leaked
    }

    @Test
    public void testFailingKeySerializerInKeyIterator()
    {
        Assert.fail();
        // TODO check off-heap mem is not leaked
    }

    @Test
    public void testFailingValueSerializerOnPut()
    {
        Assert.fail();
        // TODO check off-heap mem is not leaked
    }

    @Test
    public void testFailingValueSerializerInEntrySerialization()
    {
        Assert.fail();
        // TODO check off-heap mem is not leaked
    }
}