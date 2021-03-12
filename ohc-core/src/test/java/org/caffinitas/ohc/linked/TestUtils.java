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

import com.google.common.base.Charsets;
import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.OHCache;
import org.testng.Assert;

import java.nio.ByteBuffer;
import java.util.Random;

final class TestUtils
{
    public static final long ONE_MB = 1024 * 1024;
    public static final CacheSerializer<String> stringSerializer = new CacheSerializer<String>()
    {
        public void serialize(String s, ByteBuffer buf)
        {
            byte[] bytes = s.getBytes(Charsets.UTF_8);
            buf.put((byte) ((bytes.length >>> 8) & 0xFF));
            buf.put((byte) ((bytes.length >>> 0) & 0xFF));
            buf.put(bytes);
        }

        public String deserialize(ByteBuffer buf)
        {
            int length = (((buf.get() & 0xff) << 8) + ((buf.get() & 0xff) << 0));
            byte[] bytes = new byte[length];
            buf.get(bytes);
            return new String(bytes, Charsets.UTF_8);
        }

        public int serializedSize(String s)
        {
            return writeUTFLen(s);
        }
    };
    public static final CacheSerializer<String> stringSerializerFailSerialize = new CacheSerializer<String>()
    {
        public void serialize(String s, ByteBuffer buf)
        {
            throw new RuntimeException("foo bar");
        }

        public String deserialize(ByteBuffer buf)
        {
            int length = (buf.get() << 8) + (buf.get() << 0);
            byte[] bytes = new byte[length];
            buf.get(bytes);
            return new String(bytes, Charsets.UTF_8);
        }

        public int serializedSize(String s)
        {
            return writeUTFLen(s);
        }
    };
    public static final CacheSerializer<String> stringSerializerFailDeserialize = new CacheSerializer<String>()
    {
        public void serialize(String s, ByteBuffer buf)
        {
            byte[] bytes = s.getBytes(Charsets.UTF_8);
            buf.put((byte) ((bytes.length >>> 8) & 0xFF));
            buf.put((byte) ((bytes.length >>> 0) & 0xFF));
            buf.put(bytes);
        }

        public String deserialize(ByteBuffer buf)
        {
            throw new RuntimeException("foo bar");
        }

        public int serializedSize(String s)
        {
            return writeUTFLen(s);
        }
    };

    static int writeUTFLen(String str)
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

    public static final byte[] dummyByteArray;
    public static final CacheSerializer<Integer> intSerializer = new CacheSerializer<Integer>()
    {
        public void serialize(Integer s, ByteBuffer buf)
        {
            buf.put((byte)(1 & 0xff));
            buf.putChar('A');
            buf.putDouble(42.42424242d);
            buf.putFloat(11.111f);
            buf.putInt(s);
            buf.putLong(Long.MAX_VALUE);
            buf.putShort((short)(0x7654 & 0xFFFF));
            buf.put(dummyByteArray);
        }

        public Integer deserialize(ByteBuffer buf)
        {
            Assert.assertEquals(buf.get(), (byte) 1);
            Assert.assertEquals(buf.getChar(), 'A');
            Assert.assertEquals(buf.getDouble(), 42.42424242d);
            Assert.assertEquals(buf.getFloat(), 11.111f);
            int r = buf.getInt();
            Assert.assertEquals(buf.getLong(), Long.MAX_VALUE);
            Assert.assertEquals(buf.getShort(), 0x7654);
            byte[] b = new byte[dummyByteArray.length];
            buf.get(b);
            Assert.assertEquals(b, dummyByteArray);
            return r;
        }

        public int serializedSize(Integer s)
        {
            return 529;
        }
    };
    public static final CacheSerializer<Integer> intSerializerFailSerialize = new CacheSerializer<Integer>()
    {
        public void serialize(Integer s, ByteBuffer buf)
        {
            throw new RuntimeException("foo bar");
        }

        public Integer deserialize(ByteBuffer buf)
        {
            Assert.assertEquals(buf.get(), (byte) 1);
            Assert.assertEquals(buf.getChar(), 'A');
            Assert.assertEquals(buf.getDouble(), 42.42424242d);
            Assert.assertEquals(buf.getFloat(), 11.111f);
            int r = buf.getInt();
            Assert.assertEquals(buf.getLong(), Long.MAX_VALUE);
            Assert.assertEquals(buf.getShort(), 0x7654);
            byte[] b = new byte[dummyByteArray.length];
            buf.get(b);
            Assert.assertEquals(b, dummyByteArray);
            return r;
        }

        public int serializedSize(Integer s)
        {
            return 529;
        }
    };
    public static final CacheSerializer<Integer> intSerializerFailDeserialize = new CacheSerializer<Integer>()
    {
        public void serialize(Integer s, ByteBuffer buf)
        {
            buf.put((byte)(1 & 0xff));
            buf.putChar('A');
            buf.putDouble(42.42424242d);
            buf.putFloat(11.111f);
            buf.putInt(s);
            buf.putLong(Long.MAX_VALUE);
            buf.putShort((short)(0x7654 & 0xFFFF));
            buf.put(dummyByteArray);
        }

        public Integer deserialize(ByteBuffer buf)
        {
            throw new RuntimeException("foo bar");
        }

        public int serializedSize(Integer s)
        {
            return 529;
        }
    };
    static final String big;
    static final String bigRandom;

    static {
        dummyByteArray = new byte[500];
        for (int i = 0; i < TestUtils.dummyByteArray.length; i++)
            TestUtils.dummyByteArray[i] = (byte) ((byte) i % 199);
    }

    static int manyCount = 20000;

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

    static void fillBigRandom5(OHCache<Integer, String> cache)
    {
        cache.put(1, "one " + bigRandom);
        cache.put(2, "two " + bigRandom);
        cache.put(3, "three " + bigRandom);
        cache.put(4, "four " + bigRandom);
        cache.put(5, "five " + bigRandom);
    }

    static void checkBigRandom5(OHCache<Integer, String> cache, int serialized)
    {
        int cnt = 0;
        if (cache.get(1) != null)
        {
            Assert.assertEquals(cache.get(1), "one " + bigRandom);
            cnt ++;
        }
        if (cache.get(2) != null)
        {
            Assert.assertEquals(cache.get(2), "two " + bigRandom);
            cnt ++;
        }
        if (cache.get(3) != null)
        {
            Assert.assertEquals(cache.get(3), "three " + bigRandom);
            cnt ++;
        }
        if (cache.get(4) != null)
        {
            Assert.assertEquals(cache.get(4), "four " + bigRandom);
            cnt ++;
        }
        if (cache.get(5) != null)
        {
            Assert.assertEquals(cache.get(5), "five " + bigRandom);
            cnt ++;
        }
        Assert.assertEquals(cnt, serialized);
    }

    static void fillBig5(OHCache<Integer, String> cache)
    {
        cache.put(1, "one " + big);
        cache.put(2, "two " + big);
        cache.put(3, "three " + big);
        cache.put(4, "four " + big);
        cache.put(5, "five " + big);
    }

    static void checkBig5(OHCache<Integer, String> cache)
    {
        Assert.assertEquals(cache.get(1), "one " + big);
        Assert.assertEquals(cache.get(2), "two " + big);
        Assert.assertEquals(cache.get(3), "three " + big);
        Assert.assertEquals(cache.get(4), "four " + big);
        Assert.assertEquals(cache.get(5), "five " + big);
    }

    static void fill5(OHCache<Integer, String> cache)
    {
        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");
        cache.put(4, "four");
        cache.put(5, "five");
    }

    static void check5(OHCache<Integer, String> cache)
    {
        Assert.assertEquals(cache.get(1), "one");
        Assert.assertEquals(cache.get(2), "two");
        Assert.assertEquals(cache.get(3), "three");
        Assert.assertEquals(cache.get(4), "four");
        Assert.assertEquals(cache.get(5), "five");
    }

    static void fillMany(OHCache<Integer, String> cache)
    {
        for (int i = 0; i < manyCount; i++)
            cache.put(i, Integer.toHexString(i));
    }

    static void checkManyForSerializedKeys(OHCache<Integer, String> cache, int count)
    {
        Assert.assertTrue(count > manyCount * 9 / 10, "count=" + count); // allow some variation

    }

    static void checkManyForSerializedEntries(OHCache<Integer, String> cache, int count)
    {
        Assert.assertTrue(count > manyCount * 9 / 10, "count=" + count); // allow some variation

        int found = 0;
        for (int i = 0; i < manyCount; i++)
        {
            String v = cache.get(i);
            if (v != null)
            {
                Assert.assertEquals(v, Integer.toHexString(i));
                found++;
            }
        }

        Assert.assertEquals(found, count);
    }

    static byte[] randomBytes(int len)
    {
        Random r = new Random();
        byte[] arr = new byte[len];
        r.nextBytes(arr);
        return arr;
    }
}
