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
package org.caffinitas.ohc.chunked;

import java.nio.ByteBuffer;
import java.util.Random;

import com.google.common.base.Charsets;

import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.OHCache;
import org.testng.Assert;

final class TestUtils
{
    public static final long ONE_MB = 1024 * 1024;

    public static final byte[] dummyByteArray;
    public static final int INT_SERIALIZER_LEN = 529;
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
            return INT_SERIALIZER_LEN;
        }
    };

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

    public static final int FIXED_KEY_LEN = 68;
    static CacheSerializer<Integer> fixedKeySerializer = new CacheSerializer<Integer>()
    {
        public void serialize(Integer integer, ByteBuffer buf)
        {
            buf.putInt(integer);
            for (int i = 4; i < FIXED_KEY_LEN; i++)
                buf.put((byte) 0);
        }

        public Integer deserialize(ByteBuffer buf)
        {
            return buf.getInt();
        }

        public int serializedSize(Integer integer)
        {
            return FIXED_KEY_LEN;
        }
    };

    public static final int FIXED_VALUE_LEN = 30;
    static CacheSerializer<String> fixedValueSerializer = new CacheSerializer<String>()
    {
        public void serialize(String s, ByteBuffer buf)
        {
            byte[] bytes = s.getBytes();
            buf.putShort((short) bytes.length);
            if (bytes.length > FIXED_VALUE_LEN - 2)
                throw new IllegalArgumentException("String too long");
            buf.put(bytes);
            for (int i = 2 + bytes.length; i < FIXED_VALUE_LEN; i++)
                buf.put((byte) 0);
        }

        public String deserialize(ByteBuffer buf)
        {
            byte[] b = new byte[buf.getShort()];
            buf.get(b);
            return new String(b);
        }

        public int serializedSize(String s)
        {
            return FIXED_VALUE_LEN;
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

    static byte[] randomBytes(int len)
    {
        Random r = new Random();
        byte[] arr = new byte[len];
        r.nextBytes(arr);
        return arr;
    }
}
