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
import java.util.zip.CRC32;

import net.jpountz.xxhash.XXHashFactory;
import org.caffinitas.ohc.HashAlgorithm;

abstract class Hasher
{
    static Hasher create(HashAlgorithm hashAlgorithm)
    {
        String cls = forAlg(hashAlgorithm);
        try
        {
            return (Hasher) Class.forName(cls).newInstance();
        }
        catch (ClassNotFoundException e)
        {
            if (hashAlgorithm == HashAlgorithm.XX)
            {
                cls = forAlg(HashAlgorithm.CRC32);
                try
                {
                    return (Hasher) Class.forName(cls).newInstance();
                }
                catch (InstantiationException | ClassNotFoundException | IllegalAccessException e1)
                {
                    throw new RuntimeException(e1);
                }
            }
            throw new RuntimeException(e);
        }
        catch (InstantiationException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static String forAlg(HashAlgorithm hashAlgorithm)
    {
        return Hasher.class.getName()
                     + '$'
                     + hashAlgorithm.name().substring(0, 1)
                     + hashAlgorithm.name().substring(1).toLowerCase()
                     + "Hash";
    }

    abstract long hash(ByteBuffer buffer);

    static final class Crc32Hash extends Hasher
    {
        long hash(ByteBuffer buffer)
        {
            CRC32 crc = new CRC32();
            crc.update(buffer);
            return crc.getValue();
        }
    }

    static final class Murmur3Hash extends Hasher
    {
        long hash(ByteBuffer buffer)
        {
            long h1 = 0L;
            long h2 = 0L;
            long k1, k2;
            long length = buffer.remaining();

            while (buffer.remaining() >= 16)
            {
                k1 = getLong(buffer);
                k2 = getLong(buffer);

                // bmix64()

                h1 ^= mixK1(k1);

                h1 = Long.rotateLeft(h1, 27);
                h1 += h2;
                h1 = h1 * 5 + 0x52dce729;

                h2 ^= mixK2(k2);

                h2 = Long.rotateLeft(h2, 31);
                h2 += h1;
                h2 = h2 * 5 + 0x38495ab5;
            }

            int r = buffer.remaining();
            if (r > 0)
            {
                k1 = 0;
                k2 = 0;
                int p = buffer.position();
                switch (r)
                {
                    case 15:
                        k2 ^= toLong(buffer.get(p + 14)) << 48; // fall through
                    case 14:
                        k2 ^= toLong(buffer.get(p + 13)) << 40; // fall through
                    case 13:
                        k2 ^= toLong(buffer.get(p + 12)) << 32; // fall through
                    case 12:
                        k2 ^= toLong(buffer.get(p + 11)) << 24; // fall through
                    case 11:
                        k2 ^= toLong(buffer.get(p + 10)) << 16; // fall through
                    case 10:
                        k2 ^= toLong(buffer.get(p + 9)) << 8; // fall through
                    case 9:
                        k2 ^= toLong(buffer.get(p + 8)); // fall through
                    case 8:
                        k1 ^= getLong(buffer);
                        break;
                    case 7:
                        k1 ^= toLong(buffer.get(p + 6)) << 48; // fall through
                    case 6:
                        k1 ^= toLong(buffer.get(p + 5)) << 40; // fall through
                    case 5:
                        k1 ^= toLong(buffer.get(p + 4)) << 32; // fall through
                    case 4:
                        k1 ^= toLong(buffer.get(p + 3)) << 24; // fall through
                    case 3:
                        k1 ^= toLong(buffer.get(p + 2)) << 16; // fall through
                    case 2:
                        k1 ^= toLong(buffer.get(p + 1)) << 8; // fall through
                    case 1:
                        k1 ^= toLong(buffer.get(p));
                        break;
                    default:
                        throw new AssertionError("Should never get here.");
                }
                buffer.position(p + r);

                h1 ^= mixK1(k1);
                h2 ^= mixK2(k2);
            }

            // makeHash()

            h1 ^= length;
            h2 ^= length;

            h1 += h2;
            h2 += h1;

            h1 = fmix64(h1);
            h2 = fmix64(h2);

            h1 += h2;
            //h2 += h1;

            // padToLong()

            return h1;
        }

        private static long getLong(ByteBuffer buffer)
        {
            int o = buffer.position();
            long l = toLong(buffer.get(o + 7)) << 56;
            l |= toLong(buffer.get(o + 6)) << 48;
            l |= toLong(buffer.get(o + 5)) << 40;
            l |= toLong(buffer.get(o + 4)) << 32;
            l |= toLong(buffer.get(o + 3)) << 24;
            l |= toLong(buffer.get(o + 2)) << 16;
            l |= toLong(buffer.get(o + 1)) << 8;
            l |= toLong(buffer.get(o));
            buffer.position(o + 8);
            return l;
        }
        static final long C1 = 0x87c37b91114253d5L;
        static final long C2 = 0x4cf5ad432745937fL;

        static long fmix64(long k)
        {
            k ^= k >>> 33;
            k *= 0xff51afd7ed558ccdL;
            k ^= k >>> 33;
            k *= 0xc4ceb9fe1a85ec53L;
            k ^= k >>> 33;
            return k;
        }

        static long mixK1(long k1)
        {
            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            return k1;
        }

        static long mixK2(long k2)
        {
            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            return k2;
        }

        static long toLong(byte value)
        {
            return value & 0xff;
        }
    }

    static final class XxHash extends Hasher
    {
        private static final XXHashFactory xx = XXHashFactory.fastestInstance();

        long hash(ByteBuffer buffer)
        {
            return xx.hash64().hash(buffer, 0);
        }
    }
}
