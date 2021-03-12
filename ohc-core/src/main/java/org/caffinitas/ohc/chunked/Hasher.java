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

import org.caffinitas.ohc.HashAlgorithm;

abstract class Hasher
{
    static Hasher create(HashAlgorithm hashAlgorithm)
    {
        switch (hashAlgorithm) {
            case XX:
                try {
                    return new XxHash();
                }
                catch (Exception e) {
                    // fall through
                }
            case CRC32C:
                try {
                    return Crc32cHash.newInstance();
                }
                catch (Exception e) {
                    // fall through
                }
            case CRC32:
                return new Crc32Hash();
            case MURMUR3:
                return new Murmur3Hash();
            default:
                throw new UnsupportedOperationException("Incomplete implementation of Hasher.create()");
        }
    }

    private static String forAlg(HashAlgorithm hashAlgorithm)
    {
        return Hasher.class.getName().substring(0, Hasher.class.getName().lastIndexOf('.') + 1)
               + hashAlgorithm.name().charAt(0)
               + hashAlgorithm.name().substring(1).toLowerCase()
               + "Hash";
    }

    abstract long hash(ByteBuffer buffer);

}
