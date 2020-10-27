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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.zip.CRC32C;

final class Crc32cHash
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Hasher.class);

    static final boolean AVAILABLE;

    static {
        boolean avail;
        try {
            Class.forName("java.util.zip.CRC32C").getDeclaredConstructor().newInstance();
            avail = true;
        }
        catch (Exception e) {
            avail = false;
        }
        AVAILABLE = avail;
    }

    static Hasher newInstance() {
        if (AVAILABLE)
            return new Crc32cHashImpl();
        LOGGER.warn("CRC32C hash is only available with Java 11 or newer. Falling back to CRC32.");
        return new Crc32Hash();
    }

    static final class Crc32cHashImpl extends Hasher {
        long hash(byte[] array) {
            CRC32C crc = new CRC32C();
            crc.update(array);
            long h = crc.getValue();
            h |= h << 32;
            return h;
        }

        long hash(long address, long offset, int length) {
            Uns.validate(address, offset, length);

            CRC32C crc = new CRC32C();
            crc.update(Uns.directBufferFor(address, offset, length, true));
            long h = crc.getValue();
            h |= h << 32;
            return h;
        }
    }
}
