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

import java.util.zip.CRC32;

class Crc32Hash extends Hasher
{
    long hash(byte[] array)
    {
        CRC32 crc = new CRC32();
        crc.update(array);
        long h = crc.getValue();
        h |= h << 32;
        return h;
    }

    long hash(long address, long offset, int length)
    {
        Uns.validate(address, offset, length);

        CRC32 crc = new CRC32();
        crc.update(Uns.directBufferFor(address, offset, length, true));
        long h = crc.getValue();
        h |= h << 32;
        return h;
    }
}
