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

import net.jpountz.xxhash.XXHashFactory;

final class XxHash extends Hasher
{
    private static final XXHashFactory xx = XXHashFactory.fastestInstance();

    long hash(long address, long offset, int length)
    {
        return xx.hash64().hash(Uns.directBufferFor(address, offset, length, true), 0);
    }

    long hash(byte[] array)
    {
        return xx.hash64().hash(array, 0, array.length, 0);
    }
}
