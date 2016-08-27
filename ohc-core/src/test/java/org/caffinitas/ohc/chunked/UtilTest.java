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

import org.testng.Assert;
import org.testng.annotations.Test;

public class UtilTest
{
    @Test
    public void testBitNum()
    {
        Assert.assertEquals(Util.bitNum(0), 0);
        Assert.assertEquals(Util.bitNum(1), 1);
        Assert.assertEquals(Util.bitNum(2), 2);
        Assert.assertEquals(Util.bitNum(4), 3);
        Assert.assertEquals(Util.bitNum(8), 4);
        Assert.assertEquals(Util.bitNum(16), 5);
        Assert.assertEquals(Util.bitNum(32), 6);
        Assert.assertEquals(Util.bitNum(64), 7);
        Assert.assertEquals(Util.bitNum(128), 8);
        Assert.assertEquals(Util.bitNum(256), 9);
        Assert.assertEquals(Util.bitNum(1024), 11);
        Assert.assertEquals(Util.bitNum(65536), 17);
    }
}
