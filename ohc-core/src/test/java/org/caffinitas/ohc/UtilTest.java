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

import org.testng.Assert;
import org.testng.annotations.Test;

public class UtilTest
{
    static final long BIG = 2L << 40;

    @Test
    public void roundUp8()
    {
        Assert.assertEquals(Constants.roundUpTo8(0), 0);
        Assert.assertEquals(Constants.roundUpTo8(1), 8);
        Assert.assertEquals(Constants.roundUpTo8(2), 8);
        Assert.assertEquals(Constants.roundUpTo8(3), 8);
        Assert.assertEquals(Constants.roundUpTo8(4), 8);
        Assert.assertEquals(Constants.roundUpTo8(5), 8);
        Assert.assertEquals(Constants.roundUpTo8(6), 8);
        Assert.assertEquals(Constants.roundUpTo8(7), 8);
        Assert.assertEquals(Constants.roundUpTo8(8), 8);
        Assert.assertEquals(Constants.roundUpTo8(121), 128);
        Assert.assertEquals(Constants.roundUpTo8(128), 128);
        Assert.assertEquals(Constants.roundUpTo8(BIG + 121), BIG + 128);
        Assert.assertEquals(Constants.roundUpTo8(BIG + 128), BIG + 128);
    }
}
