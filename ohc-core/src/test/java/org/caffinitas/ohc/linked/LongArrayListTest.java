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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class LongArrayListTest
{
    @Test
    public void testLongArrayList()
    {
        LongArrayList l = new LongArrayList();

        assertEquals(l.size(), 0);

        l.add(0);
        assertEquals(l.size(), 1);

        for (int i=1;i<=20;i++)
        {
            l.add(i);
            assertEquals(l.size(), i + 1);
        }

        for (int i=0;i<=20;i++)
        {
            assertEquals(l.getLong(i), i);
        }
    }
}