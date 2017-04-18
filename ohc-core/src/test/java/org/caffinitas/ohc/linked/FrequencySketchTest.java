/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modified for OHC
 */
package org.caffinitas.ohc.linked;

import java.util.concurrent.ThreadLocalRandom;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class FrequencySketchTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    final Integer item = ThreadLocalRandom.current().nextInt();

    @Test
    public void testConstruc()
    {
        FrequencySketch sketch = new FrequencySketch(512);
        assertThat(sketch.tableOffset, not(0L));
        int size = sketch.tableLength;
        assertThat(sketch.tableLength, is(size));
        assertThat(sketch.tableMask, is(size - 1));
        assertThat(sketch.sampleSize, is(10 * size));
        sketch.release();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEnsureCapacity_negative()
    {
        FrequencySketch sketch = makeSketch(-1);
        sketch.release();
    }

    @Test
    public void testLowerIndexOfBits()
    {
        FrequencySketch sketch = new FrequencySketch(65536);
        try
        {
            ThreadLocalRandom rand = ThreadLocalRandom.current();
            for (int i = 0; i < 4; i++)
            {
                long trailing0_l2 = 0;
                long trailing0_l3 = 0;
                long trailing0_l5 = 0;
                long loops = 10_000_000;
                for (int n = 0; n < loops; n++)
                {
                    long hash = rand.nextLong();
                    int index = sketch.indexOf(hash, i);
                    int tr = Integer.numberOfTrailingZeros(index);
                    if (tr >= 2)
                        trailing0_l2 ++;
                    if (tr >= 3)
                        trailing0_l3 ++;
                    if (tr >= 5)
                        trailing0_l5 ++;
                }
                assertThat("l5 / i==" + i, trailing0_l5, lessThanOrEqualTo(loops / 15));
                assertThat("l3 / i==" + i, trailing0_l3, lessThanOrEqualTo(loops / 7));
                assertThat("l2 / i==" + i, trailing0_l2, lessThanOrEqualTo(loops / 3));
            }
        }
        finally
        {
            sketch.release();
        }
    }

    @Test
    public void testIncrementOnce()
    {
        FrequencySketch sketch = makeSketch(64);
        try
        {
            sketch.increment(item);
            assertThat(sketch.frequency(item), is(1));
        }
        finally
        {
            sketch.release();
        }
    }

    @Test
    public void testIncrementMax()
    {
        FrequencySketch sketch = makeSketch(64);
        try
        {
            for (int i = 0; i < 20; i++)
            {
                sketch.increment(item);
            }
            assertThat(sketch.frequency(item), is(15));
        }
        finally
        {
            sketch.release();
        }
    }

    @Test
    public void testIncrementDistinct()
    {
        FrequencySketch sketch = makeSketch(64);
        try
        {
            sketch.increment(item);
            sketch.increment(item + 1);
            assertThat(sketch.frequency(item), is(1));
            assertThat(sketch.frequency(item + 1), is(1));
            assertThat(sketch.frequency(item + 2), is(0));
        }
        finally
        {
            sketch.release();
        }
    }

    @Test
    public void reset()
    {
        boolean reset = false;
        FrequencySketch sketch = makeSketch(64);
        try
        {
            for (int i = 1; i < 20 * sketch.tableLength; i++)
            {
                sketch.increment(i);
                if (sketch.size != i)
                {
                    reset = true;
                    break;
                }
            }
            assertThat(reset, is(true));
            assertThat(sketch.size, lessThanOrEqualTo(sketch.sampleSize / 2));
        }
        finally
        {
            sketch.release();
        }
    }

    @Test
    public void testHeavyHitters()
    {
        FrequencySketch sketch = makeSketch(512);
        try
        {
            for (int i = 100; i < 100_000; i++)
            {
                sketch.increment(Double.valueOf(i).hashCode());
            }
            for (int i = 0; i < 10; i += 2)
            {
                for (int j = 0; j < i; j++)
                {
                    sketch.increment(Double.valueOf(i).hashCode());
                }
            }

            // A perfect popularity count yields an array [0, 0, 2, 0, 4, 0, 6, 0, 8, 0]
            int[] popularity = new int[10];
            for (int i = 0; i < 10; i++)
            {
                popularity[i] = sketch.frequency(Double.valueOf(i).hashCode());
            }
            for (int i = 0; i < popularity.length; i++)
            {
                if ((i == 0) || (i == 1) || (i == 3) || (i == 5) || (i == 7) || (i == 9))
                {
                    assertThat(popularity[i], lessThanOrEqualTo(popularity[2]));
                }
                else if (i == 2)
                {
                    assertThat(popularity[2], lessThanOrEqualTo(popularity[4]));
                }
                else if (i == 4)
                {
                    assertThat(popularity[4], lessThanOrEqualTo(popularity[6]));
                }
                else if (i == 6)
                {
                    assertThat(popularity[6], lessThanOrEqualTo(popularity[8]));
                }
            }
        }
        finally
        {
            sketch.release();
        }
    }

    private FrequencySketch makeSketch(int capacity)
    {
        return new FrequencySketch(capacity);
    }
}
