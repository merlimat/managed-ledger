/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.mledger.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

public class TestStatsBuckets {

    @Test
    public void testInvalidConstructor() {
        try {
            new StatsBuckets();
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }
    
    @Test
    public void testUnorderedBoundaries() {
        try {
            new StatsBuckets(2.0, 1.0);
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void test() {
        StatsBuckets stats = new StatsBuckets(1.0, 2.0, 3.0);

        assertEquals(stats.getAvg(), Double.NaN);
        assertEquals(stats.getMin(), Double.NaN);
        assertEquals(stats.getMax(), Double.NaN);
        assertEquals(stats.getSum(), Double.NaN);
        assertEquals(stats.getCount(), 0);
        assertEquals(stats.getBuckets(), new long[] { 0, 0, 0, 0 });

        stats.addValue(0.5);

        assertEquals(stats.getAvg(), 0.5);
        assertEquals(stats.getMin(), 0.5);
        assertEquals(stats.getMax(), 0.5);
        assertEquals(stats.getSum(), 0.5);
        assertEquals(stats.getCount(), 1);
        assertEquals(stats.getBuckets(), new long[] { 1, 0, 0, 0 });

        stats.addValue(1.5);

        assertEquals(stats.getAvg(), 1.0);
        assertEquals(stats.getMin(), 0.5);
        assertEquals(stats.getMax(), 1.5);
        assertEquals(stats.getSum(), 2.0);
        assertEquals(stats.getCount(), 2);
        assertEquals(stats.getBuckets(), new long[] { 1, 1, 0, 0 });

        stats.addValue(5.0);

        assertEquals(stats.getMin(), 0.5);
        assertEquals(stats.getMax(), 5.0);
        assertEquals(stats.getSum(), 7.0);
        assertEquals(stats.getCount(), 3);
        assertEquals(stats.getBuckets(), new long[] { 1, 1, 0, 1 });
        
        stats.addValue(0.1);

        assertEquals(stats.getMin(), 0.1);
        assertEquals(stats.getMax(), 5.0);
        assertEquals(stats.getSum(), 7.1);
        assertEquals(stats.getCount(), 4);
        assertEquals(stats.getBuckets(), new long[] { 2, 1, 0, 1 });
        
        stats.addValue(2.5);

        assertEquals(stats.getMin(), 0.1);
        assertEquals(stats.getMax(), 5.0);
        assertEquals(stats.getSum(), 9.6);
        assertEquals(stats.getCount(), 5);
        assertEquals(stats.getBuckets(), new long[] { 2, 1, 1, 1 });
    }
}
