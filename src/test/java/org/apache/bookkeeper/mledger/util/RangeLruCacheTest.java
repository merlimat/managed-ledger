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

import org.apache.bookkeeper.mledger.util.RangeLruCache.Weighter;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

@Test
public class RangeLruCacheTest {

    @Test
    void simple() {
        RangeLruCache<Integer, String> cache = new RangeLruCache<Integer, String>();

        cache.put(0, "0");
        cache.put(1, "1");

        assertEquals(cache.getSize(), 2);
        assertEquals(cache.getNumberOfEntries(), 2);

        assertEquals(cache.get(0), "0");
        assertEquals(cache.get(2), null);

        cache.put(2, "2");
        cache.put(8, "8");
        cache.put(11, "11");

        assertEquals(cache.getSize(), 5);
        assertEquals(cache.getNumberOfEntries(), 5);

        cache.removeRange(1, 5, true);
        assertEquals(cache.getSize(), 3);
        assertEquals(cache.getNumberOfEntries(), 3);

        cache.removeRange(2, 8, false);
        assertEquals(cache.getSize(), 3);
        assertEquals(cache.getNumberOfEntries(), 3);

        cache.removeRange(0, 100, false);
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);

        cache.removeRange(0, 100, false);
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    @Test
    void customWeighter() {
        RangeLruCache<Integer, String> cache = new RangeLruCache<Integer, String>(new Weighter<String>() {
            public long getSize(String value) {
                return value.length();
            }
        });

        cache.put(0, "zero");
        cache.put(1, "one");

        assertEquals(cache.getSize(), 7);
        assertEquals(cache.getNumberOfEntries(), 2);
    }

    @Test
    void doubleInsert() {
        RangeLruCache<Integer, String> cache = new RangeLruCache<Integer, String>();

        cache.put(0, "zero");
        cache.put(1, "one");

        assertEquals(cache.getSize(), 2);
        assertEquals(cache.getNumberOfEntries(), 2);
        assertEquals(cache.get(1), "one");

        cache.put(1, "uno");

        assertEquals(cache.getSize(), 2);
        assertEquals(cache.getNumberOfEntries(), 2);
        assertEquals(cache.get(1), "uno");
    }

    @Test
    void getRange() {
        RangeLruCache<Integer, String> cache = new RangeLruCache<Integer, String>();

        cache.put(0, "0");
        cache.put(1, "1");
        cache.put(3, "3");
        cache.put(5, "5");

        assertEquals(cache.getRange(1, 8), Lists.newArrayList("1", "3", "5"));

        cache.put(8, "8");
        assertEquals(cache.getRange(1, 8), Lists.newArrayList("1", "3", "5", "8"));

        cache.clear();
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    @Test
    void eviction() {
        RangeLruCache<Integer, String> cache = new RangeLruCache<Integer, String>(new Weighter<String>() {
            public long getSize(String value) {
                return value.length();
            }
        });

        cache.put(0, "zero");
        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");

        // This should remove the LRU entries: 0, 1 whose combined size is 7
        assertEquals(cache.evictLeastAccessedEntries(5), Pair.create(2, (long) 7));

        assertEquals(cache.getNumberOfEntries(), 2);
        assertEquals(cache.getSize(), 8);
        assertEquals(cache.get(0), null);
        assertEquals(cache.get(1), null);
        assertEquals(cache.get(2), "two");
        assertEquals(cache.get(3), "three");

        assertEquals(cache.evictLeastAccessedEntries(100), Pair.create(2, (long) 8));
        assertEquals(cache.getNumberOfEntries(), 0);
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.get(0), null);
        assertEquals(cache.get(1), null);
        assertEquals(cache.get(2), null);
        assertEquals(cache.get(3), null);

        try {
            cache.evictLeastAccessedEntries(0);
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            cache.evictLeastAccessedEntries(-1);
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }
}
