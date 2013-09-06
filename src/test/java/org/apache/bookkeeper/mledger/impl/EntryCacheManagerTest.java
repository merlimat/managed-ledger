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
package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;

import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.testng.annotations.Test;

@Test
public class EntryCacheManagerTest {

    @Test
    void simple() {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(10);
        config.setCacheEvictionWatermark(0.8);

        EntryCacheManager cacheManager = new EntryCacheManager(config);
        EntryCache cache1 = cacheManager.getEntryCache("cache1");
        EntryCache cache2 = cacheManager.getEntryCache("cache2");

        cache1.insert(new EntryImpl(1, 0, new byte[3]));
        cache1.insert(new EntryImpl(1, 1, new byte[4]));

        assertEquals(cache1.getSize(), 7);
        assertEquals(cacheManager.getSize(), 7);

        cache2.insert(new EntryImpl(2, 0, new byte[1]));
        cache2.insert(new EntryImpl(2, 1, new byte[1]));
        cache2.insert(new EntryImpl(2, 2, new byte[1]));

        assertEquals(cache2.getSize(), 3);
        assertEquals(cacheManager.getSize(), 10);

        // Next insert should trigger a cache eviction to force the size to 8
        // The algorithm should evict entries from cache1
        cache2.insert(new EntryImpl(2, 3, new byte[1]));

        assertEquals(cacheManager.getSize(), 7);
        assertEquals(cache1.getSize(), 4);
        assertEquals(cache2.getSize(), 3);
        
        cacheManager.removeEntryCache("cache1");
        assertEquals(cacheManager.getSize(), 3);
        assertEquals(cache2.getSize(), 3);
        
        // Should remove 2 entries
        cache2.invalidateEntries(new PositionImpl(2, 1));
        assertEquals(cacheManager.getSize(), 1);
        assertEquals(cache2.getSize(), 1);
    }
}
