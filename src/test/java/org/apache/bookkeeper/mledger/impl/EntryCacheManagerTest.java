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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class EntryCacheManagerTest extends MockedBookKeeperTestCase {

    ManagedLedgerMBeanImpl mbean;

    @BeforeClass
    void setup() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        ManagedLedgerImpl ml = mock(ManagedLedgerImpl.class);
        when(ml.getExecutor()).thenReturn(executor);

        mbean = new ManagedLedgerMBeanImpl(ml);
    }

    @Test
    void simple() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(10);
        config.setCacheEvictionWatermark(0.8);

        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle(), config);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        EntryCache cache1 = cacheManager.getEntryCache("cache1", mbean);
        EntryCache cache2 = cacheManager.getEntryCache("cache2", mbean);

        cache1.insert(new EntryImpl(1, 1, new byte[4]));
        cache1.insert(new EntryImpl(1, 0, new byte[3]));

        assertEquals(cache1.getSize(), 7);
        assertEquals(cacheManager.getSize(), 7);

        cacheManager.mlFactoryMBean.refreshStats();
        assertEquals(cacheManager.mlFactoryMBean.getCacheMaxSize(), 10);
        assertEquals(cacheManager.mlFactoryMBean.getCacheUsedSize(), 7);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMissesRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsThroughput(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getNumberOfCacheEvictions(), 0);

        cache2.insert(new EntryImpl(2, 0, new byte[1]));
        cache2.insert(new EntryImpl(2, 1, new byte[1]));
        cache2.insert(new EntryImpl(2, 2, new byte[1]));

        assertEquals(cache2.getSize(), 3);
        assertEquals(cacheManager.getSize(), 10);

        // Next insert should trigger a cache eviction to force the size to 8
        // The algorithm should evict entries from cache1
        cache2.insert(new EntryImpl(2, 3, new byte[1]));

        assertEquals(cacheManager.getSize(), 7);
        assertEquals(cache1.getSize(), 3);
        assertEquals(cache2.getSize(), 4);

        cacheManager.removeEntryCache("cache1");
        assertEquals(cacheManager.getSize(), 4);
        assertEquals(cache2.getSize(), 4);

        // Should remove 2 entries
        cache2.invalidateEntries(new PositionImpl(2, 1));
        assertEquals(cacheManager.getSize(), 2);
        assertEquals(cache2.getSize(), 2);

        cacheManager.mlFactoryMBean.refreshStats();

        assertEquals(cacheManager.mlFactoryMBean.getCacheMaxSize(), 10);
        assertEquals(cacheManager.mlFactoryMBean.getCacheUsedSize(), 2);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMissesRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsThroughput(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getNumberOfCacheEvictions(), 1);
    }

    @Test
    void cacheDisabled() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(0);
        config.setCacheEvictionWatermark(0.8);

        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle(), config);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        EntryCache cache1 = cacheManager.getEntryCache("cache1", mbean);
        EntryCache cache2 = cacheManager.getEntryCache("cache2", mbean);

        assertTrue(cache1 instanceof EntryCacheManager.EntryCacheDisabled);
        assertTrue(cache2 instanceof EntryCacheManager.EntryCacheDisabled);

        cache1.insert(new EntryImpl(1, 1, new byte[4]));
        cache1.insert(new EntryImpl(1, 0, new byte[3]));

        assertEquals(cache1.getSize(), 0);
        assertEquals(cacheManager.getSize(), 0);

        cacheManager.mlFactoryMBean.refreshStats();
        assertEquals(cacheManager.mlFactoryMBean.getCacheMaxSize(), 0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheUsedSize(), 0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheMissesRate(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getCacheHitsThroughput(), 0.0);
        assertEquals(cacheManager.mlFactoryMBean.getNumberOfCacheEvictions(), 0);

        cache2.insert(new EntryImpl(2, 0, new byte[1]));
        cache2.insert(new EntryImpl(2, 1, new byte[1]));
        cache2.insert(new EntryImpl(2, 2, new byte[1]));

        assertEquals(cache2.getSize(), 0);
        assertEquals(cacheManager.getSize(), 0);
    }
}
