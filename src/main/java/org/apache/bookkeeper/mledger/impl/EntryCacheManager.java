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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

class EntryCacheManager {

    private final long maxSize;
    private final double cacheEvictionWatermak;
    private final AtomicLong currentSize = new AtomicLong(0);
    private final ConcurrentMap<String, EntryCache> caches = Maps.newConcurrentMap();
    private final EntryCacheEvictionPolicy evictionPolicy;

    private final AtomicBoolean evictionInProgress = new AtomicBoolean(false);

    protected final ManagedLedgerFactoryMBeanImpl mlFactoryMBean;

    protected static final double MB = 1024 * 1024;

    /** 
     * 
     */
    public EntryCacheManager(ManagedLedgerFactoryImpl factory) {
        this.maxSize = factory.getConfig().getMaxCacheSize();
        this.cacheEvictionWatermak = factory.getConfig().getCacheEvictionWatermark();
        this.evictionPolicy = new EntryCacheDefaultEvictionPolicy();
        this.mlFactoryMBean = factory.mbean;

        log.info("Initialized managed-ledger entry cache of {} Mb", maxSize / MB);
    }

    EntryCache getEntryCache(String name) {
        EntryCache newEntryCache = new EntryCacheImpl(this, name);
        EntryCache currentEntryCache = caches.putIfAbsent(name, newEntryCache);
        if (currentEntryCache != null) {
            return currentEntryCache;
        } else {
            return newEntryCache;
        }
    }

    void removeEntryCache(String name) {
        EntryCache entryCache = caches.remove(name);
        checkNotNull(entryCache);

        long size = entryCache.getSize();
        long totalSize = currentSize.addAndGet(-size);
        log.debug("Removed cache for {} - Size: {} -- Current Size: {}", name, size / MB, totalSize / MB);
    }

    void entryAdded(long size) {
        long totalSize = currentSize.addAndGet(size);
        if (totalSize > maxSize && evictionInProgress.compareAndSet(false, true)) {
            // Trigger a new cache eviction cycle to bring the used memory below the cacheEvictionWatermark percentual
            // limit

            long sizeToEvict = totalSize - (long) (maxSize * cacheEvictionWatermak);
            log.info("Triggering cache eviction. total size: {} Mb -- Need to discard: {} Mb", totalSize / MB,
                    sizeToEvict / MB);

            try {
                evictionPolicy.doEviction(Lists.newArrayList(caches.values()), sizeToEvict);
                log.info("Eviction completed. New cache size: {} Mb", currentSize.get() / MB);
            } finally {
                mlFactoryMBean.recordCacheEviction();
                evictionInProgress.set(false);
            }
        }
    }

    void entriesRemoved(long size) {
        currentSize.addAndGet(-size);
    }

    public long getSize() {
        return currentSize.get();
    }

    public long getMaxSize() {
        return maxSize;
    }

    private static final Logger log = LoggerFactory.getLogger(EntryCacheManager.class);
}
