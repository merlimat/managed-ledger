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

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;

/**
 * Cache of entries used by a single ManagedLedger. An EntryCache is compared to other EntryCache instances using their
 * size (the memory that is occupied by each of them).
 */
public interface EntryCache extends Comparable<EntryCache> {

    /**
     * Insert an entry in the cache.
     * <p>
     * If the overall limit have been reached, this will triggered the eviction of other entries, possibly from other
     * EntryCache instances
     * 
     * @param entry
     *            the entry to be cached
     */
    void insert(EntryImpl entry);

    /**
     * Remove from cache all the entries related to a ledger up to lastEntry included.
     * 
     * @param ledgerId
     *            the ledger id
     * @param lastEntry
     *            the id of the last entry to be invalidated (inclusive)
     */
    void invalidateEntries(long ledgerId, long lastEntry);

    /**
     * Remove from the cache all the entries belonging to a specific ledger
     * 
     * @param ledgerId
     *            the ledger id
     */
    void invalidateAllEntries(long ledgerId);

    /**
     * Remove all the entries from the cache
     */
    void clear();

    /**
     * Force the cache to drop entries to free space.
     * 
     * @param sizeToFree
     *            the total memory size to free
     * @return the number of evicted entries
     */
    int evictEntries(long sizeToFree);

    /**
     * Read entries from the cache or from bookkeeper.
     * 
     * Get the entry data either from cache or bookkeeper and mixes up the results in a single list.
     * 
     * @param lh
     *            the ledger handle
     * @param firstEntry
     *            the first entry to read (inclusive)
     * @param lastEntry
     *            the last entry to read (inclusive)
     * @param callback
     *            the callback object that will be notified when read is done
     */
    void asyncReadEntry(LedgerHandle lh, long firstEntry, long lastEntry, ReadEntriesCallback callback);

    /**
     * Get the total size in bytes of all the entries stored in this cache
     * 
     * @return the size of the entry cache
     */
    long getSize();
}