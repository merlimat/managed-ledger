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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;

import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.util.Pair;
import org.apache.bookkeeper.mledger.util.RangeLruCache;
import org.apache.bookkeeper.mledger.util.RangeLruCache.Weighter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.Longs;

/**
 * Cache data payload for entries of all ledgers
 */
public class EntryCacheImpl implements EntryCache {

    private final EntryCacheManager manager;
    private final String name;
    private final RangeLruCache<PositionImpl, EntryImpl> entries;

    private static final double MB = 1024 * 1024;

    private static final Weighter<EntryImpl> entryWeighter = new Weighter<EntryImpl>() {
        public long getSize(EntryImpl entry) {
            return entry.getLength();
        }
    };

    public EntryCacheImpl(EntryCacheManager manager, String name) {
        this.manager = manager;
        this.name = name;
        this.entries = new RangeLruCache<PositionImpl, EntryImpl>(entryWeighter);

        log.info("[{}] Initialized managed-ledger entry cache", this.name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void insert(EntryImpl entry) {
        log.debug("[{}] Adding entry to cache: {}", name, entry.getPosition());
        entries.put(entry.getPosition(), entry);
        manager.entryAdded(entry.getLength());
    }

    @Override
    public void invalidateEntries(final PositionImpl lastPosition) {
        final PositionImpl firstPosition = new PositionImpl(-1, 0);

        Pair<Integer, Long> removed = entries.removeRange(firstPosition, lastPosition, true);
        int entriesRemoved = removed.first;
        long sizeRemoved = removed.second;
        log.debug("[{}] Invalidated entries up to {} - Entries removed: {} - Size removed: {}", name, lastPosition,
                entriesRemoved, sizeRemoved);
        manager.entriesRemoved(sizeRemoved);
    }

    @Override
    public void invalidateAllEntries(long ledgerId) {
        final PositionImpl firstPosition = new PositionImpl(ledgerId, 0);
        final PositionImpl lastPosition = new PositionImpl(ledgerId + 1, 0);

        Pair<Integer, Long> removed = entries.removeRange(firstPosition, lastPosition, false);
        int entriesRemoved = removed.first;
        long sizeRemoved = removed.second;
        log.debug("[{}] Invalidated all entries on ledger {} - Entries removed: {} - Size removed: {}", name, ledgerId,
                entriesRemoved, sizeRemoved);
        manager.entriesRemoved(sizeRemoved);
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void asyncReadEntry(LedgerHandle lh, long firstEntry, long lastEntry, final ReadEntriesCallback callback) {
        final long ledgerId = lh.getId();
        final List<EntryImpl> entriesToReturn = Lists.newArrayList();
        final PositionImpl firstPosition = new PositionImpl(lh.getId(), firstEntry);
        final PositionImpl lastPosition = new PositionImpl(lh.getId(), lastEntry);
        final Range<Long> completeRange = Range.closed(firstEntry, lastEntry);

        log.debug("[{}] Reading entries range ledger {}: {} to {}", name, ledgerId, firstEntry, lastEntry);

        RangeSet<Long> availablePositions = TreeRangeSet.create();
        for (EntryImpl entry : entries.getRange(firstPosition, lastPosition)) {
            long entryId = entry.getPosition().getEntryId();
            entriesToReturn.add(entry);
            availablePositions.add(Range.closedOpen(entryId, entryId + 1));
            manager.mlFactoryMBean.recordCacheHit(entry.getLength());
        }

        log.debug("[{}] Ledger {} -- entries: {}-{} -- found in cache: {}", name, ledgerId, firstEntry, lastEntry,
                availablePositions);

        RangeSet<Long> missingEntries = availablePositions.complement().subRangeSet(completeRange);
        log.debug("[{}] Missing entries: {}", name, missingEntries);
        if (missingEntries.isEmpty()) {
            // All entries found in cache
            callback.readEntriesComplete((List) entriesToReturn, null);
        } else {
            // Need to read some entries from bookkeeper. We do this in parallel for each missing entry segment and when
            // we get all the answers, we trigger the callback
            final Set<Range<Long>> rangeSet = missingEntries.asRanges();

            ReadCallback readCallback = new ReadCallback() {
                boolean isCompleted = false;
                int pendingCallbacks = rangeSet.size();

                public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> sequence, Object ctx) {
                    if (isCompleted) {
                        return;
                    }

                    if (rc != BKException.Code.OK) {
                        isCompleted = true;
                        callback.readEntriesFailed(new ManagedLedgerException(BKException.create(rc)), null);
                        return;
                    }

                    // We got the entries, we need to transform them to a List<> type
                    while (sequence.hasMoreElements()) {
                        // Insert the entries at the end of the list (they will be unsorted for now)
                        EntryImpl entry = new EntryImpl(sequence.nextElement());
                        entriesToReturn.add(entry);
                        manager.mlFactoryMBean.recordCacheMiss(entry.getLength());

                        // Cache the entry
                        insert(entry);
                    }

                    if (--pendingCallbacks == 0) {
                        // We have read all the entries that were missing from the cache, we need to sort the entries
                        // and trigger the callback
                        Collections.sort(entriesToReturn);
                        callback.readEntriesComplete((List) entriesToReturn, null);
                    }
                }
            };

            for (Range<Long> range : rangeSet) {
                // Schedule the reads in parallel
                long first = range.lowerEndpoint();
                long last = range.upperBoundType() == BoundType.CLOSED ? range.upperEndpoint()
                        : range.upperEndpoint() - 1;

                log.debug("[{}] Reading from bookkeeper on ledger {} - first: {} - last: {}", name, ledgerId, first,
                        last);
                lh.asyncReadEntries(first, last, readCallback, null);
            }
        }
    }

    @Override
    public void clear() {
        long removedSize = entries.clear();
        manager.entriesRemoved(removedSize);
    }

    @Override
    public long getSize() {
        return entries.getSize();
    }

    @Override
    public int compareTo(EntryCache other) {
        return Longs.compare(getSize(), other.getSize());
    }

    @Override
    public Pair<Integer, Long> evictEntries(long sizeToFree) {
        checkArgument(sizeToFree > 0);
        Pair<Integer, Long> evicted = entries.evictLeastAccessedEntries(sizeToFree);
        int evictedEntries = evicted.first;
        long evictedSize = evicted.second;
        log.info("[{}] Doing cache eviction of at least {} Mb -- Deleted {} entries - Total size: {} Mb", name,
                sizeToFree / MB, evictedEntries, evictedSize / MB);
        manager.entriesRemoved(evictedSize);
        return evicted;
    }

    private static final Logger log = LoggerFactory.getLogger(EntryCacheImpl.class);
}
