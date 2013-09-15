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

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class OpReadEntry implements ReadEntriesCallback {
    private ManagedCursorImpl cursor;
    PositionImpl readPosition;
    final int count;
    private final ReadEntriesCallback callback;
    private final Object ctx;

    // Results
    private final List<Entry> entries = Lists.newArrayList();
    PositionImpl nextReadPosition;

    public OpReadEntry(ManagedCursorImpl cursor, AtomicReference<PositionImpl> readPositionRef, int count,
            ReadEntriesCallback callback, Object ctx) {
        this.readPosition = cursor.ledger.startReadOperationOnLedger(readPositionRef);
        this.cursor = cursor;
        this.count = count;
        this.callback = callback;
        this.ctx = ctx;
        this.nextReadPosition = this.readPosition;
    }

    @Override
    public void readEntriesComplete(List<Entry> returnedEntries, Object ctx) {
        // Filter the returned entries for indivual deleted messages
        log.debug("[{}] Read entries succeeded batch_size={} cumulative_size={} requested_count={}",
                cursor.ledger.getName(), returnedEntries.size(), entries.size(), count);
        List<Entry> filteredEntries = cursor.filterReadEntries(returnedEntries);
        entries.addAll(filteredEntries);

        if (!filteredEntries.isEmpty()) {
            PositionImpl lastPosition = (PositionImpl) filteredEntries.get(filteredEntries.size() - 1).getPosition();

            // Get the "next read position", we need to advance the position taking
            // care of ledgers boundaries
            nextReadPosition = new PositionImpl(lastPosition.getLedgerId(), lastPosition.getEntryId() + 1);
        } else {
            // The filtering has left us with an empty list, we need to skip these entries and read the next block
            nextReadPosition = new PositionImpl(readPosition.getLedgerId(), readPosition.getEntryId()
                    + returnedEntries.size());
        }
        checkReadCompletion();
    }

    @Override
    public void readEntriesFailed(ManagedLedgerException status, Object ctx) {
        log.warn("[{}] read failed from ledger at position:{}", cursor.ledger.getName(), readPosition);
        cursor.ledger.endReadOperationOnLedger(readPosition.getLedgerId());
        callback.readEntriesFailed(status, ctx);
        cursor.ledger.mbean.recordReadEntriesError();
    }

    void checkReadCompletion() {
        cursor.setReadPosition(nextReadPosition);

        if (entries.size() < count && cursor.hasMoreEntries()) {
            // We still have more entries to read from the next ledger, schedule a new async operation
            if (nextReadPosition.getLedgerId() != readPosition.getLedgerId()) {
                cursor.ledger.startReadOperationOnLedger(nextReadPosition);
                cursor.ledger.endReadOperationOnLedger(readPosition.getLedgerId());
            }

            readPosition = nextReadPosition;
            cursor.ledger.asyncReadEntries(this);
        } else {
            // The reading was already completed, release resources and trigger callback
            cursor.ledger.endReadOperationOnLedger(readPosition.getLedgerId());
            callback.readEntriesComplete(entries, ctx);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(OpReadEntry.class);
}
