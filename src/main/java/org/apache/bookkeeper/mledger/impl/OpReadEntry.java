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

import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class OpReadEntry implements ReadCallback {
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

    void failed(ManagedLedgerException status) {
        cursor.ledger.endReadOperationOnLedger(readPosition.getLedgerId());
        callback.readEntriesFailed(status, ctx);
        cursor.ledger.mbean.recordReadEntriesError();
    }

    @Override
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> entriesEnum, Object ctx) {
        if (rc != BKException.Code.OK) {
            log.warn("[{}] read failed from ledger {} at position:{}", cursor.ledger.getName(), lh.getId(),
                    readPosition);
            failed(new ManagedLedgerException(BKException.create(rc)));
            return;
        }

        List<Entry> returnedEntries = Lists.newArrayList();
        while (entriesEnum.hasMoreElements()) {
            returnedEntries.add(new EntryImpl(entriesEnum.nextElement()));
        }

        // Filter the returned entries for indivual deleted messages
        log.debug("[{}] Read entries succeeded batch_size={} cumulative_size={} requested_count={}",
                cursor.ledger.getName(), returnedEntries.size(), entries.size(), count);
        entries.addAll(cursor.filterReadEntries(returnedEntries));

        PositionImpl lastPosition = (PositionImpl) entries.get(entries.size() - 1).getPosition();

        // Get the "next read position", we need to advance the position taking
        // care of ledgers boundaries
        nextReadPosition = new PositionImpl(lastPosition.getLedgerId(), lastPosition.getEntryId() + 1);
        checkReadCompletion();
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
            cursor.ledger.mbean.addReadEntriesSample(entries);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(OpReadEntry.class);
}
