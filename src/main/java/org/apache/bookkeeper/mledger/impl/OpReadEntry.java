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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpReadEntry {
    ManagedCursorImpl cursor;
    PositionImpl readPosition;
    final int count;
    final ReadEntriesCallback callback;
    final Object ctx;

    // Results
    List<Entry> entries = null;
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

    void succeeded() {
        log.debug("Read entries succeeded count={}", entries.size());
        cursor.setReadPosition(nextReadPosition);
        cursor.ledger.endReadOperationOnLedger(readPosition.getLedgerId());
        callback.readEntriesComplete(entries, ctx);
        cursor.ledger.mbean.addReadEntriesSample(entries);
    }

    void emptyResponse() {
        cursor.setReadPosition(nextReadPosition);
        cursor.ledger.endReadOperationOnLedger(readPosition.getLedgerId());
        callback.readEntriesComplete(EmptyList, ctx);
    }

    void failed(ManagedLedgerException status) {
        cursor.ledger.endReadOperationOnLedger(readPosition.getLedgerId());
        callback.readEntriesFailed(status, ctx);
        cursor.ledger.mbean.recordReadEntriesError();
    }

    private static final List<Entry> EmptyList = Collections.emptyList();

    private static final Logger log = LoggerFactory.getLogger(OpReadEntry.class);
}
