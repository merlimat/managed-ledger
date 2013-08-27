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

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the life-cycle of an addEntry() operation
 * 
 */
class OpAddEntry implements AddCallback, CloseCallback {
    private final ManagedLedgerImpl ml;
    private LedgerHandle ledger;
    private long entryId;
    private final AddEntryCallback callback;
    private final Object ctx;
    private boolean closeWhenDone;
    private final long startTime;
    final byte[] data;

    OpAddEntry(ManagedLedgerImpl ml, byte[] data, AddEntryCallback callback, Object ctx) {
        this.ml = ml;
        this.ledger = null;
        this.data = data;
        this.callback = callback;
        this.ctx = ctx;
        this.closeWhenDone = false;
        this.entryId = -1;
        this.startTime = System.nanoTime();
        ml.mbean.addAddEntrySample(data.length);
    }

    public void setLedger(LedgerHandle ledger) {
        this.ledger = ledger;
    }

    public void setCloseWhenDone(boolean closeWhenDone) {
        this.closeWhenDone = closeWhenDone;
    }

    public void initiate() {
        ledger.asyncAddEntry(data, this, ctx);
    }

    public void failed(ManagedLedgerException e) {
        callback.addFailed(e, ctx);
        ml.mbean.recordAddEntryError();
    }

    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        checkArgument(ledger.getId() == lh.getId());
        checkArgument(this.ctx == ctx);

        this.entryId = entryId;

        log.debug("[{}] write-complete: ledger-id={} entry-id={} rc={}", ml.getName(), lh.getId(), entryId, rc);
        if (rc == BKException.Code.OK) {
            ml.numberOfEntries.incrementAndGet();
            ml.totalSize.addAndGet(data.length);
            ml.entryCache.insert(new EntryImpl(lh.getId(), entryId, data));

            if (closeWhenDone) {
                log.info("[{}] Closing ledger {} for being full", ml.getName(), lh.getId());
                ledger.asyncClose(this, ctx);
            } else {
                updateLatency();
                callback.addComplete(new PositionImpl(lh.getId(), entryId), ctx);
            }
        } else {
            ManagedLedgerException status = new ManagedLedgerException(BKException.create(rc));
            ml.mbean.recordAddEntryError();
            ml.ledgerClosed(lh, status);
            callback.addFailed(status, ctx);
        }
    }

    @Override
    public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
        checkArgument(ledger.getId() == lh.getId());

        if (rc == BKException.Code.OK) {
            log.debug("Successfuly closed ledger {}", lh.getId());
        } else {
            log.warn("Error when closing ledger {}. Status={}", lh.getId(), BKException.getMessage(rc));
        }

        ml.ledgerClosed(lh, null);
        updateLatency();
        callback.addComplete(new PositionImpl(lh.getId(), entryId), ctx);
    }

    private void updateLatency() {
        double latencyMs = (System.nanoTime() - startTime) / 1e6;
        ml.mbean.addAddEntryLatencySample(latencyMs);
    }

    private static final Logger log = LoggerFactory.getLogger(OpAddEntry.class);
}
