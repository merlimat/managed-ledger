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

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.mledger.Entry;

final class EntryImpl implements Entry, Comparable<EntryImpl> {

    private final PositionImpl position;
    private final byte[] data;

    EntryImpl(LedgerEntry ledgerEntry) {
        this.position = new PositionImpl(ledgerEntry.getLedgerId(), ledgerEntry.getEntryId());
        this.data = ledgerEntry.getEntry();
    }

    // Used just for tests
    EntryImpl(long ledgerId, long entryId, byte[] data) {
        this.position = new PositionImpl(ledgerId, entryId);
        this.data = data;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public long getLength() {
        return data.length;
    }

    @Override
    public PositionImpl getPosition() {
        return position;
    }

    @Override
    public int compareTo(EntryImpl other) {
        return position.compareTo(other.getPosition());
    }
}
