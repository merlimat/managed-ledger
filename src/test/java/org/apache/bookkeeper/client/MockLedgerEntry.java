package org.apache.bookkeeper.client;

import java.io.InputStream;

public class MockLedgerEntry extends LedgerEntry {

    final long ledgerId;
    final long entryId;
    final byte[] data;

    public MockLedgerEntry(long ledgerId, long entryId, byte[] data) {
        super(ledgerId, entryId);
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.data = data;
    }

    @Override
    public long getLedgerId() {
        return ledgerId;
    }

    @Override
    public long getEntryId() {
        return entryId;
    }

    @Override
    public long getLength() {
        return data.length;
    }

    @Override
    public byte[] getEntry() {
        return data;
    }

    @Override
    public InputStream getEntryInputStream() {
        return null;
    }

}
