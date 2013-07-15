package org.apache.bookkeeper.mledger.impl;

import java.util.List;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;

public class ManagedCursorAdminOnlyImpl extends ManagedCursorImpl {

    public ManagedCursorAdminOnlyImpl(BookKeeper bookkeeper, ManagedLedgerConfig config, ManagedLedgerImpl ledger,
            String cursorName) {
        super(bookkeeper, config, ledger, cursorName);
    }

    @Override
    protected void recoverFromLedger(long ledgerId, VoidCallback callback) {
        bookkeeper.asyncOpenLedgerNoRecovery(ledgerId, config.getDigestType(), config.getPassword(),
                getOpenCallback(ledgerId, callback, true), null);
    }

    @Override
    public List<Entry> readEntries(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException {
        throw exception();
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx) {
        callback.readEntriesFailed(exception(), ctx);
    }

    @Override
    public void markDelete(Position position) throws InterruptedException, ManagedLedgerException {
        throw exception();
    }

    @Override
    public void asyncMarkDelete(Position position, MarkDeleteCallback callback, Object ctx) {
        callback.markDeleteFailed(exception(), ctx);
    }

    @Override
    public void rewind() {
        throw new RuntimeException(exception());
    }

    @Override
    public void skip(int entries) {
        throw new RuntimeException(exception());
    }

    @Override
    public void seek(Position newReadPositionInt) {
        throw new RuntimeException(exception());
    }

    private ManagedLedgerException exception() {
        return new ManagedLedgerException("Managed cursor is open only to administrative functions");
    }
}
