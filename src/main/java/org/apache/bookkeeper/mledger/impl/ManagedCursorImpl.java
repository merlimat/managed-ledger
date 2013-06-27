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
/**
 * 
 */
package org.apache.bookkeeper.mledger.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.Version;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.apache.bookkeeper.mledger.util.CallbackMutexReadWrite;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.BoundType;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.protobuf.InvalidProtocolBufferException;

@ThreadSafe
class ManagedCursorImpl implements ManagedCursor {

    protected final BookKeeper bookkeeper;
    protected final ManagedLedgerConfig config;
    protected final ManagedLedgerImpl ledger;
    private final String name;

    private final AtomicReference<PositionImpl> acknowledgedPosition = new AtomicReference<PositionImpl>();
    private final AtomicReference<PositionImpl> readPosition = new AtomicReference<PositionImpl>();

    // Cursor ledger reference will always point to an opened ledger
    private AtomicReference<LedgerHandle> cursorLedger = new AtomicReference<LedgerHandle>();
    private AtomicReference<Version> cursorLedgerVersion = new AtomicReference<Version>();

    private final RangeSet<PositionImpl> individualDeletedMessages = TreeRangeSet.create();
    private final ReadWriteLock deletedMessagesMutex = new ReentrantReadWriteLock();

    // This mutex is used to prevent mark-delete being run while we are
    // switching to a new ledger for cursor position
    private final CallbackMutexReadWrite ledgerMutex = new CallbackMutexReadWrite();

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public interface VoidCallback {
        public void operationComplete();

        public void operationFailed(ManagedLedgerException exception);
    }

    ManagedCursorImpl(BookKeeper bookkeeper, ManagedLedgerConfig config, ManagedLedgerImpl ledger, String cursorName) {
        this.bookkeeper = bookkeeper;
        this.config = config;
        this.ledger = ledger;
        this.name = cursorName;
    }

    /**
     * Performs the initial recovery, reading the mark-deleted position from the ledger and then calling initialize to
     * have a new opened ledger
     */
    void recover(final VoidCallback callback) {
        // Read the meta-data ledgerId from the store
        log.debug("[{}] Recovering from bookkeeper ledger", ledger.getName(), name);
        ledger.getStore().asyncGetConsumerLedgerId(ledger.getName(), name, new MetaStoreCallback<ManagedCursorInfo>() {
            public void operationComplete(ManagedCursorInfo info, Version version) {
                log.debug("[{}] Consumer {} meta-data recover from ledger {}", ledger.getName(), name,
                        info.getCursorsLedgerId());
                cursorLedgerVersion.set(version);
                recoverFromLedger(info.getCursorsLedgerId(), callback);
            }

            public void operationFailed(MetaStoreException e) {
                callback.operationFailed(e);
            }
        });
    }

    protected OpenCallback getOpenCallback(final long ledgerId, final VoidCallback callback, final boolean isReadOnly) {
        return new OpenCallback() {
            public void openComplete(int rc, LedgerHandle lh, Object ctx) {
                log.debug("[{}] Opened ledger {} for consumer {}. rc={}", ledger.getName(), ledgerId, name, rc);
                if (rc != BKException.Code.OK) {
                    log.warn("[{}] Error opening metadata ledger {} for consumer {}: {}", ledger.getName(), ledgerId,
                            name, BKException.create(rc));
                    callback.operationFailed(new ManagedLedgerException(BKException.create(rc)));
                    return;
                }

                // Read the last entry in the ledger
                cursorLedger.set(lh);
                final long entryId = lh.getLastAddConfirmed();
                lh.asyncReadEntries(entryId, entryId, new ReadCallback() {
                    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                        log.debug("readComplete rc={} entryId={}", rc, entryId);
                        if (rc != BKException.Code.OK) {
                            log.warn("[{}] Error reading from metadata ledger {} for consumer {}: {}",
                                    ledger.getName(), ledgerId, name, BKException.create(rc));
                            callback.operationFailed(new ManagedLedgerException(BKException.create(rc)));
                            return;
                        }

                        LedgerEntry entry = seq.nextElement();
                        PositionInfo positionInfo;
                        try {
                            positionInfo = PositionInfo.parseFrom(entry.getEntry());
                        } catch (InvalidProtocolBufferException e) {
                            callback.operationFailed(new ManagedLedgerException(e));
                            return;
                        }

                        PositionImpl position = new PositionImpl(positionInfo);
                        log.debug("[{}] Consumer {} recovered to position {}", ledger.getName(), name, position);
                        if (isReadOnly) {
                            setAcknowledgedPosition(position);
                            callback.operationComplete();
                            cursorLedger.set(lh);
                        } else {
                            initialize(position, callback);
                            lh.asyncClose(new CloseCallback() {
                                public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
                                }
                            }, null);
                        }
                    }
                }, null);
            }
        };
    }

    protected void recoverFromLedger(final long ledgerId, final VoidCallback callback) {
        // Read the acknowledged position from the metadata ledger, then create
        // a new ledger and write the position into it
        bookkeeper.asyncOpenLedger(ledgerId, config.getDigestType(), config.getPassword(),
                getOpenCallback(ledgerId, callback, false), null);
    }

    void initialize(final PositionImpl position, final VoidCallback callback) {
        setAcknowledgedPosition(position);
        createNewMetadataLedger(new VoidCallback() {
            public void operationComplete() {
                callback.operationComplete();
            }

            public void operationFailed(ManagedLedgerException exception) {
                callback.operationFailed(exception);
            }
        });
    }

    @Override
    public List<Entry> readEntries(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException {
        checkArgument(numberOfEntriesToRead > 0);

        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
            List<Entry> entries = null;
        }

        final Result result = new Result();

        asyncReadEntries(numberOfEntriesToRead, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                result.entries = entries;
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        counter.await();

        if (result.exception != null)
            throw result.exception;

        return result.entries;
    }

    @Override
    public void asyncReadEntries(final int numberOfEntriesToRead, final ReadEntriesCallback callback, final Object ctx) {
        checkArgument(numberOfEntriesToRead > 0);
        if (isClosed.get()) {
            callback.readEntriesFailed(new ManagedLedgerException("Cursor was already closed"), ctx);
            return;
        }

        OpReadEntry op = new OpReadEntry(this, readPosition, numberOfEntriesToRead, callback, ctx);
        ledger.asyncReadEntries(op);
    }

    @Override
    public boolean hasMoreEntries() {
        return ledger.hasMoreEntries(readPosition.get());
    }

    @Override
    public long getNumberOfEntries() {
        PositionImpl fromPosition = readPosition.get();
        long allEntries = ledger.getNumberOfEntries(fromPosition);
        Range<PositionImpl> accountedEntriesRange = Range.atLeast(fromPosition);

        long deletedEntries = 0;

        deletedMessagesMutex.readLock().lock();
        try {
            for (Range<PositionImpl> range : individualDeletedMessages.asRanges()) {
                if (range.isConnected(accountedEntriesRange)) {
                    Range<PositionImpl> commonEntries = range.intersection(accountedEntriesRange);
                    long commonCount = ledger.getNumberOfEntries(commonEntries);
                    log.debug("[{}] [{}] Discounting {} entries for already deleted range {}", ledger.getName(), name,
                            commonCount, commonEntries);
                    deletedEntries += commonCount;
                }
            }
        } finally {
            deletedMessagesMutex.readLock().unlock();;
        }

        return allEntries - deletedEntries;
    }

    @Override
    public void markDelete(Position position) throws InterruptedException, ManagedLedgerException {
        checkNotNull(position);
        checkArgument(position instanceof PositionImpl);

        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        asyncMarkDelete(position, new MarkDeleteCallback() {
            public void markDeleteComplete(Object ctx) {
                counter.countDown();
            }

            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }
        }, null);

        counter.await();
        if (result.exception != null) {
            throw result.exception;
        }
    }

    /**
     * 
     * @param newPosition
     *            the new acknowledged position
     * @return the previous acknowledged position
     */
    PositionImpl setAcknowledgedPosition(PositionImpl newPosition) {
        PositionImpl currentRead = null;
        do {
            currentRead = readPosition.get();
            if (currentRead != null && newPosition.compareTo(currentRead) < 0) {
                // Read position is already forward the new mark-delete point
                break;
            }

            // If the position that is markdeleted is past the read position, it
            // means that the client has skipped some entries. We need to move
            // read position forward
        } while (readPosition.compareAndSet(currentRead,
                new PositionImpl(newPosition.getLedgerId(), newPosition.getEntryId() + 1)));

        PositionImpl oldPosition = null;
        do {
            oldPosition = acknowledgedPosition.get();
            if (oldPosition != null && newPosition.compareTo(oldPosition) <= 0) {
                throw new IllegalArgumentException("Mark deleting an already mark-deleted position");
            }
        } while (!acknowledgedPosition.compareAndSet(oldPosition, newPosition));

        return oldPosition;
    }

    @Override
    public void asyncMarkDelete(final Position position, final MarkDeleteCallback callback, final Object ctx) {
        checkNotNull(position);
        checkArgument(position instanceof PositionImpl);

        if (isClosed.get()) {
            callback.markDeleteFailed(new ManagedLedgerException("Cursor was already closed"), ctx);
            return;
        }

        // Do the asyncMarkDelete in a background thread to avoid holding the current thread when ledgerMutex.lockRead()
        // becomes blocking.
        ledger.getOrderedExecutor().submitOrdered(ledger.getName(), new SafeRunnable() {
            public void safeRun() {
                ledgerMutex.lockRead();

                log.debug("[{}] Mark delete cursor {} up to position: {}", ledger.getName(), name, position);
                final PositionImpl newPosition = (PositionImpl) position;
                PositionImpl oldPosition;
                try {
                    oldPosition = setAcknowledgedPosition(newPosition);
                } catch (IllegalArgumentException e) {
                    ledgerMutex.unlockRead();
                    callback.markDeleteFailed(new ManagedLedgerException(e), ctx);
                    return;
                }

                final PositionImpl oldPositionFinal = oldPosition;

                persistPosition(cursorLedger.get(), newPosition, new VoidCallback() {
                    public void operationComplete() {
                        log.debug("[{}] Mark delete cursor {} to position {} succeeded", ledger.getName(), name,
                                position);
                        ledgerMutex.unlockRead();

                        // Remove from the individual deleted messages all the entries before the new mark delete
                        // point.
                        deletedMessagesMutex.writeLock().lock();
                        try {
                            individualDeletedMessages.remove(Range.atMost(newPosition));
                        } finally {
                            deletedMessagesMutex.writeLock().unlock();
                        }

                        ledger.updateCursor(ManagedCursorImpl.this, oldPositionFinal, (PositionImpl) position);
                        callback.markDeleteComplete(ctx);
                    }

                    public void operationFailed(ManagedLedgerException exception) {
                        log.warn("[{}] Failed to mark delete position for cursor={} ledger={} position={}",
                                ledger.getName(), ManagedCursorImpl.this, position);
                        ledgerMutex.unlockRead();
                        callback.markDeleteFailed(exception, ctx);
                    }
                });
            }
        });
    }

    @Override
    public void delete(Position position) throws InterruptedException, ManagedLedgerException {
        checkNotNull(position);
        checkArgument(position instanceof PositionImpl);

        class Result {
            ManagedLedgerException exception = null;
        }

        final Result result = new Result();
        final CountDownLatch counter = new CountDownLatch(1);

        asyncDelete(position, new AsyncCallbacks.DeleteCallback() {
            public void deleteComplete(Object ctx) {
                counter.countDown();
            }

            public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }
        }, null);

        counter.await();
        if (result.exception != null) {
            throw result.exception;
        }
    }

    @Override
    public void asyncDelete(Position pos, final AsyncCallbacks.DeleteCallback callback, Object ctx) {
        checkArgument(pos instanceof PositionImpl);

        if (isClosed.get()) {
            callback.deleteFailed(new ManagedLedgerException("Cursor was already closed"), ctx);
            return;
        }

        log.debug("[{}] [{}] Deleting single message at {}", ledger.getName(), name, pos);

        PositionImpl position = (PositionImpl) pos;
        Range<PositionImpl> range = null;

        deletedMessagesMutex.writeLock().lock();
        try {
            if (individualDeletedMessages.contains(position)) {
                throw new IllegalArgumentException("Position had already been deleted");
            }

            PositionImpl previousPosition = ledger.getPreviousPosition(position);
            // Add a range (prev, pos] to the set. Adding the previous entry as an open limit to the range will make the
            // RangeSet recognize the "continuity" between adjacent Positions
            individualDeletedMessages.add(Range.openClosed(previousPosition, position));
            log.debug("[{}] [{}] Individually deleted messages: {}", ledger.getName(), name, individualDeletedMessages);

            // If the lower bound of the range set is the current mark delete position, then we can trigger a new mark
            // delete to the upper bound of the first range segment
            range = individualDeletedMessages.asRanges().iterator().next();

            checkArgument(range.lowerBoundType() == BoundType.OPEN);
            checkArgument(range.upperBoundType() == BoundType.CLOSED);
            if (range.lowerEndpoint().compareTo(acknowledgedPosition.get()) <= 0) {
                log.debug("[{}] Found a position range to mark delete for cursor {}: {} ", ledger.getName(), name,
                        range);
                asyncMarkDelete(range.upperEndpoint(), new MarkDeleteCallback() {
                    public void markDeleteComplete(Object ctx) {
                        callback.deleteComplete(ctx);
                    }

                    public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                        callback.deleteFailed(exception, ctx);
                    }

                }, ctx);
            } else {
                // No other operation can be done at this moment, the message will be markDeleted when all its previous
                // messages are not needed anymore
                callback.deleteComplete(ctx);
            }
        } finally {
            deletedMessagesMutex.writeLock().unlock();
        }
    }

    /**
     * Given a list of entries, filter out the entries that have already been individually deleted.
     * 
     * @param entries
     *            a list of entries
     * @return a list of entries not containing deleted messages
     */
    List<Entry> filterReadEntries(List<Entry> entries) {
        deletedMessagesMutex.readLock().lock();
        try {
            Range<PositionImpl> entriesRange = Range.closed((PositionImpl) entries.get(0).getPosition(),
                    (PositionImpl) entries.get(entries.size() - 1).getPosition());
            log.debug("[{}] [{}] Filtering entries {} - alreadyDeleted: {}", ledger.getName(), name, entriesRange,
                    individualDeletedMessages);

            if (individualDeletedMessages.isEmpty() || !entriesRange.isConnected(individualDeletedMessages.span())) {
                // There are no individually deleted messages in this entry list, no need to perform filtering
                log.debug("[{}] [{}] No filtering needed for entries {}", ledger.getName(), name, entriesRange);
                return entries;
            } else {
                // Remove from the entry list all the entries that were already marked for deletion
                return Lists.newArrayList(Collections2.filter(entries, new Predicate<Entry>() {
                    public boolean apply(Entry entry) {
                        boolean includeEntry = !individualDeletedMessages.contains((PositionImpl) entry.getPosition());
                        if (!includeEntry) {
                            log.debug("[{}] [{}] Filtering entry at {} - already deleted", ledger.getName(), name,
                                    entry.getPosition());
                        }
                        return includeEntry;
                    }
                }));
            }
        } finally {
            deletedMessagesMutex.readLock().unlock();
        }
    }

    @Override
    public synchronized String toString() {
        return Objects.toStringHelper(this).add("ledger", ledger.getName()).add("name", name)
                .add("ackPos", acknowledgedPosition).add("readPos", readPosition).toString();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Position getReadPosition() {
        return readPosition.get();
    }

    @Override
    public Position getMarkDeletedPosition() {
        return acknowledgedPosition.get();
    }

    @Override
    public void skip(int entries) throws ManagedLedgerException {
        checkArgument(entries > 0);
        readPosition.set(ledger.skipEntries(readPosition.get(), entries));
    }

    @Override
    public void rewind() throws ManagedLedgerException {
        // The acked position can possibly be modified before we reset the read position. We need to make sure that this
        // doesn't happen.
        PositionImpl markDeleted;
        do {
            markDeleted = acknowledgedPosition.get();
            readPosition.set(new PositionImpl(markDeleted.getLedgerId(), markDeleted.getEntryId() + 1));
        } while (markDeleted != acknowledgedPosition.get());
    }

    @Override
    public void seek(Position newReadPositionInt) throws ManagedLedgerException {
        checkArgument(newReadPositionInt instanceof PositionImpl);
        PositionImpl newReadPosition = (PositionImpl) newReadPositionInt;
        checkArgument(newReadPosition.compareTo(acknowledgedPosition.get()) > 0,
                "new read position must be greater than the mark deleted position for this cursor");

        checkArgument(ledger.isValidPosition(newReadPosition), "new read position is not valid for this managed ledger");
        readPosition.set(newReadPosition);
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {
        if (!isClosed.compareAndSet(false, true)) {
            // Already closed
            return;
        }

        ledgerMutex.lockWrite();
        LedgerHandle lh = cursorLedger.get();

        try {
            lh.close();
        } catch (BKException e) {
            throw new ManagedLedgerException(e);
        } finally {
            ledgerMutex.unlockWrite();
        }
    }

    /**
     * Internal version of seek that doesn't do the validation check
     * 
     * @param newReadPosition
     */
    void setReadPosition(Position newReadPositionInt) {
        checkArgument(newReadPositionInt instanceof PositionImpl);
        PositionImpl newReadPosition = (PositionImpl) newReadPositionInt;

        // Make sure the read position moves monotonically forward
        PositionImpl oldPosition = null;
        do {
            oldPosition = readPosition.get();
            if (newReadPosition.compareTo(oldPosition) <= 0) {
                // The current position is already ahead of newReadPosition,
                // we should skip the update
                break;
            }
        } while (readPosition.compareAndSet(oldPosition, newReadPosition));
    }

    // //////////////////////////////////////////////////
    void createNewMetadataLedger(final VoidCallback callback) {
        bookkeeper.asyncCreateLedger(config.getMetadataEnsemblesize(), config.getMetadataWriteQuorumSize(),
                config.getMetadataAckQuorumSize(), config.getDigestType(), config.getPassword(), new CreateCallback() {
                    public void createComplete(int rc, final LedgerHandle lh, Object ctx) {
                        if (rc == BKException.Code.OK) {
                            log.debug("[{}] Created ledger {} for cursor {}", ledger.getName(), lh.getId(), name);
                            // Created the ledger, now write the last position
                            // content
                            final PositionImpl position = acknowledgedPosition.get();
                            persistPosition(lh, position, new VoidCallback() {
                                public void operationComplete() {
                                    log.debug("[{}] Persisted position {} for cursor {}", ledger.getName(), position,
                                            name);
                                    switchToNewLedger(lh, callback);
                                }

                                public void operationFailed(ManagedLedgerException exception) {
                                    log.warn("[{}] Failed to persist position {} for cursor {}", ledger.getName(),
                                            position, name);

                                    bookkeeper.asyncDeleteLedger(lh.getId(), new DeleteCallback() {
                                        public void deleteComplete(int rc, Object ctx) {
                                        }
                                    }, null);
                                    callback.operationFailed(exception);
                                }
                            });
                        } else {
                            log.warn("[{}] Error creating ledger for cursor {}: {}", ledger.getName(), name,
                                    BKException.getMessage(rc));
                            callback.operationFailed(new ManagedLedgerException(BKException.create(rc)));
                        }
                    }
                }, null);
    }

    void persistPosition(final LedgerHandle lh, final PositionImpl position, final VoidCallback callback) {
        PositionInfo pi = position.getPositionInfo();
        log.debug("[{}] Cursor {} Appending to ledger={} position={}", ledger.getName(), name, lh.getId(), position);
        lh.asyncAddEntry(pi.toByteArray(), new AddCallback() {
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                if (rc == BKException.Code.OK) {
                    log.debug("[{}] Updated cursor {} position {} in meta-ledger {}", ledger.getName(), name, position,
                            lh.getId());
                    callback.operationComplete();

                    if (lh.getLastAddConfirmed() == config.getMetadataMaxEntriesPerLedger()) {
                        log.debug("[{}] Need to create new metadata ledger for consumer {}", ledger.getName(), name);

                        // Force to create a new ledger in a background thread
                        ledger.getExecutor().execute(new Runnable() {
                            public void run() {
                                ledgerMutex.lockWrite();

                                createNewMetadataLedger(new VoidCallback() {
                                    public void operationComplete() {
                                        log.debug("[{}] Created new metadata ledger for consumer {}", ledger.getName(),
                                                name);
                                        ledgerMutex.unlockWrite();
                                    }

                                    public void operationFailed(ManagedLedgerException exception) {
                                        log.warn("[{}] Failed to create new metadata ledger for consumer {}: {}",
                                                ledger.getName(), name, exception);
                                        ledgerMutex.unlockWrite();
                                    }
                                });
                            }
                        });
                    }
                } else {
                    log.warn("[{}] Error updating cursor {} position {} in meta-ledger {}: ", ledger.getName(), name,
                            position, lh.getId(), BKException.create(rc));
                    callback.operationFailed(new ManagedLedgerException(BKException.create(rc)));
                }
            }
        }, null);
    }

    void switchToNewLedger(final LedgerHandle lh, final VoidCallback callback) {
        // Now we have an opened ledger that already has the acknowledged
        // position written into. At this point we can start using this new
        // ledger and delete the old one.
        ManagedCursorInfo info = ManagedCursorInfo.newBuilder().setCursorsLedgerId(lh.getId()).build();
        log.debug("[{}] Switchting cursor {} to ledger {}", ledger.getName(), name, lh.getId());

        ledger.getStore().asyncUpdateConsumer(ledger.getName(), name, info, cursorLedgerVersion.get(),
                new MetaStoreCallback<Void>() {
                    public void operationComplete(Void result, Version version) {
                        log.info("[{}] Updated consumer {} with ledger id {} md-position={} rd-position={}",
                                ledger.getName(), name, lh.getId(), acknowledgedPosition.get(), readPosition.get());
                        final LedgerHandle oldLedger = cursorLedger.getAndSet(lh);
                        cursorLedgerVersion.set(version);
                        closeAndDeleteLedger(oldLedger, new VoidCallback() {
                            public void operationComplete() {
                                log.debug("[{}] Successfully closed&deleted ledger {} in cursor", ledger.getName(),
                                        oldLedger, name);
                                callback.operationComplete();
                            }

                            public void operationFailed(ManagedLedgerException exception) {
                                log.warn("[{}] Error when removing ledger {} cursor {}", ledger.getName(),
                                        oldLedger.getId(), name, exception);

                                // At this point the position had already been safely markdeleted
                                callback.operationComplete();
                            }
                        });

                    }

                    public void operationFailed(MetaStoreException e) {
                        log.warn("[{}] Failed to update consumer {}", ledger.getName(), name, e);
                        callback.operationFailed(e);
                    }
                });
    }

    void closeAndDeleteLedger(final LedgerHandle lh, final VoidCallback callback) {
        if (lh == null) {
            callback.operationComplete();
            return;
        }

        lh.asyncClose(new CloseCallback() {
            public void closeComplete(int rc, final LedgerHandle lh, Object ctx) {
                if (rc != BKException.Code.OK) {
                    log.warn("[{}] Failed to close ledger {}", ledger.getName(), lh.getId());
                    callback.operationFailed(new ManagedLedgerException(BKException.create(rc)));
                    return;
                }

                bookkeeper.asyncDeleteLedger(lh.getId(), new DeleteCallback() {
                    public void deleteComplete(int rc, Object ctx) {
                        if (rc != BKException.Code.OK) {
                            log.warn("[{}] Failed to delete ledger {}", ledger.getName(), lh.getId());
                            callback.operationFailed(new ManagedLedgerException(BKException.create(rc)));
                            return;
                        }

                        callback.operationComplete();
                    }
                }, null);
            }
        }, null);
    }

    void asyncDeleteCursor(final VoidCallback callback) {
        isClosed.set(true);
        ledgerMutex.lockWrite();

        bookkeeper.asyncDeleteLedger(cursorLedger.get().getId(), new DeleteCallback() {
            public void deleteComplete(int rc, Object ctx) {
                ledgerMutex.unlockWrite();

                if (rc == BKException.Code.OK) {
                    callback.operationComplete();
                } else {
                    callback.operationFailed(new ManagedLedgerException(BKException.create(rc)));
                }
            }
        }, null);
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorImpl.class);
}
