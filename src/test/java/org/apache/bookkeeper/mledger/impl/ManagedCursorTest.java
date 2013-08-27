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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

public class ManagedCursorTest extends MockedBookKeeperTestCase {

    private static final Charset Encoding = Charsets.UTF_8;

    @Test(timeOut = 20000)
    void readFromEmptyLedger() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor c1 = ledger.openCursor("c1");
        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 0);

        ledger.addEntry("test".getBytes(Encoding));
        entries = c1.readEntries(10);
        assertEquals(entries.size(), 1);

        entries = c1.readEntries(10);
        assertEquals(entries.size(), 0);

        // Test string representation
        assertEquals(c1.toString(), "ManagedCursorImpl{ledger=my_test_ledger, name=c1, ackPos=3:-1, readPos=3:1}");

        factory.shutdown();
    }

    @Test(timeOut = 20000)
    void readTwice() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        ManagedCursor c1 = ledger.openCursor("c1");
        ManagedCursor c2 = ledger.openCursor("c2");

        ledger.addEntry("entry-1".getBytes(Encoding));
        ledger.addEntry("entry-2".getBytes(Encoding));

        assertEquals(c1.readEntries(2).size(), 2);
        assertEquals(c1.readEntries(2).size(), 0);

        assertEquals(c2.readEntries(2).size(), 2);
        assertEquals(c2.readEntries(2).size(), 0);
    }

    @Test(timeOut = 20000)
    void getEntryDataTwice() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");

        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-1".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 1);

        Entry entry = entries.get(0);
        assertEquals(entry.getLength(), "entry-1".length());
        byte[] data1 = entry.getData();
        byte[] data2 = entry.getData();
        assertEquals(data1, data2);
    }

    @Test(timeOut = 20000)
    void readFromClosedLedger() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.close();

        try {
            c1.readEntries(2);
            fail("ledger is closed, should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test(timeOut = 20000)
    void testNumberOfEntries() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));

        ManagedCursor c1 = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c2 = ledger.openCursor("c2");
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ManagedCursor c3 = ledger.openCursor("c3");
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ManagedCursor c4 = ledger.openCursor("c4");
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ManagedCursor c5 = ledger.openCursor("c5");

        assertEquals(c1.getNumberOfEntries(), 4);
        assertEquals(c1.hasMoreEntries(), true);

        assertEquals(c2.getNumberOfEntries(), 3);
        assertEquals(c2.hasMoreEntries(), true);

        assertEquals(c3.getNumberOfEntries(), 2);
        assertEquals(c3.hasMoreEntries(), true);

        assertEquals(c4.getNumberOfEntries(), 1);
        assertEquals(c4.hasMoreEntries(), true);

        assertEquals(c5.getNumberOfEntries(), 0);
        assertEquals(c5.hasMoreEntries(), false);

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 2);
        c1.markDelete(entries.get(1).getPosition());
        assertEquals(c1.getNumberOfEntries(), 2);
    }

    @Test(timeOut = 20000)
    void testNumberOfEntriesWithReopen() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        ManagedCursor c1 = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c2 = ledger.openCursor("c2");
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ManagedCursor c3 = ledger.openCursor("c3");

        factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        c1 = ledger.openCursor("c1");
        c2 = ledger.openCursor("c2");
        c3 = ledger.openCursor("c3");

        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.hasMoreEntries(), true);

        assertEquals(c2.getNumberOfEntries(), 1);
        assertEquals(c2.hasMoreEntries(), true);

        assertEquals(c3.getNumberOfEntries(), 0);
        assertEquals(c3.hasMoreEntries(), false);
    }

    @Test(timeOut = 20000)
    void asyncReadWithoutErrors() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CountDownLatch counter = new CountDownLatch(1);

        cursor.asyncReadEntries(100, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                assertNull(ctx);
                assertEquals(entries.size(), 1);
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                fail(exception.getMessage());
            }

        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    void asyncReadWithErrors() throws Exception {
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        
        final CountDownLatch counter = new CountDownLatch(1);

        stopBookKeeper();

        cursor.asyncReadEntries(100, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                counter.countDown();
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                fail("async-call should not have failed");
            }

        }, null);

        counter.await();
        
        cursor.rewind();
        
        // Clear the cache to force reading from BK
        ledger.entryCache.clear();

        final CountDownLatch counter2 = new CountDownLatch(1);

        cursor.asyncReadEntries(100, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                fail("async-call should have failed");
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                counter2.countDown();
            }

        }, null);

        counter2.await();
    }

    @Test(timeOut = 20000, expectedExceptions = IllegalArgumentException.class)
    void asyncReadWithInvalidParameter() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CountDownLatch counter = new CountDownLatch(1);

        stopBookKeeper();

        cursor.asyncReadEntries(0, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                fail("async-call should have failed");
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }

        }, null);

        counter.await();
    }

    @Test(timeOut = 20000)
    void markDeleteWithErrors() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        List<Entry> entries = cursor.readEntries(100);

        stopBookKeeper();
        assertEquals(entries.size(), 1);

        try {
            cursor.markDelete(entries.get(0).getPosition());
            fail("call should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test(timeOut = 20000)
    void seekPosition() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(10));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        PositionImpl lastPosition = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));

        cursor.seek(new PositionImpl(lastPosition.getLedgerId(), lastPosition.getEntryId() - 1));
    }

    @Test(timeOut = 20000)
    void seekPosition2() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        PositionImpl seekPosition = (PositionImpl) ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        cursor.seek(new PositionImpl(seekPosition.getLedgerId(), seekPosition.getEntryId()));
    }

    @Test(timeOut = 20000)
    void seekPosition3() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        PositionImpl seekPosition = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        Position entry5 = ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        Position entry6 = ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        cursor.seek(new PositionImpl(seekPosition.getLedgerId(), seekPosition.getEntryId()));

        assertEquals(cursor.getReadPosition(), seekPosition);
        List<Entry> entries = cursor.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(new String(entries.get(0).getData(), Encoding), "dummy-entry-4");

        cursor.seek(entry5.getNext());
        assertEquals(cursor.getReadPosition(), entry6);
        entries = cursor.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(new String(entries.get(0).getData(), Encoding), "dummy-entry-6");
    }

    @Test
    void seekPositionWithError3() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));
        ManagedCursor cursor = ledger.openCursor("c1");
        PositionImpl firstPosition = (PositionImpl) ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        PositionImpl lastPosition = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));

        cursor.seek(new PositionImpl(lastPosition.getLedgerId(), lastPosition.getEntryId() + 1));
        try {
            cursor.seek(new PositionImpl(lastPosition.getLedgerId(), lastPosition.getEntryId() + 2));
            fail("Should have failed");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            cursor.seek(new PositionImpl(firstPosition.getLedgerId(), 1000));
            fail("Should have failed");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            cursor.seek(new PositionImpl(firstPosition.getLedgerId() + 1000, 0));
            fail("Should have failed");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        cursor.markDelete(new PositionImpl(lastPosition.getLedgerId(), lastPosition.getEntryId() - 1));
        try {
            cursor.seek(new PositionImpl(lastPosition.getLedgerId(), lastPosition.getEntryId() - 1));
            fail("Should have failed");
        } catch (IllegalArgumentException e) {
            // Ok
        }
    }

    @Test(timeOut = 20000)
    void seekPosition4() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        cursor.markDelete(p1);
        cursor.readEntries(2);
        assertEquals(cursor.getMarkDeletedPosition(), p1);

        try {
            cursor.seek(cursor.getMarkDeletedPosition());
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // Ok
        }
    }

    @Test(timeOut = 20000)
    void rewind() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor c1 = ledger.openCursor("c1");
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        Position p4 = ledger.addEntry("dummy-entry-4".getBytes(Encoding));

        assertEquals(c1.getNumberOfEntries(), 4);
        c1.markDelete(p1);
        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c1.readEntries(10).size(), 3);
        assertEquals(c1.getNumberOfEntries(), 0);
        c1.rewind();
        assertEquals(c1.getNumberOfEntries(), 3);
        c1.markDelete(p2);
        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.readEntries(10).size(), 2);
        assertEquals(c1.readEntries(10).size(), 0);
        assertEquals(c1.getNumberOfEntries(), 0);
        c1.rewind();
        assertEquals(c1.getNumberOfEntries(), 2);
        c1.markDelete(p4);
        assertEquals(c1.getNumberOfEntries(), 0);
        c1.rewind();
        assertEquals(c1.getNumberOfEntries(), 0);
        ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        assertEquals(c1.getNumberOfEntries(), 1);
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));
        assertEquals(c1.getNumberOfEntries(), 2);
    }

    @Test(timeOut = 20000)
    void markDeleteSkippingMessage() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(10));
        ManagedCursor cursor = ledger.openCursor("c1");
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        PositionImpl p4 = (PositionImpl) ledger.addEntry("dummy-entry-4".getBytes(Encoding));

        assertEquals(cursor.getNumberOfEntries(), 4);

        cursor.markDelete(p1);
        assertEquals(cursor.hasMoreEntries(), true);
        assertEquals(cursor.getNumberOfEntries(), 3);

        assertEquals(cursor.getReadPosition(), p2);

        List<Entry> entries = cursor.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(new String(entries.get(0).getData(), Encoding), "dummy-entry-2");

        cursor.markDelete(p4);
        assertEquals(cursor.hasMoreEntries(), false);
        assertEquals(cursor.getNumberOfEntries(), 0);

        assertEquals(cursor.getReadPosition(), new PositionImpl(p4.getLedgerId(), p4.getEntryId() + 1));
    }

    @Test(timeOut = 20000)
    void removingCursor() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        assertEquals(cursor.getNumberOfEntries(), 6);
        assertEquals(ledger.getNumberOfEntries(), 6);
        ledger.deleteCursor("c1");

        // Verify that it's a new empty cursor
        cursor = ledger.openCursor("c1");
        assertEquals(cursor.getNumberOfEntries(), 0);
        ledger.addEntry("dummy-entry-7".getBytes(Encoding));

        // Verify that GC trimming kicks in
        while (ledger.getNumberOfEntries() > 2) {
            Thread.sleep(10);
        }
    }

    @Test(timeOut = 20000)
    void cursorPersistence() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");
        ManagedCursor c2 = ledger.openCursor("c2");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        List<Entry> entries = c1.readEntries(3);
        Position p1 = entries.get(2).getPosition();
        c1.markDelete(p1);

        entries = c1.readEntries(4);
        Position p2 = entries.get(2).getPosition();
        c2.markDelete(p2);

        // Reopen

        factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory.open("my_test_ledger");
        c1 = ledger.openCursor("c1");
        c2 = ledger.openCursor("c2");

        assertEquals(c1.getMarkDeletedPosition(), p1);
        assertEquals(c2.getMarkDeletedPosition(), p2);
    }

    @Test
    void cursorPersistence2() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger",
                new ManagedLedgerConfig().setMetadataMaxEntriesPerLedger(1));
        ManagedCursor c1 = ledger.openCursor("c1");
        ManagedCursor c2 = ledger.openCursor("c2");
        ManagedCursor c3 = ledger.openCursor("c3");
        Position p0 = c3.getMarkDeletedPosition();
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c4 = ledger.openCursor("c4");
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        Position p3 = ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        Position p4 = ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        Position p5 = ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        c1.markDelete(p1);
        c1.markDelete(p2);
        c1.markDelete(p3);
        c1.markDelete(p4);
        c1.markDelete(p5);

        c2.markDelete(p1);

        // Reopen

        factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory.open("my_test_ledger");
        c1 = ledger.openCursor("c1");
        c2 = ledger.openCursor("c2");
        c3 = ledger.openCursor("c3");
        c4 = ledger.openCursor("c4");

        assertEquals(c1.getMarkDeletedPosition(), p5);
        assertEquals(c2.getMarkDeletedPosition(), p1);
        assertEquals(c3.getMarkDeletedPosition(), p0);
        assertEquals(c4.getMarkDeletedPosition(), p1);
    }

    @Test(timeOut = 20000)
    public void asyncMarkDeleteBlocking() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        config.setMetadataMaxEntriesPerLedger(5);
        ManagedLedger ledger = factory.open("my_test_ledger", config);
        final ManagedCursor c1 = ledger.openCursor("c1");
        final AtomicReference<Position> lastPosition = new AtomicReference<Position>();

        final int N = 100;
        final CountDownLatch latch = new CountDownLatch(N);
        for (int i = 0; i < N; i++) {
            ledger.asyncAddEntry("entry".getBytes(Encoding), new AddEntryCallback() {
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                }

                public void addComplete(Position position, Object ctx) {
                    lastPosition.set(position);
                    c1.asyncMarkDelete(position, new MarkDeleteCallback() {
                        public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                        }

                        public void markDeleteComplete(Object ctx) {
                            latch.countDown();
                        }
                    }, null);
                }
            }, null);
        }

        latch.await();

        assertEquals(c1.getNumberOfEntries(), 0);

        // Reopen
        factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory.open("my_test_ledger");
        ManagedCursor c2 = ledger.openCursor("c1");

        assertEquals(c2.getMarkDeletedPosition(), lastPosition.get());

        factory.shutdown();
    }

    @Test(timeOut = 20000)
    void cursorPersistenceAsyncMarkDeleteSameThread() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger",
                new ManagedLedgerConfig().setMetadataMaxEntriesPerLedger(5));
        final ManagedCursor c1 = ledger.openCursor("c1");

        final int N = 100;
        List<Position> positions = Lists.newArrayList();
        for (int i = 0; i < N; i++) {
            Position p = ledger.addEntry("dummy-entry".getBytes(Encoding));
            positions.add(p);
        }

        Position lastPosition = positions.get(N - 1);

        final CountDownLatch latch = new CountDownLatch(N);
        for (final Position p : positions) {
            c1.asyncMarkDelete(p, new MarkDeleteCallback() {
                public void markDeleteComplete(Object ctx) {
                    latch.countDown();
                }

                public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("Failed to markdelete", exception);
                    latch.countDown();
                }
            }, null);
        }

        latch.await();

        // Reopen
        factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory.open("my_test_ledger");
        ManagedCursor c2 = ledger.openCursor("c1");

        assertEquals(c2.getMarkDeletedPosition(), lastPosition);
    }

    @Test
    void unorderedMarkDelete() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        final ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("entry-2".getBytes(Encoding));

        c1.markDelete(p2);
        try {
            c1.markDelete(p1);
            fail("Should have thrown exception");
        } catch (ManagedLedgerException e) {
            // ok
        }

        assertEquals(c1.getMarkDeletedPosition(), p2);
    }

    @Test
    void unorderedAsyncMarkDelete() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        final ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("entry-2".getBytes(Encoding));

        final CountDownLatch latch = new CountDownLatch(2);
        c1.asyncMarkDelete(p2, new MarkDeleteCallback() {
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                fail();
            }

            public void markDeleteComplete(Object ctx) {
                latch.countDown();
            }
        }, null);

        c1.asyncMarkDelete(p1, new MarkDeleteCallback() {
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                latch.countDown();
            }

            public void markDeleteComplete(Object ctx) {
                fail();
            }
        }, null);

        latch.await();

        assertEquals(c1.getMarkDeletedPosition(), p2);
    }

    @Test
    void deleteCursor() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("entry-2".getBytes(Encoding));

        assertEquals(c1.getNumberOfEntries(), 2);

        // Remove and recreate the same cursor
        ledger.deleteCursor("c1");

        try {
            c1.readEntries(10);
            fail("must fail, the cursor should be closed");
        } catch (ManagedLedgerException e) {
            // ok
        }

        try {
            c1.markDelete(p2);
            fail("must fail, the cursor should be closed");
        } catch (ManagedLedgerException e) {
            // ok
        }

        c1 = ledger.openCursor("c1");
        assertEquals(c1.getNumberOfEntries(), 0);

        c1.close();
        try {
            c1.readEntries(10);
            fail("must fail, the cursor should be closed");
        } catch (ManagedLedgerException e) {
            // ok
        }

        c1.close();
    }

    @Test(timeOut = 20000)
    void readOnlyCursor() throws Exception {
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");
        Position p1 = ledger.addEntry("entry-1".getBytes(Encoding));

        ledger.close();

        try {
            c1.readEntries(10);
            fail("must fail, the cursor should be closed");
        } catch (ManagedLedgerException e) {
            // ok
        }

        try {
            factory.openReadOnly("non_existing_ledger");
            fail("The managed ledger doesn't exist");
        } catch (ManagedLedgerException e) {
            // ok
        }

        // Re-open in read only mode
        ledger = factory.openReadOnly("my_test_ledger");
        List<ManagedCursor> cursors = Lists.newArrayList(ledger.getCursors());
        assertEquals(cursors.size(), 1);
        c1 = cursors.get(0);
        assertEquals(c1.getNumberOfEntries(), 1);

        try {
            c1.readEntries(1);
            fail("read-only mode");
        } catch (ManagedLedgerException e) {
            // ok
        }

        try {
            c1.markDelete(p1);
            fail("read-only mode");
        } catch (ManagedLedgerException e) {
            // ok
        }

        try {
            c1.rewind();
            fail("read-only mode");
        } catch (RuntimeException e) {
            // ok
        }

        try {
            c1.seek(p1);
            fail("read-only mode");
        } catch (RuntimeException e) {
            // ok
        }

        final CountDownLatch latch = new CountDownLatch(2);

        c1.asyncMarkDelete(p1, new MarkDeleteCallback() {
            public void markDeleteComplete(Object ctx) {
                fail("operation should not succeed");
            }

            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                latch.countDown();
            }
        }, null);

        c1.asyncReadEntries(10, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                fail("operation should not succeed");
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                latch.countDown();
            }
        }, null);

        latch.await();
    }

    @Test(timeOut = 20000)
    void errorCreatingCursor() throws Exception {
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");

        bkc.failAfter(1, BKException.Code.AuthTimeoutException);
        try {
            ledger.openCursor("c1");
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test(timeOut = 20000)
    void errorRecoveringCursor() throws Exception {
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");

        ledger.close();

        factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        bkc.failAfter(3, BKException.Code.AuthTimeoutException);

        try {
            ledger = factory.open("my_test_ledger");
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test(timeOut = 20000)
    void errorRecoveringCursor2() throws Exception {
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");

        ledger.close();

        factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());

        bkc.failAfter(4, BKException.Code.AuthTimeoutException);

        try {
            ledger = factory.open("my_test_ledger");
            fail("should have failed");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test(timeOut = 20000)
    void testSingleDelete() throws Exception {
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(3));
        ManagedCursor cursor = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry1".getBytes());
        Position p2 = ledger.addEntry("entry2".getBytes());
        Position p3 = ledger.addEntry("entry3".getBytes());
        Position p4 = ledger.addEntry("entry4".getBytes());
        Position p5 = ledger.addEntry("entry5".getBytes());
        Position p6 = ledger.addEntry("entry6".getBytes());

        Position p0 = cursor.getMarkDeletedPosition();

        cursor.delete(p4);
        assertEquals(cursor.getMarkDeletedPosition(), p0);

        cursor.delete(p1);
        assertEquals(cursor.getMarkDeletedPosition(), p1);

        cursor.delete(p3);
        assertEquals(cursor.getMarkDeletedPosition(), p1);

        try {
            cursor.delete(p3);
            fail("already deleted");
        } catch (ManagedLedgerException e) {
            // ok
        }

        cursor.delete(p2);
        assertEquals(cursor.getMarkDeletedPosition(), p4);

        cursor.delete(p5);
        assertEquals(cursor.getMarkDeletedPosition(), p5);

        cursor.close();
        try {
            cursor.delete(p6);
        } catch (ManagedLedgerException e) {
            // Ok
        }
    }

    @Test(timeOut = 20000)
    void testFilteringReadEntries() throws Exception {
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(3));
        ManagedCursor cursor = ledger.openCursor("c1");

        /* Position p1 = */ledger.addEntry("entry1".getBytes());
        /* Position p2 = */ledger.addEntry("entry2".getBytes());
        /* Position p3 = */ledger.addEntry("entry3".getBytes());
        /* Position p4 = */ledger.addEntry("entry4".getBytes());
        Position p5 = ledger.addEntry("entry5".getBytes());
        /* Position p6 = */ledger.addEntry("entry6".getBytes());

        assertEquals(cursor.getNumberOfEntries(), 6);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 6);

        assertEquals(cursor.readEntries(3).size(), 3);

        assertEquals(cursor.getNumberOfEntries(), 3);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 6);

        log.info("Deleting {}", p5);
        cursor.delete(p5);

        assertEquals(cursor.getNumberOfEntries(), 2);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 5);
        assertEquals(cursor.readEntries(3).size(), 2);
        assertEquals(cursor.getNumberOfEntries(), 0);
        assertEquals(cursor.getNumberOfEntriesInBacklog(), 5);
    }

    @Test(timeOut = 20000)
    void testCountingWithDeletedEntries() throws Exception {
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry1".getBytes());
        /* Position p2 = */ledger.addEntry("entry2".getBytes());
        /* Position p3 = */ledger.addEntry("entry3".getBytes());
        /* Position p4 = */ledger.addEntry("entry4".getBytes());
        Position p5 = ledger.addEntry("entry5".getBytes());
        Position p6 = ledger.addEntry("entry6".getBytes());
        Position p7 = ledger.addEntry("entry7".getBytes());
        Position p8 = ledger.addEntry("entry8".getBytes());

        assertEquals(cursor.getNumberOfEntries(), 8);

        cursor.delete(p8);
        assertEquals(cursor.getNumberOfEntries(), 7);

        cursor.delete(p1);
        assertEquals(cursor.getNumberOfEntries(), 6);

        cursor.delete(p7);
        cursor.delete(p6);
        cursor.delete(p5);
        assertEquals(cursor.getNumberOfEntries(), 2);
    }

    @Test(timeOut = 20000)
    void testMarkDeleteTwice() throws Exception {
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry1".getBytes());
        cursor.markDelete(p1);
        cursor.markDelete(p1);

        assertEquals(cursor.getMarkDeletedPosition(), p1);
    }

    @Test(timeOut = 20000)
    void testClearBacklog() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        ManagedCursor c1 = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ManagedCursor c2 = ledger.openCursor("c2");
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ManagedCursor c3 = ledger.openCursor("c3");
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));

        c1.clearBacklog();
        c3.clearBacklog();

        assertEquals(c1.getNumberOfEntriesInBacklog(), 0);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.hasMoreEntries(), false);

        assertEquals(c2.getNumberOfEntriesInBacklog(), 2);
        assertEquals(c2.getNumberOfEntries(), 2);
        assertEquals(c2.hasMoreEntries(), true);

        assertEquals(c3.getNumberOfEntriesInBacklog(), 0);
        assertEquals(c3.getNumberOfEntries(), 0);
        assertEquals(c3.hasMoreEntries(), false);

        factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        c1 = ledger.openCursor("c1");
        c2 = ledger.openCursor("c2");
        c3 = ledger.openCursor("c3");

        assertEquals(c1.getNumberOfEntriesInBacklog(), 0);
        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.hasMoreEntries(), false);

        assertEquals(c2.getNumberOfEntriesInBacklog(), 2);
        assertEquals(c2.getNumberOfEntries(), 2);
        assertEquals(c2.hasMoreEntries(), true);

        assertEquals(c3.getNumberOfEntriesInBacklog(), 0);
        assertEquals(c3.getNumberOfEntries(), 0);
        assertEquals(c3.hasMoreEntries(), false);
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorTest.class);
}
