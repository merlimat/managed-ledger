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

import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;

public class ManagedCursorTest extends BookKeeperClusterTestCase {

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

        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c2.getNumberOfEntries(), 2);
        assertEquals(c3.getNumberOfEntries(), 1);
        assertEquals(c4.getNumberOfEntries(), 0);

        List<Entry> entries = c1.readEntries(2);
        assertEquals(entries.size(), 2);
        c1.markDelete(entries.get(1).getPosition());
        assertEquals(c1.getNumberOfEntries(), 1);
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
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        final CountDownLatch counter = new CountDownLatch(1);

        stopBookKeeper();

        cursor.asyncReadEntries(100, new ReadEntriesCallback() {
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                fail("async-call should have failed");
            }

            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                counter.countDown();
            }

        }, null);

        counter.await();
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
    void skipEntries() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        log.info("ok1");
        assertEquals(cursor.getNumberOfEntries(), 1);
        cursor.skip(1);
        log.info("ok2");
        assertEquals(cursor.getNumberOfEntries(), 0);

        log.info("ok3");
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        log.info("ok4");
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        log.info("ok5");
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        log.info("ok6");

        assertEquals(cursor.getNumberOfEntries(), 3);
        cursor.skip(2);
        assertEquals(cursor.getNumberOfEntries(), 1);
        List<Entry> entries = cursor.readEntries(10);
        assertEquals(entries.size(), 1);
    }

    @Test(timeOut = 20000)
    void skipEntries2() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        cursor.readEntries(2);
        cursor.skip(1);
        assertEquals(cursor.getNumberOfEntries(), 3);

        try {
            cursor.skip(4);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        cursor.skip(3);

        try {
            cursor.skip(1);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    void skipEntriesError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));
        ManagedCursor cursor = ledger.openCursor("c1");
        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        assertEquals(cursor.getNumberOfEntries(), 1);
        cursor.skip(-1);
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
        ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        ledger.addEntry("dummy-entry-6".getBytes(Encoding));

        cursor.seek(new PositionImpl(seekPosition.getLedgerId(), seekPosition.getEntryId()));
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

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorTest.class);
}
