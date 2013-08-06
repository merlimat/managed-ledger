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
import static org.testng.Assert.fail;

import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.zookeeper.KeeperException.Code;
import org.testng.annotations.Test;

public class ManagedLedgerErrorsTest extends MockedBookKeeperTestCase {

    @Test
    public void removingCursor() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        assertEquals(zkc.exists("/managed-ledgers/my_test_ledger/c1", false) != null, true);

        bkc.failNow(BKException.Code.NoSuchLedgerExistsException);

        try {
            c1.close();
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        bkc.failNow(BKException.Code.NoSuchLedgerExistsException);

        try {
            ledger.deleteCursor("c1");
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        assertEquals(zkc.exists("/managed-ledgers/my_test_ledger/c1", false) != null, false);
        assertEquals(bkc.getLedgers().size(), 2);
    }

    @Test
    public void removingCursor2() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");

        zkc.failNow(Code.CONNECTIONLOSS);

        try {
            ledger.deleteCursor("c1");
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test
    public void closingManagedLedger() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        bkc.failNow(BKException.Code.NoSuchLedgerExistsException);

        try {
            ledger.close();
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        // ML should be still healthy
        Position p1 = ledger.addEntry("entry".getBytes());
        assertEquals(c1.readEntries(1).size(), 1);
        c1.markDelete(p1);
    }

    @Test
    public void asyncClosingManagedLedger() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");

        bkc.failNow(BKException.Code.NoSuchLedgerExistsException);

        final CountDownLatch latch = new CountDownLatch(1);
        ledger.asyncClose(new CloseCallback() {
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                latch.countDown();
            }

            public void closeComplete(Object ctx) {
                fail("should have failed");
            }
        }, null);

        latch.await();
    }

    @Test
    public void errorInRecovering() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.addEntry("entry".getBytes());

        ledger.close();

        factory = new ManagedLedgerFactoryImpl(bkc, zkc);

        bkc.failNow(BKException.Code.LedgerFencedException);

        try {
            ledger = factory.open("my_test_ledger");
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        // It should be fine now
        ledger = factory.open("my_test_ledger");
    }

    @Test
    public void errorInRecovering2() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.addEntry("entry".getBytes());

        ledger.close();

        factory = new ManagedLedgerFactoryImpl(bkc, zkc);

        bkc.failAfter(1, BKException.Code.LedgerFencedException);

        try {
            ledger = factory.open("my_test_ledger");
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        // It should be fine now
        ledger = factory.open("my_test_ledger");
    }

    @Test
    public void errorInRecovering3() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.addEntry("entry".getBytes());

        ledger.close();

        factory = new ManagedLedgerFactoryImpl(bkc, zkc);

        bkc.failAfter(2, BKException.Code.LedgerFencedException);

        try {
            ledger = factory.open("my_test_ledger");
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        // It should be fine now
        ledger = factory.open("my_test_ledger");
    }

    @Test
    public void errorInRecovering4() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.addEntry("entry".getBytes());

        ledger.close();

        factory = new ManagedLedgerFactoryImpl(bkc, zkc);

        zkc.failAfter(1, Code.CONNECTIONLOSS);

        try {
            ledger = factory.open("my_test_ledger");
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        // It should be fine now
        ledger = factory.open("my_test_ledger");
    }

    @Test
    public void errorInRecovering5() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.addEntry("entry".getBytes());

        ledger.close();

        factory = new ManagedLedgerFactoryImpl(bkc, zkc);

        zkc.failAfter(2, Code.CONNECTIONLOSS);

        try {
            ledger = factory.open("my_test_ledger");
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        // It should be fine now
        ledger = factory.open("my_test_ledger");
    }

    @Test
    public void errorInRecovering6() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger");
        ledger.openCursor("c1");
        ledger.addEntry("entry".getBytes());

        ledger.close();

        factory = new ManagedLedgerFactoryImpl(bkc, zkc);

        zkc.failAfter(3, Code.CONNECTIONLOSS);

        try {
            ledger = factory.open("my_test_ledger");
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        // It should be fine now
        ledger = factory.open("my_test_ledger");
    }

    @Test
    public void passwordError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setPassword("password"));
        ledger.openCursor("c1");
        ledger.addEntry("entry".getBytes());

        ledger.close();

        try {
            ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setPassword("wrong-password"));
            fail("should fail for password error");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test
    public void digestError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory
                .open("my_test_ledger", new ManagedLedgerConfig().setDigestType(DigestType.CRC32));
        ledger.openCursor("c1");
        ledger.addEntry("entry".getBytes());

        ledger.close();

        try {
            ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setDigestType(DigestType.MAC));
            fail("should fail for digest error");
        } catch (ManagedLedgerException e) {
            // ok
        }
    }

    @Test(timeOut = 20000, invocationCount = 1, skipFailedInvocations = true)
    public void errorInUpdatingLedgersList() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(1));

        final CountDownLatch latch = new CountDownLatch(1);

        zkc.failAfter(0, Code.CONNECTIONLOSS);

        ledger.asyncAddEntry("entry".getBytes(), new AddEntryCallback() {
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                // not-ok
            }

            public void addComplete(Position position, Object ctx) {
                // ok
            }
        }, null);

        ledger.asyncAddEntry("entry".getBytes(), new AddEntryCallback() {
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                latch.countDown();
            }

            public void addComplete(Position position, Object ctx) {
                fail("should have failed");
            }
        }, null);

        latch.await();
    }

    @Test
    public void recoverAfterWriteError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger");

        bkc.failNow(BKException.Code.BookieHandleNotAvailableException);

        try {
            ledger.addEntry("entry".getBytes());
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        // Next add should succeed
        ledger.addEntry("entry".getBytes());
    }

    @Test
    public void recoverAfterMarkDeleteError() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("my-cursor");
        Position position = ledger.addEntry("entry".getBytes());

        bkc.failNow(BKException.Code.BookieHandleNotAvailableException);

        try {
            cursor.markDelete(position);
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        // The metadata ledger is reopened in background, until it's not reopened the mark-delete will fail
        Thread.sleep(100);

        // Next markDelete should succeed
        cursor.markDelete(position);
    }
}
