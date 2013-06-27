package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

public class ManagedCursorConcurrencyTest extends BookKeeperClusterTestCase {

    @Test
    public void testMarkDeleteAndRead() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));

        final ManagedCursor cursor = ledger.openCursor("c1");

        final List<Position> addedEntries = Lists.newArrayList();

        for (int i = 0; i < 1000; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread() {
            public void run() {
                try {
                    barrier.await();

                    for (Position position : addedEntries) {
                        cursor.markDelete(position);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread reader = new Thread() {
            public void run() {
                try {
                    barrier.await();

                    for (int i = 0; i < 1000; i++) {
                        cursor.readEntries(1);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        deleter.start();
        reader.start();

        counter.await();

        assertEquals(gotException.get(), false);
    }
}
