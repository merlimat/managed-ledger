package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class ManagedCursorConcurrencyTest extends BookKeeperClusterTestCase {

    Executor executor = Executors.newCachedThreadPool();

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

    @Test
    public void testConcurrentIndividualDeletes() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(100));

        final ManagedCursor cursor = ledger.openCursor("c1");

        final int N = 1000;
        final List<Position> addedEntries = Lists.newArrayListWithExpectedSize(N);

        for (int i = 0; i < N; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }

        final int Threads = 10;
        final CyclicBarrier barrier = new CyclicBarrier(Threads);
        final CountDownLatch counter = new CountDownLatch(Threads);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        for (int thread = 0; thread < Threads; thread++) {
            final int myThread = thread;
            executor.execute(new Runnable() {
                public void run() {
                    try {
                        barrier.await();

                        for (int i = 0; i < N; i++) {
                            int threadId = i % Threads;
                            if (threadId == myThread) {
                                cursor.delete(addedEntries.get(i));
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                        gotException.set(true);
                    } finally {
                        counter.countDown();
                    }
                }
            });
        }

        counter.await();

        assertEquals(gotException.get(), false);
        assertEquals(cursor.getMarkDeletedPosition(), addedEntries.get(addedEntries.size() - 1));
    }
}
