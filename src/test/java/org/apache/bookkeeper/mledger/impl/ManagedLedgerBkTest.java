/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.mledger.impl;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.testng.annotations.Test;

public class ManagedLedgerBkTest extends BookKeeperClusterTestCase {

    public ManagedLedgerBkTest() {
        super(2);
    }

    @Test
    public void testBookieFailure() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        ManagedLedger ledger = factory.open("my-ledger", config);
        ManagedCursor cursor = ledger.openCursor("my-cursor");
        ledger.addEntry("entry-0".getBytes());

        killBookie(1);

        // Now we want to simulate that:
        // 1. The write operation fails because we only have 1 bookie available
        // 2. The bk client cannot properly close the ledger (finalizing the number of entries) because ZK is also
        // not available
        // 3. When we re-establish the service one, the ledger recovery will be triggered and the half-committed entry
        // is restored

        // Force to close the ZK client object so that BK will fail to close the ledger
        bkc.getZkHandle().close();

        try {
            ledger.addEntry("entry-1".getBytes());
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        bkc = new BookKeeperTestClient(baseClientConf);
        startNewBookie();

        // Reconnect a new bk client
        factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ledger = factory.open("my-ledger", config);
        cursor = ledger.openCursor("my-cursor");

        // Next add should succeed
        ledger.addEntry("entry-2".getBytes());

        assertEquals(3, cursor.getNumberOfEntriesInBacklog());

        List<Entry> entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-0", new String(entries.get(0).getData()));

        // entry-1 which was half-committed will get fully committed during the recovery phase
        entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-1", new String(entries.get(0).getData()));

        entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-2", new String(entries.get(0).getData()));
    }

    /**
     * Reproduce a race condition between opening cursors and concurrent mark delete operations
     */
    @Test(timeOut = 20000)
    public void testRaceCondition() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        final ManagedLedger ledger = factory.open("my-ledger", config);
        final ManagedCursor c1 = ledger.openCursor("c1");

        final int N = 1000;
        final Position position = ledger.addEntry("entry-0".getBytes());
        Executor executor = Executors.newCachedThreadPool();
        final CountDownLatch counter = new CountDownLatch(2);
        executor.execute(new Runnable() {
            public void run() {
                try {
                    for (int i = 0; i < N; i++) {
                        c1.markDelete(position);
                    }
                    counter.countDown();
                } catch (Exception e) {
                }
            }
        });

        executor.execute(new Runnable() {
            public void run() {
                try {
                    for (int i = 0; i < N; i++) {
                        ledger.openCursor("cursor-" + i);
                    }
                    counter.countDown();
                } catch (Exception e) {
                }
            }
        });

        // If there is the race condition, this method will not complete triggering the test timeout
        counter.await();
    }
}
