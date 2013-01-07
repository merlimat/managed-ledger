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
package org.apache.bookkeeper.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test BookKeeperClient which allows access to members we don't wish to expose in the public API.
 */
public class MockBookKeeper extends BookKeeper {

    Executor executor = Executors.newFixedThreadPool(1);

    public ZooKeeper getZkHandle() {
        return super.getZkHandle();
    }

    public ClientConfiguration getConf() {
        return super.getConf();
    }

    Map<Long, MockLedgerHandle> ledgers = new ConcurrentHashMap<Long, MockLedgerHandle>();
    AtomicLong sequence = new AtomicLong(3);
    AtomicBoolean stopped = new AtomicBoolean(false);

    public MockBookKeeper(ClientConfiguration conf, ZooKeeper zk) throws Exception {
        super(conf, zk);
    }

    public LedgerHandle createLedger(DigestType digestType, byte passwd[]) throws BKException {
        return createLedger(3, 2, digestType, passwd);
    }

    public LedgerHandle createLedger(int ensSize, int qSize, DigestType digestType, byte passwd[]) throws BKException {
        return createLedger(ensSize, qSize, qSize, digestType, passwd);
    }

    @Override
    public void asyncCreateLedger(int ensSize, int writeQuorumSize, int ackQuorumSize, DigestType digestType,
            byte[] passwd, final CreateCallback cb, final Object ctx) {
        executor.execute(new Runnable() {
            public void run() {

                if (stopped.get()) {
                    cb.createComplete(BKException.Code.WriteException, null, ctx);
                }

                try {
                    long id = sequence.getAndIncrement();
                    log.info("Creating ledger {}", id);
                    MockLedgerHandle lh = new MockLedgerHandle(MockBookKeeper.this, id);
                    ledgers.put(id, lh);
                    cb.createComplete(0, lh, ctx);
                } catch (Throwable t) {
                }
            }
        });
    }

    @Override
    public LedgerHandle createLedger(int ensSize, int writeQuorumSize, int ackQuorumSize, DigestType digestType,
            byte[] passwd) throws BKException {
        if (stopped.get()) {
            throw BKException.create(BKException.Code.WriteException);
        }

        try {
            long id = sequence.getAndIncrement();
            log.info("Creating ledger {}", id);
            MockLedgerHandle lh = new MockLedgerHandle(this, id);
            ledgers.put(id, lh);
            return lh;
        } catch (Throwable t) {
            log.error("Exception:", t);
            return null;
        }
    }

    @Override
    public void asyncCreateLedger(int ensSize, int qSize, DigestType digestType, byte[] passwd, CreateCallback cb,
            Object ctx) {
        asyncCreateLedger(ensSize, qSize, qSize, digestType, passwd, cb, ctx);
    }

    @Override
    public void asyncOpenLedger(long lId, DigestType digestType, byte[] passwd, OpenCallback cb, Object ctx) {
        if (stopped.get()) {
            cb.openComplete(BKException.Code.WriteException, null, ctx);
            return;
        }

        LedgerHandle lh = ledgers.get(lId);
        if (lh != null) {
            cb.openComplete(0, lh, ctx);
        } else {
            cb.openComplete(BKException.Code.NoSuchLedgerExistsException, null, ctx);
        }
    }

    @Override
    public void asyncOpenLedgerNoRecovery(long lId, DigestType digestType, byte[] passwd, OpenCallback cb, Object ctx) {
        if (stopped.get()) {
            cb.openComplete(BKException.Code.WriteException, null, ctx);
        } else {
            asyncOpenLedger(lId, digestType, passwd, cb, ctx);
        }
    }

    @Override
    public void asyncDeleteLedger(long lId, DeleteCallback cb, Object ctx) {
        if (stopped.get()) {
            cb.deleteComplete(BKException.Code.WriteException, ctx);
        } else if (ledgers.containsKey(lId)) {
            ledgers.remove(lId);
            cb.deleteComplete(0, ctx);
        } else {
            cb.deleteComplete(BKException.Code.NoSuchLedgerExistsException, ctx);
        }
    }

    @Override
    public void deleteLedger(long lId) throws InterruptedException, BKException {
        if (stopped.get()) {
            throw BKException.create(BKException.Code.WriteException);
        }

        if (!ledgers.containsKey(lId)) {
            throw BKException.create(BKException.Code.NoSuchLedgerExistsException);
        }

        ledgers.remove(lId);
    }

    @Override
    public void close() throws InterruptedException, BKException {
    }

    public void shutdown() {
        stopped.set(true);
        for (MockLedgerHandle ledger : ledgers.values()) {
            ledger.entries.clear();
        }

        ledgers.clear();
    }

    public boolean isStopped() {
        return stopped.get();
    }

    private static final Logger log = LoggerFactory.getLogger(MockBookKeeper.class);
}
