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

import static org.testng.Assert.fail;

import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.OpenMode;
import org.apache.bookkeeper.mledger.impl.MetaStore.Version;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.testng.annotations.Test;

public class MetaStoreImplZookeeperTest extends BookKeeperClusterTestCase {

    @Test
    void getMLList() throws Exception {
        MetaStore store = new MetaStoreImplZookeeper(zkc);

        zkc.failNow(Code.CONNECTIONLOSS);

        try {
            store.getManagedLedgers();
            fail("should fail in getting the list");
        } catch (MetaStoreException e) {
            // ok
        }
    }

    @Test
    void deleteNonExistingML() throws Exception {
        MetaStore store = new MetaStoreImplZookeeper(zkc);

        try {
            store.removeManagedLedger("non-existing");
            fail("should fail");
        } catch (MetaStoreException e) {
            // ok
        }
    }

    @Test(timeOut = 20000)
    void readMalformedML() throws Exception {
        MetaStore store = new MetaStoreImplZookeeper(zkc);

        zkc.create("/managed-ledgers/my_test", "non-valid".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        final CountDownLatch latch = new CountDownLatch(1);

        store.getManagedLedgerInfo("my_test", OpenMode.CreateIfNotFound,
                new MetaStoreCallback<MLDataFormats.ManagedLedgerInfo>() {
                    public void operationFailed(MetaStoreException e) {
                        // Ok
                        latch.countDown();
                    }

                    public void operationComplete(ManagedLedgerInfo result, Version version) {
                        fail("Operation should have failed");
                    }
                });

        latch.await();
    }

    @Test(timeOut = 20000)
    void readMalformedCursorNode() throws Exception {
        MetaStore store = new MetaStoreImplZookeeper(zkc);

        zkc.create("/managed-ledgers/my_test", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.create("/managed-ledgers/my_test/c1", "non-valid".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        final CountDownLatch latch = new CountDownLatch(1);
        store.asyncGetConsumerLedgerId("my_test", "c1", new MetaStoreCallback<MLDataFormats.ManagedCursorInfo>() {

            public void operationFailed(MetaStoreException e) {
                // Ok
                latch.countDown();
            }

            public void operationComplete(ManagedCursorInfo result, Version version) {
                fail("Operation should have failed");
            }
        });

        latch.await();
    }

    @Test(timeOut = 20000)
    void failInCreatingMLnode() throws Exception {
        MetaStore store = new MetaStoreImplZookeeper(zkc);

        final CountDownLatch latch = new CountDownLatch(1);

        zkc.failAfter(1, Code.CONNECTIONLOSS);

        store.getManagedLedgerInfo("my_test", OpenMode.CreateIfNotFound,
                new MetaStoreCallback<MLDataFormats.ManagedLedgerInfo>() {
                    public void operationFailed(MetaStoreException e) {
                        // Ok
                        latch.countDown();
                    }

                    public void operationComplete(ManagedLedgerInfo result, Version version) {
                        fail("Operation should have failed");
                    }
                });

        latch.await();
    }

    @Test(timeOut = 20000)
    void updatingCursorNode() throws Exception {
        final MetaStore store = new MetaStoreImplZookeeper(zkc);

        zkc.create("/managed-ledgers/my_test", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        final CountDownLatch latch = new CountDownLatch(1);

        ManagedCursorInfo info = ManagedCursorInfo.newBuilder().setCursorsLedgerId(1).build();
        store.asyncUpdateConsumer("my_test", "c1", info, null, new MetaStoreCallback<Void>() {
            public void operationFailed(MetaStoreException e) {
                fail("should have succeeded");
            }

            public void operationComplete(Void result, Version version) {
                // Update again using the version
                zkc.failNow(Code.CONNECTIONLOSS);

                ManagedCursorInfo info = ManagedCursorInfo.newBuilder().setCursorsLedgerId(2).build();
                store.asyncUpdateConsumer("my_test", "c1", info, version, new MetaStoreCallback<Void>() {
                    public void operationFailed(MetaStoreException e) {
                        // ok
                        latch.countDown();
                    }

                    @Override
                    public void operationComplete(Void result, Version version) {
                        fail("should have failed");
                    }
                });
            }
        });

        latch.await();
    }

    @Test(timeOut = 20000)
    void updatingMLNode() throws Exception {
        final MetaStore store = new MetaStoreImplZookeeper(zkc);

        zkc.create("/managed-ledgers/my_test", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        final CountDownLatch latch = new CountDownLatch(1);

        store.getManagedLedgerInfo("my_test", OpenMode.CreateIfNotFound, new MetaStoreCallback<ManagedLedgerInfo>() {
            public void operationFailed(MetaStoreException e) {
                fail("should have succeeded");
            }

            public void operationComplete(ManagedLedgerInfo mlInfo, Version version) {
                // Update again using the version
                zkc.failNow(Code.BADVERSION);

                store.asyncUpdateLedgerIds("my_test", mlInfo, version, new MetaStoreCallback<Void>() {
                    public void operationFailed(MetaStoreException e) {
                        // ok
                        latch.countDown();
                    }

                    @Override
                    public void operationComplete(Void result, Version version) {
                        fail("should have failed");
                    }
                });
            }
        });

        latch.await();
    }
}
