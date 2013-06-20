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

import java.nio.charset.Charset;
import java.util.List;

import org.apache.bookkeeper.mledger.ManagedLedgerException.BadVersionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;

class MetaStoreImplZookeeper implements MetaStore {

    private static final Charset Encoding = Charsets.UTF_8;
    private static final List<ACL> Acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    private static final String prefixName = "/managed-ledgers";
    private static final String prefix = prefixName + "/";

    private final ZooKeeper zk;

    private static class ZKVersion implements Version {
        final int version;

        ZKVersion(int version) {
            this.version = version;
        }
    }

    public MetaStoreImplZookeeper(ZooKeeper zk) throws Exception {
        this.zk = zk;

        if (zk.exists(prefixName, false) == null) {
            zk.create(prefixName, new byte[0], Acl, CreateMode.PERSISTENT);
        }
    }

    @Override
    public void getManagedLedgerInfo(final String ledgerName, final OpenMode mode,
            final MetaStoreCallback<ManagedLedgerInfo> callback) {
        // Try to get the content or create an empty node
        zk.getData(prefix + ledgerName, false, new DataCallback() {
            public void processResult(int rc, String path, Object ctx, final byte[] readData, Stat stat) {
                if (rc == KeeperException.Code.OK.intValue()) {
                    try {
                        ManagedLedgerInfo.Builder builder = ManagedLedgerInfo.newBuilder();
                        TextFormat.merge(new String(readData, Encoding), builder);
                        ManagedLedgerInfo info = builder.build();
                        callback.operationComplete(info, new ZKVersion(stat.getVersion()));
                    } catch (ParseException e) {
                        callback.operationFailed(new MetaStoreException(e));
                    }
                } else if (rc == KeeperException.Code.NONODE.intValue() && mode == OpenMode.CreateIfNotFound) {
                    log.info("Creating '{}'", prefix + ledgerName);

                    StringCallback createcb = new StringCallback() {
                        public void processResult(int rc, String path, Object ctx, String name) {
                            if (rc == KeeperException.Code.OK.intValue()) {
                                ManagedLedgerInfo info = ManagedLedgerInfo.getDefaultInstance();
                                callback.operationComplete(info, new ZKVersion(0));
                            } else {
                                callback.operationFailed(new MetaStoreException(KeeperException
                                        .create(KeeperException.Code.get(rc))));
                            }
                        }
                    };

                    ZkUtils.createFullPathOptimistic(zk, prefix + ledgerName, new byte[0], Acl, CreateMode.PERSISTENT,
                            createcb, null);
                } else {
                    callback.operationFailed(new MetaStoreException(
                            KeeperException.create(KeeperException.Code.get(rc))));
                }
            }
        }, null);
    }

    @Override
    public void asyncUpdateLedgerIds(String ledgerName, ManagedLedgerInfo mlInfo, Version version,
            final MetaStoreCallback<Void> callback) {

        ZKVersion zkVersion = (ZKVersion) version;
        log.debug("Updating {} version={} with content={}", prefix + ledgerName, zkVersion.version, mlInfo);

        zk.setData(prefix + ledgerName, mlInfo.toString().getBytes(Encoding), zkVersion.version, new StatCallback() {
            public void processResult(int rc, String path, Object zkCtx, Stat stat) {
                log.debug("UpdateLedgersIdsCallback.processResult rc={}", rc);
                MetaStoreException status = null;
                if (rc == KeeperException.Code.BADVERSION.intValue()) {
                    // Content has been modified on ZK since our last read
                    status = new BadVersionException(KeeperException.create(KeeperException.Code.get(rc)));
                    callback.operationFailed(status);
                } else if (rc != KeeperException.Code.OK.intValue()) {
                    status = new MetaStoreException(KeeperException.create(KeeperException.Code.get(rc)));
                    callback.operationFailed(status);
                } else {
                    callback.operationComplete(null, new ZKVersion(stat.getVersion()));
                }
            }
        }, null);

        log.debug("asyncUpdateLedgerIds done");
    }

    @Override
    public void getConsumers(final String ledgerName, final MetaStoreCallback<List<String>> callback) {
        log.debug("Get consumers for path {}", ledgerName);
        zk.getChildren(prefix + ledgerName, false, new Children2Callback() {
            public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                log.debug("getConsumers complete rc={} children={}", rc, children);
                if (rc != KeeperException.Code.OK.intValue()) {
                    callback.operationFailed(new MetaStoreException(
                            KeeperException.create(KeeperException.Code.get(rc))));
                    return;
                }

                log.debug("version={}", stat.getVersion());
                ZKVersion version = new ZKVersion(stat.getVersion());
                callback.operationComplete(children, version);
            }
        }, null);
    }

    @Override
    public void asyncGetConsumerLedgerId(String ledgerName, String consumerName,
            final MetaStoreCallback<ManagedCursorInfo> callback) {
        String path = prefix + ledgerName + "/" + consumerName;
        log.debug("Reading from {}", path);
        zk.getData(path, false, new DataCallback() {
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (rc != KeeperException.Code.OK.intValue()) {
                    callback.operationFailed(new MetaStoreException(
                            KeeperException.create(KeeperException.Code.get(rc))));
                } else {
                    try {
                        ManagedCursorInfo.Builder info = ManagedCursorInfo.newBuilder();
                        TextFormat.merge(new String(data, Encoding), info);
                        callback.operationComplete(info.build(), new ZKVersion(stat.getVersion()));
                    } catch (ParseException e) {
                        callback.operationFailed(new MetaStoreException(e));
                    }
                }
            }
        }, null);
        log.debug("Reading from {} ok", path);
    }

    @Override
    public void asyncUpdateConsumer(final String ledgerName, final String consumerName, final ManagedCursorInfo info,
            Version version, final MetaStoreCallback<Void> callback) {
        log.info("[{}] Updating ledger_id consumer={} {}", ledgerName, consumerName, info);

        String path = prefix + ledgerName + "/" + consumerName;
        byte[] content = info.toString().getBytes(Encoding);

        if (version == null) {
            log.debug("[{}] Creating consumer {} on meta-data store with {}", ledgerName, consumerName, info);
            zk.create(path, content, Acl, CreateMode.PERSISTENT, new StringCallback() {
                public void processResult(int rc, String path, Object ctx, String name) {
                    if (rc != KeeperException.Code.OK.intValue()) {
                        log.warn("[{}] Error creating cosumer {} node on meta-data store with {}", ledgerName,
                                consumerName, info);
                        callback.operationFailed(new MetaStoreException(KeeperException.create(KeeperException.Code
                                .get(rc))));
                    } else {
                        log.debug("[{}] Created consumer {} on meta-data store with {}", ledgerName, consumerName, info);
                        callback.operationComplete(null, new ZKVersion(0));
                    }
                }
            }, null);
        } else {
            ZKVersion zkVersion = (ZKVersion) version;
            log.debug("[{}] Updating consumer {} on meta-data store with {}", ledgerName, consumerName, info);
            zk.setData(path, content, zkVersion.version, new StatCallback() {
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    if (rc != KeeperException.Code.OK.intValue()) {
                        callback.operationFailed(new MetaStoreException(KeeperException.create(KeeperException.Code
                                .get(rc))));
                    } else {
                        callback.operationComplete(null, new ZKVersion(stat.getVersion()));
                    }

                }
            }, null);
        }
    }

    @Override
    public void asyncRemoveConsumer(String ledgerName, String consumerName, final MetaStoreCallback<Void> callback) {
        log.info("[{}] Remove consumer={}", ledgerName, consumerName);
        zk.delete(prefix + ledgerName + "/" + consumerName, -1, new VoidCallback() {
            public void processResult(int rc, String path, Object ctx) {
                if (rc == KeeperException.Code.OK.intValue()) {
                    callback.operationComplete(null, null);
                } else {
                    callback.operationFailed(new MetaStoreException(
                            KeeperException.create(KeeperException.Code.get(rc))));
                }
            }
        }, null);
    }

    @Override
    public void removeManagedLedger(String ledgerName) throws MetaStoreException {
        try {
            log.info("[{}] Remove ManagedLedger", ledgerName);
            zk.delete(prefix + ledgerName, -1);
        } catch (Exception e) {
            throw new MetaStoreException(e);
        }
    }

    @Override
    public Iterable<String> getManagedLedgers() throws MetaStoreException {
        try {
            return zk.getChildren(prefixName, false);
        } catch (Exception e) {
            throw new MetaStoreException(e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(MetaStoreImplZookeeper.class);
}
