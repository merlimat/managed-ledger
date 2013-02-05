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

import java.util.List;

import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;

/**
 * Interface that describes the operations that the ManagedLedger need to do on
 * the metadata store.
 */
public interface MetaStore {

    public static interface Version {
    }

    public static interface UpdateLedgersIdsCallback {
        void updateLedgersIdsComplete(MetaStoreException status, Version version);
    }

    public static interface MetaStoreCallback<T> {
        void operationComplete(T result, Version version);

        void operationFailed(MetaStoreException e);
    }

    enum OpenMode {
        AdminObserver, CreateIfNotFound
    }

    /**
     * Get the metadata used by the ManagedLedger
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param mode
     *            opening mode (ReadOnly or CreateIfNotFound)
     * @return a version object and a list of LedgerStats
     * @throws MetaStoreException
     */
    void getManagedLedgerInfo(String ledgerName, OpenMode mode, MetaStoreCallback<ManagedLedgerInfo> callback);

    /**
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * 
     * @param ManagedLedgerInfo
     *            the metadata object to be persisted
     * @param version
     *            version object associated with current state
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context object
     */
    void asyncUpdateLedgerIds(String ledgerName, ManagedLedgerInfo mlInfo, Version version,
            MetaStoreCallback<Void> callback);

    /**
     * Get the list of consumer registered on a ManagedLedger.
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @return a list of the consumer Ids
     * @throws MetaStoreException
     */
    void getConsumers(String ledgerName, MetaStoreCallback<List<String>> callback);

    /**
     * Get the ledger id associated with a consumer.
     * 
     * This ledger id will contains the mark-deleted position for the consumer.
     * 
     * @param ledgerName
     * @param consumerName
     * @param callback
     */
    void asyncGetConsumerLedgerId(String ledgerName, String consumerName, MetaStoreCallback<ManagedCursorInfo> callback);

    /**
     * Update the persisted position of a consumer
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param consumerName
     * @param ledgerId
     * @param callback
     *            the callback
     * @throws MetaStoreException
     */
    void asyncUpdateConsumer(String ledgerName, String consumerName, ManagedCursorInfo info, Version version,
            MetaStoreCallback<Void> callback);

    /**
     * Drop the persistent state of a consumer from the metadata store
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param consumerName
     *            the consumer name
     * @param callback
     *            callback object
     */
    void asyncRemoveConsumer(String ledgerName, String consumerName, MetaStoreCallback<Void> callback);

    /**
     * Drop the persistent state for the ManagedLedger and all its associated
     * consumers.
     * 
     * @param ledgerName
     *            the name of the ManagedLedger
     * @throws MetaStoreException
     */
    void removeManagedLedger(String ledgerName) throws MetaStoreException;

    /**
     * Get a list of all the managed ledgers in the system
     * 
     * @return an Iterable of the names of the managed ledgers
     * @throws MetaStoreException
     */
    Iterable<String> getManagedLedgers() throws MetaStoreException;
}
