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
package org.apache.bookkeeper.mledger;

public interface ManagedLedgerMXBean {

    /**
     * @return the number of currently opened managed ledger
     */
    long getOpenedManagedLedgers();

    /**
     * @return the total size of the messages in active ledgers (accounting for the multiple copies stored)
     */
    long getStoredMessagesSize();

    /**
     * @return the number of backlog messages for all the consumers
     */
    long getNumberOfMessagesInBacklog();

    /**
     * @return the msg/s rate of messages added
     */
    double getAddEntryMessagesRate();

    /**
     * @return the bytes/s rate of messages added
     */
    double getAddEntryBytesRate();

    /**
     * @return the msg/s rate of messages read
     */
    double getReadEntriesRate();

    /**
     * @return the bytes/s rate of messages read
     */
    double getReadEntriesBytesRate();

    /**
     * @return the number of addEntry requests that succeeded
     */
    long getAddEntrySucceed();

    /**
     * @return the number of addEntry requests that failed
     */
    long getAddEntryErrors();

    /**
     * @return the number of readEntries requests that succeeded
     */
    long getReadEntriesSucceeded();

    /**
     * @return the number of readEntries requests that failed
     */
    long getReadEntriesErrors();

    // Entry size statistics

    double getEntrySizeMin();

    double getEntrySizeMax();

    long[] getEntrySizeBuckets();

    // Add entry latency statistics

    double getAddEntryLatencyMin();

    double getAddEntryLatencyMax();

    long[] getAddEntryLatencyBuckets();

}
