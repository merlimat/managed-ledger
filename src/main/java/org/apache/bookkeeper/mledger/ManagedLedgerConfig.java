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
/**
 * 
 */
package org.apache.bookkeeper.mledger;

import java.util.Arrays;

import org.apache.bookkeeper.client.BookKeeper.DigestType;

import com.google.common.annotations.Beta;
import com.google.common.base.Charsets;

/**
 * Configuration class for a ManagedLedger
 */
@Beta
public class ManagedLedgerConfig {

    private int maxEntriesPerLedger = 50000;
    private int maxSizePerLedgerMb = 100;
    private int ensembleSize = 3;
    private int quorumSize = 2;
    private int metadataEnsemblesize = 3;
    private int metadataQuorumSize = 2;
    private int metadataMaxEntriesPerLedger = (int) 1e6;
    private DigestType digestType = DigestType.MAC;
    private byte[] password = "".getBytes(Charsets.UTF_8);

    /**
     * @return the maxEntriesPerLedger
     */
    public int getMaxEntriesPerLedger() {
        return maxEntriesPerLedger;
    }

    /**
     * @param maxEntriesPerLedger
     *            the maxEntriesPerLedger to set
     */
    public ManagedLedgerConfig setMaxEntriesPerLedger(int maxEntriesPerLedger) {
        this.maxEntriesPerLedger = maxEntriesPerLedger;
        return this;
    }

    /**
     * @return the maxSizePerLedgerMb
     */
    public int getMaxSizePerLedgerMb() {
        return maxSizePerLedgerMb;
    }

    /**
     * @param maxSizePerLedgerMb
     *            the maxSizePerLedgerMb to set
     */
    public ManagedLedgerConfig setMaxSizePerLedgerMb(int maxSizePerLedgerMb) {
        this.maxSizePerLedgerMb = maxSizePerLedgerMb;
        return this;
    }

    /**
     * @return the ensembleSize
     */
    public int getEnsembleSize() {
        return ensembleSize;
    }

    /**
     * @param ensembleSize
     *            the ensembleSize to set
     */
    public ManagedLedgerConfig setEnsembleSize(int ensembleSize) {
        this.ensembleSize = ensembleSize;
        return this;
    }

    /**
     * @return the quorumSize
     */
    public int getQuorumSize() {
        return quorumSize;
    }

    /**
     * @param quorumSize
     *            the quorumSize to set
     */
    public ManagedLedgerConfig setQuorumSize(int quorumSize) {
        this.quorumSize = quorumSize;
        return this;
    }

    /**
     * @return the digestType
     */
    public DigestType getDigestType() {
        return digestType;
    }

    /**
     * @param digestType
     *            the digestType to set
     */
    public ManagedLedgerConfig setDigestType(DigestType digestType) {
        this.digestType = digestType;
        return this;
    }

    /**
     * @return the password
     */
    public byte[] getPassword() {
        return Arrays.copyOf(password, password.length);
    }

    /**
     * @param password
     *            the password to set
     */
    public ManagedLedgerConfig setPassword(String password) {
        this.password = password.getBytes(Charsets.UTF_8);
        return this;
    }

    /**
     * @return the metadataEnsemblesize
     */
    public int getMetadataEnsemblesize() {
        return metadataEnsemblesize;
    }

    /**
     * @param metadataEnsemblesize
     *            the metadataEnsemblesize to set
     */
    public ManagedLedgerConfig setMetadataEnsemblesize(int metadataEnsemblesize) {
        this.metadataEnsemblesize = metadataEnsemblesize;
        return this;
    }

    /**
     * @return the metadataQuorumSize
     */
    public int getMetadataQuorumSize() {
        return metadataQuorumSize;
    }

    /**
     * @param metadataQuorumSize
     *            the metadataQuorumSize to set
     */
    public ManagedLedgerConfig setMetadataQuorumSize(int metadataQuorumSize) {
        this.metadataQuorumSize = metadataQuorumSize;
        return this;
    }

    /**
     * @return the metadataMaxEntriesPerLedger
     */
    public int getMetadataMaxEntriesPerLedger() {
        return metadataMaxEntriesPerLedger;
    }

    /**
     * @param metadataMaxEntriesPerLedger
     *            the metadataMaxEntriesPerLedger to set
     */
    public ManagedLedgerConfig setMetadataMaxEntriesPerLedger(int metadataMaxEntriesPerLedger) {
        this.metadataMaxEntriesPerLedger = metadataMaxEntriesPerLedger;
        return this;
    }

}
