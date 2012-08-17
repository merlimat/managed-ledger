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

import com.google.common.annotations.Beta;

@SuppressWarnings("serial")
@Beta
public class ManagedLedgerException extends Exception {
    public ManagedLedgerException(String msg) {
        super(msg);
    }

    public ManagedLedgerException(Exception e) {
        super(e);
    }

    public static class MetaStoreException extends ManagedLedgerException {
        public MetaStoreException(Exception e) {
            super(e);
        }
    }

    public static class BadVersionException extends MetaStoreException {
        public BadVersionException(Exception e) {
            super(e);
        }
    }

    public static class ManagedLedgerFencedException extends ManagedLedgerException {
        public ManagedLedgerFencedException() {
            super(new Exception("Attempted to use a fenced managed ledger"));
        }

        public ManagedLedgerFencedException(Exception e) {
            super(e);
        }
    }
}
