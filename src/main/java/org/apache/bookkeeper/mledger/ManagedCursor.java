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

import java.util.List;

import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;

import com.google.common.annotations.Beta;

/**
 * A ManangedCursor is a persisted cursor inside a ManagedLedger.
 * <p>
 * The ManagedCursor is used to read from the ManagedLedger and to signal when
 * the consumer is done with the messages that it has read before.
 */
@Beta
public interface ManagedCursor {

    /**
     * Get the unique cursor name.
     * 
     * @return the cursor name
     */
    public String getName();

    /**
     * Read entries from the ManagedLedger, up to the specified number. The
     * returned list can be smaller.
     * 
     * @param numberOfEntriesToRead
     *            maximum number of entries to return
     * @return the list of entries
     * @throws ManagedLedgerException
     */
    public List<Entry> readEntries(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException;

    /**
     * Asynchronously read entries from the ManagedLedger.
     * 
     * @see #readEntries(int)
     * @param numberOfEntriesToRead
     *            maximum number of entries to return
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncReadEntries(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx);

    /**
     * Tells whether this cursor has already consumed all the available entries.
     * <p>
     * This method is not blocking.
     * 
     * @return true if there are pending entries to read, false otherwise
     */
    public boolean hasMoreEntries();

    /**
     * Return the number of messages that this cursor still has to read.
     * 
     * This method has linear time complexity on the number of ledgers included
     * in the managed ledger.
     * 
     * @return the number of entries
     */
    public long getNumberOfEntries();

    /**
     * This signals that the reader is done with all the entries up to
     * "position" (included). This can potentially trigger a ledger deletion, if
     * all the other cursors are done too with the underlying ledger.
     * 
     * @param position
     *            the last position that have been successfully consumed
     * @throws ManagedLedgerException
     */
    public void markDelete(Position position) throws InterruptedException, ManagedLedgerException;

    /**
     * Asynchronous mark delete
     * 
     * @see #markDelete(Position)
     * @param position
     *            the last position that have been successfully consumed
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncMarkDelete(Position position, MarkDeleteCallback callback, Object ctx);

    /**
     * Get the read position. This points to the next message to be read from
     * the cursor.
     * 
     * @return the read position
     */
    public Position getReadPosition();

    /**
     * Get the newest mark deleted position on this cursor.
     * 
     * @return the mark deleted position
     */
    public Position getMarkDeletedPosition();

    /**
     * Advance the read position by n entries.
     * 
     * The number of entries to be skipped must be less/equal than the total
     * number of entries for this cursor.
     * 
     * @param n
     *            the number of messages the cursor has to skip
     */
    public void skip(int n) throws ManagedLedgerException;

    /**
     * Move the cursor to a different read position.
     * 
     * The new position cannot be before the already mark deleted position and
     * cannot be past the last written entry in the ManagedLedger.
     * 
     * @param newReadPosition
     *            the position where to move the cursor
     */
    public void seek(Position newReadPosition) throws ManagedLedgerException;

    /**
     * Close the cursor and releases the associated resources.
     * 
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    public void close() throws InterruptedException, ManagedLedgerException;
}
