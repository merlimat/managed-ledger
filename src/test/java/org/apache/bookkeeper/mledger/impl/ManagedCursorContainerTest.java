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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.List;

import org.apache.bookkeeper.mledger.AsyncCallbacks.ClearBacklogCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

@Test
public class ManagedCursorContainerTest {

    private static class MockManagedCursor implements ManagedCursor {

        ManagedCursorContainer container;
        Position position;
        String name;

        public MockManagedCursor(ManagedCursorContainer container, String name, Position position) {
            this.container = container;
            this.name = name;
            this.position = position;
        }

        @Override
        public List<Entry> readEntries(int numberOfEntriesToRead) throws ManagedLedgerException {
            return Lists.newArrayList();
        }

        @Override
        public void asyncReadEntries(int numberOfEntriesToRead, ReadEntriesCallback callback, Object ctx) {
            callback.readEntriesComplete(null, ctx);
        }

        @Override
        public boolean hasMoreEntries() {
            return true;
        }

        @Override
        public long getNumberOfEntries() {
            return 0;
        }

        @Override
        public long getNumberOfEntriesInBacklog() {
            return 0;
        }

        @Override
        public void markDelete(Position position) throws ManagedLedgerException {
            this.position = position;
            container.cursorUpdated(this);
        }

        @Override
        public void asyncMarkDelete(Position position, MarkDeleteCallback callback, Object ctx) {
            fail();
        }

        @Override
        public Position getMarkDeletedPosition() {
            return position;
        }

        @Override
        public String getName() {
            return name;
        }

        public String toString() {
            return String.format("%s=%s", name, position);
        }

        @Override
        public Position getReadPosition() {
            return null;
        }

        @Override
        public void rewind() {
        }

        @Override
        public void seek(Position newReadPosition) {
        }

        @Override
        public void close() {
        }

        @Override
        public void delete(Position position) throws InterruptedException, ManagedLedgerException {
        }

        @Override
        public void asyncDelete(Position position, DeleteCallback callback, Object ctx) {
        }

        @Override
        public void clearBacklog() throws InterruptedException, ManagedLedgerException {
        }

        @Override
        public void asyncClearBacklog(ClearBacklogCallback callback, Object ctx) {
        }

    }

    @Test
    void simple() throws Exception {
        ManagedCursorContainer container = new ManagedCursorContainer();
        assertEquals(container.getSlowestReaderPosition(), null);

        ManagedCursor cursor1 = new MockManagedCursor(container, "test1", new PositionImpl(5, 5));
        container.add(cursor1);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 5));

        ManagedCursor cursor2 = new MockManagedCursor(container, "test2", new PositionImpl(2, 2));
        container.add(cursor2);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 2));

        ManagedCursor cursor3 = new MockManagedCursor(container, "test3", new PositionImpl(2, 0));
        container.add(cursor3);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 2));

        assertEquals(container.toString(), "[test2=2:2, test3=2:0, test1=5:5]");

        ManagedCursor cursor4 = new MockManagedCursor(container, "test4", new PositionImpl(4, 0));
        container.add(cursor4);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 2));

        ManagedCursor cursor5 = new MockManagedCursor(container, "test5", new PositionImpl(3, 5));
        container.add(cursor5);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 2));

        cursor3.markDelete(new PositionImpl(3, 0));
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 2));

        cursor2.markDelete(new PositionImpl(10, 5));
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(3, 0));

        container.removeCursor(cursor3.getName());
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(3, 5));

        container.removeCursor(cursor2.getName());
        container.removeCursor(cursor5.getName());
        container.removeCursor(cursor1.getName());
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(4, 0));
        container.removeCursor(cursor4.getName());
        assertEquals(container.getSlowestReaderPosition(), null);

        ManagedCursor cursor6 = new MockManagedCursor(container, "test6", new PositionImpl(6, 5));
        container.add(cursor6);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(6, 5));

        assertEquals(container.toString(), "[test6=6:5]");
    }

    @Test
    void updatingCursorOutsideContainer() throws Exception {
        ManagedCursorContainer container = new ManagedCursorContainer();

        ManagedCursor cursor1 = new MockManagedCursor(container, "test1", new PositionImpl(5, 5));
        container.add(cursor1);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 5));

        MockManagedCursor cursor2 = new MockManagedCursor(container, "test2", new PositionImpl(2, 2));
        container.add(cursor2);
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 2));

        cursor2.position = new PositionImpl(8, 8);

        // Until we don't update the container, the ordering will not change
        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(2, 2));

        container.cursorUpdated(cursor2);

        assertEquals(container.getSlowestReaderPosition(), new PositionImpl(5, 5));
    }

}
