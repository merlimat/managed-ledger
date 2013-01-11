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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.mledger.ManagedCursor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Contains all the cursors for a ManagedLedger.
 * 
 * The goal is to always know the slowest consumer and hence decide which is the
 * oldest ledger we need to keep.
 * 
 * This data structure maintains a list and a map of cursors. The map is used to
 * relate a cursor name with an entry in the linked-list. The list is a sorted
 * double linked-list of cursors.
 * 
 * When a cursor is markDeleted, this list is updated and the cursor is moved in
 * its new position.
 * 
 * To minimize the moving around, the order is maintained using the ledgerId,
 * but not the entryId, since we only care about ledgers to be deleted.
 * 
 */
class ManagedCursorContainer {

    private static class Node {
        ManagedCursor cursor;
        PositionImpl position;
        Node previous;
        Node next;
    }

    Node head;
    Node tail;

    // Maps a cursor to its position in the heap
    Map<String, Node> cursorEntries = Maps.newTreeMap();

    public void add(ManagedCursor cursor) {
        checkNotNull(cursor);

        // Append a new entry at the end of the list
        Node node = new Node();
        node.cursor = cursor;
        node.position = (PositionImpl) cursor.getMarkDeletedPosition();
        node.next = null;
        node.previous = tail;
        if (head == null)
            head = node;

        if (tail != null)
            tail.next = node;

        tail = node;

        cursorEntries.put(cursor.getName(), node);
        pushTowardHead(node);
    }

    public ManagedCursor get(String name) {
        Node node = cursorEntries.get(name);
        if (node != null)
            return node.cursor;
        else
            return null;
    }

    public void removeCursor(String name) {
        Node node = cursorEntries.get(name);
        checkNotNull(node);

        cursorEntries.remove(name);

        // Remove the node from the linked list
        if (node == head) {
            head = node.next;
            if (head != null)
                head.previous = null;
        }

        if (node == tail) {
            tail = node.previous;
            if (tail != null)
                tail.next = null;
        }
    }

    /**
     * Signal that a cursor position has been updated and that the container
     * must re-order the cursor list.
     * 
     * @param cursor
     */
    public void cursorUpdated(ManagedCursor cursor) {
        checkNotNull(cursor);

        Node node = cursorEntries.get(cursor.getName());
        checkNotNull(node);

        // The cursor can only move forward, so we need to push it toward the
        // end of the list to ensure the list maintains the order.
        node.position = (PositionImpl)cursor.getMarkDeletedPosition();
        pushTowardTail(node);
    }

    /**
     * Get the slowest reader position, meaning older acknowledged position
     * between all the cursors.
     * 
     * @return the slowest reader position
     */
    public PositionImpl getSlowestReaderPosition() {
        if (head == null)
            return null;
        else
            return (PositionImpl) head.position;
    }

    /**
     * Push a node toward the head of the list. Stops when it encounter a node
     * whose position is lesser than this node position.
     * 
     * @param node
     *            the node to push
     */
    private void pushTowardHead(Node node) {
        while (node.previous != null) {
            // While this node is "bigger" than its previous, swap the two.
            long currentId = node.position.getLedgerId();
            long previousId = node.previous.position.getLedgerId();
            if (currentId < previousId) {
                // Swap the 2 entries
                if (node.previous == head)
                    head = node;

                if (node == tail)
                    tail = node.previous;

                swapWithPrevious(node);
            } else {
                break;
            }
        }
    }

    /**
     * Push a node toward the tail of the list. Stops when it encounter a node
     * whose position is greater than this node position.
     * 
     * @param node
     *            the node to push
     */
    private void pushTowardTail(Node node) {
        while (node.next != null) {
            // While this node is "bigger" than its previous, swap the two.
            long current = node.position.getLedgerId();
            long next = node.next.position.getLedgerId();
            if (current > next) {
                // Swap the 2 entries

                if (node.next == tail)
                    tail = node;

                if (node == head)
                    head = node.next;

                swapWithPrevious(node.next);
            } else {
                break;
            }
        }
    }

    /**
     * Swap a node with its previous in the linked list updating all the
     * pointers.
     * 
     * @param node
     */
    private void swapWithPrevious(Node node) {
        Node previous = node.previous;
        node.previous = previous.previous;
        if (previous.previous != null)
            previous.previous.next = node;
        previous.previous = node;

        Node next = node.next;
        node.next = previous;
        previous.next = next;
        if (next != null)
            next.previous = previous;
    }

    public boolean isEmpty() {
        return cursorEntries.isEmpty();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');

        Node node = head;
        while (node != null) {
            if (node != head)
                sb.append(", ");
            sb.append(node.cursor);
            node = node.next;
        }

        sb.append(']');
        return sb.toString();
    }

    public Iterable<ManagedCursor> toList() {
        List<ManagedCursor> cursors = Lists.newArrayList();

        Node node = head;
        while (node != null) {
            cursors.add(node.cursor);
            node = node.next;
        }

        return cursors;
    }
}
