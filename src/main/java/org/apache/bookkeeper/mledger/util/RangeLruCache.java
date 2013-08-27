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
package org.apache.bookkeeper.mledger.util;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * Special type of LRU where get() and delete() operations can be done over a range of keys.
 * <p>
 * The LRU eviction is implemented by having a linked list and pushing entries to the head of the list during access.
 * 
 * @param <Key>
 *            Cache key. Needs to be Comparable
 * @param <Value>
 *            Cache value
 */
public class RangeLruCache<Key extends Comparable<Key>, Value> {
    // Map from key to nodes inside the linked list
    private final ConcurrentNavigableMap<Key, Node<Key, Value>> entries;
    private Node<Key, Value> head; // Head of the linked list
    private Node<Key, Value> tail; // Tail of the linked list
    private AtomicLong size; // Total size of values stored in cache
    private final Weighter<Value> weighter; // Weighter object used to extract the size from values

    /**
     * Construct a new RangeLruCache with default Weighter
     */
    public RangeLruCache() {
        this(new DefaultWeighter<Value>());
    }

    /**
     * Construct a new RangeLruCache
     * 
     * @param weighter
     *            a custom weighter to compute the size of each stored value
     */
    public RangeLruCache(Weighter<Value> weighter) {
        this.size = new AtomicLong(0);
        this.entries = new ConcurrentSkipListMap<Key, Node<Key, Value>>();
        this.weighter = weighter;
        this.head = null;
        this.tail = null;
    }

    /**
     * Inser
     * 
     * @param key
     * @param value
     * @return
     */
    public Value put(Key key, Value value) {
        Node<Key, Value> newNode = new Node<Key, Value>(key, value);
        Node<Key, Value> oldNode = entries.put(key, newNode);

        // Modification to the LRU list need to be synchronized
        synchronized (this) {
            // Add the
            addFirst(newNode);
            size.addAndGet(weighter.getSize(newNode.value));

            if (oldNode != null) {
                // If there were a older value for the same key, we need to remove it and adjust the cache size
                removeFromList(oldNode);
                size.addAndGet(-weighter.getSize(oldNode.value));
                return oldNode.value;
            }

            return null;
        }
    }

    public Value get(Key key) {
        Node<Key, Value> node = entries.get(key);
        if (node == null) {
            return null;
        } else {
            synchronized (this) {
                moveToHead(node);
            }

            return node.value;
        }
    }

    /**
     * 
     * @param first
     *            the first key in the range
     * @param last
     *            the last key in the range (inclusive)
     * @return a collections of the value found in cache
     */
    public Collection<Value> getRange(Key first, Key last) {
        Collection<Node<Key, Value>> nodesInRange = entries.subMap(first, true, last, true).values();

        // Move all the entries to the head of the list, per the LRU logic
        synchronized (this) {
            for (Node<Key, Value> node : nodesInRange) {
                moveToHead(node);
            }
        }

        // Return the values of the entries found in cache
        return Collections2.transform(nodesInRange, new Function<Node<Key, Value>, Value>() {
            public Value apply(Node<Key, Value> node) {
                return node.value;
            }
        });
    }

    /**
     * 
     * @param first
     * @param last
     * @param lastInclusive
     * @return the number of removed entries
     */
    public synchronized int removeRange(Key first, Key last, boolean lastInclusive) {
        Map<Key, Node<Key, Value>> subMap = entries.subMap(first, true, last, lastInclusive);

        int removedEntries = 0;

        for (Key key : subMap.keySet()) {
            Node<Key, Value> node = entries.remove(key);
            removeFromList(node);

            size.addAndGet(-weighter.getSize(node.value));
            ++removedEntries;
        }

        return removedEntries;
    }

    /**
     * 
     * @param minSize
     * @return
     */
    public synchronized int evictLeastAccessedEntries(long minSize) {
        checkArgument(minSize > 0);

        long removedSize = 0;
        int removedEntries = 0;

        while (tail != null && removedSize < minSize) {
            Node<Key, Value> node = tail;
            entries.remove(node.key);
            removeFromList(node);

            ++removedEntries;
            removedSize += weighter.getSize(node.value);
        }

        size.addAndGet(-removedSize);
        return removedEntries;
    }

    public long getNumberOfEntries() {
        return entries.size();
    }

    public long getSize() {
        return size.get();
    }

    /**
     * Remove all the entries from the cache
     */
    public synchronized void clear() {
        entries.clear();
        head = tail = null;
        size.set(0);
    }

    /**
     * Interface of a object that is able to the extract the "weight" (size/cost/space) of the cached values
     * 
     * @param <Value>
     */
    public static interface Weighter<Value> {
        long getSize(Value value);
    }

    /**
     * Default cache weighter, every value is assumed the same cost
     * 
     * @param <Value>
     */
    private static class DefaultWeighter<Value> implements Weighter<Value> {
        public long getSize(Value value) {
            return 1;
        }
    }

    // // Private helpers

    /**
     * Remove a node from the linked list
     * 
     * @param node
     */
    private void removeFromList(Node<Key, Value> node) {
        if (node.prev == null) {
            head = node.next;
        } else {
            node.prev.next = node.next;
        }

        if (node.next == null) {
            tail = node.prev;
        } else {
            node.next.prev = node.prev;
        }

        node.next = null;
        node.prev = null;
    }

    /**
     * Insert the node in the first position of the list
     * 
     * @param node
     */
    private void addFirst(Node<Key, Value> node) {
        if (head == null) {
            head = tail = node;
        } else {
            head.prev = node;
            node.next = head;
            head = node;
        }
    }

    /**
     * Move a node to the head of the list
     * 
     * @param node
     */
    private void moveToHead(Node<Key, Value> node) {
        removeFromList(node);
        addFirst(node);
    }

    /**
     * Data structure to hold a linked-list node
     * 
     * @param <Key>
     * @param <Value>
     */
    private static class Node<Key, Value> {
        final Key key;
        final Value value;
        Node<Key, Value> prev;
        Node<Key, Value> next;

        Node(Key key, Value value) {
            this.key = key;
            this.value = value;
            this.prev = null;
            this.next = null;
        }
    }

}
