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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mutex object that can be acquired from a thread and released from a different
 * thread.
 * 
 * This is meant to be acquired when calling an asynchronous method and released
 * in its callback which is probably executed in a different thread.
 */
public class CallbackMutex {

    private AtomicBoolean locked = new AtomicBoolean(false);
    private Queue<Thread> waiters = new ConcurrentLinkedQueue<Thread>();
    String owner;
    String position;

    public void lock() {
        boolean wasInterrupted = false;
        Thread current = Thread.currentThread();
        waiters.add(current);

        // Block while not first in queue or cannot acquire lock
        while (waiters.peek() != current && !locked.compareAndSet(false, true)) {
            LockSupport.park();
            if (Thread.interrupted()) // ignore interrupts while waiting
                wasInterrupted = true;
        }

        waiters.remove();
        if (wasInterrupted) { // reassert interrupt status on exit
            current.interrupt();
        }

        owner = Thread.currentThread().getName();
        position = Thread.currentThread().getStackTrace()[2].toString();
        log.debug("<<< Lock {} acquired at {}", this, position);
    }

    public void unlock() {
        owner = null;
        position = null;
        log.debug(">>> Lock {} released at {}", this, Thread.currentThread().getStackTrace()[2]);

        locked.set(false);
        LockSupport.unpark(waiters.peek());
    }

    private static final Logger log = LoggerFactory.getLogger(CallbackMutex.class);
}
