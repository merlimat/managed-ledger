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

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read/Write Mutex that can be lock from one thread and released from a different one. There can be multiple readers
 * and only one writer with no readers at a time.
 */
public class CallbackMutexReadWrite {
    private final Lock lock = new ReentrantLock(true);
    private final Condition condition = lock.newCondition();
    private final Semaphore semaphore = new Semaphore(1, true);
    private int readers = 0;

    private String writeThread;
    private String writeThreadPosition;

    public void lockRead() {
        log.debug("{} lockRead entering", hashCode());
        semaphore.acquireUninterruptibly();
        lock.lock();
        ++readers;

        log.debug("{} lockRead got lock readers={}", this, readers);

        lock.unlock();
        semaphore.release();
    }

    public void unlockRead() {
        log.debug("{} unlockRead entering", hashCode());
        lock.lock();
        --readers;
        if (readers == 0) {
            log.debug("{} unlockRead signal writer", hashCode());
            condition.signalAll();
        }

        log.debug("{} unlockRead released --- readers={}", hashCode(), readers);
        lock.unlock();
    }

    public void lockWrite() {
        log.debug("{} lockWrite entering", hashCode());
        semaphore.acquireUninterruptibly();
        lock.lock();
        writeThread = Thread.currentThread().getName();
        writeThreadPosition = Thread.currentThread().getStackTrace()[2].toString();

        if (readers > 0) {
            // Wait until all the readers are done.
            log.debug("{} lockWrite wait for {} readers", hashCode(), readers);
            condition.awaitUninterruptibly();
        }

        lock.unlock();
        log.debug("<<<<<<<<  {} lockWrite got lock at {}", hashCode(), writeThreadPosition);
    }

    public void unlockWrite() {
        log.debug(">>>> {} unlockWrite that was locked by {}", hashCode(), writeThread);
        writeThread = null;
        writeThreadPosition = null;
        semaphore.release();
    }

    private static final Logger log = LoggerFactory.getLogger(CallbackMutexReadWrite.class);
}
