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

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * Read/Write Mutex that can be lock from one thread and released from a different one. There can be multiple readers
 * and only one writer with no readers at a time.
 */
public class CallbackMutexReadWrite {
    private final Lock lock = new ReentrantLock(true);
    private final Condition condition = lock.newCondition();
    private final Semaphore semaphore = new Semaphore(1, true);
    private AtomicInteger readers = new AtomicInteger(0);

    private String writeThread;
    private String writeThreadPosition;
    private Map<String, String> readerThreads = Maps.newHashMap();

    public void lockRead() {
        log.debug("lockRead entering");
        semaphore.acquireUninterruptibly();
        lock.lock();
        int r = readers.incrementAndGet();

        String threadName = Thread.currentThread().getName();
        String threadPosition = Thread.currentThread().getStackTrace()[2].toString();
        readerThreads.put(threadName, threadPosition);

        lock.unlock();
        semaphore.release();
        log.debug("lockRead got lock readers={} at {} ", r, threadPosition);
    }

    public void unlockRead() {
        log.debug("unlockRead entering");
        lock.lock();
        int r = readers.decrementAndGet();
        if (r == 0) {
            log.debug("unlockRead signal writer");
            condition.signalAll();
        }

        lock.unlock();
        log.debug("unlockRead released --- readers={}", r);
    }

    public void lockWrite() {
        log.debug("lockWrite entering");
        semaphore.acquireUninterruptibly();
        lock.lock();

        writeThread = Thread.currentThread().getName();
        writeThreadPosition = Thread.currentThread().getStackTrace()[2].toString();

        if (readers.get() > 0) {
            // Wait until all the readers are done.
            log.debug("lockWrite wait for {} readers", readers);
            condition.awaitUninterruptibly();
        }

        lock.unlock();
        log.debug("<<<<<<<<  lockWrite got lock at {}", writeThreadPosition);
    }

    public void unlockWrite() {
        writeThread = null;
        writeThreadPosition = null;

        log.debug(">>>> unlockWrite");
        semaphore.release();
    }

    private static final Logger log = LoggerFactory.getLogger(CallbackMutexReadWrite.class);
}
