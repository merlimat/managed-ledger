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

/**
 * Read/Write Mutex that can be lock from one thread and released from a different one. There can be multiple readers
 * and only one writer with no readers at a time.
 */
public class CallbackMutexReadWrite {
    private final Lock lock = new ReentrantLock(true);
    private final Condition condition = lock.newCondition();
    private final Semaphore semaphore = new Semaphore(1, true);
    private int readers = 0;

    public void lockRead() {
        semaphore.acquireUninterruptibly();
        lock.lock();
        ++readers;
        lock.unlock();
        semaphore.release();
    }

    public void unlockRead() {
        lock.lock();
        readers -= 1;
        if (readers == 0) {
            condition.signal();
        }

        lock.unlock();
    }

    public void lockWrite() {
        semaphore.acquireUninterruptibly();
        lock.lock();

        if (readers > 0) {
            // Wait until all the readers are done.
            condition.awaitUninterruptibly();
        }

        lock.unlock();
    }

    public void unlockWrite() {
        semaphore.release();
    }

}
