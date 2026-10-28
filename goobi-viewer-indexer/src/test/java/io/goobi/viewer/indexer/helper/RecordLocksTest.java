/**
 * This file is part of the Goobi Solr Indexer - a content indexing tool for the Goobi viewer and OAI-PMH/SRU interfaces.
 *
 * Visit these websites for more information.
 *          - http://www.intranda.com
 *          - http://digiverso.com
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package io.goobi.viewer.indexer.helper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RecordLocksTest {

    /**
     * @see RecordLocks#lock(String)
     * @verifies block other threads until unlocked
     */
    @Test
    void lock_shouldBlockOtherThreadsUntilUnlocked() throws Exception {
        RecordLocks locks = new RecordLocks();
        locks.lock("PPN123");
        CountDownLatch acquired = new CountDownLatch(1);
        Thread other = new Thread(() -> {
            locks.lock("PPN123");
            acquired.countDown();
            locks.unlock("PPN123");
        });
        other.start();
        try {
            Assertions.assertFalse(acquired.await(200, TimeUnit.MILLISECONDS), "second thread must wait while the lock is held");
        } finally {
            locks.unlock("PPN123");
        }
        Assertions.assertTrue(acquired.await(5, TimeUnit.SECONDS), "second thread must get the lock once it is released");
        other.join(5000);
        Assertions.assertEquals(0, locks.size());
    }

    /**
     * @see RecordLocks#lock(String)
     * @verifies not block other pis
     */
    @Test
    void lock_shouldNotBlockOtherPis() throws Exception {
        RecordLocks locks = new RecordLocks();
        locks.lock("PPN123");
        CountDownLatch acquired = new CountDownLatch(1);
        Thread other = new Thread(() -> {
            locks.lock("PPN456");
            acquired.countDown();
            locks.unlock("PPN456");
        });
        other.start();
        try {
            Assertions.assertTrue(acquired.await(5, TimeUnit.SECONDS), "a different PI must not be blocked");
        } finally {
            locks.unlock("PPN123");
        }
        other.join(5000);
    }

    /**
     * @see RecordLocks#lock(String)
     * @verifies be reentrant
     */
    @Test
    void lock_shouldBeReentrant() {
        RecordLocks locks = new RecordLocks();
        locks.lock("PPN123");
        locks.lock("PPN123");
        Assertions.assertTrue(locks.isHeldByCurrentThread("PPN123"));
        locks.unlock("PPN123");
        Assertions.assertTrue(locks.isHeldByCurrentThread("PPN123"));
        locks.unlock("PPN123");
        Assertions.assertFalse(locks.isHeldByCurrentThread("PPN123"));
    }

    /**
     * @see RecordLocks#unlock(String)
     * @verifies remove lock after last unlock
     */
    @Test
    void unlock_shouldRemoveLockAfterLastUnlock() {
        RecordLocks locks = new RecordLocks();
        locks.lock("PPN123");
        locks.lock("PPN456");
        Assertions.assertEquals(2, locks.size());
        locks.unlock("PPN123");
        Assertions.assertEquals(1, locks.size());
        locks.unlock("PPN456");
        Assertions.assertEquals(0, locks.size());
        Assertions.assertThrows(IllegalMonitorStateException.class, () -> locks.unlock("PPN456"));
    }
}
