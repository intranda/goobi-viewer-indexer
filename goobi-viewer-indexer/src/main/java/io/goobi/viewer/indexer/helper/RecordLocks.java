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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One lock per record identifier (PI), shared by all hotfolders. With record-level parallelism, two record files carrying the same PI (from
 * different hotfolders, or under different file names, e.g. a Goobi export named after the process title and a viewer re-index copy named after
 * the PI) must not be indexed at the same time: both would find and delete the same previous instance and add their own documents, leaving two
 * instances of the record in the index, and both would write the same files in the data repository.
 * <p>
 * Locks are reentrant and removed once no thread holds or waits for them, so the map only contains the records currently being indexed. Callers
 * must never hold the locks of two different PIs at the same time (this keeps the locks free of deadlocks).
 */
public class RecordLocks {

    /** Lock and number of threads holding or waiting for it. Both are only changed inside {@link ConcurrentMap#compute}. */
    private static final class Entry {
        private final ReentrantLock lock = new ReentrantLock();
        private int users = 0;
    }

    private final ConcurrentMap<String, Entry> locks = new ConcurrentHashMap<>();

    /**
     * Acquires the lock for the given PI, waiting until no other thread holds it.
     *
     * @param pi Record identifier
     * @should block other threads until unlocked
     * @should not block other pis
     * @should be reentrant
     */
    public void lock(String pi) {
        Entry entry = locks.compute(pi, (k, e) -> {
            Entry ret = e != null ? e : new Entry();
            ret.users++;
            return ret;
        });
        entry.lock.lock();
    }

    /**
     * Releases the lock for the given PI, previously acquired by the current thread via {@link #lock(String)}.
     *
     * @param pi Record identifier
     * @throws IllegalMonitorStateException if the current thread does not hold the lock
     * @should remove lock after last unlock
     */
    public void unlock(String pi) {
        Entry entry = locks.get(pi);
        if (entry == null || !entry.lock.isHeldByCurrentThread()) {
            throw new IllegalMonitorStateException("Record lock for '" + pi + "' is not held by the current thread.");
        }
        entry.lock.unlock();
        locks.computeIfPresent(pi, (k, e) -> --e.users > 0 ? e : null);
    }

    /**
     * @param pi Record identifier
     * @return true if the current thread holds the lock for the given PI; false otherwise
     */
    public boolean isHeldByCurrentThread(String pi) {
        Entry entry = locks.get(pi);
        return entry != null && entry.lock.isHeldByCurrentThread();
    }

    /**
     * @return Number of PIs that are currently locked or waited for
     */
    int size() {
        return locks.size();
    }
}
