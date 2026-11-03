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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Remembers when records were last indexed from which hotfolder, so that an older copy of a record in a lower-priority hotfolder does not overwrite
 * the version indexed from a higher-priority hotfolder (see {@link #isSuperseded(String, int, long)}). Records are identified by their PI, not by
 * their file name: Goobi workflow names exported files after the process title, the viewer names re-index copies after the PI.
 * <p>
 * Bounded: only the most recently indexed records are kept (see {@link #MAX_ENTRIES}). Thread-safe.
 */
public class IndexedRecordRegistry {

    /** Maximum number of remembered records; beyond that, the records registered first are forgotten. */
    public static final int MAX_ENTRIES = 100000;

    private final int maxEntries;
    /** PI -> completion time per hotfolder priority (index 0 = highest priority; 0 = never indexed from that hotfolder). */
    private final Map<String, long[]> indexed;

    /**
     *
     * @param maxEntries Maximum number of remembered records
     */
    public IndexedRecordRegistry(int maxEntries) {
        this.maxEntries = maxEntries;
        this.indexed = new LinkedHashMap<>(16, 0.75f, false) {

            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<String, long[]> eldest) {
                return size() > IndexedRecordRegistry.this.maxEntries;
            }
        };
    }

    /**
     * Registers that the given record has been indexed successfully from the hotfolder with the given priority.
     *
     * @param pi Record identifier
     * @param priority Priority of the hotfolder (0 = highest)
     * @param time Completion time (epoch millis)
     */
    public synchronized void register(String pi, int priority, long time) {
        if (pi == null || priority < 0) {
            return;
        }
        // Re-insert so the record counts as recently indexed for the size bound
        long[] times = indexed.remove(pi);
        if (times == null || times.length <= priority) {
            long[] newTimes = new long[priority + 1];
            if (times != null) {
                System.arraycopy(times, 0, newTimes, 0, times.length);
            }
            times = newTimes;
        }
        times[priority] = Math.max(times[priority], time);
        indexed.put(pi, times);
    }

    /**
     * Checks whether a record file in the hotfolder with the given priority is superseded, i.e. the same record has been indexed from a
     * higher-priority hotfolder after the file was last modified. Such a file is an older copy that must not overwrite the newer version (a file
     * placed after that, e.g. a later re-index request, is not superseded).
     * <p>
     * Caveat: the decision relies on the record file's modification time. Goobi workflow exports and the viewer's re-index copies get a fresh
     * modification time. Tools that preserve the original modification time when copying (e.g. <code>rsync -a</code>/<code>rsync -t</code>,
     * <code>cp -p</code>) make a freshly copied file look old: if the same record was indexed from a higher-priority hotfolder after the copied
     * file's original modification time, the copy is treated as superseded and deleted without being indexed. Copy files into lower-priority
     * hotfolders without preserving modification times (or <code>touch</code> them afterwards).
     *
     * @param pi Record identifier
     * @param priority Priority of the hotfolder containing the record file (0 = highest)
     * @param fileModified Modification time of the record file (epoch millis)
     * @return true if the record file is superseded; false otherwise
     * @should return true if indexed from higher priority hotfolder after file modification
     * @should return false if indexed from higher priority hotfolder before file modification
     * @should return false if indexed from same or lower priority hotfolder
     * @should return false if pi unknown
     * @should forget eldest entries beyond max entries
     */
    public synchronized boolean isSuperseded(String pi, int priority, long fileModified) {
        long[] times = indexed.get(pi);
        if (times == null) {
            return false;
        }
        for (int i = 0; i < Math.min(priority, times.length); i++) {
            if (times[i] > fileModified) {
                return true;
            }
        }
        return false;
    }
}
