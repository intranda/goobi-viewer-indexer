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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IndexedRecordRegistryTest {

    /**
     * @see IndexedRecordRegistry#isSuperseded(String,int,long)
     * @verifies return true if indexed from higher priority hotfolder after file modification
     */
    @Test
    void isSuperseded_shouldReturnTrueIfIndexedFromHigherPriorityHotfolderAfterFileModification() {
        IndexedRecordRegistry registry = new IndexedRecordRegistry(10);
        registry.register("PPN123", 0, 2000);
        Assertions.assertTrue(registry.isSuperseded("PPN123", 1, 1000));
        Assertions.assertTrue(registry.isSuperseded("PPN123", 2, 1000));
    }

    /**
     * @see IndexedRecordRegistry#isSuperseded(String,int,long)
     * @verifies return false if indexed from higher priority hotfolder before file modification
     */
    @Test
    void isSuperseded_shouldReturnFalseIfIndexedFromHigherPriorityHotfolderBeforeFileModification() {
        IndexedRecordRegistry registry = new IndexedRecordRegistry(10);
        registry.register("PPN123", 0, 1000);
        Assertions.assertFalse(registry.isSuperseded("PPN123", 1, 2000));
    }

    /**
     * @see IndexedRecordRegistry#isSuperseded(String,int,long)
     * @verifies return false if indexed from same or lower priority hotfolder
     */
    @Test
    void isSuperseded_shouldReturnFalseIfIndexedFromSameOrLowerPriorityHotfolder() {
        IndexedRecordRegistry registry = new IndexedRecordRegistry(10);
        registry.register("PPN123", 1, 2000);
        registry.register("PPN123", 2, 2000);
        Assertions.assertFalse(registry.isSuperseded("PPN123", 1, 1000));
        Assertions.assertFalse(registry.isSuperseded("PPN123", 0, 1000));
    }

    /**
     * @see IndexedRecordRegistry#isSuperseded(String,int,long)
     * @verifies return false if pi unknown
     */
    @Test
    void isSuperseded_shouldReturnFalseIfPiUnknown() {
        IndexedRecordRegistry registry = new IndexedRecordRegistry(10);
        registry.register("PPN123", 0, 2000);
        Assertions.assertFalse(registry.isSuperseded("PPN456", 1, 1000));
    }

    /**
     * @see IndexedRecordRegistry#isSuperseded(String,int,long)
     * @verifies forget eldest entries beyond max entries
     */
    @Test
    void isSuperseded_shouldForgetEldestEntriesBeyondMaxEntries() {
        IndexedRecordRegistry registry = new IndexedRecordRegistry(2);
        registry.register("PPN1", 0, 2000);
        registry.register("PPN2", 0, 2000);
        // Registering PPN1 again makes it the most recent entry, so PPN2 is forgotten first
        registry.register("PPN1", 0, 3000);
        registry.register("PPN3", 0, 2000);
        Assertions.assertTrue(registry.isSuperseded("PPN1", 1, 1000));
        Assertions.assertFalse(registry.isSuperseded("PPN2", 1, 1000));
        Assertions.assertTrue(registry.isSuperseded("PPN3", 1, 1000));
    }
}
