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
package io.goobi.viewer.indexer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.List;

import org.jdom2.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;

class SolrIndexerDaemonTest extends AbstractTest {

    @BeforeAll
    public static void setUpClass() throws Exception {
        AbstractTest.setUpClass();
    }

    @BeforeEach
    void setUp() {
        // Reset config before every test
        SolrIndexerDaemon.getInstance().injectConfiguration(new Configuration(TEST_CONFIG_PATH));
    }

    /**
     * @see SolrIndexerDaemon#getInstance()
     * @verifies return same instance on repeated calls
     */
    @Test
    void getInstance_shouldReturnSameInstanceOnRepeatedCalls() {
        Assertions.assertSame(SolrIndexerDaemon.getInstance(), SolrIndexerDaemon.getInstance());
    }

    /**
     * @see SolrIndexerDaemon#init()
     * @verifies throw FatalIndexerException if solr schema name could not be checked
     */
    @Test
    void init_shouldThrowFatalIndexerExceptionIfSolrSchemaNameCouldNotBeChecked() {
        SolrIndexerDaemon instance = SolrIndexerDaemon.getInstance();
        instance.getConfiguration().overrideValue("init.solrUrl", "https://foo.bar/schema.xml");
        Assertions.assertThrows(FatalIndexerException.class, instance::init);
    }

    /**
     * @see SolrIndexerDaemon#stop()
     * @verifies set running to false
     */
    @Test
    void stop_shouldSetRunningToFalse() {
        SolrIndexerDaemon instance = SolrIndexerDaemon.getInstance();
        instance.setRunning(true);
        instance.stop();
        Assertions.assertFalse(instance.isRunning());
    }

    /**
     * @see SolrIndexerDaemon#checkSolrSchemaName(Document)
     * @verifies return false if doc null
     */
    @Test
    void checkSolrSchemaName_shouldReturnFalseIfDocNull() {
        Assertions.assertFalse(SolrIndexerDaemon.checkSolrSchemaName(null));
    }

    /**
     * @see SolrIndexerDaemon#checkSolrSchemaName(Document)
     * @verifies return true if schema compatible
     */
    @Test
    void checkSolrSchemaName_shouldReturnTrueIfSchemaCompatible() throws Exception {
        org.jdom2.Document doc = SolrSearchIndex.getSolrSchemaDocument(SolrIndexerDaemon.getInstance().getConfiguration().getSolrUrl());
        Assertions.assertNotNull(doc);
        Assertions.assertTrue(SolrIndexerDaemon.checkSolrSchemaName(doc));
    }

    /**
     * @see SolrIndexerDaemon#setConfFileName(String)
     * @verifies set confFileName correctly
     */
    @Test
    void setConfFileName_shouldSetConfFileNameCorrectly() {
        SolrIndexerDaemon instance = SolrIndexerDaemon.getInstance().setConfFileName("config_new.xml");
        Assertions.assertEquals("config_new.xml", instance.getConfFileName());
    }

    /**
     * @see SolrIndexerDaemon#getConfiguration()
     * @verifies return same instance on repeated calls
     */
    @Test
    void getConfiguration_shouldReturnSameInstanceOnRepeatedCalls() {
        SolrIndexerDaemon instance = SolrIndexerDaemon.getInstance();
        Assertions.assertSame(instance.getConfiguration(), instance.getConfiguration());
    }

    /**
     * @see SolrIndexerDaemon#getSearchIndex()
     * @verifies return same instance on repeated calls
     */
    @Test
    void getSearchIndex_shouldReturnSameInstanceOnRepeatedCalls() throws Exception {
        SolrIndexerDaemon instance = SolrIndexerDaemon.getInstance();
        instance.injectSearchIndex(new SolrSearchIndex(null));
        Assertions.assertSame(instance.getSearchIndex(), instance.getSearchIndex());
    }

    /**
     * Runs the given check with the two hotfolders of the test configuration registered in the daemon (hotfolder2 has the lower priority).
     *
     * @param check Check to run with hotfolder 1 and 2
     * @throws Exception
     */
    private static void withTwoHotfolders(HotfolderCheck check) throws Exception {
        List<String> paths = SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPaths();
        Assertions.assertTrue(paths.size() >= 2, "test configuration must define two hotfolders");
        Hotfolder hotfolder1 = new Hotfolder(paths.get(0));
        Hotfolder hotfolder2 = new Hotfolder(paths.get(1));
        List<Hotfolder> hotfolders = SolrIndexerDaemon.getInstance().getHotfolders();
        hotfolders.add(hotfolder1);
        hotfolders.add(hotfolder2);
        try {
            check.run(hotfolder1, hotfolder2);
        } finally {
            hotfolders.clear();
        }
    }

    @FunctionalInterface
    private interface HotfolderCheck {
        void run(Hotfolder hotfolder1, Hotfolder hotfolder2) throws Exception;
    }

    /**
     * 
     * @param folder
     * @param fileName
     * @param modified Modification time to set
     * @return Created file
     * @throws Exception
     */
    private static Path createRecordFile(Path folder, String fileName, long modified) throws Exception {
        Files.createDirectories(folder);
        Path file = Files.writeString(folder.resolve(fileName), "<mets/>");
        Files.setLastModifiedTime(file, FileTime.fromMillis(modified));
        return file;
    }

    /**
     * @see SolrIndexerDaemon#isSupersededByHigherPriorityHotfolder(String,Hotfolder,Path)
     * @verifies return true if indexed from higher priority hotfolder after file modification
     */
    @Test
    void isSupersededByHigherPriorityHotfolder_shouldReturnTrueIfIndexedFromHigherPriorityHotfolderAfterFileModification() throws Exception {
        withTwoHotfolders((hotfolder1, hotfolder2) -> {
            // File name differs from the PI (Goobi exports are named after the process title)
            Path file = createRecordFile(hotfolder2.getHotfolderPath(), "process_title_1.xml", System.currentTimeMillis() - 60000);
            try {
                SolrIndexerDaemon.getInstance().registerIndexedRecord("SUPERSEDED_TEST_1", hotfolder1);
                Assertions.assertTrue(SolrIndexerDaemon.getInstance().isSupersededByHigherPriorityHotfolder("SUPERSEDED_TEST_1", hotfolder2, file));
            } finally {
                Files.deleteIfExists(file);
            }
        });
    }

    /**
     * @see SolrIndexerDaemon#isSupersededByHigherPriorityHotfolder(String,Hotfolder,Path)
     * @verifies return false if indexed from higher priority hotfolder before file modification
     */
    @Test
    void isSupersededByHigherPriorityHotfolder_shouldReturnFalseIfIndexedFromHigherPriorityHotfolderBeforeFileModification() throws Exception {
        withTwoHotfolders((hotfolder1, hotfolder2) -> {
            Path file = createRecordFile(hotfolder2.getHotfolderPath(), "process_title_2.xml", System.currentTimeMillis() + 60000);
            try {
                SolrIndexerDaemon.getInstance().registerIndexedRecord("SUPERSEDED_TEST_2", hotfolder1);
                Assertions.assertFalse(SolrIndexerDaemon.getInstance().isSupersededByHigherPriorityHotfolder("SUPERSEDED_TEST_2", hotfolder2, file));
            } finally {
                Files.deleteIfExists(file);
            }
        });
    }

    /**
     * @see SolrIndexerDaemon#isSupersededByHigherPriorityHotfolder(String,Hotfolder,Path)
     * @verifies return false for highest priority hotfolder
     */
    @Test
    void isSupersededByHigherPriorityHotfolder_shouldReturnFalseForHighestPriorityHotfolder() throws Exception {
        withTwoHotfolders((hotfolder1, hotfolder2) -> {
            Path file = createRecordFile(hotfolder1.getHotfolderPath(), "process_title_3.xml", System.currentTimeMillis() - 60000);
            try {
                // Indexed from hotfolder2 (lowest priority, not registered) and from hotfolder1 itself
                SolrIndexerDaemon.getInstance().registerIndexedRecord("SUPERSEDED_TEST_3", hotfolder2);
                SolrIndexerDaemon.getInstance().registerIndexedRecord("SUPERSEDED_TEST_3", hotfolder1);
                Assertions.assertFalse(SolrIndexerDaemon.getInstance().isSupersededByHigherPriorityHotfolder("SUPERSEDED_TEST_3", hotfolder1, file));
            } finally {
                Files.deleteIfExists(file);
            }
        });
    }

    /**
     * @see SolrIndexerDaemon#isSupersededByHigherPriorityHotfolder(String,Hotfolder,Path)
     * @verifies return false for files outside the hotfolder
     */
    @Test
    void isSupersededByHigherPriorityHotfolder_shouldReturnFalseForFilesOutsideTheHotfolder() throws Exception {
        withTwoHotfolders((hotfolder1, hotfolder2) -> {
            // E.g. an anchor file from the data repository (indexed_mets) in hotfolder2's priority queue
            Path file = createRecordFile(Paths.get("target/viewer/indexed_mets_test"), "SUPERSEDED_TEST_4.xml", System.currentTimeMillis() - 60000);
            Path updatedFile =
                    createRecordFile(hotfolder2.getHotfolderPath(), "SUPERSEDED_TEST_4#1" + MetsIndexer.ANCHOR_UPDATE_EXTENSION,
                            System.currentTimeMillis() - 60000);
            try {
                SolrIndexerDaemon.getInstance().registerIndexedRecord("SUPERSEDED_TEST_4", hotfolder1);
                Assertions.assertFalse(SolrIndexerDaemon.getInstance().isSupersededByHigherPriorityHotfolder("SUPERSEDED_TEST_4", hotfolder2, file));
                Assertions.assertFalse(
                        SolrIndexerDaemon.getInstance().isSupersededByHigherPriorityHotfolder("SUPERSEDED_TEST_4", hotfolder2, updatedFile));
            } finally {
                Files.deleteIfExists(file);
                Files.deleteIfExists(updatedFile);
            }
        });
    }
}
