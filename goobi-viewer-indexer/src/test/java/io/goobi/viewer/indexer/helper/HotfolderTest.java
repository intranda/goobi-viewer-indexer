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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractSolrEnabledTest;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.SolrUnavailableException;
import io.goobi.viewer.indexer.model.IndexingResult;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

/**
 * TODO "Connection pool closed" error if more than one test (Jenkins only)
 */
class HotfolderTest extends AbstractSolrEnabledTest {

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        // Reset config for every test to reset overrides
        SolrIndexerDaemon.getInstance().injectConfiguration(new Configuration(TEST_CONFIG_PATH));

    }

    @Override
    @AfterEach
    public void tearDown() {
        Path hotfolderPath = Paths.get(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        if (Files.isDirectory(hotfolderPath)) {
            try {
                FileUtils.deleteDirectory(hotfolderPath.toFile());
            } catch (IOException e) {
            }
        }
    }

    /**
     * @see Hotfolder#initFolders(String,Configuration)
     * @verifies throw FatalIndexerException if hotfolderPathString null
     */
    @Test
    void initFolders_shouldThrowFatalIndexerExceptionIfHotfolderPathStringNull() throws Exception {
        // Create local hotfolder the default one has already been initialized
        hotfolder = new Hotfolder();
        Assertions.assertThrows(FatalIndexerException.class, () -> hotfolder.initFolders(null, SolrIndexerDaemon.getInstance().getConfiguration()));
    }

    /**
     * @see Hotfolder#initFolders(String,Configuration)
     * @verifies throw FatalIndexerException if viewerHome not defined
     */
    @Test
    void initFolders_shouldThrowFatalIndexerExceptionIfViewerHomeNotDefined() throws Exception {
        Configuration config = SolrIndexerDaemon.getInstance().getConfiguration();
        config.overrideValue("init.viewerHome", "");

        // Create local hotfolder the default one has already been initialized
        hotfolder = new Hotfolder();
        Assertions.assertThrows(FatalIndexerException.class, () -> hotfolder.initFolders(config.getHotfolderPath(), config));
    }

    /**
     * @see Hotfolder#initFolders(String,Configuration)
     * @verifies throw FatalIndexerException if tempFolder not defined
     */
    @Test
    void initFolders_shouldThrowFatalIndexerExceptionIfTempFolderNotDefined() throws Exception {
        Configuration config = SolrIndexerDaemon.getInstance().getConfiguration();
        config.overrideValue("init.tempFolder", "");

        // Create local hotfolder the default one has already been initialized
        hotfolder = new Hotfolder();
        Assertions.assertThrows(FatalIndexerException.class, () -> hotfolder.initFolders(config.getHotfolderPath(), config));
    }

    /**
     * @see Hotfolder#initFolders(String,Configuration)
     * @verifies throw FatalIndexerException if successFolder not defined
     */
    @Test
    void initFolders_shouldThrowFatalIndexerExceptionIfSuccessFolderNotDefined() throws Exception {
        // Disable indexed record file configurations for test coverage
        Configuration config = SolrIndexerDaemon.getInstance().getConfiguration();
        config.overrideValue("init." + DataRepository.PARAM_INDEXED_METS, "");
        config.overrideValue("init." + DataRepository.PARAM_INDEXED_LIDO, "");
        config.overrideValue("init." + DataRepository.PARAM_INDEXED_DENKXWEB, "");
        config.overrideValue("init." + DataRepository.PARAM_INDEXED_DUBLINCORE, "");
        config.overrideValue("init." + DataRepository.PARAM_INDEXED_CMS, "");

        config.overrideValue("init.successFolder", "");

        // Create local hotfolder the default one has already been initialized
        hotfolder = new Hotfolder();
        Assertions.assertThrows(FatalIndexerException.class, () -> hotfolder.initFolders(config.getHotfolderPath(), config));
    }

    /**
     * @see Hotfolder#countRecordFiles()
     * @verifies count files correctly
     */
    @Test
    void countRecordFiles_shouldCountFilesCorrectly() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("performance.countHotfolderFiles", true);
        Assertions.assertTrue(SolrIndexerDaemon.getInstance().getConfiguration().isCountHotfolderFiles());

        {
            Path path = Paths.get("target/viewer/hotfolder", "1.xml");
            Files.createFile(path);
            Assertions.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "1A.XML");
            Files.createFile(path);
            Assertions.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "2.delete");
            Files.createFile(path);
            Assertions.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "3.purge");
            Files.createFile(path);
            Assertions.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "4.docupdate");
            Files.createFile(path);
            Assertions.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "5.UPDATED");
            Files.createFile(path);
            Assertions.assertTrue(Files.isRegularFile(path));
        }

        // Uncounted files
        {
            Path path = Paths.get("target/viewer/hotfolder", "6.exe");
            Files.createFile(path);
            Assertions.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "7");
            Files.createDirectories(path);
            Assertions.assertTrue(Files.isDirectory(path));
        }

        Assertions.assertEquals(6, hotfolder.countRecordFiles());

    }

    /**
     * @see Hotfolder#enqueueOrphanedAnchorUpdateFiles()
     * @verifies enqueue orphaned UPDATED files
     */
    @Test
    void enqueueOrphanedAnchorUpdateFiles_shouldEnqueueOrphanedUpdatedFiles() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        assertTrue(hotfolder.getHighPriorityQueue().isEmpty());

        Path updatedFile = Paths.get(hotfolder.getHotfolderPath().toAbsolutePath().toString(), "PPN123.UPDATED");
        Files.createFile(updatedFile);
        Assertions.assertTrue(Files.isRegularFile(updatedFile));

        // Startup scan must move the orphaned file into the priority queue
        Assertions.assertEquals(1, hotfolder.enqueueOrphanedAnchorUpdateFiles());
        Assertions.assertEquals(1, hotfolder.getHighPriorityQueue().size());
        Assertions.assertEquals("PPN123.UPDATED", hotfolder.getHighPriorityQueue().peek().getFileName().toString());

        // Calling again must not enqueue the same file twice
        Assertions.assertEquals(0, hotfolder.enqueueOrphanedAnchorUpdateFiles());
        Assertions.assertEquals(1, hotfolder.getHighPriorityQueue().size());
    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if body contains no error
     */
    @Test
    void checkAndSendErrorReport_shouldReturnFalseIfBodyContainsNoError() {
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "bar"));
    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if recipients not configured
     */
    @Test
    void checkAndSendErrorReport_shouldReturnFalseIfRecipientsNotConfigured() {
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "ERROR bar"));
    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if smtpServer not configured
     */
    @Test
    void checkAndSendErrorReport_shouldReturnFalseIfSmtpServerNotConfigured() {
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.recipients", "foo@bar.com");
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "ERROR bar"));
    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if smtpSenderAddress not configured
     */
    @Test
    void checkAndSendErrorReport_shouldReturnFalseIfSmtpSenderAddressNotConfigured() {
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.recipients", "foo@bar.com");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpServer", "bar.com");
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "ERROR bar"));
    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if smtpSenderName not configured
     */
    @Test
    void checkAndSendErrorReport_shouldReturnFalseIfSmtpSenderNameNotConfigured() {
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.recipients", "user@example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpServer", "smtp.example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpSenderAddress", "indexer@example.foo");
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "ERROR bar"));
    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if smtpSecurity not configured
     */
    @Test
    void checkAndSendErrorReport_shouldReturnFalseIfSmtpSecurityNotConfigured() {
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.recipients", "user@example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpServer", "smtp.example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpSenderAddress", "indexer@example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpSenderName", "Goobi viewer Indexer");
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "ERROR bar"));
    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if sending mail fails
     */
    @Test
    void checkAndSendErrorReport_shouldReturnFalseIfSendingMailFails() {
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.recipients", "user@example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpServer", "smtp.example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpSenderAddress", "indexer@example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpSenderName", "Goobi viewer Indexer");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpSecurity", "SSL");
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "ERROR bar"));
    }

    /**
     * @see Hotfolder#doIndex(Path)
     * @verifies return false if recordFile null
     */
    @Test
    void doIndex_shouldReturnFalseIfRecordFileNull() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        assertFalse(hotfolder.doIndex(null));
    }

    /**
     * @see Hotfolder#doIndex(Path)
     * @verifies return true if successful
     */
    @Test
    void doIndex_shouldReturnTrueIfSuccessful() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        Path srcPath = Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        Path destPath = Paths.get(hotfolder.getHotfolderPath().toAbsolutePath().toString(), "kleiuniv_PPN517154005.xml");
        Files.copy(srcPath, destPath);
        Assertions.assertTrue(Files.isRegularFile(destPath));
        assertTrue(hotfolder.doIndex(destPath));
    }

    /**
     * @see Hotfolder#isDataFolderExportDone(Path)
     * @verifies return true if hotfolder content not changing
     */
    @Test
    void isDataFolderExportDone_shouldReturnTrueIfHotfolderContentNotChanging() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        assertTrue(Files.isDirectory(Paths.get(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath())));
        Path recordFile = Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        assertTrue(Files.isRegularFile(recordFile));

        Path tempFile = null;
        try {
            tempFile =
                    Files.copy(recordFile, Paths.get(hotfolder.getHotfolderPath().toAbsolutePath().toString(), recordFile.getFileName().toString()));
            assertTrue(Files.isRegularFile(tempFile));
            assertTrue(hotfolder.isDataFolderExportDone(tempFile));
        } finally {
            if (tempFile != null) {
                FileUtils.delete(tempFile.toFile());
            }
        }
    }

    /**
     * @see Hotfolder#isDataFolderExportDone(Path)
     * @verifies return false if data folder changes while measuring
     */
    @Test
    void isDataFolderExportDone_shouldReturnFalseIfDataFolderChangesWhileMeasuring() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath()) {
            @Override
            long getDataFolderTotalSize(String fileNameRoot) {
                // Simulate files vanishing during the walk, as FileUtils.sizeOfDirectory() reports it during an ongoing copy
                throw new UncheckedIOException(new IOException("File vanished while measuring"));
            }
        };
        // A freshly written record file skips the quiet-period shortcut, so the sizes are measured
        Path recordFile = Files.writeString(hotfolder.getHotfolderPath().resolve("foo.xml"), "<mets/>");
        try {
            assertFalse(hotfolder.isDataFolderExportDone(recordFile));
        } finally {
            Files.deleteIfExists(recordFile);
        }
    }

    /**
     * @see Hotfolder#listNewFiles()
     * @verifies not queue files already in the priority queue
     */
    @Test
    void listNewFiles_shouldNotQueueFilesAlreadyInThePriorityQueue() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        Path priorityFile = Files.writeString(hotfolder.getHotfolderPath().resolve("foo.xml"), "<mets/>");
        Path regularFile = Files.writeString(hotfolder.getHotfolderPath().resolve("bar.xml"), "<mets/>");
        try {
            // Absolute path, as MetsIndexer.writeAnchorXmlFile() enqueues it; the listing yields hotfolder-relative paths
            hotfolder.getHighPriorityQueue().add(priorityFile.toAbsolutePath());

            assertEquals(1, hotfolder.listNewFiles(), "only the file that is not in the priority queue may be queued");
        } finally {
            Files.deleteIfExists(priorityFile);
            Files.deleteIfExists(regularFile);
        }
    }

    /**
     * @see Hotfolder#listNewFiles()
     * @verifies keep files beyond queue capacity in backlog
     */
    @Test
    void listNewFiles_shouldKeepFilesBeyondQueueCapacityInBacklog() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        List<Path> files = createRecordFiles(hotfolder.getHotfolderPath(), 510);
        try {
            assertEquals(510, hotfolder.listNewFiles());
            assertEquals(500, hotfolder.getIndexQueueSize());
            assertEquals(10, hotfolder.getListingBacklogSize());
        } finally {
            deleteFiles(files);
        }
    }

    /**
     * @see Hotfolder#fillQueueFromBacklog()
     * @verifies move files from backlog into queue
     */
    @Test
    void fillQueueFromBacklog_shouldMoveFilesFromBacklogIntoQueue() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        List<Path> files = createRecordFiles(hotfolder.getHotfolderPath(), 510);
        try {
            hotfolder.listNewFiles();
            // Queue still full: nothing can be moved
            assertEquals(0, hotfolder.fillQueueFromBacklog());
            // Simulate the queue having been processed
            hotfolder.clearIndexQueue();
            assertEquals(10, hotfolder.fillQueueFromBacklog());
            assertEquals(10, hotfolder.getIndexQueueSize());
            assertEquals(0, hotfolder.getListingBacklogSize());
        } finally {
            deleteFiles(files);
        }
    }

    /**
     * @see Hotfolder#fillQueueFromBacklog()
     * @verifies skip files that no longer exist
     */
    @Test
    void fillQueueFromBacklog_shouldSkipFilesThatNoLongerExist() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        List<Path> files = createRecordFiles(hotfolder.getHotfolderPath(), 510);
        try {
            hotfolder.listNewFiles();
            hotfolder.clearIndexQueue();
            // Delete all files; the 10 in the backlog must not be queued anymore
            deleteFiles(files);
            assertEquals(0, hotfolder.fillQueueFromBacklog());
            assertEquals(0, hotfolder.getIndexQueueSize());
            assertEquals(0, hotfolder.getListingBacklogSize());
        } finally {
            deleteFiles(files);
        }
    }

    /**
     * @see Hotfolder#pollExistingRecordFile()
     * @verifies skip files that no longer exist
     */
    @Test
    void pollExistingRecordFile_shouldSkipFilesThatNoLongerExist() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        List<Path> files = createRecordFiles(hotfolder.getHotfolderPath(), 3);
        try {
            assertEquals(3, hotfolder.listNewFiles());
            // Simulate a record that was indexed (and its file removed) after the listing had read its name
            Files.delete(files.get(1));

            List<Path> polled = new ArrayList<>();
            Path recordFile;
            while ((recordFile = hotfolder.pollExistingRecordFile()) != null) {
                polled.add(recordFile.getFileName());
            }
            assertEquals(2, polled.size());
            assertFalse(polled.contains(files.get(1).getFileName()));
            assertEquals(0, hotfolder.getIndexQueueSize());
        } finally {
            deleteFiles(files);
        }
    }

    private static List<Path> createRecordFiles(Path folder, int count) throws IOException {
        List<Path> ret = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ret.add(Files.writeString(folder.resolve(String.format("BACKLOGTEST%04d.xml", i)), "<mets/>"));
        }
        return ret;
    }

    private static void deleteFiles(List<Path> files) throws IOException {
        for (Path file : files) {
            Files.deleteIfExists(file);
        }
    }

    /**
     * @see Hotfolder#isUnmodifiedSince(Path,String,long)
     * @verifies return true if nothing modified after threshold
     */
    @Test
    void isUnmodifiedSince_shouldReturnTrueIfNothingModifiedAfterThreshold() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        Path recordFile = Files.writeString(hotfolder.getHotfolderPath().resolve("foo.xml"), "<mets/>");
        Path mediaFolder = Files.createDirectory(hotfolder.getHotfolderPath().resolve("foo_media"));
        Path imageFile = Files.writeString(mediaFolder.resolve("00000001.tif"), "image");
        long oldTime = System.currentTimeMillis() - 60_000;
        for (Path path : List.of(recordFile, imageFile, mediaFolder)) {
            Files.setLastModifiedTime(path, FileTime.fromMillis(oldTime));
        }

        assertTrue(hotfolder.isUnmodifiedSince(recordFile, "foo", oldTime + 1000));
    }

    /**
     * @see Hotfolder#isUnmodifiedSince(Path,String,long)
     * @verifies return false if record file modified after threshold
     */
    @Test
    void isUnmodifiedSince_shouldReturnFalseIfRecordFileModifiedAfterThreshold() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        Path recordFile = Files.writeString(hotfolder.getHotfolderPath().resolve("foo.xml"), "<mets/>");

        assertFalse(hotfolder.isUnmodifiedSince(recordFile, "foo", System.currentTimeMillis() - 60_000));
    }

    /**
     * @see Hotfolder#isUnmodifiedSince(Path,String,long)
     * @verifies return false if data folder content modified after threshold
     */
    @Test
    void isUnmodifiedSince_shouldReturnFalseIfDataFolderContentModifiedAfterThreshold() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        Path recordFile = Files.writeString(hotfolder.getHotfolderPath().resolve("foo.xml"), "<mets/>");
        Path mediaFolder = Files.createDirectory(hotfolder.getHotfolderPath().resolve("foo_media"));
        Files.writeString(mediaFolder.resolve("00000001.tif"), "image");
        long threshold = System.currentTimeMillis() - 60_000;
        Files.setLastModifiedTime(recordFile, FileTime.fromMillis(threshold - 1000));
        Files.setLastModifiedTime(mediaFolder, FileTime.fromMillis(threshold - 1000));

        assertFalse(hotfolder.isUnmodifiedSince(recordFile, "foo", threshold));
    }

    /**
     * @see Hotfolder#isUnmodifiedSince(Path,String,long)
     * @verifies return false if listed data folder content modified after threshold
     */
    @Test
    void isUnmodifiedSince_shouldReturnFalseIfListedDataFolderContentModifiedAfterThreshold() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        Path recordFile = Files.writeString(hotfolder.getHotfolderPath().resolve("foo.xml"), "<mets/>");
        Path mediaFolder = Files.createDirectory(hotfolder.getHotfolderPath().resolve("foo_media"));
        Files.writeString(mediaFolder.resolve("00000001.tif"), "image");
        long threshold = System.currentTimeMillis() - 60_000;
        Files.setLastModifiedTime(recordFile, FileTime.fromMillis(threshold - 1000));
        Files.setLastModifiedTime(mediaFolder, FileTime.fromMillis(threshold - 1000));
        // The listing records foo_media as a data folder of foo.xml, so only that folder is checked
        assertEquals(1, hotfolder.listNewFiles());

        assertFalse(hotfolder.isUnmodifiedSince(recordFile, "foo", threshold));
        // Once the folder content is old enough, the export counts as finished
        Files.setLastModifiedTime(mediaFolder.resolve("00000001.tif"), FileTime.fromMillis(threshold - 1000));
        assertTrue(hotfolder.isUnmodifiedSince(recordFile, "foo", threshold));
    }

    /**
     * @see Hotfolder#getDataFolderTotalSize(String)
     * @verifies sum sizes of all data folders of the record
     */
    @Test
    void getDataFolderTotalSize_shouldSumSizesOfAllDataFoldersOfTheRecord() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        Path mediaFolder = Files.createDirectory(hotfolder.getHotfolderPath().resolve("foo_media"));
        Files.writeString(mediaFolder.resolve("00000001.tif"), "1234567890");
        Path altoFolder = Files.createDirectory(hotfolder.getHotfolderPath().resolve("foo_alto"));
        Files.writeString(altoFolder.resolve("00000001.xml"), "12345");

        assertEquals(15, hotfolder.getDataFolderTotalSize("foo"));
    }

    /**
     * @see Hotfolder#getDataFolderTotalSize(String)
     * @verifies ignore data folders of other records
     */
    @Test
    void getDataFolderTotalSize_shouldIgnoreDataFoldersOfOtherRecords() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        Path otherMediaFolder = Files.createDirectory(hotfolder.getHotfolderPath().resolve("foo2_media"));
        Files.writeString(otherMediaFolder.resolve("00000001.tif"), "1234567890");

        assertEquals(0, hotfolder.getDataFolderTotalSize("foo"));
    }

    /**
     * @see Hotfolder#buildRecordSummary(IndexingResult,Long,int)
     * @verifies build summary for committed record
     */
    @Test
    void buildRecordSummary_shouldBuildSummaryForCommittedRecord() {
        IndexingResult result = new IndexingResult().setPi("PPN123").setDocStructType("Monograph").setNumPages(2).setNumSourcePages(3);
        assertEquals("Indexed 'PPN123' (Monograph, 2 of 3 pages); 42 record files remaining in hotfolder.",
                Hotfolder.buildRecordSummary(result, 42L, 1));
    }

    /**
     * @see Hotfolder#buildRecordSummary(IndexingResult,Long,int)
     * @verifies mention commit batch if batch size greater than one
     */
    @Test
    void buildRecordSummary_shouldMentionCommitBatchIfBatchSizeGreaterThanOne() {
        IndexingResult result = new IndexingResult().setPi("PPN123").setDocStructType("Monograph").setNumPages(2).setNumSourcePages(2);
        assertEquals("Prepared 'PPN123' (Monograph, 2 of 2 pages); will be committed with the next commit batch;"
                + " 0 record files remaining in hotfolder.", Hotfolder.buildRecordSummary(result, 0L, 100));
    }

    /**
     * @see Hotfolder#buildRecordSummary(IndexingResult,Long,int)
     * @verifies omit page counts if not available
     */
    @Test
    void buildRecordSummary_shouldOmitPageCountsIfNotAvailable() {
        IndexingResult result = new IndexingResult().setPi("PPN123").setDocStructType("Periodical");
        assertEquals("Indexed 'PPN123' (Periodical); 5 record files remaining in hotfolder.", Hotfolder.buildRecordSummary(result, 5L, 1));
    }

    /**
     * @see Hotfolder#buildRecordSummary(IndexingResult,Long,int)
     * @verifies omit remaining files if not counted
     */
    @Test
    void buildRecordSummary_shouldOmitRemainingFilesIfNotCounted() {
        IndexingResult result = new IndexingResult().setPi("PPN123").setDocStructType("Monograph").setNumPages(1).setNumSourcePages(1);
        assertEquals("Indexed 'PPN123' (Monograph, 1 of 1 pages).", Hotfolder.buildRecordSummary(result, null, 1));
    }

    /**
     * @see Hotfolder#doIndex(Path)
     * @verifies keep record file in hotfolder if solr unavailable
     */
    @Test
    void doIndex_shouldKeepRecordFileInHotfolderIfSolrUnavailable() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath()) {
            @Override
            boolean handleSourceFile(Path sourceFile, Map<String, Boolean> reindexSettings) {
                throw new SolrUnavailableException("Solr request 'commit' failed: Connection refused");
            }
        };
        Path recordFile = Files.writeString(hotfolder.getHotfolderPath().resolve("foo.xml"), "<mets/>");
        try {
            assertFalse(hotfolder.doIndex(recordFile));
            assertTrue(Files.isRegularFile(recordFile));
            assertFalse(Files.exists(hotfolder.getErrorMets().resolve("foo.xml")));
        } finally {
            Files.deleteIfExists(recordFile);
        }
    }

    /**
     * @see Hotfolder#doIndex(Path)
     * @verifies move record file to error folder on unexpected error
     */
    @Test
    void doIndex_shouldMoveRecordFileToErrorFolderOnUnexpectedError() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath()) {
            @Override
            boolean handleSourceFile(Path sourceFile, Map<String, Boolean> reindexSettings) {
                throw new IllegalStateException("unexpected");
            }
        };
        Path recordFile = Files.writeString(hotfolder.getHotfolderPath().resolve("foo.xml"), "<mets/>");
        Path errorFile = hotfolder.getErrorMets().resolve("foo.xml");
        Path errorLogFile = hotfolder.getErrorMets().resolve("foo.log");
        try {
            assertFalse(hotfolder.doIndex(recordFile));
            assertFalse(Files.exists(recordFile));
            assertTrue(Files.isRegularFile(errorFile));
            assertTrue(Files.readString(errorLogFile).contains("unexpected"));
        } finally {
            Files.deleteIfExists(recordFile);
            Files.deleteIfExists(errorFile);
            Files.deleteIfExists(errorLogFile);
        }
    }

    /**
     * @see Hotfolder#moveRecordFileToErrorFolder(Path,String)
     * @verifies move record file to error folder
     */
    @Test
    void moveRecordFileToErrorFolder_shouldMoveRecordFileToErrorFolder() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        Path recordFile = Files.writeString(hotfolder.getHotfolderPath().resolve("bar.xml"), "<mets/>");
        Path errorFile = hotfolder.getErrorMets().resolve("bar.xml");
        Path errorLogFile = hotfolder.getErrorMets().resolve("bar.log");
        try {
            hotfolder.moveRecordFileToErrorFolder(recordFile, "some error");
            assertFalse(Files.exists(recordFile));
            assertEquals("<mets/>", Files.readString(errorFile));
            assertEquals("some error", Files.readString(errorLogFile));
        } finally {
            Files.deleteIfExists(recordFile);
            Files.deleteIfExists(errorFile);
            Files.deleteIfExists(errorLogFile);
        }
    }
}
