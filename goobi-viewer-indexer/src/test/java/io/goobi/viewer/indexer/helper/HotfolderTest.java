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
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractSolrEnabledTest;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
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
}