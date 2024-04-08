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
package io.goobi.viewer.indexer.model.datarepository;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.helper.Hotfolder;

class DataRepositoryTest extends AbstractTest {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(DataRepositoryTest.class);

    private static final String BASE_FILE_NAME = "12345";

    @SuppressWarnings("unused")
    private static Hotfolder hotfolder;

    @BeforeAll
    public static void setUpClass() throws Exception {
        AbstractTest.setUpClass();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
    }

    @AfterEach
    public void tearDown() throws Exception {
        File indexerFolder = new File("target/indexer");
        if (indexerFolder.isDirectory()) {
            logger.info("Deleting {}...", indexerFolder);
            FileUtils.deleteDirectory(indexerFolder);
        }
        File viewerRootFolder = new File("target/viewer");
        if (viewerRootFolder.isDirectory()) {
            logger.info("Deleting {}...", viewerRootFolder);
            FileUtils.deleteDirectory(viewerRootFolder);
        }
    }

    /**
     * @see DataRepository#DataRepository(File,String)
     * @verifies create dummy repository correctly
     */
    @Test
    void DataRepository_shouldCreateDummyRepositoryCorrectly() throws Exception {
        DataRepository dataRepository = new DataRepository(SolrIndexerDaemon.getInstance().getConfiguration().getString("init.viewerHome"), true);
        Assertions.assertTrue(dataRepository.isValid());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_EAD).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_DENKXWEB).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_CMS).toFile().isDirectory());

        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_MEDIA).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_ALTO).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_ALTOCROWD).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_FULLTEXT).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_TEIMETADATA).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_ABBYY).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_PAGEPDF).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_SOURCE).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_UGC).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_MIX).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_CMS).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_ANNOTATIONS).toFile().isDirectory());
    }

    /**
     * @see DataRepository#DataRepository(File,String)
     * @verifies create real repository correctly
     */
    @Test
    void DataRepository_shouldCreateRealRepositoryCorrectly() throws Exception {
        DataRepository dataRepository = new DataRepository("target/viewer/data/1", true);
        Assertions.assertTrue(dataRepository.isValid());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_EAD).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_DENKXWEB).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_CMS).toFile().isDirectory());

        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_MEDIA).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_ALTO).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_ALTOCROWD).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_FULLTEXT).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_TEIMETADATA).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_ABBYY).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_PAGEPDF).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_SOURCE).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_UGC).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_MIX).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_CMS).toFile().isDirectory());
        Assertions.assertTrue(dataRepository.getDir(DataRepository.PARAM_ANNOTATIONS).toFile().isDirectory());
    }

    /**
     * @see DataRepository#DataRepository(String)
     * @verifies set rootDir to viewerHome path if empty string was given
     */
    @Test
    void DataRepository_shouldSetRootDirToViewerHomePathIfEmptyStringWasGiven() throws Exception {
        DataRepository dataRepository = new DataRepository("", false);
        Assertions.assertEquals("", dataRepository.getPath());
        Assertions.assertEquals(Paths.get(SolrIndexerDaemon.getInstance().getConfiguration().getViewerHome()), dataRepository.getRootDir());
    }

    /**
     * @see DataRepository#DataRepository(String,boolean)
     * @verifies add each data directory to the dirMap of dummy repository
     */
    @Test
    void DataRepository_shouldAddEachDataDirectoryToTheDirMapOfDummyRepository() throws Exception {
        DataRepository dataRepository = new DataRepository("", false);
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_EAD));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_DENKXWEB));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_CMS));

        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_MEDIA));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ALTO));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ALTOCROWD));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_FULLTEXT));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_TEIMETADATA));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ABBYY));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_PAGEPDF));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_SOURCE));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_UGC));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_MIX));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_CMS));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ANNOTATIONS));
    }

    /**
     * @see DataRepository#DataRepository(String,boolean)
     * @verifies add each data directory to the dirMap of real repository
     */
    @Test
    void DataRepository_shouldAddEachDataDirectoryToTheDirMapOfRealRepository() throws Exception {
        DataRepository dataRepository = new DataRepository("1", false);
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_EAD));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_DENKXWEB));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_CMS));

        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_MEDIA));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ALTO));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ALTOCROWD));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_FULLTEXT));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_TEIMETADATA));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ABBYY));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_PAGEPDF));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_SOURCE));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_UGC));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_MIX));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_CMS));
        Assertions.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ANNOTATIONS));
    }

    /**
     * @see DataRepository#getNumRecords()
     * @verifies calculate number correctly
     */
    @Test
    void getNumRecords_shouldCalculateNumberCorrectly() throws Exception {
        DataRepository dataRepository = new DataRepository(SolrIndexerDaemon.getInstance().getConfiguration().getString("init.viewerHome"), true);

        File srcFile = new File("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        File destFile = new File(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), srcFile.getName());
        FileUtils.copyFile(srcFile, destFile);
        Assertions.assertTrue(destFile.isFile());

        srcFile = new File("src/test/resources/METS/baltst_559838239/baltst_559838239_NF_75.xml");
        destFile = new File(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), srcFile.getName());
        FileUtils.copyFile(srcFile, destFile);
        Assertions.assertTrue(destFile.isFile());

        srcFile = new File("src/test/resources/METS/baltst_559838239/baltst_559838239_NF_75.xml");
        destFile = new File(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toAbsolutePath().toString(), srcFile.getName());
        FileUtils.copyFile(srcFile, destFile);
        Assertions.assertTrue(destFile.isFile());
        
        srcFile = new File("src/test/resources/EAD/Akte_Koch.xml");
        destFile = new File(dataRepository.getDir(DataRepository.PARAM_INDEXED_EAD).toAbsolutePath().toString(), srcFile.getName());
        FileUtils.copyFile(srcFile, destFile);
        Assertions.assertTrue(destFile.isFile());

        srcFile = new File("src/test/resources/DenkXweb/10973880.xml");
        destFile = new File(dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE).toAbsolutePath().toString(), srcFile.getName());
        FileUtils.copyFile(srcFile, destFile);
        Assertions.assertTrue(destFile.isFile());

        srcFile = new File("src/test/resources/DC/record.xml");
        destFile = new File(dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE).toAbsolutePath().toString(), srcFile.getName());
        FileUtils.copyFile(srcFile, destFile);
        Assertions.assertTrue(destFile.isFile());

        srcFile = new File("src/test/resources/cms/cms1.xml");
        destFile = new File(dataRepository.getDir(DataRepository.PARAM_INDEXED_CMS).toAbsolutePath().toString(), srcFile.getName());
        FileUtils.copyFile(srcFile, destFile);
        Assertions.assertTrue(destFile.isFile());

        Assertions.assertEquals(7, dataRepository.getNumRecords());
    }

    /**
     * @see DataRepository#deleteFolder(File)
     * @verifies delete folder correctly
     */
    @Test
    void deleteFolder_shouldDeleteFolderCorrectly() throws Exception {
        Path folder = Paths.get("target/folder");
        Files.createDirectory(folder);
        Assertions.assertTrue(Files.exists(folder));
        DataRepository.deleteFolder(folder);
        Assertions.assertFalse(Files.exists(folder));
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete ALTO folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteALTOFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete ALTO crowdsourcing folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteALTOCrowdsourcingFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_ALTOCROWD).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete fulltext folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteFulltextFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_FULLTEXT).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete fulltext crowdsourcing folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteFulltextCrowdsourcingFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete CMDI folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteCMDIFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_CMDI).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete TEI folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteTEIFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_TEIMETADATA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete word coords folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteWordCoordsFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_TEIWC).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete ABBYY folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteABBYYFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_ABBYY).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete media folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteMediaFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete source folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteSourceFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_SOURCE).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete user generated content folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteUserGeneratedContentFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_UGC).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete MIX folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteMIXFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_MIX).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete page PDF folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeletePagePDFFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_PAGEPDF).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete CMS folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteCMSFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_CMS).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete annotations folder correctly
     */
    @Test
    void deleteDataFoldersForRecord_shouldDeleteAnnotationsFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_ANNOTATIONS).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#moveDataFolderToRepository(DataRepository,String,String)
     * @verifies move data folder correctly
     */
    @Test
    void moveDataFolderToRepository_shouldMoveDataFolderCorrectly() throws Exception {
        DataRepository oldRepository = new DataRepository("target/viewer/data/old", true);
        File oldDataFolder = new File(oldRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(oldDataFolder.mkdirs());
        Assertions.assertTrue(oldDataFolder.exists());
        File oldDataFile = new File(oldDataFolder, "file.txt");
        Assertions.assertTrue(oldDataFile.createNewFile());
        Assertions.assertTrue(oldDataFile.exists());

        DataRepository newRepository = new DataRepository("target/viewer/data/new", true);
        oldRepository.moveDataFolderToRepository(newRepository, BASE_FILE_NAME, DataRepository.PARAM_MEDIA);

        File newDataFolder = new File(newRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(newDataFolder.exists());
        File newDataFile = new File(newDataFolder, "file.txt");
        Assertions.assertTrue(newDataFile.exists());

        Assertions.assertFalse(oldDataFile.exists());
        Assertions.assertFalse(oldDataFolder.exists());
    }

    /**
     * @see DataRepository#getAbsolutePath(String)
     * @verifies return correct path
     */
    @Test
    void getAbsolutePath_shouldReturnCorrectPath() throws Exception {
        Assertions.assertEquals("target/viewer/data/1", DataRepository.getAbsolutePath("1"));
    }

    /**
     * @see DataRepository#deleteDataFolder(Map,Map,String)
     * @verifies delete folders correctly
     */
    @Test
    void deleteDataFolder_shouldDeleteFoldersCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFolder(Collections.singletonMap(DataRepository.PARAM_MEDIA, dataFolder.toPath()),
                Collections.singletonMap("other", true), DataRepository.PARAM_MEDIA);
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFolder(Map,Map,String)
     * @verifies not delete reindexed folders
     */
    @Test
    void deleteDataFolder_shouldNotDeleteReindexedFolders() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFolder(Collections.singletonMap(DataRepository.PARAM_MEDIA, dataFolder.toPath()),
                Collections.singletonMap(DataRepository.PARAM_MEDIA, true), DataRepository.PARAM_MEDIA);
        Assertions.assertTrue(dataFolder.exists());

    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete ALTO folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteALTOFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_ALTO);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_ALTO, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete ALTO crowdsourcing folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteALTOCrowdsourcingFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_ALTOCROWD);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_ALTOCROWD, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete fulltext folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteFulltextFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_FULLTEXT);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_FULLTEXT, dataFolder.toPath()),
                Collections.emptyMap());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete fulltext crowdsourcing folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteFulltextCrowdsourcingFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_FULLTEXTCROWD);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_FULLTEXTCROWD, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete CMDI folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteCMDIFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_CMDI);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_CMDI, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete TEI folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteTEIFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_TEIMETADATA);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_TEIMETADATA, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete word coords folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteWordCoordsFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_TEIWC);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_TEIWC, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete ABBYY folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteABBYYFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_ABBYY);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_ABBYY, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete media folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteMediaFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_MEDIA);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_MEDIA, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete source folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteSourceFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_SOURCE);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_SOURCE, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete user generated content folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteUserGeneratedContentFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_UGC);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_UGC, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete MIX folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteMIXFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_MIX);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_MIX, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete page PDF folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeletePagePDFFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_PAGEPDF);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_PAGEPDF, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete CMS folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteCMSFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_CMS);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_CMS, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete annotations folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteAnnotationsFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_ANNOTATIONS);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_ANNOTATIONS, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete download images trigger folder correctly
     */
    @Test
    void deleteDataFoldersFromHotfolder_shouldDeleteDownloadImagesTriggerFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER);
        Assertions.assertTrue(dataFolder.mkdirs());
        Assertions.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER, dataFolder.toPath()),
                Collections.emptyMap());
        Assertions.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#checkOtherRepositoriesForRecordFileDuplicates(String,String,List)
     * @verifies delete only misplaced files
     */
    @Test
    void checkOtherRepositoriesForRecordFileDuplicates_shouldDeleteOnlyMisplacedFiles() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/1/", true);
        DataRepository otherRepository = new DataRepository("target/viewer/data/2/", true);

        List<DataRepository> repositories = Arrays.asList(useRepository, otherRepository);

        Path file = Paths.get("target/viewer/data/1/indexed_mets/foo.xml");
        Files.createFile(file);
        Assertions.assertTrue(Files.isRegularFile(file));

        Path misplacedFile = Paths.get("target/viewer/data/2/indexed_mets/foo.xml");
        Files.createFile(misplacedFile);
        Assertions.assertTrue(Files.isRegularFile(misplacedFile));

        useRepository.checkOtherRepositoriesForRecordFileDuplicates("foo.xml", DataRepository.PARAM_INDEXED_METS, repositories);
        Assertions.assertTrue(Files.exists(file));
        Assertions.assertFalse(Files.exists(misplacedFile));
    }
}
