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
import java.util.Collections;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;

public class DataRepositoryTest extends AbstractTest {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(DataRepositoryTest.class);

    private static final String BASE_FILE_NAME = "12345";

    @SuppressWarnings("unused")
    private static Hotfolder hotfolder;

    @BeforeClass
    public static void setUpClass() throws Exception {
        AbstractTest.setUpClass();

        hotfolder = new Hotfolder(TEST_CONFIG_PATH, null);
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
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

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    /**
     * @see DataRepository#DataRepository(File,String)
     * @verifies create dummy repository correctly
     */
    @Test
    public void DataRepository_shouldCreateDummyRepositoryCorrectly() throws Exception {
        DataRepository dataRepository = new DataRepository(Configuration.getInstance().getString("init.viewerHome"), true);
        Assert.assertTrue(dataRepository.isValid());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_MEDIA).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_ALTO).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_ALTOCROWD).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_FULLTEXT).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_TEIMETADATA).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_ABBYY).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_PAGEPDF).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_SOURCE).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_UGC).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_MIX).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_CMS).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_ANNOTATIONS).toFile().isDirectory());
    }

    /**
     * @see DataRepository#DataRepository(File,String)
     * @verifies create real repository correctly
     */
    @Test
    public void DataRepository_shouldCreateRealRepositoryCorrectly() throws Exception {
        DataRepository dataRepository = new DataRepository("target/viewer/data/1", true);
        Assert.assertTrue(dataRepository.isValid());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_MEDIA).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_ALTO).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_ALTOCROWD).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_FULLTEXT).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_TEIMETADATA).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_ABBYY).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_PAGEPDF).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_SOURCE).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_UGC).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_MIX).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_CMS).toFile().isDirectory());
        Assert.assertTrue(dataRepository.getDir(DataRepository.PARAM_ANNOTATIONS).toFile().isDirectory());
    }

    /**
     * @see DataRepository#DataRepository(String)
     * @verifies set rootDir to viewerHome path if empty string was given
     */
    @Test
    public void DataRepository_shouldSetRootDirToViewerHomePathIfEmptyStringWasGiven() throws Exception {
        DataRepository dataRepository = new DataRepository("", false);
        Assert.assertEquals("", dataRepository.getPath());
        Assert.assertEquals(Paths.get(Configuration.getInstance().getViewerHome()), dataRepository.getRootDir());
    }

    /**
     * @see DataRepository#DataRepository(String,boolean)
     * @verifies add each data directory to the dirMap of dummy repository
     */
    @Test
    public void DataRepository_shouldAddEachDataDirectoryToTheDirMapOfDummyRepository() throws Exception {
        DataRepository dataRepository = new DataRepository("", false);
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_MEDIA));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ALTO));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ALTOCROWD));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_FULLTEXT));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_TEIMETADATA));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ABBYY));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_PAGEPDF));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_SOURCE));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_UGC));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_MIX));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_CMS));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ANNOTATIONS));
    }

    /**
     * @see DataRepository#DataRepository(String,boolean)
     * @verifies add each data directory to the dirMap of real repository
     */
    @Test
    public void DataRepository_shouldAddEachDataDirectoryToTheDirMapOfRealRepository() throws Exception {
        DataRepository dataRepository = new DataRepository("1", false);
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_MEDIA));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ALTO));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ALTOCROWD));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_FULLTEXT));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_TEIMETADATA));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ABBYY));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_PAGEPDF));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_SOURCE));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_UGC));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_MIX));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_CMS));
        Assert.assertNotNull(dataRepository.getDir(DataRepository.PARAM_ANNOTATIONS));
    }

    /**
     * @see DataRepository#getNumRecords()
     * @verifies calculate number correctly
     */
    @Test
    public void getNumRecords_shouldCalculateNumberCorrectly() throws Exception {
        DataRepository dataRepository = new DataRepository(Configuration.getInstance().getString("init.viewerHome"), true);

        File srcFile = new File("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        File destFile = new File(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), srcFile.getName());
        FileUtils.copyFile(srcFile, destFile);
        Assert.assertTrue(destFile.isFile());

        srcFile = new File("src/test/resources/METS/baltst_559838239/baltst_559838239_NF_75.xml");
        destFile = new File(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), srcFile.getName());
        FileUtils.copyFile(srcFile, destFile);
        Assert.assertTrue(destFile.isFile());

        srcFile = new File("src/test/resources/METS/baltst_559838239/baltst_559838239_NF_75.xml");
        destFile = new File(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toAbsolutePath().toString(), srcFile.getName());
        FileUtils.copyFile(srcFile, destFile);
        Assert.assertTrue(destFile.isFile());

        Assert.assertEquals(3, dataRepository.getNumRecords());
    }

    /**
     * @see DataRepository#deleteFolder(File)
     * @verifies delete folder correctly
     */
    @Test
    public void deleteFolder_shouldDeleteFolderCorrectly() throws Exception {
        Path folder = Paths.get("target/folder");
        Files.createDirectory(folder);
        Assert.assertTrue(Files.exists(folder));
        DataRepository.deleteFolder(folder);
        Assert.assertFalse(Files.exists(folder));
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete ALTO folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteALTOFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete ALTO crowdsourcing folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteALTOCrowdsourcingFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_ALTOCROWD).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete fulltext folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteFulltextFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_FULLTEXT).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete fulltext crowdsourcing folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteFulltextCrowdsourcingFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete CMDI folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteCMDIFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_CMDI).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete TEI folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteTEIFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_TEIMETADATA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete word coords folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteWordCoordsFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_TEIWC).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete ABBYY folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteABBYYFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_ABBYY).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete media folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteMediaFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete source folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteSourceFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_SOURCE).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete user generated content folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteUserGeneratedContentFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_UGC).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete MIX folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteMIXFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_MIX).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete page PDF folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeletePagePDFFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_PAGEPDF).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete CMS folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteCMSFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_CMS).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersForRecord(String)
     * @verifies delete annotations folder correctly
     */
    @Test
    public void deleteDataFoldersForRecord_shouldDeleteAnnotationsFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_ANNOTATIONS).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());
        useRepository.deleteDataFoldersForRecord(BASE_FILE_NAME);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#moveDataFolderToRepository(DataRepository,String,String)
     * @verifies move data folder correctly
     */
    @Test
    public void moveDataFolderToRepository_shouldMoveDataFolderCorrectly() throws Exception {
        DataRepository oldRepository = new DataRepository("target/viewer/data/old", true);
        File oldDataFolder = new File(oldRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(oldDataFolder.mkdirs());
        Assert.assertTrue(oldDataFolder.exists());
        File oldDataFile = new File(oldDataFolder, "file.txt");
        Assert.assertTrue(oldDataFile.createNewFile());
        Assert.assertTrue(oldDataFile.exists());

        DataRepository newRepository = new DataRepository("target/viewer/data/new", true);
        oldRepository.moveDataFolderToRepository(newRepository, BASE_FILE_NAME, DataRepository.PARAM_MEDIA);

        File newDataFolder = new File(newRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(newDataFolder.exists());
        File newDataFile = new File(newDataFolder, "file.txt");
        Assert.assertTrue(newDataFile.exists());

        Assert.assertFalse(oldDataFile.exists());
        Assert.assertFalse(oldDataFolder.exists());
    }

    /**
     * @see DataRepository#getAbsolutePath(String)
     * @verifies return correct path
     */
    @Test
    public void getAbsolutePath_shouldReturnCorrectPath() throws Exception {
        Assert.assertEquals("target/viewer/data/1", DataRepository.getAbsolutePath("1"));
    }

    /**
     * @see DataRepository#deleteDataFolder(Map,Map,String)
     * @verifies delete folders correctly
     */
    @Test
    public void deleteDataFolder_shouldDeleteFoldersCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFolder(Collections.singletonMap(DataRepository.PARAM_MEDIA, dataFolder.toPath()),
                Collections.singletonMap("other", true), DataRepository.PARAM_MEDIA);
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFolder(Map,Map,String)
     * @verifies not delete reindexed folders
     */
    @Test
    public void deleteDataFolder_shouldNotDeleteReindexedFolders() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFolder(Collections.singletonMap(DataRepository.PARAM_MEDIA, dataFolder.toPath()),
                Collections.singletonMap(DataRepository.PARAM_MEDIA, true), DataRepository.PARAM_MEDIA);
        Assert.assertTrue(dataFolder.exists());

    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete ALTO folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteALTOFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_ALTO);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_ALTO, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete ALTO crowdsourcing folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteALTOCrowdsourcingFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_ALTOCROWD);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_ALTOCROWD, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete fulltext folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteFulltextFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_FULLTEXT);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_FULLTEXT, dataFolder.toPath()),
                Collections.emptyMap());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete fulltext crowdsourcing folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteFulltextCrowdsourcingFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_FULLTEXTCROWD);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_FULLTEXTCROWD, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete CMDI folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteCMDIFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_CMDI);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_CMDI, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete TEI folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteTEIFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_TEIMETADATA);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_TEIMETADATA, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete word coords folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteWordCoordsFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_TEIWC);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_TEIWC, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete ABBYY folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteABBYYFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_ABBYY);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_ABBYY, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete media folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteMediaFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_MEDIA);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_MEDIA, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete source folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteSourceFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_SOURCE);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_SOURCE, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete user generated content folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteUserGeneratedContentFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_UGC);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_UGC, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete MIX folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteMIXFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_MIX);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_MIX, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete page PDF folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeletePagePDFFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_PAGEPDF);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_PAGEPDF, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete CMS folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteCMSFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_CMS);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_CMS, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete annotations folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteAnnotationsFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_ANNOTATIONS);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_ANNOTATIONS, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }

    /**
     * @see DataRepository#deleteDataFoldersFromHotfolder(Map,Map)
     * @verifies delete download images trigger folder correctly
     */
    @Test
    public void deleteDataFoldersFromHotfolder_shouldDeleteDownloadImagesTriggerFolderCorrectly() throws Exception {
        DataRepository useRepository = new DataRepository("target/viewer/data/", true);
        File dataFolder = new File(useRepository.getRootDir().toAbsolutePath().toString(), DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER);
        Assert.assertTrue(dataFolder.mkdirs());
        Assert.assertTrue(dataFolder.exists());

        DataRepository.deleteDataFoldersFromHotfolder(Collections.singletonMap(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER, dataFolder.toPath()),
                Collections.emptyMap());
        Assert.assertFalse(dataFolder.exists());
    }
}
