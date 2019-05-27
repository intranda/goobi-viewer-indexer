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
package de.intranda.digiverso.presentation.solr.model.datarepository;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.AbstractTest;
import de.intranda.digiverso.presentation.solr.helper.Configuration;
import de.intranda.digiverso.presentation.solr.helper.Hotfolder;

public class DataRepositoryTest extends AbstractTest {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(DataRepositoryTest.class);

    private static final String BASE_FILE_NAME = "12345";

    @SuppressWarnings("unused")
    private static Hotfolder hotfolder;

    @BeforeClass
    public static void setUpClass() throws Exception {
        AbstractTest.setUpClass();
        
        hotfolder = new Hotfolder("resources/test/indexerconfig_solr_test.xml", null);
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        File indexerFolder = new File("build/indexer");
        if (indexerFolder.isDirectory()) {
            logger.info("Deleting {}...", indexerFolder);
            FileUtils.deleteDirectory(indexerFolder);
        }
        File viewerRootFolder = new File("build/viewer");
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
    }

    /**
     * @see DataRepository#DataRepository(File,String)
     * @verifies create real repository correctly
     */
    @Test
    public void DataRepository_shouldCreateRealRepositoryCorrectly() throws Exception {
        DataRepository dataRepository = new DataRepository("build/viewer/data/1", true);
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
     * @see DataRepository#getNumRecords()
     * @verifies calculate number correctly
     */
    @Test
    public void getNumRecords_shouldCalculateNumberCorrectly() throws Exception {
        DataRepository dataRepository = new DataRepository(Configuration.getInstance().getString("init.viewerHome"), true);

        File srcFile = new File("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        File destFile = new File(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), srcFile.getName());
        FileUtils.copyFile(srcFile, destFile);
        Assert.assertTrue(destFile.isFile());

        srcFile = new File("resources/test/METS/baltst_559838239/baltst_559838239_NF_75.xml");
        destFile = new File(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), srcFile.getName());
        FileUtils.copyFile(srcFile, destFile);
        Assert.assertTrue(destFile.isFile());

        srcFile = new File("resources/test/METS/baltst_559838239/baltst_559838239_NF_75.xml");
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
        Path folder = Paths.get("build/folder");
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
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
        DataRepository useRepository = new DataRepository("build/viewer/data/", true);
        File dataFolder = new File(useRepository.getDir(DataRepository.PARAM_CMS).toAbsolutePath().toString(), BASE_FILE_NAME);
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
        DataRepository oldRepository = new DataRepository("build/viewer/data/old", true);
        File oldDataFolder = new File(oldRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), BASE_FILE_NAME);
        Assert.assertTrue(oldDataFolder.mkdirs());
        Assert.assertTrue(oldDataFolder.exists());
        File oldDataFile = new File(oldDataFolder, "file.txt");
        Assert.assertTrue(oldDataFile.createNewFile());
        Assert.assertTrue(oldDataFile.exists());

        DataRepository newRepository = new DataRepository("build/viewer/data/new", true);
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
        Assert.assertEquals("build/viewer/data/1", DataRepository.getAbsolutePath("1"));
    }
}
