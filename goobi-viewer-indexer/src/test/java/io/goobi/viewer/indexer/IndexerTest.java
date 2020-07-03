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

import java.awt.Dimension;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

public class IndexerTest extends AbstractSolrEnabledTest {

    private static Hotfolder hotfolder;

    private Path metsFile;
    private Path lidoFile;
    private static String libraryPath = "";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        hotfolder = new Hotfolder("src/test/resources/indexerconfig_solr_test.xml", client);
        metsFile = Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        Assert.assertTrue(Files.isRegularFile(metsFile));
        lidoFile = Paths.get("src/test/resources/LIDO/khm_lido_export.xml");
        Assert.assertTrue(Files.isRegularFile(lidoFile));
    }

    @BeforeClass
    public static void setUpBeforeClass() {
        File libraryFile = new File("src/test/resources/lib/libopenjp2.so");
        libraryPath = System.getProperty("java.library.path");
        System.setProperty("java.library.path", libraryPath + ":" + libraryFile.getParentFile().getAbsolutePath());
    }

    @AfterClass
    public static void cleanUpAfterClass() {
        if (StringUtils.isNotBlank(libraryPath)) {
            System.setProperty("java.library.path", libraryPath);
        }
    }

    /**
     * @see Indexer#delete(String,boolean)
     * @verifies delete METS record from index completely
     */
    @Test
    public void delete_shouldDeleteMETSRecordFromIndexCompletely() throws Exception {
        String pi = "PPN517154005";
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_MEDIA, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_tif"));
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, false, dataFolders, null, 1);
        Assert.assertEquals(pi + ".xml", ret[0]);
        Assert.assertNull(ret[1]);
        {
            SolrDocumentList docList = hotfolder.getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":" + pi, null);
            Assert.assertEquals(22, docList.size());
        }
        Assert.assertTrue(Indexer.delete(pi, false, hotfolder.getSearchIndex()));
        {
            SolrDocumentList docList = hotfolder.getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":" + pi, null);
            Assert.assertTrue(docList.isEmpty());
        }
    }

    /**
     * @see Indexer#delete(String,boolean)
     * @verifies delete LIDO record from index completely
     */
    @SuppressWarnings("unchecked")
    @Test
    public void delete_shouldDeleteLIDORecordFromIndexCompletely() throws Exception {
        String pi = "V0011127";
        Map<String, Path> dataFolders = new HashMap<>();
        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile.toFile());
        String[] ret = new LidoIndexer(hotfolder).index(lidoDocs.get(0), dataFolders, null, 1,
                Configuration.getInstance().getList("init.lido.imageXPath"), false);
        Assert.assertEquals("ERROR: " + ret[1], pi, ret[0]);
        String iddoc;
        {
            SolrDocumentList docList = hotfolder.getSearchIndex().search(SolrConstants.PI + ":" + pi, null);
            Assert.assertEquals(1, docList.size());
            iddoc = (String) docList.get(0).getFieldValue(SolrConstants.IDDOC);
            Assert.assertNotNull(iddoc);
        }
        {
            SolrDocumentList docList =
                    hotfolder.getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":" + pi + " OR " + SolrConstants.IDDOC_OWNER + ":" + iddoc, null);
            Assert.assertEquals(3, docList.size());
        }
        Assert.assertTrue(Indexer.delete(pi, false, hotfolder.getSearchIndex()));
        {
            SolrDocumentList docList =
                    hotfolder.getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":" + pi + " OR " + SolrConstants.IDDOC_OWNER + ":" + iddoc, null);
            Assert.assertTrue(docList.isEmpty());
        }
    }

    /**
     * @see Indexer#delete(String,boolean)
     * @verifies leave trace document for METS record if requested
     */
    @Test
    public void delete_shouldLeaveTraceDocumentForMETSRecordIfRequested() throws Exception {
        String pi = "PPN517154005";
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_MEDIA, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_tif"));
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, false, dataFolders, null, 1);
        Assert.assertEquals(pi + ".xml", ret[0]);
        Assert.assertNull(ret[1]);
        {
            SolrDocumentList docList = hotfolder.getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":" + pi, null);
            Assert.assertEquals(22, docList.size());
        }
        Assert.assertTrue(Indexer.delete(pi, true, hotfolder.getSearchIndex()));
        {
            SolrDocumentList docList = hotfolder.getSearchIndex().search(SolrConstants.PI + ":" + pi, null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATEDELETED));
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.GROUPFIELD));
            Assert.assertNotNull(doc.getFieldValues(SolrConstants.IMAGEURN_OAI));
            Assert.assertEquals(16, doc.getFieldValues(SolrConstants.IMAGEURN_OAI).size());
        }
    }

    /**
     * @see Indexer#delete(String,boolean)
     * @verifies leave trace document for LIDO record if requested
     */
    @SuppressWarnings("unchecked")
    @Test
    public void delete_shouldLeaveTraceDocumentForLIDORecordIfRequested() throws Exception {
        String pi = "V0011127";
        Map<String, Path> dataFolders = new HashMap<>();
        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile.toFile());
        String[] ret = new LidoIndexer(hotfolder).index(lidoDocs.get(0), dataFolders, null, 1,
                Configuration.getInstance().getList("init.lido.imageXPath"), false);
        Assert.assertEquals(pi, ret[0]);
        String iddoc;
        {
            SolrDocumentList docList = hotfolder.getSearchIndex().search(SolrConstants.PI + ":" + pi, null);
            Assert.assertEquals(1, docList.size());
            iddoc = (String) docList.get(0).getFieldValue(SolrConstants.IDDOC);
            Assert.assertNotNull(iddoc);
        }
        {
            SolrDocumentList docList =
                    hotfolder.getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":" + pi + " OR " + SolrConstants.IDDOC_OWNER + ":" + iddoc, null);
            Assert.assertEquals(3, docList.size());
        }
        Assert.assertTrue(Indexer.delete(pi, true, hotfolder.getSearchIndex()));
        {
            SolrDocumentList docList = hotfolder.getSearchIndex().search(SolrConstants.PI + ":" + pi, null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATEDELETED));
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.GROUPFIELD));
        }
    }

    /**
     * @see Indexer#cleanUpDefaultField(String)
     * @verifies replace irrelevant chars with spaces correctly
     */
    @Test
    public void cleanUpDefaultField_shouldReplaceIrrelevantCharsWithSpacesCorrectly() throws Exception {
        Assert.assertEquals("A B C D", Indexer.cleanUpDefaultField(" A,B;C:D,  "));
    }

    /**
     * @see Indexer#cleanUpNamedEntityValue(String)
     * @verifies clean up value correctly
     */
    @Test
    public void cleanUpNamedEntityValue_shouldCleanUpValueCorrectly() throws Exception {
        Assert.assertEquals("abcd", Indexer.cleanUpNamedEntityValue("\"(abcd,\""));
    }

    /**
     * @see Indexer#cleanUpNamedEntityValue(String)
     * @verifies throw IllegalArgumentException given null
     */
    @Test(expected = IllegalArgumentException.class)
    public void cleanUpNamedEntityValue_shouldThrowIllegalArgumentExceptionGivenNull() throws Exception {
        Indexer.cleanUpNamedEntityValue(null);
    }

    /**
     * @see Indexer#getSize(Map,SolrInputDocument)
     * @verifies return size correctly
     */
    @Test
    public void getSize_shouldReturnSizeCorrectly() throws Exception {

        String[] filenames = { "00000001.tif", "00225231.png", "test1.jp2" };
        Dimension[] imageSizes = { new Dimension(3192, 4790), new Dimension(2794, 3838), new Dimension(3448, 6499) };

        MetsIndexer indexer = new MetsIndexer(hotfolder);
        File dataFolder = new File("src/test/resources/image_size");

        int i = 0;
        File outputFolder = new File(dataFolder, "output");
        try {
            for (String filename : filenames) {
                if (outputFolder.isDirectory()) {
                    FileUtils.deleteDirectory(outputFolder);
                }
                outputFolder.mkdirs();

                Optional<Dimension> dim = Indexer.getSize(dataFolder.toPath(), filename);
                // jp2 image files cannot be read because of missing jp2 library
                Assert.assertTrue(dim.isPresent());
                Assert.assertEquals("image size of " + filename + " is " + dim + ", but should be " + imageSizes[i], imageSizes[i], dim.get());

                i++;
            }
        } finally {
            if (outputFolder.isDirectory()) {
                FileUtils.deleteDirectory(outputFolder);
            }
        }
    }

    /**
     * @see Indexer#generateAnnotationDocs(Map,Path,String,String,Map)
     * @verifies create docs correctly
     */
    @Test
    public void generateAnnotationDocs_shouldCreateDocsCorrectly() throws Exception {
        Map<Integer, SolrInputDocument> pageDocs = new HashMap<>(2);
        {
            SolrInputDocument pageDoc = new SolrInputDocument();
            pageDoc.setField(SolrConstants.IDDOC, 123);
            pageDoc.setField(SolrConstants.ORDER, 1);
            pageDoc.setField(SolrConstants.DOCSTRCT_TOP, "topstruct");
            pageDocs.put(1, pageDoc);
        }
        {
            SolrInputDocument pageDoc = new SolrInputDocument();
            pageDoc.setField(SolrConstants.IDDOC, 124);
            pageDoc.setField(SolrConstants.ORDER, 2);
            pageDoc.setField(SolrConstants.DOCSTRCT_TOP, "topstruct");
            pageDocs.put(2, pageDoc);
        }

        Path dataFolder = Paths.get("src/test/resources/WebAnnotations");
        Assert.assertTrue(Files.isDirectory(dataFolder));

        List<SolrInputDocument> docs = new MetsIndexer(hotfolder).generateAnnotationDocs(pageDocs, dataFolder, "PPN517154005", null, null);
        Assert.assertEquals(2, docs.size());
        {
            Assert.assertEquals("PPN517154005", docs.get(0).getFieldValue(SolrConstants.PI_TOPSTRUCT));
            Assert.assertEquals("topstruct", docs.get(0).getFieldValue(SolrConstants.DOCSTRCT_TOP));
            Assert.assertEquals(1, docs.get(0).getFieldValue(SolrConstants.ORDER));
            Assert.assertEquals(123, docs.get(0).getFieldValue(SolrConstants.IDDOC_OWNER));
            Assert.assertEquals("9.967025 51.521737", docs.get(0).getFieldValue("MD_COORDS"));
            Assert.assertEquals(SolrConstants._UGC_TYPE_ADDRESS, docs.get(0).getFieldValue(SolrConstants.UGCTYPE));
            Assert.assertNotNull(docs.get(0).getFieldValue("MD_BODY"));
            //            Assert.assertEquals(SolrConstants._UGC_TYPE_ADDRESS + " Leipzig", docs.get(0).getFieldValue(SolrConstants.UGCTERMS));
        }
        {
            Assert.assertEquals("PPN517154005", docs.get(1).getFieldValue(SolrConstants.PI_TOPSTRUCT));
            Assert.assertEquals("topstruct", docs.get(1).getFieldValue(SolrConstants.DOCSTRCT_TOP));
            Assert.assertEquals(1, docs.get(1).getFieldValue(SolrConstants.ORDER));
            Assert.assertEquals(123, docs.get(1).getFieldValue(SolrConstants.IDDOC_OWNER));
            Assert.assertEquals("Leipzig", docs.get(1).getFieldValue("MD_TEXT"));
            Assert.assertEquals("xywh=1378,3795,486,113", docs.get(1).getFieldValue(SolrConstants.UGCCOORDS));
            Assert.assertEquals(SolrConstants._UGC_TYPE_ADDRESS, docs.get(1).getFieldValue(SolrConstants.UGCTYPE));
            Assert.assertEquals(SolrConstants._UGC_TYPE_ADDRESS + " Leipzig", docs.get(1).getFieldValue(SolrConstants.UGCTERMS));
        }
    }
}
