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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.model.GroupedMetadata;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.LazySolrWriteStrategy;

public class IndexerTest extends AbstractSolrEnabledTest {

    private Path metsFile;
    private Path lidoFile;
    private static String libraryPath = "";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        hotfolder = new Hotfolder(TEST_CONFIG_PATH, client);
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
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, false, dataFolders, null, 1, false);
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
                Configuration.getInstance().getList("init.lido.imageXPath"), false, false);
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
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, false, dataFolders, null, 1, false);
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
                Configuration.getInstance().getList("init.lido.imageXPath"), false, false);
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
        {
            SolrInputDocument pageDoc = new SolrInputDocument();
            pageDoc.setField(SolrConstants.IDDOC, 133);
            pageDoc.setField(SolrConstants.ORDER, 10);
            pageDoc.setField(SolrConstants.DOCSTRCT_TOP, "topstruct");
            pageDocs.put(10, pageDoc);
        }

        Path dataFolder = Paths.get("src/test/resources/WebAnnotations");
        Assert.assertTrue(Files.isDirectory(dataFolder));

        List<SolrInputDocument> docs = new MetsIndexer(hotfolder).generateAnnotationDocs(pageDocs, dataFolder, "PPN517154005", null, null);
        Assert.assertEquals(3, docs.size());
        {
            SolrInputDocument doc = docs.stream()
                    .filter(d -> d.getFieldValue(SolrConstants.MD_ANNOTATION_ID).equals("geo"))
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("No annotation with id 'geo'"));
            Assert.assertEquals("PPN517154005", doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            Assert.assertEquals("topstruct", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
            Assert.assertNull(doc.getFieldValue(SolrConstants.ORDER));
            Assert.assertNull(doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            Assert.assertEquals("9.967025 51.521737", doc.getFieldValue("MD_COORDS"));
            Assert.assertEquals(SolrConstants._UGC_TYPE_ADDRESS, doc.getFieldValue(SolrConstants.UGCTYPE));
            Assert.assertNotNull(doc.getFieldValue("MD_BODY"));
            //            Assert.assertEquals(SolrConstants._UGC_TYPE_ADDRESS + " Leipzig", docs.get(0).getFieldValue(SolrConstants.UGCTERMS));
        }
        {
            SolrInputDocument doc = docs.stream()
                    .filter(d -> d.getFieldValue(SolrConstants.MD_ANNOTATION_ID).equals("PPN517154005_3"))
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("No annotation with id 'PPN517154005_3'"));
            Assert.assertEquals("PPN517154005", doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            Assert.assertEquals("topstruct", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
            Assert.assertEquals(2, doc.getFieldValue(SolrConstants.ORDER));
            Assert.assertEquals(124, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            Assert.assertEquals("Leipzig", doc.getFieldValue("MD_TEXT"));
            Assert.assertEquals("xywh=1378,3795,486,113", doc.getFieldValue(SolrConstants.UGCCOORDS));
            Assert.assertNotNull(doc.getFieldValue("MD_BODY"));
            Assert.assertEquals(SolrConstants._UGC_TYPE_ADDRESS, doc.getFieldValue(SolrConstants.UGCTYPE));
            Assert.assertEquals(SolrConstants._UGC_TYPE_ADDRESS + " Leipzig", doc.getFieldValue(SolrConstants.UGCTERMS));
        }
        {
            SolrInputDocument doc = docs.stream()
                    .filter(d -> d.getFieldValue(SolrConstants.MD_ANNOTATION_ID).equals("normdata"))
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("No annotation with id 'normdata'"));
            Assert.assertEquals("PPN517154005", doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            Assert.assertEquals("topstruct", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
            Assert.assertEquals(10, doc.getFieldValue(SolrConstants.ORDER));
            Assert.assertEquals(133, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            Assert.assertNotNull(doc.getFieldValue("MD_BODY"));
            Assert.assertEquals("Spaß in AC02949962", doc.getFieldValue(SolrConstants.ACCESSCONDITION));
            Assert.assertEquals(SolrConstants._UGC_TYPE_ADDRESS, doc.getFieldValue(SolrConstants.UGCTYPE));
        }
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies add docs correctly
     */
    @Test
    public void addGroupedMetadataDocs_shouldAddDocsCorrectly() throws Exception {
        LazySolrWriteStrategy strategy = (LazySolrWriteStrategy) AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Indexer indexer = new MetsIndexer(hotfolder);
        IndexObject indexObj = new IndexObject(1);
        GroupedMetadata gmd = new GroupedMetadata();
        gmd.setLabel("MD_GROUP");
        gmd.setMainValue("a");
        gmd.getFields().add(new LuceneField("MD_ONE", "foo"));
        gmd.getFields().add(new LuceneField("MD_TWO", "bar"));
        indexObj.getGroupedMetadataFields().add(gmd);

        indexer.addGroupedMetadataDocs(strategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());
        Assert.assertEquals(1, strategy.getDocsToAdd().size());
        Assert.assertEquals("foo", strategy.getDocsToAdd().get(0).getFieldValue("MD_ONE"));
        Assert.assertEquals("bar", strategy.getDocsToAdd().get(0).getFieldValue("MD_TWO"));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies set PI_TOPSTRUCT to child docstruct metadata
     */
    @Test
    public void addGroupedMetadataDocs_shouldSetPI_TOPSTRUCTToChildDocstructMetadata() throws Exception {
        LazySolrWriteStrategy strategy = (LazySolrWriteStrategy) AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Indexer indexer = new MetsIndexer(hotfolder);
        IndexObject indexObj = new IndexObject(1);
        indexObj.setTopstructPI("PPN123");
        GroupedMetadata gmd = new GroupedMetadata();
        gmd.setLabel("MD_GROUP");
        gmd.setMainValue("a");
        indexObj.getGroupedMetadataFields().add(gmd);

        indexer.addGroupedMetadataDocs(strategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());
        Assert.assertEquals(1, strategy.getDocsToAdd().size());
        Assert.assertEquals("PPN123", strategy.getDocsToAdd().get(0).getFieldValue(SolrConstants.PI_TOPSTRUCT));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies set DOCSTRCT_TOP
     */
    @Test
    public void addGroupedMetadataDocs_shouldSetDOCSTRCT_TOP() throws Exception {
        LazySolrWriteStrategy strategy = (LazySolrWriteStrategy) AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Indexer indexer = new MetsIndexer(hotfolder);
        IndexObject indexObj = new IndexObject(1);
        indexObj.addToLucene(SolrConstants.DOCSTRCT_TOP, "monograph");
        GroupedMetadata gmd = new GroupedMetadata();
        gmd.setLabel("MD_GROUP");
        gmd.setMainValue("a");
        indexObj.getGroupedMetadataFields().add(gmd);

        indexer.addGroupedMetadataDocs(strategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());
        Assert.assertEquals(1, strategy.getDocsToAdd().size());
        Assert.assertEquals("monograph", strategy.getDocsToAdd().get(0).getFieldValue(SolrConstants.DOCSTRCT_TOP));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies skip fields correctly
     */
    @Test
    public void addGroupedMetadataDocs_shouldSkipFieldsCorrectly() throws Exception {
        LazySolrWriteStrategy strategy = (LazySolrWriteStrategy) AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Indexer indexer = new MetsIndexer(hotfolder);
        IndexObject indexObj = new IndexObject(1);
        {
            GroupedMetadata gmd = new GroupedMetadata();
            gmd.setLabel("MD_GROUP");
            gmd.setMainValue("a");
            gmd.getFields().add(new LuceneField("MD_ONE", "foo"));
            gmd.setSkip(true); // skip this group
            indexObj.getGroupedMetadataFields().add(gmd);
        }
        {
            GroupedMetadata gmd = new GroupedMetadata();
            gmd.setLabel("MD_GROUP2");
            gmd.setMainValue("a");
            gmd.getFields().add(new LuceneField("MD_TWO", "bar"));
            indexObj.getGroupedMetadataFields().add(gmd);
        }

        indexer.addGroupedMetadataDocs(strategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());
        Assert.assertEquals(1, strategy.getDocsToAdd().size());
        Assert.assertNull(strategy.getDocsToAdd().get(0).getFieldValue("MD_ONE"));
        Assert.assertEquals("bar", strategy.getDocsToAdd().get(0).getFieldValue("MD_TWO"));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies add authority metadata to group metadata docs correctly
     */
    @Test
    public void addGroupedMetadataDocs_shouldAddAuthorityMetadataToGroupMetadataDocsCorrectly() throws Exception {
        LazySolrWriteStrategy strategy = (LazySolrWriteStrategy) AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Indexer indexer = new MetsIndexer(hotfolder);
        IndexObject indexObj = new IndexObject(1);
        GroupedMetadata gmd = new GroupedMetadata();
        gmd.setLabel("MD_GROUP");
        gmd.setMainValue("a");
        gmd.getAuthorityDataFields().add(new LuceneField("MD_ONE", "foo"));
        gmd.getAuthorityDataFields().add(new LuceneField("MD_TWO", "bar"));
        gmd.getAuthorityDataFields().add(new LuceneField("BOOL_WHAT", "true"));
        gmd.getAuthorityDataFields().add(new LuceneField(MetadataHelper.FIELD_WKT_COORDS, "1,2,3,1"));
        gmd.getAuthorityDataFields().add(new LuceneField(MetadataHelper.FIELD_HAS_WKT_COORDS, "true"));
        indexObj.getGroupedMetadataFields().add(gmd);

        indexer.addGroupedMetadataDocs(strategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

        Assert.assertFalse(gmd.isAddCoordsToDocstruct());

        // Values are not added to metadata docs
        Assert.assertEquals(1, strategy.getDocsToAdd().size());
        Assert.assertNotNull(strategy.getDocsToAdd().get(0).getFieldValue("MD_ONE"));
        Assert.assertEquals("foo", strategy.getDocsToAdd().get(0).getFieldValue("MD_ONE"));
        Assert.assertNotNull(strategy.getDocsToAdd().get(0).getFieldValue("MD_TWO"));
        Assert.assertEquals("bar", strategy.getDocsToAdd().get(0).getFieldValue("MD_TWO"));

    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies add authority metadata to docstruct doc correctly except coordinates
     */
    @Test
    public void addGroupedMetadataDocs_shouldAddAuthorityMetadataToDocstructDocCorrectlyExceptCoordinates() throws Exception {
        LazySolrWriteStrategy strategy = (LazySolrWriteStrategy) AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Indexer indexer = new MetsIndexer(hotfolder);
        IndexObject indexObj = new IndexObject(1);
        GroupedMetadata gmd = new GroupedMetadata();
        gmd.setLabel("MD_GROUP");
        gmd.setMainValue("a");
        gmd.setAddAuthorityDataToDocstruct(true);
        gmd.getAuthorityDataFields().add(new LuceneField("MD_ONE", "foo"));
        gmd.getAuthorityDataFields().add(new LuceneField("MD_TWO", "bar"));
        gmd.getAuthorityDataFields().add(new LuceneField("BOOL_WHAT", "true"));
        gmd.getAuthorityDataFields().add(new LuceneField(MetadataHelper.FIELD_WKT_COORDS, "1,2,3,1"));
        gmd.getAuthorityDataFields().add(new LuceneField(MetadataHelper.FIELD_HAS_WKT_COORDS, "true"));
        indexObj.getGroupedMetadataFields().add(gmd);

        indexer.addGroupedMetadataDocs(strategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

        Assert.assertFalse(gmd.isAddCoordsToDocstruct());

        // Values are not added to metadata docs
        Assert.assertEquals(1, strategy.getDocsToAdd().size());
        Assert.assertNull(strategy.getDocsToAdd().get(0).getFieldValue("MD_ONE"));
        Assert.assertNull(strategy.getDocsToAdd().get(0).getFieldValue("MD_TWO"));

        // Coordinate fields are still on metadata docs
        Assert.assertNotNull(strategy.getDocsToAdd().get(0).getFieldValue(MetadataHelper.FIELD_WKT_COORDS));
        Assert.assertNotNull(strategy.getDocsToAdd().get(0).getFieldValue(MetadataHelper.FIELD_HAS_WKT_COORDS));

        // Regular authority metadata are rerouted to IndexObject
        Assert.assertNotNull(indexObj.getLuceneFieldWithName("MD_ONE"));
        Assert.assertEquals("foo", indexObj.getLuceneFieldWithName("MD_ONE").getValue());
        Assert.assertNotNull(indexObj.getLuceneFieldWithName("MD_TWO"));
        Assert.assertEquals("bar", indexObj.getLuceneFieldWithName("MD_TWO").getValue());
        Assert.assertNotNull(indexObj.getLuceneFieldWithName("BOOL_WHAT"));
        Assert.assertEquals("true", indexObj.getLuceneFieldWithName("BOOL_WHAT").getValue());

        // Except for coordinate fields
        Assert.assertNull(indexObj.getLuceneFieldWithName(MetadataHelper.FIELD_WKT_COORDS));
        Assert.assertNull(indexObj.getLuceneFieldWithName(MetadataHelper.FIELD_HAS_WKT_COORDS));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies add coordinates to docstruct doc correctly
     */
    @Test
    public void addGroupedMetadataDocs_shouldAddCoordinatesToDocstructDocCorrectly() throws Exception {
        LazySolrWriteStrategy strategy = (LazySolrWriteStrategy) AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Indexer indexer = new MetsIndexer(hotfolder);
        IndexObject indexObj = new IndexObject(1);
        GroupedMetadata gmd = new GroupedMetadata();
        gmd.setLabel("MD_GROUP");
        gmd.setMainValue("a");
        gmd.setAddCoordsToDocstruct(true);
        gmd.getAuthorityDataFields().add(new LuceneField(MetadataHelper.FIELD_WKT_COORDS, "1,2,3,1"));
        gmd.getAuthorityDataFields().add(new LuceneField(MetadataHelper.FIELD_HAS_WKT_COORDS, "true"));
        indexObj.getGroupedMetadataFields().add(gmd);

        indexer.addGroupedMetadataDocs(strategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

        // Coordinate fields are no longer on metadata docs
        Assert.assertNull(strategy.getDocsToAdd().get(0).getFieldValue(MetadataHelper.FIELD_WKT_COORDS));
        Assert.assertNull(strategy.getDocsToAdd().get(0).getFieldValue(MetadataHelper.FIELD_HAS_WKT_COORDS));

        // Coordinate fields are rerouted to IndexObject
        Assert.assertNotNull(indexObj.getLuceneFieldWithName(MetadataHelper.FIELD_WKT_COORDS));
        Assert.assertEquals("1,2,3,1", indexObj.getLuceneFieldWithName(MetadataHelper.FIELD_WKT_COORDS).getValue());
        Assert.assertNotNull(indexObj.getLuceneFieldWithName(MetadataHelper.FIELD_HAS_WKT_COORDS));
        Assert.assertEquals("true", indexObj.getLuceneFieldWithName(MetadataHelper.FIELD_HAS_WKT_COORDS).getValue());
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject,List)
     * @verifies recursively add child metadata
     */
    @Test
    public void addGroupedMetadataDocs_shouldRecursivelyAddChildMetadata() throws Exception {
        LazySolrWriteStrategy strategy = (LazySolrWriteStrategy) AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Indexer indexer = new MetsIndexer(hotfolder);
        IndexObject indexObj = new IndexObject(1);
        GroupedMetadata gmd = new GroupedMetadata();
        gmd.setLabel("MD_GROUP");
        gmd.setMainValue("a");
        gmd.getFields().add(new LuceneField("MD_ONE", "foo"));
        gmd.getFields().add(new LuceneField("MD_TWO", "bar"));
        indexObj.getGroupedMetadataFields().add(gmd);

        GroupedMetadata gmdChild = new GroupedMetadata();
        gmdChild.setLabel("MD_GROUP");
        gmdChild.setMainValue("a");
        gmdChild.getFields().add(new LuceneField("MD_ONE", "foo"));
        gmdChild.getFields().add(new LuceneField("MD_TWO", "bar"));
        gmd.getChildren().add(gmdChild);

        GroupedMetadata gmdGrandchild = new GroupedMetadata();
        gmdGrandchild.setLabel("MD_GROUP");
        gmdGrandchild.setMainValue("a");
        gmdGrandchild.getFields().add(new LuceneField("MD_ONE", "foo"));
        gmdGrandchild.getFields().add(new LuceneField("MD_TWO", "bar"));
        gmdChild.getChildren().add(gmdGrandchild);

        indexer.addGroupedMetadataDocs(strategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());
        Assert.assertEquals(3, strategy.getDocsToAdd().size());

        Long iddocParent = (Long) strategy.getDocsToAdd().get(0).getFieldValue(SolrConstants.IDDOC);
        Assert.assertNotNull(iddocParent);
        Assert.assertEquals("foo", strategy.getDocsToAdd().get(0).getFieldValue("MD_ONE"));
        Assert.assertEquals("bar", strategy.getDocsToAdd().get(0).getFieldValue("MD_TWO"));

        Long iddocChild = (Long) strategy.getDocsToAdd().get(1).getFieldValue(SolrConstants.IDDOC);
        Assert.assertNotNull(iddocChild);
        Assert.assertEquals(iddocParent, strategy.getDocsToAdd().get(1).getFieldValue(SolrConstants.IDDOC_OWNER));
        Assert.assertEquals("foo", strategy.getDocsToAdd().get(1).getFieldValue("MD_ONE"));
        Assert.assertEquals("bar", strategy.getDocsToAdd().get(1).getFieldValue("MD_TWO"));

        Long iddocGrandchild = (Long) strategy.getDocsToAdd().get(2).getFieldValue(SolrConstants.IDDOC);
        Assert.assertNotNull(iddocGrandchild);
        Assert.assertEquals(iddocChild, strategy.getDocsToAdd().get(2).getFieldValue(SolrConstants.IDDOC_OWNER));
        Assert.assertEquals("foo", strategy.getDocsToAdd().get(2).getFieldValue("MD_ONE"));
        Assert.assertEquals("bar", strategy.getDocsToAdd().get(2).getFieldValue("MD_TWO"));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add filename for native alto file
     */
    @Test
    public void addIndexFieldsFromAltoData_shouldAddFilenameForNativeAltoFile() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        indexer.setDataRepository(new DataRepository("src/test/resources", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ALTO, Paths.get("src/test/resources/ALTO/"));
        Assert.assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTO)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File altoFile = new File(dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), "00000010.xml");
        Map<String, Object> altoData = TextHelper.readAltoFile(altoFile);

        Assert.assertTrue(indexer.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
        Assert.assertEquals("alto/PPN123/00000010.xml", doc.getFieldValue(SolrConstants.FILENAME_ALTO));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add filename for crowdsourcing alto file
     */
    @Test
    public void addIndexFieldsFromAltoData_shouldAddFilenameForCrowdsourcingAltoFile() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        indexer.setDataRepository(new DataRepository("src/test/resources", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ALTOCROWD, Paths.get("src/test/resources/ALTO/"));
        Assert.assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTOCROWD)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File altoFile = new File(dataFolders.get(DataRepository.PARAM_ALTOCROWD).toAbsolutePath().toString(), "00000010.xml");
        Map<String, Object> altoData = TextHelper.readAltoFile(altoFile);

        Assert.assertTrue(
                indexer.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTOCROWD, "PPN123", "00000010", 10, false));
        Assert.assertEquals("alto_crowd/PPN123/00000010.xml", doc.getFieldValue(SolrConstants.FILENAME_ALTO));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add filename for converted alto file
     */
    @Test
    public void addIndexFieldsFromAltoData_shouldAddFilenameForConvertedAltoFile() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        indexer.setDataRepository(new DataRepository("build/viewer", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ABBYY, Paths.get("src/test/resources/ABBYYXML"));
        Assert.assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ABBYY)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File abbyyfile = new File(dataFolders.get(DataRepository.PARAM_ABBYY).toAbsolutePath().toString(), "00000001.xml");
        Map<String, Object> altoData = TextHelper.readAbbyyToAlto(abbyyfile);

        Assert.assertTrue(
                indexer.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO_CONVERTED, "PPN123", "00000001", 1, true));
        Assert.assertEquals("alto/PPN123/00000001.xml", doc.getFieldValue(SolrConstants.FILENAME_ALTO));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add fulltext
     */
    @Test
    public void addIndexFieldsFromAltoData_shouldAddFulltext() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        indexer.setDataRepository(new DataRepository("src/test/resources", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ALTO, Paths.get("src/test/resources/ALTO/"));
        Assert.assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTO)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File altoFile = new File(dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), "00000010.xml");
        Map<String, Object> altoData = TextHelper.readAltoFile(altoFile);

        Assert.assertTrue(indexer.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
        Assert.assertNotNull(doc.getFieldValue(SolrConstants.FULLTEXT));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add width and height
     */
    @Test
    public void addIndexFieldsFromAltoData_shouldAddWidthAndHeight() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        indexer.setDataRepository(new DataRepository("src/test/resources", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ALTO, Paths.get("src/test/resources/ALTO/"));
        Assert.assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTO)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File altoFile = new File(dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), "00000010.xml");
        Map<String, Object> altoData = TextHelper.readAltoFile(altoFile);

        Assert.assertTrue(indexer.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
        Assert.assertEquals("2480", doc.getFieldValue(SolrConstants.WIDTH));
        Assert.assertEquals("3508", doc.getFieldValue(SolrConstants.HEIGHT));
    }

    /**
     * @see Indexer#addNamedEntitiesFields(Map,SolrInputDocument)
     * @verifies add field
     */
    @Test
    public void addNamedEntitiesFields_shouldAddField() throws Exception {
        Map<String, Object> altoData = new HashMap<>(1);
        altoData.put(SolrConstants.NAMEDENTITIES, Collections.singletonList("LOCATION_Göttingen"));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());

        Indexer.addNamedEntitiesFields(altoData, doc);
        Assert.assertEquals("Göttingen", doc.getFieldValue("NE_LOCATION"));
    }

    /**
     * @see Indexer#addNamedEntitiesFields(Map,SolrInputDocument)
     * @verifies add untokenized field
     */
    @Test
    public void addNamedEntitiesFields_shouldAddUntokenizedField() throws Exception {
        Map<String, Object> altoData = new HashMap<>(1);
        altoData.put(SolrConstants.NAMEDENTITIES, Collections.singletonList("LOCATION_Göttingen"));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());

        Indexer.addNamedEntitiesFields(altoData, doc);
        Assert.assertEquals("Göttingen", doc.getFieldValue("NE_LOCATION_UNTOKENIZED"));
    }

    /**
     * @see Indexer#generateUserCommentDocsForPage(SolrInputDocument,Path,String,String,Map,int,String)
     * @verifies construct doc correctly
     */
    @Test
    public void generateUserCommentDocsForPage_shouldConstructDocCorrectly() throws Exception {
        Path dataFolder = Paths.get("src/test/resources/ugc");
        Assert.assertTrue(Files.isDirectory(dataFolder));

        DocUpdateIndexer indexer = new DocUpdateIndexer(hotfolder);
        
        String docstrct = "monograph";
        SolrInputDocument ownerDoc = new SolrInputDocument();
        ownerDoc.setField(SolrConstants.IDDOC_OWNER, 123L);
        ownerDoc.setField(SolrConstants.DOCSTRCT_TOP, docstrct);
        List<SolrInputDocument> docs =
                indexer.generateUserCommentDocsForPage(ownerDoc, dataFolder, "PPN123", null, null, 1);
        Assert.assertNotNull(docs);
        Assert.assertEquals(2, docs.size());

        {
            SolrInputDocument doc = docs.get(0);
            Assert.assertEquals(1, doc.getFieldValue(SolrConstants.ORDER));
            Assert.assertEquals("a comment", doc.getFieldValue("MD_TEXT"));
            Assert.assertEquals("COMMENT  a comment", doc.getFieldValue(SolrConstants.UGCTERMS));
            Assert.assertEquals("http://localhost:8080/viewer/api/v1/annotations/comment_13/", doc.getFieldValue("MD_ANNOTATION_ID"));
            Assert.assertEquals(123L, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            Assert.assertEquals(docstrct, doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
        }
        {
            SolrInputDocument doc = docs.get(1);
            Assert.assertEquals(1, doc.getFieldValue(SolrConstants.ORDER));
            Assert.assertEquals("another comment", doc.getFieldValue("MD_TEXT"));
            Assert.assertEquals("COMMENT  another comment", doc.getFieldValue(SolrConstants.UGCTERMS));
            Assert.assertEquals("http://localhost:8080/viewer/api/v1/annotations/comment_14/", doc.getFieldValue("MD_ANNOTATION_ID"));
            Assert.assertEquals(123L, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            Assert.assertEquals(docstrct, doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
        }
    }
}
