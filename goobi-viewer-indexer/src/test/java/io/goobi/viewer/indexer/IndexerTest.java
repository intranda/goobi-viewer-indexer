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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.Element;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.model.GroupedMetadata;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.PhysicalElement;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.LazySolrWriteStrategy;

class IndexerTest extends AbstractSolrEnabledTest {

    protected static final String PI_KLEIUNIV = "PPN517154005";

    private Path metsFile;
    private Path lidoFile;
    private static String libraryPath = "";

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());

        metsFile = Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        Assertions.assertTrue(Files.isRegularFile(metsFile));
        lidoFile = Paths.get("src/test/resources/LIDO/khm_lido_export.xml");
        Assertions.assertTrue(Files.isRegularFile(lidoFile));
    }

    @BeforeAll
    public static void setUpBeforeClass() {
        File libraryFile = new File("src/test/resources/lib/libopenjp2.so");
        libraryPath = System.getProperty("java.library.path");
        System.setProperty("java.library.path", libraryPath + ":" + libraryFile.getParentFile().getAbsolutePath());
    }

    @AfterAll
    public static void cleanUpAfterClass() {
        if (StringUtils.isNotBlank(libraryPath)) {
            System.setProperty("java.library.path", libraryPath);
        }
    }

    /**
     * @see Indexer#handleError(Path,String,FileFormat)
     * @verifies write log file and copy of mets file into errorMets
     */
    @Test
    void handleError_shouldWriteLogFileAndCopyOfMetsFileIntoErrorMets() {
        Indexer indexer = new MetsIndexer(hotfolder);
        indexer.handleError(metsFile, "lorem ipsum dolor sit amet", FileFormat.METS);
        Assertions.assertTrue(Files.isRegularFile(
                Paths.get(hotfolder.getErrorMets().toString(), FilenameUtils.getBaseName(metsFile.getFileName().toString()) + ".log")));
        Assertions.assertTrue(Files.isRegularFile(Paths.get(hotfolder.getErrorMets().toString(), metsFile.getFileName().toString())));
    }

    /**
     * @see Indexer#delete(String,boolean,SolrSearchIndex)
     * @verifies throw IllegalArgumentException if pi empty
     */
    @Test
    void delete_shouldThrowIllegalArgumentExceptionIfPiEmpty() {
        SolrSearchIndex searchIndex = SolrIndexerDaemon.getInstance().getSearchIndex();
        Assertions.assertThrows(IllegalArgumentException.class, () -> Indexer.delete("", false, searchIndex));
    }

    /**
     * @see Indexer#delete(String,boolean,SolrSearchIndex)
     * @verifies throw IllegalArgumentException if searchIndex null
     */
    @Test
    void delete_shouldThrowIllegalArgumentExceptionIfSearchIndexNull() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> Indexer.delete("foo", false, null));
    }

    /**
     * @see Indexer#delete(String,boolean,SolrSearchIndex)
     * @verifies return false if pi not found
     */
    @Test
    void delete_shouldReturnFalseIfPiNotFound() throws Exception {
        Assertions.assertFalse(Indexer.delete("foo", false, SolrIndexerDaemon.getInstance().getSearchIndex()));
    }

    /**
     * @see Indexer#delete(String,boolean)
     * @verifies delete METS record from index completely
     */
    @Test
    void delete_shouldDeleteMETSRecordFromIndexCompletely() throws Exception {
        String pi = PI_KLEIUNIV;
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_MEDIA, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_tif"));
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, dataFolders, null, 1, false);
        Assertions.assertEquals(pi + ".xml", ret[0]);
        Assertions.assertNull(ret[1]);
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":" + pi, null);
            Assertions.assertEquals(22, docList.size());
        }
        Assertions.assertTrue(Indexer.delete(pi, false, SolrIndexerDaemon.getInstance().getSearchIndex()));
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":" + pi, null);
            Assertions.assertTrue(docList.isEmpty());
        }
    }

    /**
     * @see Indexer#delete(String,boolean)
     * @verifies delete LIDO record from index completely
     */
    @Test
    void delete_shouldDeleteLIDORecordFromIndexCompletely() throws Exception {
        String pi = "V0011127";
        Map<String, Path> dataFolders = new HashMap<>();
        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile.toFile());
        String[] ret = new LidoIndexer(hotfolder).index(lidoDocs.get(0), dataFolders, null, 1,
                SolrIndexerDaemon.getInstance().getConfiguration().getStringList("init.lido.imageXPath"), false, false);
        Assertions.assertEquals(pi, ret[0], "ERROR: " + ret[1]);
        String iddoc;
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + pi, null);
            Assertions.assertEquals(1, docList.size());
            iddoc = (String) docList.get(0).getFieldValue(SolrConstants.IDDOC);
            Assertions.assertNotNull(iddoc);
        }
        {
            SolrDocumentList docList =
                    SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search(SolrConstants.PI_TOPSTRUCT + ":" + pi + " OR " + SolrConstants.IDDOC_OWNER + ":" + iddoc, null);
            Assertions.assertEquals(3, docList.size());
        }
        Assertions.assertTrue(Indexer.delete(pi, false, SolrIndexerDaemon.getInstance().getSearchIndex()));
        {
            SolrDocumentList docList =
                    SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search(SolrConstants.PI_TOPSTRUCT + ":" + pi + " OR " + SolrConstants.IDDOC_OWNER + ":" + iddoc, null);
            Assertions.assertTrue(docList.isEmpty());
        }
    }

    /**
     * @see Indexer#delete(String,boolean)
     * @verifies leave trace document for METS record if requested
     */
    @Test
    void delete_shouldLeaveTraceDocumentForMETSRecordIfRequested() throws Exception {
        String pi = PI_KLEIUNIV;
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_MEDIA, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_tif"));
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, dataFolders, null, 1, false);
        Assertions.assertEquals(pi + ".xml", ret[0]);
        Assertions.assertNull(ret[1]);
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":" + pi, null);
            Assertions.assertEquals(22, docList.size());
        }
        Assertions.assertTrue(Indexer.delete(pi, true, SolrIndexerDaemon.getInstance().getSearchIndex()));
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + pi, null);
            Assertions.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assertions.assertNotNull(doc.getFieldValue(SolrConstants.DATEDELETED));
            Assertions.assertNotNull(doc.getFieldValue(SolrConstants.GROUPFIELD));
            Assertions.assertNotNull(doc.getFieldValues(SolrConstants.IMAGEURN_OAI));
            Assertions.assertEquals(16, doc.getFieldValues(SolrConstants.IMAGEURN_OAI).size());
        }
    }

    /**
     * @see Indexer#delete(String,boolean)
     * @verifies leave trace document for LIDO record if requested
     */
    @Test
    void delete_shouldLeaveTraceDocumentForLIDORecordIfRequested() throws Exception {
        String pi = "V0011127";
        Map<String, Path> dataFolders = new HashMap<>();
        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile.toFile());
        String[] ret = new LidoIndexer(hotfolder).index(lidoDocs.get(0), dataFolders, null, 1,
                SolrIndexerDaemon.getInstance().getConfiguration().getStringList("init.lido.imageXPath"), false, false);
        Assertions.assertEquals(pi, ret[0]);
        String iddoc;
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + pi, null);
            Assertions.assertEquals(1, docList.size());
            iddoc = (String) docList.get(0).getFieldValue(SolrConstants.IDDOC);
            Assertions.assertNotNull(iddoc);
        }
        {
            SolrDocumentList docList =
                    SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search(SolrConstants.PI_TOPSTRUCT + ":" + pi + " OR " + SolrConstants.IDDOC_OWNER + ":" + iddoc, null);
            Assertions.assertEquals(3, docList.size());
        }
        Assertions.assertTrue(Indexer.delete(pi, true, SolrIndexerDaemon.getInstance().getSearchIndex()));
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + pi, null);
            Assertions.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assertions.assertNotNull(doc.getFieldValue(SolrConstants.DATEDELETED));
            Assertions.assertNotNull(doc.getFieldValue(SolrConstants.GROUPFIELD));
        }
    }

    /**
     * @see Indexer#cleanUpDefaultField(String)
     * @verifies replace irrelevant chars with spaces correctly
     */
    @Test
    void cleanUpDefaultField_shouldReplaceIrrelevantCharsWithSpacesCorrectly() {
        Assertions.assertEquals("A B C D", Indexer.cleanUpDefaultField(" A,B;C:D,  "));
    }

    /**
     * @see Indexer#cleanUpDefaultField(String)
     * @verifies return null if field null
     */
    @Test
    void cleanUpDefaultField_shouldReturnNullIfFieldNull() {
        Assertions.assertNull(Indexer.cleanUpDefaultField(null));
    }

    /**
     * @see Indexer#cleanUpNamedEntityValue(String)
     * @verifies clean up value correctly
     */
    @Test
    void cleanUpNamedEntityValue_shouldCleanUpValueCorrectly() {
        Assertions.assertEquals("abcd", Indexer.cleanUpNamedEntityValue("\"(abcd,\""));
    }

    /**
     * @see Indexer#cleanUpNamedEntityValue(String)
     * @verifies throw IllegalArgumentException given null
     */
    @Test
    void cleanUpNamedEntityValue_shouldThrowIllegalArgumentExceptionGivenNull() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> Indexer.cleanUpNamedEntityValue(null));
    }

    /**
     * @see Indexer#getSize(Map,SolrInputDocument)
     * @verifies return size correctly
     */
    @Test
    void getSize_shouldReturnSizeCorrectly() throws Exception {

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
                Assertions.assertTrue(dim.isPresent());
                Assertions.assertEquals(imageSizes[i], dim.get(), "Image size of " + filename + " is " + dim + ", but should be " + imageSizes[i]);
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
     * @verifies return empty list if dataFolder null
     */
    @Test
    void generateAnnotationDocs_shouldReturnEmptyListIfDataFolderNull() throws Exception {
        List<SolrInputDocument> docs = new MetsIndexer(hotfolder).generateAnnotationDocs(Collections.emptyMap(), null, PI_KLEIUNIV, null, null);
        Assertions.assertTrue(docs.isEmpty());
    }

    /**
     * @see Indexer#generateAnnotationDocs(Map,Path,String,String,Map)
     * @verifies create docs correctly
     */
    @Test
    void generateAnnotationDocs_shouldCreateDocsCorrectly() throws Exception {
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
        Assertions.assertTrue(Files.isDirectory(dataFolder));

        List<SolrInputDocument> docs = new MetsIndexer(hotfolder).generateAnnotationDocs(pageDocs, dataFolder, PI_KLEIUNIV, null, null);
        Assertions.assertEquals(3, docs.size());
        {
            SolrInputDocument doc = docs.stream()
                    .filter(d -> d.getFieldValue(SolrConstants.MD_ANNOTATION_ID).equals("geo"))
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("No annotation with id 'geo'"));
            Assertions.assertEquals(PI_KLEIUNIV, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            Assertions.assertEquals("topstruct", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
            Assertions.assertNull(doc.getFieldValue(SolrConstants.ORDER));
            Assertions.assertNull(doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            Assertions.assertEquals("9.967025 51.521737", doc.getFieldValue("MD_COORDS"));
            Assertions.assertEquals(SolrConstants.UGC_TYPE_ADDRESS, doc.getFieldValue(SolrConstants.UGCTYPE));
            Assertions.assertNotNull(doc.getFieldValue("MD_BODY"));
        }
        {
            SolrInputDocument doc = docs.stream()
                    .filter(d -> d.getFieldValue(SolrConstants.MD_ANNOTATION_ID).equals("PPN517154005_3"))
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("No annotation with id 'PPN517154005_3'"));
            Assertions.assertEquals(PI_KLEIUNIV, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            Assertions.assertEquals("topstruct", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
            Assertions.assertEquals(2, doc.getFieldValue(SolrConstants.ORDER));
            Assertions.assertEquals(124, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            Assertions.assertEquals("Leipzig", doc.getFieldValue("MD_TEXT"));
            Assertions.assertEquals("xywh=1378,3795,486,113", doc.getFieldValue(SolrConstants.UGCCOORDS));
            Assertions.assertNotNull(doc.getFieldValue("MD_BODY"));
            Assertions.assertEquals(SolrConstants.UGC_TYPE_ADDRESS, doc.getFieldValue(SolrConstants.UGCTYPE));
            Assertions.assertEquals(SolrConstants.UGC_TYPE_ADDRESS + " Leipzig", doc.getFieldValue(SolrConstants.UGCTERMS));
        }
        {
            SolrInputDocument doc = docs.stream()
                    .filter(d -> d.getFieldValue(SolrConstants.MD_ANNOTATION_ID).equals("normdata"))
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("No annotation with id 'normdata'"));
            Assertions.assertEquals(PI_KLEIUNIV, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            Assertions.assertEquals("topstruct", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
            Assertions.assertEquals(10, doc.getFieldValue(SolrConstants.ORDER));
            Assertions.assertEquals(133, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            Assertions.assertNotNull(doc.getFieldValue("MD_BODY"));
            Assertions.assertEquals("Spaß in AC02949962", doc.getFieldValue(SolrConstants.ACCESSCONDITION));
            Assertions.assertEquals(SolrConstants.UGC_TYPE_ADDRESS, doc.getFieldValue(SolrConstants.UGCTYPE));
        }
    }

    /**
     * @see Indexer#addGroupedMetadataDocsForPage(PhysicalElement,String,ISolrWriteStrategy)
     * @verifies add grouped metadata docs from given page to writeStrategy correctly
     */
    @Test
    void addGroupedMetadataDocsForPage_shouldAddGroupedMetadataDocsFromGivenPageToWriteStrategyCorrectly() throws Exception {
        LazySolrWriteStrategy strategy = (LazySolrWriteStrategy) AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Indexer indexer = new MetsIndexer(hotfolder);
        PhysicalElement pe = Indexer.createPhysicalElement(2, "222", "PHYS_0000");
        pe.getDoc().addField(SolrConstants.DC, "varia");
        pe.getDoc().addField(SolrConstants.ACCESSCONDITION, "restricted");

        GroupedMetadata gmd = new GroupedMetadata();
        gmd.getFields().add(new LuceneField(SolrConstants.LABEL, "MD_TECH_FOO"));
        gmd.getFields().add(new LuceneField("MD_VALUE", "800x600"));
        gmd.getFields().add(new LuceneField("MD_FOO", "bar"));
        pe.getGroupedMetadata().add(gmd);

        Assertions.assertEquals(1, indexer.addGroupedMetadataDocsForPage(pe, PI_KLEIUNIV, strategy));
        Assertions.assertEquals(1, strategy.getDocsToAdd().size());
        SolrInputDocument doc = strategy.getDocsToAdd().get(0);
        Assertions.assertNotNull(doc);
        Assertions.assertNotNull(doc.getFieldValue(SolrConstants.IDDOC));
        Assertions.assertEquals("222", doc.getFieldValue(SolrConstants.IDDOC_OWNER));
        Assertions.assertEquals(PI_KLEIUNIV, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
        Assertions.assertEquals(DocType.METADATA.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
        Assertions.assertEquals("varia", SolrSearchIndex.getSingleFieldStringValue(doc, SolrConstants.DC));
        Assertions.assertEquals("restricted", SolrSearchIndex.getSingleFieldStringValue(doc, SolrConstants.ACCESSCONDITION));
        Assertions.assertEquals("MD_TECH_FOO", doc.getFieldValue(SolrConstants.LABEL));
        Assertions.assertEquals("800x600", doc.getFieldValue("MD_VALUE"));
        Assertions.assertEquals("bar", doc.getFieldValue("MD_FOO"));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies add docs correctly
     */
    @Test
    void addGroupedMetadataDocs_shouldAddDocsCorrectly() throws Exception {
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
        Assertions.assertEquals(1, strategy.getDocsToAdd().size());
        Assertions.assertEquals("foo", strategy.getDocsToAdd().get(0).getFieldValue("MD_ONE"));
        Assertions.assertEquals("bar", strategy.getDocsToAdd().get(0).getFieldValue("MD_TWO"));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies set PI_TOPSTRUCT to child docstruct metadata
     */
    @Test
    void addGroupedMetadataDocs_shouldSetPI_TOPSTRUCTToChildDocstructMetadata() throws Exception {
        LazySolrWriteStrategy strategy = (LazySolrWriteStrategy) AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Indexer indexer = new MetsIndexer(hotfolder);
        IndexObject indexObj = new IndexObject(1);
        indexObj.setTopstructPI("PPN123");
        GroupedMetadata gmd = new GroupedMetadata();
        gmd.setLabel("MD_GROUP");
        gmd.setMainValue("a");
        indexObj.getGroupedMetadataFields().add(gmd);

        indexer.addGroupedMetadataDocs(strategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());
        Assertions.assertEquals(1, strategy.getDocsToAdd().size());
        Assertions.assertEquals("PPN123", strategy.getDocsToAdd().get(0).getFieldValue(SolrConstants.PI_TOPSTRUCT));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies set DOCSTRCT_TOP
     */
    @Test
    void addGroupedMetadataDocs_shouldSetDOCSTRCT_TOP() throws Exception {
        LazySolrWriteStrategy strategy = (LazySolrWriteStrategy) AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Indexer indexer = new MetsIndexer(hotfolder);
        IndexObject indexObj = new IndexObject(1);
        indexObj.addToLucene(SolrConstants.DOCSTRCT_TOP, "monograph");
        GroupedMetadata gmd = new GroupedMetadata();
        gmd.setLabel("MD_GROUP");
        gmd.setMainValue("a");
        indexObj.getGroupedMetadataFields().add(gmd);

        indexer.addGroupedMetadataDocs(strategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());
        Assertions.assertEquals(1, strategy.getDocsToAdd().size());
        Assertions.assertEquals("monograph", strategy.getDocsToAdd().get(0).getFieldValue(SolrConstants.DOCSTRCT_TOP));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies skip fields correctly
     */
    @Test
    void addGroupedMetadataDocs_shouldSkipFieldsCorrectly() throws Exception {
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
        Assertions.assertEquals(1, strategy.getDocsToAdd().size());
        Assertions.assertNull(strategy.getDocsToAdd().get(0).getFieldValue("MD_ONE"));
        Assertions.assertEquals("bar", strategy.getDocsToAdd().get(0).getFieldValue("MD_TWO"));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies add authority metadata to group metadata docs correctly
     */
    @Test
    void addGroupedMetadataDocs_shouldAddAuthorityMetadataToGroupMetadataDocsCorrectly() throws Exception {
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

        Assertions.assertFalse(gmd.isAddCoordsToDocstruct());

        // Values are not added to metadata docs
        Assertions.assertEquals(1, strategy.getDocsToAdd().size());
        Assertions.assertNotNull(strategy.getDocsToAdd().get(0).getFieldValue("MD_ONE"));
        Assertions.assertEquals("foo", strategy.getDocsToAdd().get(0).getFieldValue("MD_ONE"));
        Assertions.assertNotNull(strategy.getDocsToAdd().get(0).getFieldValue("MD_TWO"));
        Assertions.assertEquals("bar", strategy.getDocsToAdd().get(0).getFieldValue("MD_TWO"));

    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies add authority metadata to docstruct doc correctly except coordinates
     */
    @Test
    void addGroupedMetadataDocs_shouldAddAuthorityMetadataToDocstructDocCorrectlyExceptCoordinates() throws Exception {
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

        Assertions.assertFalse(gmd.isAddCoordsToDocstruct());

        // Values are not added to metadata docs
        Assertions.assertEquals(1, strategy.getDocsToAdd().size());
        Assertions.assertNull(strategy.getDocsToAdd().get(0).getFieldValue("MD_ONE"));
        Assertions.assertNull(strategy.getDocsToAdd().get(0).getFieldValue("MD_TWO"));

        // Coordinate fields are still on metadata docs
        Assertions.assertNotNull(strategy.getDocsToAdd().get(0).getFieldValue(MetadataHelper.FIELD_WKT_COORDS));
        Assertions.assertNotNull(strategy.getDocsToAdd().get(0).getFieldValue(MetadataHelper.FIELD_HAS_WKT_COORDS));

        // Regular authority metadata are rerouted to IndexObject
        Assertions.assertNotNull(indexObj.getLuceneFieldWithName("MD_ONE"));
        Assertions.assertEquals("foo", indexObj.getLuceneFieldWithName("MD_ONE").getValue());
        Assertions.assertNotNull(indexObj.getLuceneFieldWithName("MD_TWO"));
        Assertions.assertEquals("bar", indexObj.getLuceneFieldWithName("MD_TWO").getValue());
        Assertions.assertNotNull(indexObj.getLuceneFieldWithName("BOOL_WHAT"));
        Assertions.assertEquals("true", indexObj.getLuceneFieldWithName("BOOL_WHAT").getValue());

        // Except for coordinate fields
        Assertions.assertNull(indexObj.getLuceneFieldWithName(MetadataHelper.FIELD_WKT_COORDS));
        Assertions.assertNull(indexObj.getLuceneFieldWithName(MetadataHelper.FIELD_HAS_WKT_COORDS));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject)
     * @verifies add coordinates to docstruct doc correctly
     */
    @Test
    void addGroupedMetadataDocs_shouldAddCoordinatesToDocstructDocCorrectly() throws Exception {
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
        Assertions.assertNull(strategy.getDocsToAdd().get(0).getFieldValue(MetadataHelper.FIELD_WKT_COORDS));
        Assertions.assertNull(strategy.getDocsToAdd().get(0).getFieldValue(MetadataHelper.FIELD_HAS_WKT_COORDS));

        // Coordinate fields are rerouted to IndexObject
        Assertions.assertNotNull(indexObj.getLuceneFieldWithName(MetadataHelper.FIELD_WKT_COORDS));
        Assertions.assertEquals("1,2,3,1", indexObj.getLuceneFieldWithName(MetadataHelper.FIELD_WKT_COORDS).getValue());
        Assertions.assertNotNull(indexObj.getLuceneFieldWithName(MetadataHelper.FIELD_HAS_WKT_COORDS));
        Assertions.assertEquals("true", indexObj.getLuceneFieldWithName(MetadataHelper.FIELD_HAS_WKT_COORDS).getValue());
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(ISolrWriteStrategy,IndexObject,List)
     * @verifies recursively add child metadata
     */
    @Test
    void addGroupedMetadataDocs_shouldRecursivelyAddChildMetadata() throws Exception {
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
        Assertions.assertEquals(3, strategy.getDocsToAdd().size());

        Long iddocParent = (Long) strategy.getDocsToAdd().get(0).getFieldValue(SolrConstants.IDDOC);
        Assertions.assertNotNull(iddocParent);
        Assertions.assertEquals("foo", strategy.getDocsToAdd().get(0).getFieldValue("MD_ONE"));
        Assertions.assertEquals("bar", strategy.getDocsToAdd().get(0).getFieldValue("MD_TWO"));

        Long iddocChild = (Long) strategy.getDocsToAdd().get(1).getFieldValue(SolrConstants.IDDOC);
        Assertions.assertNotNull(iddocChild);
        Assertions.assertEquals(iddocParent, strategy.getDocsToAdd().get(1).getFieldValue(SolrConstants.IDDOC_OWNER));
        Assertions.assertEquals("foo", strategy.getDocsToAdd().get(1).getFieldValue("MD_ONE"));
        Assertions.assertEquals("bar", strategy.getDocsToAdd().get(1).getFieldValue("MD_TWO"));

        Long iddocGrandchild = (Long) strategy.getDocsToAdd().get(2).getFieldValue(SolrConstants.IDDOC);
        Assertions.assertNotNull(iddocGrandchild);
        Assertions.assertEquals(iddocChild, strategy.getDocsToAdd().get(2).getFieldValue(SolrConstants.IDDOC_OWNER));
        Assertions.assertEquals("foo", strategy.getDocsToAdd().get(2).getFieldValue("MD_ONE"));
        Assertions.assertEquals("bar", strategy.getDocsToAdd().get(2).getFieldValue("MD_TWO"));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies return false if altodata null
     */
    @Test
    void addIndexFieldsFromAltoData_shouldReturnFalseIfAltodataNull() throws Exception {
        Assertions.assertFalse(
                new MetsIndexer(hotfolder).addIndexFieldsFromAltoData(new SolrInputDocument(new HashMap<>()), null, Collections.emptyMap(),
                        DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies throw IllegalArgumentException if doc null
     */
    @Test
    void addIndexFieldsFromAltoData_shouldThrowIllegalArgumentExceptionIfDocNull() {
        MetsIndexer metsIndeer = new MetsIndexer(hotfolder);
        Map<String, Object> altoData = Collections.emptyMap();
        Map<String, Path> dataFolders = Collections.emptyMap();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> metsIndeer
                        .addIndexFieldsFromAltoData(null, altoData, dataFolders, DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies throw IllegalArgumentException if dataFolders null
     */
    @Test
    void addIndexFieldsFromAltoData_shouldThrowIllegalArgumentExceptionIfDataFoldersNull() {
        MetsIndexer metsIndeer = new MetsIndexer(hotfolder);
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        Map<String, Object> altoData = Collections.emptyMap();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> metsIndeer.addIndexFieldsFromAltoData(doc, altoData, null, DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies throw IllegalArgumentException if pi null
     */
    @Test
    void addIndexFieldsFromAltoData_shouldThrowIllegalArgumentExceptionIfPiNull() {
        MetsIndexer metsIndeer = new MetsIndexer(hotfolder);
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        Map<String, Object> altoData = Collections.emptyMap();
        Map<String, Path> dataFolders = Collections.emptyMap();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> metsIndeer.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, null, "00000010", 10, false));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies throw IllegalArgumentException if baseFileName null
     */
    @Test
    void addIndexFieldsFromAltoData_shouldThrowIllegalArgumentExceptionIfBaseFileNameNull() {
        MetsIndexer metsIndeer = new MetsIndexer(hotfolder);
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        Map<String, Object> altoData = Collections.emptyMap();
        Map<String, Path> dataFolders = Collections.emptyMap();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> metsIndeer.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, "PPN123", null, 10, false));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add filename for native alto file
     */
    @Test
    void addIndexFieldsFromAltoData_shouldAddFilenameForNativeAltoFile() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        indexer.setDataRepository(new DataRepository("src/test/resources", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ALTO, Paths.get("src/test/resources/ALTO/"));
        Assertions.assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTO)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File altoFile = new File(dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), "00000010.xml");
        Map<String, Object> altoData = TextHelper.readAltoFile(altoFile);

        Assertions.assertTrue(
                indexer.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
        Assertions.assertEquals("alto/PPN123/00000010.xml", doc.getFieldValue(SolrConstants.FILENAME_ALTO));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add filename for crowdsourcing alto file
     */
    @Test
    void addIndexFieldsFromAltoData_shouldAddFilenameForCrowdsourcingAltoFile() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        indexer.setDataRepository(new DataRepository("src/test/resources", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ALTOCROWD, Paths.get("src/test/resources/ALTO/"));
        Assertions.assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTOCROWD)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File altoFile = new File(dataFolders.get(DataRepository.PARAM_ALTOCROWD).toAbsolutePath().toString(), "00000010.xml");
        Map<String, Object> altoData = TextHelper.readAltoFile(altoFile);

        Assertions.assertTrue(
                indexer.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTOCROWD, "PPN123", "00000010", 10, false));
        Assertions.assertEquals("alto_crowd/PPN123/00000010.xml", doc.getFieldValue(SolrConstants.FILENAME_ALTO));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add filename for converted alto file
     */
    @Test
    void addIndexFieldsFromAltoData_shouldAddFilenameForConvertedAltoFile() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        indexer.setDataRepository(new DataRepository("build/viewer", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ABBYY, Paths.get("src/test/resources/ABBYYXML"));
        Assertions.assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ABBYY)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File abbyyfile = new File(dataFolders.get(DataRepository.PARAM_ABBYY).toAbsolutePath().toString(), "00000001.xml");
        Map<String, Object> altoData = TextHelper.readAbbyyToAlto(abbyyfile);

        Assertions.assertTrue(
                indexer.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO_CONVERTED, "PPN123", "00000001", 1, true));
        Assertions.assertEquals("alto/PPN123/00000001.xml", doc.getFieldValue(SolrConstants.FILENAME_ALTO));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add fulltext
     */
    @Test
    void addIndexFieldsFromAltoData_shouldAddFulltext() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        indexer.setDataRepository(new DataRepository("src/test/resources", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ALTO, Paths.get("src/test/resources/ALTO/"));
        Assertions.assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTO)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File altoFile = new File(dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), "00000010.xml");
        Map<String, Object> altoData = TextHelper.readAltoFile(altoFile);

        Assertions.assertTrue(
                indexer.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
        Assertions.assertNotNull(doc.getFieldValue(SolrConstants.FULLTEXT));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add width and height
     */
    @Test
    void addIndexFieldsFromAltoData_shouldAddWidthAndHeight() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        indexer.setDataRepository(new DataRepository("src/test/resources", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ALTO, Paths.get("src/test/resources/ALTO/"));
        Assertions.assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTO)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File altoFile = new File(dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), "00000010.xml");
        Map<String, Object> altoData = TextHelper.readAltoFile(altoFile);

        Assertions.assertTrue(
                indexer.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
        Assertions.assertEquals("2480", doc.getFieldValue(SolrConstants.WIDTH));
        Assertions.assertEquals("3508", doc.getFieldValue(SolrConstants.HEIGHT));
    }

    /**
     * @see Indexer#addNamedEntitiesFields(Map,SolrInputDocument)
     * @verifies add field
     */
    @Test
    void addNamedEntitiesFields_shouldAddField() {
        Map<String, Object> altoData = new HashMap<>(1);
        altoData.put(SolrConstants.NAMEDENTITIES, Collections.singletonList("LOCATION###Göttingen###https://www.geonames.org/2918632"));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());

        Indexer.addNamedEntitiesFields(altoData, doc);
        Assertions.assertEquals("Göttingen", doc.getFieldValue("NE_LOCATION"));
    }

    /**
     * @see Indexer#addNamedEntitiesFields(Map,SolrInputDocument)
     * @verifies add untokenized field
     */
    @Test
    void addNamedEntitiesFields_shouldAddUntokenizedField() {
        Map<String, Object> altoData = new HashMap<>(1);
        altoData.put(SolrConstants.NAMEDENTITIES, Collections.singletonList("LOCATION###Göttingen###https://www.geonames.org/2918632"));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());

        Indexer.addNamedEntitiesFields(altoData, doc);
        Assertions.assertEquals("Göttingen", doc.getFieldValue("NE_LOCATION_UNTOKENIZED"));
    }

    /**
     * @see Indexer#generateUserGeneratedContentDocsForPage(SolrInputDocument,Path,String,String,Map,int,String)
     * @verifies return empty list if dataFolder null
     */
    @Test
    void generateUserGeneratedContentDocsForPage_shouldReturnEmptyListIfDataFolderNull() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        List<SolrInputDocument> result =
                indexer.generateUserGeneratedContentDocsForPage(new SolrInputDocument("foo", "bar"), null, "foo", null, Collections.emptyMap(), 1,
                        "foo");
        Assertions.assertTrue(result.isEmpty());
    }

    /**
     * @see Indexer#generateUserGeneratedContentDocForPage(Element,SolrInputDocument,String,String,Map,int)
     * @verifies throw IllegalArgumentException if eleContent null
     */
    @Test
    void generateUserGeneratedContentDocForPage_shouldThrowIllegalArgumentExceptionIfEleContentNull() {
        MetsIndexer metsIndeer = new MetsIndexer(hotfolder);
        Map<String, String> groupIds = Collections.emptyMap();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> metsIndeer.generateUserGeneratedContentDocForPage(null, null, "foo", null, groupIds, 1));
    }

    /**
     * @see Indexer#generateUserCommentDocsForPage(SolrInputDocument,Path,String,String,Map,int)
     * @verifies return empty list if dataFolder null
     */
    @Test
    void generateUserCommentDocsForPage_shouldReturnEmptyListIfDataFolderNull() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        List<SolrInputDocument> result =
                indexer.generateUserCommentDocsForPage(new SolrInputDocument("foo", "bar"), null, "foo", null, Collections.emptyMap(), 1);
        Assertions.assertTrue(result.isEmpty());
    }

    /**
     * @see Indexer#generateUserCommentDocsForPage(SolrInputDocument,Path,String,String,Map,int,String)
     * @verifies construct doc correctly
     */
    @Test
    void generateUserCommentDocsForPage_shouldConstructDocCorrectly() throws Exception {
        Path dataFolder = Paths.get("src/test/resources/ugc");
        Assertions.assertTrue(Files.isDirectory(dataFolder));

        DocUpdateIndexer indexer = new DocUpdateIndexer(hotfolder);

        String docstrct = "monograph";
        SolrInputDocument ownerDoc = new SolrInputDocument();
        ownerDoc.setField(SolrConstants.IDDOC_OWNER, 123L);
        ownerDoc.setField(SolrConstants.DOCSTRCT_TOP, docstrct);
        List<SolrInputDocument> docs =
                indexer.generateUserCommentDocsForPage(ownerDoc, dataFolder, "PPN123", "PPN-anchor", null, 2);
        Assertions.assertNotNull(docs);
        Assertions.assertEquals(0, docs.size());
    }

    /**
     * @see Indexer#generateUserCommentDocsForPage(SolrInputDocument,Path,String,String,Map,int,String)
     * @verifies skip comments for other pages
     */
    @Test
    void generateUserCommentDocsForPage_shouldSkipCommentsForOtherPages() throws Exception {
        Path dataFolder = Paths.get("src/test/resources/ugc");
        Assertions.assertTrue(Files.isDirectory(dataFolder));

        DocUpdateIndexer indexer = new DocUpdateIndexer(hotfolder);

        String docstrct = "monograph";
        SolrInputDocument ownerDoc = new SolrInputDocument();
        ownerDoc.setField(SolrConstants.IDDOC_OWNER, 123L);
        ownerDoc.setField(SolrConstants.DOCSTRCT_TOP, docstrct);
        List<SolrInputDocument> docs =
                indexer.generateUserCommentDocsForPage(ownerDoc, dataFolder, "PPN123", "PPN-anchor", null, 1);
        Assertions.assertNotNull(docs);
        Assertions.assertEquals(2, docs.size());
    }

    /**
     * @see Indexer#parseMimeType(SolrInputDocument,String)
     * @verifies parse mime type from mp4 file correctly
     */
    @Test
    void parseMimeType_shouldParseMimeTypeFromMp4FileCorrectly() {
        SolrInputDocument doc = new SolrInputDocument();
        Indexer.parseMimeType(doc, "src/text/resouces/LIDO/1292624_media/Film77.mp4");

        Assertions.assertEquals("video/mp4", doc.getFieldValue(SolrConstants.MIMETYPE));
        Assertions.assertEquals("Film77.mp4", doc.getFieldValue(SolrConstants.FILENAME + "_MP4"));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(GroupedMetadata,ISolrWriteStrategy,IndexObject,long,Set,List)
     * @verifies throw IllegalArgumentException if gmd null
     */
    @Test
    void addGroupedMetadataDocs_shouldThrowIllegalArgumentExceptionIfGmdNull() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        IndexObject indexObj = new IndexObject(2L);
        ISolrWriteStrategy strategy = AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Set<String> skipFields = new HashSet<>();
        List<LuceneField> dcFields = Collections.emptyList();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> indexer.addGroupedMetadataDocs(null, strategy, indexObj, 1L, skipFields, dcFields));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(GroupedMetadata,ISolrWriteStrategy,IndexObject,long,Set,List)
     * @verifies throw IllegalArgumentException indexObj null
     */
    @Test
    void addGroupedMetadataDocs_shouldThrowIllegalArgumentExceptionIndexObjNull() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        GroupedMetadata gmd = new GroupedMetadata();
        ISolrWriteStrategy strategy = AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder);
        Set<String> skipFields = new HashSet<>();
        List<LuceneField> dcFields = Collections.emptyList();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> indexer.addGroupedMetadataDocs(gmd, strategy, null, 1L, skipFields, dcFields));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(GroupedMetadata,ISolrWriteStrategy,IndexObject,long,Set,List)
     * @verifies throw IllegalArgumentException if writeStrategy null
     */
    @Test
    void addGroupedMetadataDocs_shouldThrowIllegalArgumentExceptionIfWriteStrategyNull() {
        Indexer indexer = new MetsIndexer(hotfolder);
        IndexObject indexObj = new IndexObject(2L);
        GroupedMetadata gmd = new GroupedMetadata();
        Set<String> skipFields = new HashSet<>();
        List<LuceneField> dcFields = Collections.emptyList();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> indexer.addGroupedMetadataDocs(gmd, null, indexObj, 1L, skipFields, dcFields));
    }

    /**
     * @see Indexer#addGroupedMetadataDocs(GroupedMetadata,ISolrWriteStrategy,IndexObject,long,Set,List)
     * @verifies add BOOL_WKT_COORDINATES true to docstruct if WKT_COORDS found
     */
    @Test
    void addGroupedMetadataDocs_shouldAddBOOL_WKT_COORDINATESTrueToDocstructIfWKT_COORDSFound() throws Exception {
        Indexer indexer = new MetsIndexer(hotfolder);
        indexer.setDataRepository(new DataRepository("src/test/resources", true));

        IndexObject indexObj = new IndexObject(2L);
        GroupedMetadata gmd = new GroupedMetadata();
        gmd.setMainValue("foo");
        gmd.setAddCoordsToDocstruct(true);
        gmd.getAuthorityDataFields().add(new LuceneField(MetadataHelper.FIELD_WKT_COORDS, "1.0, 2.0, 3.0, 4.0"));
        gmd.getAuthorityDataFields().add(new LuceneField(MetadataHelper.FIELD_HAS_WKT_COORDS, "false"));
        indexer.addGroupedMetadataDocs(gmd, AbstractWriteStrategy.create(null, new HashMap<>(), hotfolder), indexObj, 1L, new HashSet<>(),
                Collections.emptyList());
        Assertions.assertEquals(2, indexObj.getLuceneFields().size());
        Assertions.assertEquals(MetadataHelper.FIELD_HAS_WKT_COORDS, indexObj.getLuceneFields().get(0).getField());
        Assertions.assertEquals("true", indexObj.getLuceneFields().get(0).getValue());
    }

    /**
     * @see Indexer#checkOldDataFolder(Map,String,String)
     * @verifies throw IllegalArgumentException if dataFolders null
     */
    @Test
    void checkOldDataFolder_shouldThrowIllegalArgumentExceptionIfDataFoldersNull() {
        Indexer indexer = new MetsIndexer(hotfolder);
        Assertions.assertThrows(IllegalArgumentException.class, () -> indexer.checkOldDataFolder(null, DataRepository.PARAM_ALTO, "foo"));
    }

    /**
     * @see Indexer#checkOldDataFolder(Map,String,String)
     * @verifies throw IllegalArgumentException if paramName null
     */
    @Test
    void checkOldDataFolder_shouldThrowIllegalArgumentExceptionIfParamNameNull() {
        Indexer indexer = new MetsIndexer(hotfolder);
        Map<String, Path> dataFolders = Collections.emptyMap();

        Assertions.assertThrows(IllegalArgumentException.class, () -> indexer.checkOldDataFolder(dataFolders, null, "foo"));
    }

    /**
     * @see Indexer#checkOldDataFolder(Map,String,String)
     * @verifies throw IllegalArgumentException if pi null
     */
    @Test
    void checkOldDataFolder_shouldThrowIllegalArgumentExceptionIfPiNull() {
        Indexer indexer = new MetsIndexer(hotfolder);
        Map<String, Path> dataFolders = Collections.emptyMap();

        Assertions.assertThrows(IllegalArgumentException.class, () -> indexer.checkOldDataFolder(dataFolders, DataRepository.PARAM_ALTO, null));
    }

    /**
     * @see Indexer#checkDataFolders(String)
     * @verifies add data folder paths correctly
     */
    @Test
    void checkDataFolders_shouldAddDataFolderPathsCorrectly() throws Exception {
        String fileNameRoot = "foo";
        Assertions.assertTrue(Files
                .isDirectory(Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + Indexer.FOLDER_SUFFIX_MEDIA))));
        Assertions.assertTrue(Files.isDirectory(Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + "_txt"))));
        Assertions.assertTrue(
                Files.isDirectory(
                        Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + Indexer.FOLDER_SUFFIX_TXTCROWD))));
        Assertions.assertTrue(Files.isDirectory(Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + "_wc"))));
        Assertions.assertTrue(Files.isDirectory(Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + "_alto"))));
        Assertions.assertTrue(
                Files.isDirectory(
                        Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + Indexer.FOLDER_SUFFIX_ALTOCROWD))));
        Assertions.assertTrue(Files.isDirectory(Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + "_xml"))));
        Assertions.assertTrue(Files.isDirectory(Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + "_pdf"))));
        Assertions.assertTrue(Files.isDirectory(Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + "_mix"))));
        Assertions.assertTrue(Files.isDirectory(Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + "_src"))));
        Assertions.assertTrue(Files.isDirectory(Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + "_ugc"))));
        Assertions.assertTrue(Files.isDirectory(Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + "_cms"))));
        Assertions.assertTrue(Files.isDirectory(Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + "_tei"))));
        Assertions.assertTrue(
                Files.isDirectory(Files.createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + "_annotations"))));
        Assertions.assertTrue(
                Files.isDirectory(Files
                        .createDirectory(Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + Indexer.FOLDER_SUFFIX_DOWNLOADIMAGES))));

        Map<String, Path> result = Indexer.checkDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);
        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.get(DataRepository.PARAM_MEDIA));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_FULLTEXT));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_FULLTEXTCROWD));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_TEIWC));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_ALTO));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_ALTOCROWD));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_ABBYY));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_PAGEPDF));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_MIX));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_SOURCE));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_UGC));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_CMS));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_TEIMETADATA));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_ANNOTATIONS));
        Assertions.assertNotNull(result.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER));
    }

    /**
     * @see Indexer#checkReindexSettings(Map,Map)
     * @verifies throw IllegalArgumentException if dataFolders null
     */
    @Test
    void checkReindexSettings_shouldThrowIllegalArgumentExceptionIfDataFoldersNull() {
        Map<String, Boolean> reindexSettings = Collections.emptyMap();
        Assertions.assertThrows(IllegalArgumentException.class, () -> Indexer.checkReindexSettings(null, reindexSettings));
    }

    /**
     * @see Indexer#checkReindexSettings(Map,Map)
     * @verifies throw IllegalArgumentException if reindexSettings null
     */
    @Test
    void checkReindexSettings_shouldThrowIllegalArgumentExceptionIfReindexSettingsNull() {
        Map<String, Path> dataFolders = Collections.emptyMap();
        Assertions.assertThrows(IllegalArgumentException.class, () -> Indexer.checkReindexSettings(dataFolders, null));
    }

    /**
     * @see Indexer#checkReindexSettings(Map,Map)
     * @verifies add reindex flags correctly if data folders missing
     */
    @Test
    void checkReindexSettings_shouldAddReindexFlagsCorrectlyIfDataFoldersMissing() {
        Map<String, Boolean> reindexSettings = new HashMap<>();

        Indexer.checkReindexSettings(Collections.emptyMap(), reindexSettings);
        Assertions.assertTrue(reindexSettings.get(DataRepository.PARAM_MEDIA));
        Assertions.assertTrue(reindexSettings.get(DataRepository.PARAM_FULLTEXT));
        Assertions.assertTrue(reindexSettings.get(DataRepository.PARAM_FULLTEXTCROWD));
        Assertions.assertTrue(reindexSettings.get(DataRepository.PARAM_TEIWC));
        Assertions.assertTrue(reindexSettings.get(DataRepository.PARAM_ALTO));
        Assertions.assertTrue(reindexSettings.get(DataRepository.PARAM_ALTOCROWD));
        Assertions.assertTrue(reindexSettings.get(DataRepository.PARAM_ABBYY));
        Assertions.assertTrue(reindexSettings.get(DataRepository.PARAM_MIX));
        Assertions.assertTrue(reindexSettings.get(DataRepository.PARAM_UGC));
        Assertions.assertTrue(reindexSettings.get(DataRepository.PARAM_CMS));
        Assertions.assertTrue(reindexSettings.get(DataRepository.PARAM_TEIMETADATA));
        Assertions.assertTrue(reindexSettings.get(DataRepository.PARAM_ANNOTATIONS));
    }

    /**
     * @see Indexer#checkReindexSettings(Map,Map)
     * @verifies not add reindex flags if data folders present
     */
    @Test
    void checkReindexSettings_shouldNotAddReindexFlagsIfDataFoldersPresent() {
        Map<String, Boolean> reindexSettings = new HashMap<>();

        Map<String, Path> dataFolders = new HashMap<>();
        Path p = Paths.get("foo");
        dataFolders.put(DataRepository.PARAM_MEDIA, p);
        dataFolders.put(DataRepository.PARAM_FULLTEXT, p);
        dataFolders.put(DataRepository.PARAM_FULLTEXTCROWD, p);
        dataFolders.put(DataRepository.PARAM_TEIWC, p);
        dataFolders.put(DataRepository.PARAM_ALTO, p);
        dataFolders.put(DataRepository.PARAM_ALTOCROWD, p);
        dataFolders.put(DataRepository.PARAM_ABBYY, p);
        dataFolders.put(DataRepository.PARAM_MIX, p);
        dataFolders.put(DataRepository.PARAM_UGC, p);
        dataFolders.put(DataRepository.PARAM_CMS, p);
        dataFolders.put(DataRepository.PARAM_TEIMETADATA, p);
        dataFolders.put(DataRepository.PARAM_ANNOTATIONS, p);

        Indexer.checkReindexSettings(dataFolders, reindexSettings);
        Assertions.assertNull(reindexSettings.get(DataRepository.PARAM_MEDIA));
        Assertions.assertNull(reindexSettings.get(DataRepository.PARAM_FULLTEXT));
        Assertions.assertNull(reindexSettings.get(DataRepository.PARAM_FULLTEXTCROWD));
        Assertions.assertNull(reindexSettings.get(DataRepository.PARAM_TEIWC));
        Assertions.assertNull(reindexSettings.get(DataRepository.PARAM_ALTO));
        Assertions.assertNull(reindexSettings.get(DataRepository.PARAM_ALTOCROWD));
        Assertions.assertNull(reindexSettings.get(DataRepository.PARAM_ABBYY));
        Assertions.assertNull(reindexSettings.get(DataRepository.PARAM_MIX));
        Assertions.assertNull(reindexSettings.get(DataRepository.PARAM_UGC));
        Assertions.assertNull(reindexSettings.get(DataRepository.PARAM_CMS));
        Assertions.assertNull(reindexSettings.get(DataRepository.PARAM_TEIMETADATA));
        Assertions.assertNull(reindexSettings.get(DataRepository.PARAM_ANNOTATIONS));
    }

    /**
     * @see Indexer#checkReindexSettings(Map,Map)
     * @verifies return alt value correctly
     */
    @Test
    void checkThumbnailFileName_shouldReturnAltValueCorrectly() {
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("FILENAME_JPEG", "001.jpg");
        Assertions.assertEquals("001.jpg", Indexer.checkThumbnailFileName("001.ogg", doc));

        doc.setField("FILENAME_TIFF", "001.tif");
        Assertions.assertEquals("001.tif", Indexer.checkThumbnailFileName("001.ogg", doc));
    }

    /**
     * @see Indexer#checkReindexSettings(Map,Map)
     * @verifies return fileName if image
     */
    @Test
    void checkThumbnailFileName_shouldReturnFileNameIfImage() {
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("FILENAME_JPEG", "001.jpg");
        Assertions.assertEquals("001.png", Indexer.checkThumbnailFileName("001.png", doc));
    }

    /**
     * @see Indexer#checkReindexSettings(Map,Map)
     * @verifies return fileName if url
     */
    @Test
    void checkThumbnailFileName_shouldReturnFileNameIfUrl() {
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("FILENAME_JPEG", "001.jpg");
        Assertions.assertEquals("https://foo.bar/info.json", Indexer.checkThumbnailFileName("https://foo.bar/info.json", doc));
    }
}
