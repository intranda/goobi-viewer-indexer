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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Element;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.helper.DateTools;
import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.PhysicalElement;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.datarepository.strategy.AbstractDataRepositoryStrategy;
import io.goobi.viewer.indexer.model.datarepository.strategy.IDataRepositoryStrategy;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

class MetsIndexerTest extends AbstractSolrEnabledTest {

    /** Logger for this class. */
    //    private static final Logger logger = LogManager.getLogger(MetsIndexerTest.class);

    private static final String PI = IndexerTest.PI_KLEIUNIV;
    private static final String PI2 = "H030001";

    private Path metsFile;
    private Path metsFile2;
    private Path metsFile3;
    private Path metsFileVol1;
    private Path metsFileVol2;
    private Path metsFileAnchor1;
    private Path metsFileAnchor2;

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());

        metsFile = Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        assertTrue(Files.isRegularFile(metsFile));
        metsFile2 = Paths.get("src/test/resources/METS/H030001_mets.xml");
        assertTrue(Files.isRegularFile(metsFile2));
        metsFile3 = Paths.get("src/test/resources/METS/AC06736966.xml");
        assertTrue(Files.isRegularFile(metsFile3));
        metsFileVol1 = Paths.get("src/test/resources/METS/baltst_559838239/baltst_559838239_NF_75.xml");
        assertTrue(Files.isRegularFile(metsFileVol1));
        metsFileVol2 = Paths.get("src/test/resources/METS/baltst_559838239/baltst_559838239_NF_78.xml");
        assertTrue(Files.isRegularFile(metsFileVol2));
        metsFileAnchor1 = Paths.get("src/test/resources/METS/baltst_559838239/baltst_559838239_NF_75_anchor.xml");
        assertTrue(Files.isRegularFile(metsFileAnchor1));
        metsFileAnchor2 = Paths.get("src/test/resources/METS/baltst_559838239/baltst_559838239_NF_78_anchor.xml");
        assertTrue(Files.isRegularFile(metsFileAnchor2));
    }

    /**
     * @see MetsIndexer#MetsIndexer(Hotfolder)
     * @verifies set attributes correctly
     */
    @Test
    void MetsIndexer_shouldSetAttributesCorrectly() {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        assertEquals(hotfolder, indexer.hotfolder);
    }

    /**
     * @see MetsIndexer#index(File,ISolrWriteStrategy,boolean,Map)
     * @verifies index record correctly
     */
    @SuppressWarnings("unchecked")
    @Test
    void index_shouldIndexRecordCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        dataFolders.put(DataRepository.PARAM_CMS, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_cms"));
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, dataFolders, null, 1, false);
        assertEquals(PI + ".xml", ret[0]);
        Assertions.assertNull(ret[1]);

        Map<String, Boolean> iddocMap = new HashMap<>();
        String iddoc;

        // Top document
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + PI, null);
            assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            {
                Collection<Object> values = doc.getFieldValues(SolrConstants.ACCESSCONDITION);
                assertNotNull(values);
                assertEquals(1, values.size());
                assertEquals("OPENACCESS", values.iterator().next());
            }
            assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            assertNotNull(doc.getFieldValues(SolrConstants.DATEUPDATED));
            assertEquals(1, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
            assertEquals(doc.getFieldValue(SolrConstants.DATECREATED), doc.getFirstValue(SolrConstants.DATEUPDATED));
            assertEquals(DocType.DOCSTRCT.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
            assertEquals("Monograph", doc.getFieldValue(SolrConstants.DOCSTRCT));
            assertEquals("Monograph", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
            {
                List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
                assertNotNull(mdList);
                assertEquals(2, mdList.size());
                assertEquals("varia", mdList.get(0));
                assertEquals("digiwunschbuch", mdList.get(1));
            }
            assertTrue(doc.containsKey(SolrConstants.DEFAULT));
            // assertTrue(doc.containsKey(SolrConstants.SUPERDEFAULT));
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            assertNotNull(iddoc);
            iddocMap.put(iddoc, true);
            assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
            assertEquals(true, doc.getFieldValue(SolrConstants.ISWORK));
            assertEquals("Universität und Technische Hochschule", doc.getFieldValue(SolrConstants.LABEL));
            assertEquals("LOG_0000", doc.getFieldValue(SolrConstants.LOGID));
            assertEquals(16, doc.getFieldValue(SolrConstants.NUMPAGES));
            assertEquals(PI, doc.getFieldValue(SolrConstants.PI));
            assertEquals(PI, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            assertEquals(FileFormat.METS.name(), doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
            assertEquals("00000002.tif", doc.getFieldValue(SolrConstants.THUMBNAIL)); // representative image is set
            assertEquals("00000002.tif", doc.getFieldValue(SolrConstants.THUMBNAILREPRESENT)); // not really used
            assertEquals(2, doc.getFieldValue(SolrConstants.THUMBPAGENO)); // representative image should not affect the number
            assertEquals(" - ", doc.getFieldValue(SolrConstants.THUMBPAGENOLABEL));
            assertEquals("urn:nbn:de:hebis:66:fuldig-1946", doc.getFieldValue(SolrConstants.URN));
            Assertions.assertNull(doc.getFieldValue(SolrConstants.IMAGEURN_OAI)); // only docs representing deleted records should have this field
            assertEquals("http://opac.sub.uni-goettingen.de/DB=1/PPN?PPN=517154005", doc.getFieldValue(SolrConstants.OPACURL));
            assertEquals(true, doc.getFieldValue(SolrConstants.FULLTEXTAVAILABLE));
            {
                List<String> mdList = (List<String>) doc.getFieldValue("MD_AUTHOR");
                assertNotNull(mdList);
                assertEquals(1, mdList.size());
                assertEquals("Klein, Felix", mdList.get(0));
            }
            {
                List<String> mdList = (List<String>) doc.getFieldValue("MD_AUTHOR" + SolrConstants.SUFFIX_UNTOKENIZED);
                assertNotNull(mdList);
                assertEquals(1, mdList.size());
                assertEquals("Klein, Felix", mdList.get(0));
            }
            assertEquals("Klein, Felix", doc.getFieldValue("SORT_AUTHOR"));

            {
                List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.YEAR);
                assertNotNull(mdList);
                assertEquals(1, mdList.size());
                assertEquals(Long.valueOf(1980), mdList.get(0));
                assertEquals(Long.valueOf(1980), doc.getFieldValue(SolrConstants.PREFIX_SORTNUM + SolrConstants.YEAR));
            }
            {
                List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.YEARMONTH);
                assertNotNull(mdList);
                assertEquals(1, mdList.size());
                assertEquals(Long.valueOf(198007), mdList.get(0));
                assertEquals(Long.valueOf(198007), doc.getFieldValue(SolrConstants.PREFIX_SORTNUM + SolrConstants.YEARMONTH));
            }
            {
                List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.YEARMONTHDAY);
                assertNotNull(mdList);
                assertEquals(1, mdList.size());
                assertEquals(Long.valueOf(19800710), mdList.get(0));
                assertEquals(Long.valueOf(19800710), doc.getFieldValue(SolrConstants.PREFIX_SORTNUM + SolrConstants.YEARMONTHDAY));
            }
            {
                List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.MONTHDAY);
                assertNotNull(mdList);
                assertEquals(1, mdList.size());
                assertEquals(Long.valueOf(710), mdList.get(0));
                assertEquals(Long.valueOf(710), doc.getFieldValue(SolrConstants.PREFIX_SORTNUM + SolrConstants.MONTHDAY));
            }
            {
                List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.CENTURY);
                assertNotNull(mdList);
                assertEquals(1, mdList.size());
                assertEquals(Long.valueOf(20), mdList.get(0));
                assertEquals(Long.valueOf(20), doc.getFieldValue(SolrConstants.PREFIX_SORTNUM + SolrConstants.CENTURY));
            }
        }

        // Child docstructs
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(new StringBuilder(SolrConstants.PI_TOPSTRUCT).append(":")
                            .append(PI)
                            .append(" AND ")
                            .append(SolrConstants.IDDOC_PARENT)
                            .append(":*")
                            .toString(), null);
            assertEquals(3, docList.size());

            Map<String, Boolean> logIdMap = new HashMap<>();

            for (SolrDocument doc : docList) {
                {
                    Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                    assertNotNull(values);
                    assertEquals(1, values.size());
                    assertEquals("OPENACCESS", values.iterator().next());
                }
                {
                    //                    Collection<Object> values = doc.getFieldValues(SolrConstants.DC);
                    //                    assertNotNull(values);
                    //                    assertEquals(2, values.size());
                }
                assertTrue(doc.containsKey(SolrConstants.DEFAULT));
                assertEquals(DocType.DOCSTRCT.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
                assertNotNull(doc.getFieldValue(SolrConstants.DOCSTRCT));
                assertEquals(doc.getFieldValue(SolrConstants.DOCSTRCT), doc.getFieldValue(SolrConstants.DOCSTRCT_SUB));
                assertEquals("Monograph", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
                {
                    List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
                    assertNotNull(mdList);
                    assertEquals(2, mdList.size());
                    assertEquals("varia", mdList.get(0));
                    assertEquals("digiwunschbuch", mdList.get(1));
                }
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    assertNotNull(value);
                    Assertions.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
                assertEquals(iddoc, doc.getFieldValue(SolrConstants.IDDOC_TOPSTRUCT));
                {
                    String value = (String) doc.getFieldValue(SolrConstants.LOGID);
                    assertNotNull(value);
                    Assertions.assertNull(logIdMap.get(value));
                    logIdMap.put(value, true);
                }
                assertNotNull(doc.getFieldValue(SolrConstants.THUMBNAIL));
                assertNotNull(doc.getFieldValue(SolrConstants.THUMBPAGENO));
                assertNotNull(doc.getFieldValue(SolrConstants.THUMBPAGENOLABEL));
                assertNotNull(doc.getFieldValue(SolrConstants.NUMPAGES));
                assertNotNull(doc.getFieldValue(SolrConstants.URN));
                assertNotNull(doc.getFieldValues(SolrConstants.DATEUPDATED));
            }
        }

        // Pages
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.DOCTYPE + ":" + DocType.PAGE, null);
            assertEquals(16, docList.size());

            for (SolrDocument doc : docList) {
                {
                    Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                    assertNotNull(values);
                    assertEquals(1, values.size());
                    assertEquals("OPENACCESS", values.iterator().next());
                }
                {
                    Collection<Object> values = doc.getFieldValues(SolrConstants.DC);
                    assertNotNull(values);
                    assertEquals(2, values.size());
                }
                Integer order = (Integer) doc.getFieldValue(SolrConstants.ORDER);
                assertNotNull(order);
                String fileName = (String) doc.getFieldValue(SolrConstants.FILENAME);
                assertNotNull(fileName);
                assertNotNull(doc.getFieldValue(SolrConstants.DOCSTRCT));
                assertEquals("Monograph", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
                assertNotNull(doc.getFieldValue(SolrConstants.IMAGEURN));
                assertNotNull(doc.getFieldValue(SolrConstants.FILEIDROOT));
                {
                    String value = (String) doc.getFieldValue(SolrConstants.FILENAME_FULLTEXT);
                    assertNotNull(value);
                    assertEquals("fulltext/PPN517154005/" + FilenameUtils.getBaseName(fileName) + FileTools.TXT_EXTENSION, value);
                    assertEquals(true, doc.getFieldValue(SolrConstants.FULLTEXTAVAILABLE));
                }
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    assertNotNull(value);
                    Assertions.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
                assertNotNull(doc.getFieldValue(SolrConstants.IDDOC_OWNER));
                // DATEUPDATED from the top docstruct
                assertNotNull(doc.getFieldValue(SolrConstants.DATEUPDATED));
                // SORT_* fields from the direct owner docstruct
                if (order == 2) {
                    assertEquals("Universität und Technische Hochschule", doc.getFieldValue("SORT_TITLE"));
                }
            }
        }
    }

    /**
     * @see MetsIndexer#index(File,ISolrWriteStrategy,boolean,Map)
     * @verifies index metadata groups correctly
     */
    @SuppressWarnings("unchecked")
    @Test
    void index_shouldIndexMetadataGroupsCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        String[] ret = new MetsIndexer(hotfolder).index(metsFile2, dataFolders, null, 1, false);
        assertEquals(PI2 + ".xml", ret[0]);
        Assertions.assertNull(ret[1]);

        Map<String, Boolean> iddocMap = new HashMap<>();
        String iddoc;

        // Top document
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + PI2, null);
            assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            {
                Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                assertNotNull(values);
                assertEquals(1, values.size());
                assertEquals("OPENACCESS", values.iterator().next());
            }
            assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            assertNotNull(doc.getFieldValue(SolrConstants.DATEUPDATED));
            assertEquals(DocType.DOCSTRCT.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
            assertNotNull(doc.getFieldValue(SolrConstants.DOCSTRCT));
            assertTrue(doc.getFieldValues(SolrConstants.DC) != null && doc.getFieldValues(SolrConstants.DC).size() == 1);
            assertEquals("casualia", doc.getFieldValues(SolrConstants.DC).iterator().next());
            assertTrue(doc.containsKey(SolrConstants.DEFAULT));
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            assertNotNull(iddoc);
            iddocMap.put(iddoc, true);
            assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
            assertEquals(true, doc.getFieldValue(SolrConstants.ISWORK));
            Assertions.assertNull(doc.getFieldValue(SolrConstants.NUMPAGES));
            assertEquals(PI2, doc.getFieldValue(SolrConstants.PI));
            assertEquals(PI2, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            assertEquals(FileFormat.METS.name(), doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
        }

        // Grouped metadata
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(new StringBuilder(SolrConstants.DOCTYPE).append(":")
                            .append(DocType.METADATA.name())
                            .append(" AND ")
                            .append(SolrConstants.IDDOC_OWNER)
                            .append(":")
                            .append(iddoc)
                            .toString(), null);
            assertEquals(4, docList.size());

            for (SolrDocument doc : docList) {
                assertTrue("MD_CREATOR".equals(doc.getFieldValue(SolrConstants.LABEL))
                        || "MD_PHYSICALCOPY".equals(doc.getFieldValue(SolrConstants.LABEL)));
                assertNotNull(doc.getFieldValue(SolrConstants.GROUPFIELD));
                if ("MD_CREATOR".equals(doc.getFieldValue(SolrConstants.LABEL))) {
                    assertNotNull(doc.getFieldValue("MD_VALUE"));
                    assertNotNull(doc.getFieldValue("MD_FIRSTNAME"));
                    assertNotNull(doc.getFieldValue("MD_LASTNAME"));
                    assertNotNull(doc.getFieldValue("NORM_URI"));
                    assertNotNull(doc.getFieldValue(SolrConstants.DEFAULT));
                } else if ("MD_PHYSICALCOPY".equals(doc.getFieldValue(SolrConstants.LABEL))) {
                    assertNotNull(doc.getFieldValue("MD_LOCATION"));
                    assertNotNull(doc.getFieldValue("MD_SHELFMARK"));
                    //                    assertNotNull(doc.getFieldValue(SolrConstants.NORMDATATERMS));
                }
                // Commented out because inherited access conditions trigger metadata locking where not wanted
                {
                    //                    Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                    //                    assertNotNull(values);
                    //                    assertEquals(1, values.size());
                    //                    assertEquals("OPENACCESS", values.iterator().next());
                }
            }
        }

        // Pages
        {
            SolrDocumentList docList =
                    SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search(SolrConstants.PI_TOPSTRUCT + ":" + PI2 + " AND " + SolrConstants.FILENAME + ":*", null);
            assertEquals(0, docList.size());
        }
    }

    /**
     * @see MetsIndexer#index(File,ISolrWriteStrategy,boolean,Map)
     * @verifies index multi volume records correctly
     */
    @Test
    void index_shouldIndexMultiVolumeRecordsCorrectly() throws Exception {
        String piVol1 = "PPN612054551";
        String piVol2 = "PPN612060039";
        String piAnchor = "PPN559838239";
        Map<String, Path> dataFolders = new HashMap<>();

        // Index first volume
        {
            String[] ret = new MetsIndexer(hotfolder).index(metsFileVol1, dataFolders, null, 1, false);
            assertEquals(piVol1 + ".xml", ret[0]);
            Assertions.assertNull(ret[1]);
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + piVol1, null);
            assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            assertEquals(piAnchor, doc.getFieldValue(SolrConstants.PI_PARENT));
            assertEquals(piAnchor, doc.getFieldValue(SolrConstants.PI_ANCHOR));

            // All child docstructs should have the PI_ANCHOR field
            SolrDocumentList vol1Children = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(new StringBuilder().append('+')
                            .append(SolrConstants.PI_TOPSTRUCT)
                            .append(":")
                            .append(piVol1)
                            .append(" +")
                            .append(SolrConstants.DOCTYPE)
                            .append(':')
                            .append(DocType.DOCSTRCT)
                            .append(" +")
                            .append(SolrConstants.PI_ANCHOR)
                            .append(':')
                            .append(piAnchor)
                            .toString(), null);
            assertEquals(95, vol1Children.size());

            // All pages should have the PI_ANCHOR field
            SolrDocumentList vol1Pages = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(new StringBuilder().append('+')
                            .append(SolrConstants.PI_TOPSTRUCT)
                            .append(":")
                            .append(piVol1)
                            .append(" +")
                            .append(SolrConstants.DOCTYPE)
                            .append(':')
                            .append(DocType.PAGE)
                            .append(" +")
                            .append(SolrConstants.PI_ANCHOR)
                            .append(':')
                            .append(piAnchor)
                            .toString(), null);
            assertEquals(162, vol1Pages.size());
        }

        // Index anchor
        String anchorIddoc;
        {
            String[] ret = new MetsIndexer(hotfolder).index(metsFileAnchor1, dataFolders, null, 1, false);
            assertEquals(piAnchor + ".xml", ret[0]);
            Assertions.assertNull(ret[1]);
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + piAnchor, null);
            assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            anchorIddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            assertNotNull(anchorIddoc);
            assertEquals(anchorIddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
        }

        // Re-index first volume
        {
            String[] ret = new MetsIndexer(hotfolder).index(metsFileVol1, dataFolders, null, 1, false);
            Assertions.assertNull(ret[1]);
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + piVol1, null);
            assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            assertEquals(anchorIddoc, doc.getFieldValue(SolrConstants.IDDOC_PARENT));
        }

        // Index second volume
        {
            String[] ret = new MetsIndexer(hotfolder).index(metsFileVol2, dataFolders, null, 1, false);
            Assertions.assertNull(ret[1]);
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + piVol2, null);
            assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            assertEquals(anchorIddoc, doc.getFieldValue(SolrConstants.IDDOC_PARENT));
        }

        assertEquals(1, hotfolder.getHighPriorityQueue().size());

        String[] ret = new MetsIndexer(hotfolder).index(hotfolder.getHighPriorityQueue().poll(), dataFolders, null, 1, false);
        assertEquals(piAnchor + ".xml", ret[0]);
        Assertions.assertNull(ret[1]);
    }

    /**
     * @see MetsIndexer#index(File,ISolrWriteStrategy,boolean,Map)
     * @verifies update record correctly
     */
    @Test
    void index_shouldUpdateRecordCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, dataFolders, null, 1, false);
        assertEquals(PI + ".xml", ret[0]);
        Assertions.assertNull(ret[1]);

        Map<String, Boolean> iddocMap = new HashMap<>();
        String iddoc;
        long dateCreated;
        Collection<Object> dateUpdated;

        // Top document
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + PI, null);
            assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            dateCreated = (Long) doc.getFieldValue(SolrConstants.DATECREATED);
            assertNotNull(doc.getFieldValue(SolrConstants.DATEUPDATED));
            dateUpdated = doc.getFieldValues(SolrConstants.DATEUPDATED);
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            assertNotNull(iddoc);
            iddocMap.put(iddoc, true);
            assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
        }

        // Child docstructs
        {
            SolrDocumentList docList =
                    SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.IDDOC_PARENT + ":*", null);
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    assertNotNull(value);
                    Assertions.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
            }
        }

        // Pages
        {
            SolrDocumentList docList =
                    SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.FILENAME + ":*", null);
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    assertNotNull(value);
                    Assertions.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
            }
        }

        // Re-index
        ret = new MetsIndexer(hotfolder).index(metsFile, dataFolders, null, 1, false);
        assertEquals(PI + ".xml", ret[0]);
        Assertions.assertNull(ret[1]);

        // Top document
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + PI, null);
            assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            assertEquals(dateCreated, doc.getFieldValue(SolrConstants.DATECREATED));
            assertNotNull(doc.getFieldValue(SolrConstants.DATEUPDATED));
            // No new DATEUPDATED value
            assertEquals(dateUpdated.size(), doc.getFieldValues(SolrConstants.DATEUPDATED).size());
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            assertNotNull(iddoc);
            Assertions.assertNull(iddocMap.get(iddoc));
            iddocMap.put(iddoc, true);
            assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
        }

        // Child docstructs
        {
            SolrDocumentList docList =
                    SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.IDDOC_PARENT + ":*", null);
            assertEquals(3, docList.size());
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    assertNotNull(value);
                    Assertions.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
            }
        }

        // Pages
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.DOCTYPE + ":" + DocType.PAGE.name(), null);
            assertEquals(16, docList.size());
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    assertNotNull(value);
                    Assertions.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
            }
        }
    }

    /**
     * @see MetsIndexer#index(Path,boolean,Map,ISolrWriteStrategy,int)
     * @verifies set access conditions correctly
     */
    @Test
    void index_shouldSetAccessConditionsCorrectly() throws Exception {
        String pi = "AC06736966";
        Map<String, Path> dataFolders = new HashMap<>();
        String[] ret = new MetsIndexer(hotfolder).index(metsFile3, dataFolders, null, 1, false);
        assertEquals(pi + ".xml", ret[0]);
        Assertions.assertNull(ret[1]);

        // Top document
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + pi, null);
            assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Collection<Object> values = doc.getFieldValues(SolrConstants.ACCESSCONDITION);
            assertNotNull(values);
            assertEquals(1, values.size());
            assertEquals("OPENACCESS", values.iterator().next());
        }

        // Child docstructs
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(new StringBuilder().append("+")
                            .append(SolrConstants.PI_TOPSTRUCT)
                            .append(":")
                            .append(pi)
                            .append(" +")
                            .append(SolrConstants.DOCTYPE)
                            .append(":DOCSTRCT  +")
                            .append(SolrConstants.LOGID)
                            .append(":LOG_0035")
                            .toString(), null);
            assertEquals(1, docList.size());
            for (SolrDocument doc : docList) {
                Collection<Object> values = doc.getFieldValues(SolrConstants.ACCESSCONDITION);
                assertNotNull(values);
                assertEquals(1, values.size());
                assertEquals("bib_network", values.iterator().next());
            }
        }

        // Pages
        {
            SolrDocumentList docList =
                    SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search(SolrConstants.PI_TOPSTRUCT + ":" + pi + " AND " + SolrConstants.PHYSID + ":PHYS_0032", null);
            assertEquals(1, docList.size());
            for (SolrDocument doc : docList) {
                Collection<Object> values = doc.getFieldValues(SolrConstants.ACCESSCONDITION);
                assertNotNull(values);
                assertEquals(1, values.size(), values.toString());
                assertEquals("bib_network", values.iterator().next());
            }
        }
    }

    /**
     * @see MetsIndexer#index(Path,boolean,Map,ISolrWriteStrategy,int)
     * @verifies write cms page texts into index
     */
    @Test
    void index_shouldWriteCmsPageTextsIntoIndex() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_CMS, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_cms"));
        new MetsIndexer(hotfolder).index(metsFile, dataFolders, null, 1, false);

        {
            // CMS page content is not stored and must be searched for (and should have any HTML tags removed)
            SolrDocumentList docList =
                    SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search("+" + SolrConstants.PREFIX_CMS_TEXT + "DESCRIPTION" + ":\"test description\"", null);
            assertEquals(1, docList.size());
        }
        {
            // CMS page content is not stored and must be searched for (and should have any HTML tags removed)
            SolrDocumentList docList =
                    SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search("+" + SolrConstants.PREFIX_CMS_TEXT + "PUBLICATIONTEXT" + ":\"test publication text\"", null);
            assertEquals(1, docList.size());
        }
        {
            // CMS page content is not stored and must be searched for (and should have any HTML tags removed)
            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search("+" + SolrConstants.CMS_TEXT_ALL + ":\"test description\" +" + SolrConstants.CMS_TEXT_ALL + ":\"test publication text\"",
                            null);
            assertEquals(1, docList.size());
        }
    }

    /**
     * @see MetsIndexer#index(Path,boolean,Map,ISolrWriteStrategy,int)
     * @verifies write shape metadata correctly
     */
    @Test
    void index_shouldWriteShapeMetadataCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        Path metsFile = Paths.get("src/test/resources/METS/74241.xml");
        new MetsIndexer(hotfolder).index(metsFile, dataFolders, null, 1, false);
        assertEquals(1, SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search("+" + SolrConstants.PI_TOPSTRUCT + ":74241 +" + SolrConstants.LOGID + ":LOG_0004 +" + SolrConstants.METADATATYPE + ":SHAPE",
                        null)
                .size());
        assertEquals(1, SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search("+" + SolrConstants.PI_TOPSTRUCT + ":74241 +" + SolrConstants.LOGID + ":LOG_0005 +" + SolrConstants.METADATATYPE + ":SHAPE",
                        null)
                .size());
        assertEquals(1, SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search("+" + SolrConstants.PI_TOPSTRUCT + ":74241 +" + SolrConstants.LOGID + ":LOG_0006 +" + SolrConstants.METADATATYPE + ":SHAPE",
                        null)
                .size());
        assertEquals(1, SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search("+" + SolrConstants.PI_TOPSTRUCT + ":74241 +" + SolrConstants.LOGID + ":LOG_0007 +" + SolrConstants.METADATATYPE + ":SHAPE",
                        null)
                .size());
        assertEquals(1, SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search("+" + SolrConstants.PI_TOPSTRUCT + ":74241 +" + SolrConstants.LOGID + ":LOG_0009 +" + SolrConstants.METADATATYPE + ":SHAPE",
                        null)
                .size());
        assertEquals(1, SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search("+" + SolrConstants.PI_TOPSTRUCT + ":74241 +" + SolrConstants.LOGID + ":LOG_0010 +" + SolrConstants.METADATATYPE + ":SHAPE",
                        null)
                .size());
        assertEquals(1, SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search("+" + SolrConstants.PI_TOPSTRUCT + ":74241 +" + SolrConstants.LOGID + ":LOG_0011 +" + SolrConstants.METADATATYPE + ":SHAPE",
                        null)
                .size());
        assertEquals(1, SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search("+" + SolrConstants.PI_TOPSTRUCT + ":74241 +" + SolrConstants.LOGID + ":LOG_0012 +" + SolrConstants.METADATATYPE + ":SHAPE",
                        null)
                .size());
    }

    @Test
    void index_shouldWriteThumbnailCorrectly() throws Exception {
        // WRITE THUMBNAIL FROM use="banner"
        {
            Map<String, Path> dataFolders = new HashMap<>();
            Path metsFile = Paths.get("src/test/resources/METS/74241.xml");
            new MetsIndexer(hotfolder).index(metsFile, dataFolders, null, 1, false);

            SolrDocumentList docs = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search("+" + SolrConstants.PI_TOPSTRUCT + ":74241 +ISWORK:true", Collections.singletonList(SolrConstants.THUMBNAIL));
            assertEquals(1, docs.size());
            String thumbnail = docs.get(0).getFirstValue(SolrConstants.THUMBNAIL).toString();
            assertEquals("flb000645_0007.jpg", thumbnail);
        }
        // WRITE THUMBNAIL FROM xlink:label="START_PAGE"
        {
            Map<String, Path> dataFolders = new HashMap<>();
            Path metsFile = Paths.get("src/test/resources/METS/rosdok_ppn1011383616.dv.mets.xml");
            String[] ret = new MetsIndexer(hotfolder).index(metsFile, dataFolders, null, 1, false);
            assertEquals("PPN1011383616.xml", ret[0]);

            SolrDocumentList docs = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search("+" + SolrConstants.PI_TOPSTRUCT + ":PPN1011383616 +ISWORK:true", Collections.singletonList(SolrConstants.THUMBNAIL));
            assertEquals(1, docs.size());
            String thumbnail = docs.get(0).getFirstValue(SolrConstants.THUMBNAIL).toString();
            assertEquals("https://rosdok.uni-rostock.de/iiif/image-api/rosdok%252Fppn1011383616%252Fphys_0005/full/full/0/native.jpg",
                    thumbnail);

        }
    }

    /**
     * @see MetsIndexer#index(Path,boolean,Map,ISolrWriteStrategy,int)
     * @verifies read datecreated from mets with correct time zone
     */
    @Test
    void index_shouldReadDatecreatedFromMetsWithCorrectTimeZone() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        String[] ret = new MetsIndexer(hotfolder).index(metsFile2, dataFolders, null, 1, false);
        assertEquals(PI2 + ".xml", ret[0]);
        Assertions.assertNull(ret[1]);

        // Top document
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + PI2, null);
            assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);

            Long dateCreated = (Long) doc.getFieldValue(SolrConstants.DATECREATED);
            assertNotNull(dateCreated);
            assertEquals(Long.valueOf(1372770217000L), dateCreated);
        }
    }

    /**
     * @see MetsIndexer#index(Path,boolean,Map,ISolrWriteStrategy,int,boolean)
     * @verifies not add dateupdated if value already exists
     */
    @Test
    void index_shouldNotAddDateupdatedIfValueAlreadyExists() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        String[] ret = new MetsIndexer(hotfolder).index(metsFile2, dataFolders, null, 1, false);
        assertEquals(PI2 + ".xml", ret[0]);
        Assertions.assertNull(ret[1]);

        Long timestamp = 1372770217000L;

        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + PI2, null);
            assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);

            assertEquals(1, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
            Long dateUpdated = SolrSearchIndex.getSingleFieldLongValue(doc, SolrConstants.DATEUPDATED);
            assertNotNull(dateUpdated);
            assertEquals(timestamp, dateUpdated);
        }
        // Second indexing
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + PI2, null);
            assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);

            // DATEUPDATED is still the same
            assertEquals(1, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
            Long dateUpdated = SolrSearchIndex.getSingleFieldLongValue(doc, SolrConstants.DATEUPDATED);
            assertNotNull(dateUpdated);
            assertEquals(timestamp, dateUpdated);
        }
    }

    /**
     * @see MetsIndexer#index(File,ISolrWriteStrategy,boolean,Map)
     * @verifies index page metadata correctly
     */
    @Test
    void index_shouldIndexPageMetadataCorrectly() throws Exception {
        String pi = "4cbbdeb2-1279-4b1f-96a7-05c2ec30caa3";
        Path localMetsFile = Paths.get("src/test/resources/METS/MIX/" + pi + ".xml");
        assertTrue(Files.isRegularFile(localMetsFile));
        Map<String, Path> dataFolders = new HashMap<>();
        String[] ret = new MetsIndexer(hotfolder).index(localMetsFile, dataFolders, null, 1, false);
        assertEquals("4cbbdeb2-1279-4b1f-96a7-05c2ec30caa3.xml", ret[0]);
        Assertions.assertNull(ret[1]);

        // Pages

        SolrDocumentList pageDocList = SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search("+" + SolrConstants.PI_TOPSTRUCT + ":" + pi + " +" + SolrConstants.DOCTYPE + ":" + DocType.PAGE, null);
        assertEquals(7, pageDocList.size());
        for (SolrDocument pageDoc : pageDocList) {
            Integer order = (Integer) pageDoc.getFieldValue(SolrConstants.ORDER);
            assertNotNull(order);
            String pageIddoc = (String) pageDoc.getFieldValue(SolrConstants.IDDOC);
            assertNotNull(pageIddoc);

            // Simple page metadata
            assertEquals("image/tiff", SolrSearchIndex.getSingleFieldStringValue(pageDoc, "MD_TECH_FORMAT_NAME"));
            assertEquals("little endian", SolrSearchIndex.getSingleFieldStringValue(pageDoc, "MD_TECH_BYTE_ORDER"));
            assertNotNull(pageDoc.getFieldValue("MD_TECH_BASIC_IMAGE_INFO"));

            // Grouped page metadata
            SolrDocumentList metadataDocList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(new StringBuilder().append("+")
                            .append(SolrConstants.DOCTYPE)
                            .append(":")
                            .append(DocType.METADATA.name())
                            .append(" +")
                            .append(SolrConstants.IDDOC_OWNER)
                            .append(":")
                            .append(pageIddoc)
                            .toString(), null);
            assertEquals(1, metadataDocList.size());
            SolrDocument doc = metadataDocList.get(0);
            assertEquals(pi, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            assertEquals("MD_TECH_BASIC_IMAGE_INFO", doc.getFieldValue(SolrConstants.LABEL));
            assertEquals("RGB", SolrSearchIndex.getSingleFieldStringValue(doc, "MD_TECH_IMAGE_COLOR_SPACE"));
            assertNotNull(SolrSearchIndex.getSingleFieldStringValue(doc, "MD_VALUE"));
            assertNotNull(SolrSearchIndex.getSingleFieldStringValue(doc, "MD_TECH_IMAGE_WIDTH"));
            assertNotNull(SolrSearchIndex.getSingleFieldStringValue(doc, "MD_TECH_IMAGE_HEIGHT"));
        }
    }

    /**
     * @see MetsIndexer#generatePageDocuments(JDomXP)
     * @verifies create documents for all mapped pages
     */
    @Test
    void generatePageDocuments_shouldCreateDocumentsForAllMappedPages() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        ISolrWriteStrategy writeStrategy = AbstractWriteStrategy.create(metsFile, new HashMap<>(), hotfolder);
        IDataRepositoryStrategy dataRepositoryStrategy = AbstractDataRepositoryStrategy.create(SolrIndexerDaemon.getInstance().getConfiguration());

        indexer.generatePageDocuments(writeStrategy, new HashMap<>(),
                dataRepositoryStrategy.selectDataRepository(PI, metsFile, null, SolrIndexerDaemon.getInstance().getSearchIndex(), null)[0], PI, 1,
                false);
        assertEquals(16, writeStrategy.getPageDocsSize());
    }

    /**
     * @see MetsIndexer#generatePageDocuments(JDomXP)
     * @verifies set correct ORDER values
     */
    @Test
    void generatePageDocuments_shouldSetCorrectORDERValues() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        ISolrWriteStrategy writeStrategy = AbstractWriteStrategy.create(metsFile, new HashMap<>(), hotfolder);
        IDataRepositoryStrategy dataRepositoryStrategy = AbstractDataRepositoryStrategy.create(SolrIndexerDaemon.getInstance().getConfiguration());
        indexer.generatePageDocuments(writeStrategy, new HashMap<>(),
                dataRepositoryStrategy.selectDataRepository(PI, metsFile, null, SolrIndexerDaemon.getInstance().getSearchIndex(), null)[0], PI, 1,
                false);
        for (int i = 1; i <= writeStrategy.getPageDocsSize(); ++i) {
            SolrInputDocument doc = writeStrategy.getPageDocForOrder(i);
            assertEquals(i, doc.getFieldValue(SolrConstants.ORDER));
        }
    }

    /**
     * @see MetsIndexer#getMetsCreateDate()
     * @verifies return CREATEDATE value
     */
    @Test
    void getMetsCreateDate_shouldReturnCREATEDATEValue() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile2);
        ZonedDateTime dateCreated = indexer.getMetsCreateDate();
        assertNotNull(dateCreated);
        assertEquals("2013-07-02T13:03:37", dateCreated.format(DateTools.FORMATTER_ISO8601_LOCALDATETIME));

    }

    /**
     * @see MetsIndexer#getMetsCreateDate()
     * @verifies return null if date does not exist in METS
     */
    @Test
    void getMetsCreateDate_shouldReturnNullIfDateDoesNotExistInMETS() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        ZonedDateTime dateCreated = indexer.getMetsCreateDate();
        Assertions.assertNull(dateCreated);
    }

    /**
     * @see MetsIndexer#generatePageDocument(Element,String,Integer,ISolrWriteStrategy,Map)
     * @verifies add FILENAME_HTML-SANDBOXED field for url paths
     */
    @Test
    void generatePageDocument_shouldAddFILENAME_HTMLSANDBOXEDFieldForUrlPaths() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(Paths.get("src/test/resources/METS/ppn750544996.dv.mets.xml"));
        String xpath = "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div";
        List<Element> eleStructMapPhysicalList = indexer.xp.evaluateToElements(xpath, null);
        assertNotNull(eleStructMapPhysicalList);
        Assertions.assertFalse(eleStructMapPhysicalList.isEmpty());

        int page = 1;
        indexer.setUseFileGroupGlobal(MetsIndexer.DEFAULT_FILEGROUP);
        IDataRepositoryStrategy dataRepositoryStrategy = AbstractDataRepositoryStrategy.create(SolrIndexerDaemon.getInstance().getConfiguration());
        PhysicalElement pe = indexer.generatePageDocument(MetsIndexer.DEFAULT_FILEGROUP, eleStructMapPhysicalList.get(page - 1),
                String.valueOf(MetsIndexer.getNextIddoc()), "ppn750544996", page, new HashMap<>(),
                dataRepositoryStrategy.selectDataRepository("ppn750544996", metsFile, new HashMap<>(),
                        SolrIndexerDaemon.getInstance().getSearchIndex(), null)[0],
                false);
        assertNotNull(pe);
        SolrInputDocument doc = pe.getDoc();
        assertNotNull(doc);
        assertEquals("http://rosdok.uni-rostock.de/iiif/image-api/rosdok%252Fppn750544996%252Fphys_0001/full/full/0/native.jpg",
                doc.getFieldValue("FILENAME_HTML-SANDBOXED"));
        assertEquals("http://rosdok.uni-rostock.de/iiif/image-api/rosdok%252Fppn750544996%252Fphys_0001/full/full/0/native.jpg",
                doc.getFieldValue(SolrConstants.FILENAME));
    }

    //    /**
    //     * @see MetsIndexer#generatePageDocument(Element,String,Integer,ISolrWriteStrategy,Map)
    //     * @verifies create ALTO file from fileId if none provided in data folders
    //     */
    //    @Test
    //    void generatePageDocument_shouldCreateALTOFileFromFileIdIfNoneProvidedInDataFolders() throws Exception {
    //        MetsIndexer indexer = new MetsIndexer(hotfolder);
    //        indexer.initJDomXP(Paths.get("src/test/resources/METS/ppn750542047_intrandatest.dv.mets.xml"));
    //        ISolrWriteStrategy writeStrategy = new LazySolrWriteStrategy(searchIndex);
    //        String xpath = "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div";
    //        List<Element> eleStructMapPhysicalList = indexer.xp.evaluateToElements(xpath, null);
    //        assertNotNull(eleStructMapPhysicalList);
    //        Assertions.assertFalse(eleStructMapPhysicalList.isEmpty());
    //
    //        Map<String, Path> dataFolders = new HashMap<>();
    //        Path altoPath = Paths.get("target/viewer/alto/750542047");
    //        Utils.checkAndCreateDirectory(altoPath);
    //        assertTrue(Files.isDirectory(altoPath));
    //        dataFolders.put(DataRepository.PARAM_ALTO_CONVERTED, altoPath);
    //
    //        int page = 5;
    //        IDataRepositoryStrategy dataRepositoryStrategy = new SingleRepositoryStrategy(SolrIndexerDaemon.getInstance().getConfiguration());
    //        assertTrue(indexer.generatePageDocument(eleStructMapPhysicalList.get(page - 1),
    //                String.valueOf(MetsIndexer.getNextIddoc(hotfolder.getsearchIndex())), "ppn750542047", page, writeStrategy, dataFolders,
    //                dataRepositoryStrategy.selectDataRepository("ppn750542047", metsFile, dataFolders, searchIndex)[0]));
    //        SolrInputDocument doc = writeStrategy.getPageDocForOrder(page);
    //        assertNotNull(doc);
    //        assertTrue(Files.isRegularFile(Paths.get(altoPath.toAbsolutePath().toString(), "00000005.xml")));
    //    }

    /**
     * @see MetsIndexer#generatePageDocument(Element,String,Integer,ISolrWriteStrategy,Map)
     * @verifies create ALTO file from TEI correctly
     */
    @Test
    void generatePageDocument_shouldCreateALTOFileFromTEICorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        Path altoPath = Paths.get("target/viewer/alto/PPN517154005");
        Utils.checkAndCreateDirectory(altoPath);
        assertTrue(Files.isDirectory(altoPath));
        dataFolders.put(DataRepository.PARAM_ALTO_CONVERTED, altoPath);
        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));

        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        indexer.setDataRepository(new DataRepository("build/viewer", true));
        String xpath = "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div";
        List<Element> eleStructMapPhysicalList = indexer.xp.evaluateToElements(xpath, null);
        IDataRepositoryStrategy dataRepositoryStrategy = AbstractDataRepositoryStrategy.create(SolrIndexerDaemon.getInstance().getConfiguration());

        int page = 1;
        indexer.setUseFileGroupGlobal(MetsIndexer.PRESENTATION_FILEGROUP);
        PhysicalElement pe = indexer.generatePageDocument(MetsIndexer.PRESENTATION_FILEGROUP, eleStructMapPhysicalList.get(page - 1),
                String.valueOf(MetsIndexer.getNextIddoc()), PI, page, dataFolders,
                dataRepositoryStrategy.selectDataRepository(PI, metsFile, dataFolders, SolrIndexerDaemon.getInstance().getSearchIndex(), null)[0],
                false);
        assertNotNull(pe);
        SolrInputDocument doc = pe.getDoc();
        assertNotNull(doc);
        assertTrue(Files.isRegularFile(Paths.get(altoPath.toAbsolutePath().toString(), "00000001.xml")));
    }

    /**
     * @see MetsIndexer#generatePageDocument(Element,String,Integer,ISolrWriteStrategy,Map)
     * @verifies add fulltext field correctly
     */
    @Test
    void generatePageDocument_shouldAddFulltextFieldCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));

        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        String xpath = "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div";
        List<Element> eleStructMapPhysicalList = indexer.xp.evaluateToElements(xpath, null);
        IDataRepositoryStrategy dataRepositoryStrategy = AbstractDataRepositoryStrategy.create(SolrIndexerDaemon.getInstance().getConfiguration());

        int page = 1;
        indexer.setUseFileGroupGlobal(MetsIndexer.PRESENTATION_FILEGROUP);
        PhysicalElement pe = indexer.generatePageDocument(MetsIndexer.PRESENTATION_FILEGROUP, eleStructMapPhysicalList.get(page - 1),
                String.valueOf(MetsIndexer.getNextIddoc()), PI, page, dataFolders,
                dataRepositoryStrategy.selectDataRepository(PI, metsFile, dataFolders, SolrIndexerDaemon.getInstance().getSearchIndex(), null)[0],
                false);
        assertNotNull(pe);
        SolrInputDocument doc = pe.getDoc();
        assertNotNull(doc);
        assertEquals("fulltext/" + PI + "/00000001.txt", doc.getFieldValue(SolrConstants.FILENAME_FULLTEXT));
        assertEquals(true, doc.getFieldValue(SolrConstants.FULLTEXTAVAILABLE));
    }

    /**
     * @see MetsIndexer#generatePageDocument(Element,String,Integer,ISolrWriteStrategy,Map)
     * @verifies add all basic fields
     */
    @Test
    void generatePageDocument_shouldAddAllBasicFields() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        String xpath = "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div";
        List<Element> eleStructMapPhysicalList = indexer.xp.evaluateToElements(xpath, null);
        IDataRepositoryStrategy dataRepositoryStrategy = AbstractDataRepositoryStrategy.create(SolrIndexerDaemon.getInstance().getConfiguration());

        int page = 3; //3 + DOWNLOAD elements 
        indexer.setUseFileGroupGlobal(MetsIndexer.PRESENTATION_FILEGROUP);
        PhysicalElement pe = indexer.generatePageDocument(MetsIndexer.PRESENTATION_FILEGROUP, eleStructMapPhysicalList.get(page - 1),
                String.valueOf(MetsIndexer.getNextIddoc()), PI, page, new HashMap<>(),
                dataRepositoryStrategy.selectDataRepository(PI, metsFile, null, SolrIndexerDaemon.getInstance().getSearchIndex(), null)[0], false);
        assertNotNull(pe);
        SolrInputDocument doc = pe.getDoc();
        assertNotNull(doc);
        assertNotNull(doc.getFieldValue(SolrConstants.IDDOC));
        assertNotNull(doc.getFieldValue(SolrConstants.GROUPFIELD));
        assertNotNull(doc.getFieldValue(SolrConstants.DOCTYPE), DocType.PAGE.name());
        assertEquals("image/tiff", doc.getFieldValue(SolrConstants.MIMETYPE));
        assertEquals("PHYS_0003", doc.getFieldValue(SolrConstants.PHYSID));
        assertEquals(3, doc.getFieldValue(SolrConstants.ORDER));
        assertEquals("1", doc.getFieldValue(SolrConstants.ORDERLABEL));
        assertEquals("urn:nbn:de:hebis:66:fuldig-2004", doc.getFieldValue(SolrConstants.IMAGEURN));
        assertNotNull(doc.getFieldValue(SolrConstants.FILENAME), "Filename null, should be 00000003.tif");
    }

    /**
     * @see MetsIndexer#anchorSuperupdate(Path,Path,DataRepository)
     * @verifies copy new METS file correctly
     */
    @Test
    void anchorSuperupdate_shouldCopyNewMETSFileCorrectly() throws Exception {
        Path viewerRootFolder = Paths.get("target/viewer");
        assertTrue(Files.isDirectory(viewerRootFolder));
        Path updatedMetsFolder = Paths.get(viewerRootFolder.toAbsolutePath().toString(), "updated_mets");
        assertTrue(Files.isDirectory(updatedMetsFolder));

        Path metsFile = Paths.get(viewerRootFolder.toAbsolutePath().toString(), "PPN123.UPDATED");
        Files.createFile(metsFile);
        assertTrue(Files.isRegularFile(metsFile));

        DataRepository dataRepository = new DataRepository(viewerRootFolder.toAbsolutePath().toString(), true);
        MetsIndexer.anchorSuperupdate(metsFile, updatedMetsFolder, dataRepository);

        Path newMetsFile = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), "PPN123.xml");
        assertTrue(Files.isRegularFile(newMetsFile));
    }

    /**
     * @see MetsIndexer#anchorSuperupdate(Path,Path,DataRepository)
     * @verifies copy old METS file to updated mets folder if file already exists
     */
    @Test
    void anchorSuperupdate_shouldCopyOldMETSFileToUpdatedMetsFolderIfFileAlreadyExists() throws Exception {
        Path viewerRootFolder = Paths.get("target/viewer");
        assertTrue(Files.isDirectory(viewerRootFolder));
        Path updatedMetsFolder = Paths.get(viewerRootFolder.toAbsolutePath().toString(), "updated_mets");
        assertTrue(Files.isDirectory(updatedMetsFolder));

        DataRepository dataRepository = new DataRepository(viewerRootFolder.toAbsolutePath().toString(), true);
        Path metsFile = Paths.get(viewerRootFolder.toAbsolutePath().toString(), "PPN123.UPDATED");
        Files.createFile(metsFile);
        assertTrue(Files.isRegularFile(metsFile));
        MetsIndexer.anchorSuperupdate(metsFile, updatedMetsFolder, dataRepository);

        Path newMetsFile = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), "PPN123.xml");
        assertTrue(Files.isRegularFile(newMetsFile));

        metsFile = Paths.get(viewerRootFolder.toAbsolutePath().toString(), "PPN123.UPDATED");
        Files.createFile(metsFile);
        assertTrue(Files.isRegularFile(metsFile));
        MetsIndexer.anchorSuperupdate(metsFile, updatedMetsFolder, dataRepository);

        String[] files = updatedMetsFolder.toFile().list();
        assertNotNull(files);
        assertEquals(1, files.length);
        assertTrue(files[0].startsWith("PPN123"));
        assertTrue(files[0].endsWith(".xml"));
    }

    /**
     * @see MetsIndexer#anchorSuperupdate(Path,Path,DataRepository)
     * @verifies remove anti-collision name parts
     */
    @Test
    void anchorSuperupdate_shouldRemoveAnticollisionNameParts() throws Exception {
        Path viewerRootFolder = Paths.get("target/viewer");
        assertTrue(Files.isDirectory(viewerRootFolder));
        Path updatedMetsFolder = Paths.get(viewerRootFolder.toAbsolutePath().toString(), "updated_mets");
        assertTrue(Files.isDirectory(updatedMetsFolder));

        Path metsFile = Paths.get(viewerRootFolder.toAbsolutePath().toString(), "PPN123#0.UPDATED");
        Files.createFile(metsFile);
        assertTrue(Files.isRegularFile(metsFile));

        DataRepository dataRepository = new DataRepository(viewerRootFolder.toAbsolutePath().toString(), true);
        MetsIndexer.anchorSuperupdate(metsFile, updatedMetsFolder, dataRepository);

        Path wrongMetsFile = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), "PPN123#0.xml");
        Assertions.assertFalse(Files.isRegularFile(wrongMetsFile));
        Path newMetsFile = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), "PPN123.xml");
        assertTrue(Files.isRegularFile(newMetsFile));
    }

    /**
     * @see MetsIndexer#parseCreateDate(String)
     * @verifies parse iso instant correctly
     */
    @Test
    void parseCreateDate_shouldParseIsoInstantCorrectly() {
        ZonedDateTime date = MetsIndexer.parseCreateDate("2021-09-21T16:05:00Z");
        assertNotNull(date);
    }

    /**
     * @see MetsIndexer#parseCreateDate(String)
     * @verifies parse iso local dateTime correctly
     */
    @Test
    void parseCreateDate_shouldParseIsoLocalDateTimeCorrectly() {
        ZonedDateTime date = MetsIndexer.parseCreateDate("2021-09-21T16:05:00");
        assertNotNull(date);
    }

    /**
     * @see MetsIndexer#parseCreateDate(String)
     * @verifies parse iso offset dateTime correctly
     */
    @Test
    void parseCreateDate_shouldParseIsoOffsetDateTimeCorrectly() {
        ZonedDateTime date = MetsIndexer.parseCreateDate("2021-09-21T16:05:00+01:00");
        assertNotNull(date);
    }

    /**
     * @see MetsIndexer#isVolume()
     * @verifies return true if record is volume
     */
    @Test
    void isVolume_shouldReturnTrueIfRecordIsVolume() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFileVol1);
        assertTrue(indexer.isVolume());
    }

    /**
     * @see MetsIndexer#isVolume()
     * @verifies return false if relatedItem not anchor
     */
    @Test
    void isVolume_shouldReturnFalseIfRelatedItemNotAnchor() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(Paths.get("src/test/resources/METS/oai5164685802946115306.xml"));
        Assertions.assertFalse(indexer.isVolume());
    }

    @SuppressWarnings("unchecked")
    @Test
    void index_shouldIndexDownloadResourcesCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_MEDIA, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_media"));
        Path metPath = Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_download_resources.xml").toAbsolutePath();
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        String[] ret = indexer.index(metPath, dataFolders, null, 1, false);
        assertEquals(PI + ".xml", ret[0]);
        Assertions.assertNull(ret[1]);

        SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":" + PI, null);

        assertEquals(3, docList.stream().filter(doc -> "DOWNLOAD_RESOURCE".equals(doc.getFieldValue(SolrConstants.DOCTYPE))).count());
        assertEquals(16, docList.stream().filter(doc -> "PAGE".equals(doc.getFieldValue(SolrConstants.DOCTYPE))).count());

    }

    @SuppressWarnings("unchecked")
    @Test
    void index_shouldIndexSingleDownloadResourceCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        Path metPath = Paths.get("src/test/resources/METS/download_resources/34192383.xml").toAbsolutePath();
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        String[] ret = indexer.index(metPath, dataFolders, null, 1, false);
        assertEquals("34192383.xml", ret[0]);
        Assertions.assertNull(ret[1]);

        SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":34192383", null);

        assertEquals(1, docList.stream().filter(doc -> "PAGE".equals(doc.getFieldValue(SolrConstants.DOCTYPE))).count());
        assertEquals(1, docList.stream().filter(doc -> "DOWNLOAD_RESOURCE".equals(doc.getFieldValue(SolrConstants.DOCTYPE))).count());
        assertEquals(1, docList.stream().filter(doc -> "DOCSTRCT".equals(doc.getFieldValue(SolrConstants.DOCTYPE))).count());
        assertTrue(docList.stream()
                .filter(doc -> "DOWNLOAD_RESOURCE".equals(doc.getFieldValue(SolrConstants.DOCTYPE)))
                .allMatch(doc -> doc.containsKey(SolrConstants.ACCESSCONDITION)));
    }

    @Test
    void index_shouldPassAccessConditionToDownloadResources() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        Path metPath = Paths.get("src/test/resources/METS/download_resources/34192383.xml").toAbsolutePath();
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        String[] ret = indexer.index(metPath, dataFolders, null, 1, false);
        assertEquals("34192383.xml", ret[0]);
        Assertions.assertNull(ret[1]);

        SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":34192383", null);
        assertTrue(docList.stream()
                .filter(doc -> "DOWNLOAD_RESOURCE".equals(doc.getFieldValue(SolrConstants.DOCTYPE)))
                .allMatch(doc -> doc.containsKey(SolrConstants.ACCESSCONDITION)));
    }

    @Test
    void index_shouldIndexNumDownloadResources() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        Path metPath = Paths.get("src/test/resources/METS/download_resources/34192383.xml").toAbsolutePath();
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        String[] ret = indexer.index(metPath, dataFolders, null, 1, false);
        assertEquals("34192383.xml", ret[0]);
        Assertions.assertNull(ret[1]);

        SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI_TOPSTRUCT + ":34192383", null);
        assertEquals(1, docList.stream().filter(doc -> "DOCSTRCT".equals(doc.getFieldValue(SolrConstants.DOCTYPE))).count());
        assertTrue(docList.stream()
                .filter(doc -> "DOCSTRCT".equals(doc.getFieldValue(SolrConstants.DOCTYPE)))
                .allMatch(doc -> Long.valueOf(1).equals(doc.getFieldValue(SolrConstants.MDNUM_DOWNLOAD_RESOURCES))));
    }

    @Test
    void index_shouldIndexImagesAndAudio() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        Path metPath = Paths.get("src/test/resources/audio/02008070428708.xml").toAbsolutePath();
        MetsIndexer indexer = new MetsIndexer(hotfolder, List.of("OGG", "MP3", "PRESENTATION"));
        String[] ret = indexer.index(metPath, dataFolders, null, 1, false);
        assertEquals("02008070428708.xml", ret[0]);
        Assertions.assertNull(ret[1]);

        SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search("+%s:%s +%s:%s".formatted(SolrConstants.PI_TOPSTRUCT, "02008070428708", SolrConstants.DOCTYPE, "PAGE"),
                        List.of("MIMETYPE", "FILENAME*", "ORDER"));
        Assertions.assertEquals(32, docList.size());
        Map<String, List<SolrDocument>> mimetypeMap = docList.stream()
                .collect(Collectors.toMap(doc -> doc.getFirstValue("MIMETYPE").toString(), doc -> new ArrayList<>(List.of(doc)),
                        (d1, d2) -> new ArrayList<>(CollectionUtils.union(d1, d2))));
        Assertions.assertEquals(22, mimetypeMap.get("image/tiff").size());
        Assertions.assertEquals(10, mimetypeMap.get("audio/ogg").size());

        Assertions.assertTrue(mimetypeMap.get("image/tiff").stream().allMatch(doc -> doc.getFieldValue("FILENAME").toString().matches("\\w+\\.tif")));
        Assertions.assertTrue(mimetypeMap.get("audio/ogg").stream().allMatch(doc -> doc.getFieldValue("FILENAME").toString().matches("\\w+\\.ogg")));

    }

}
