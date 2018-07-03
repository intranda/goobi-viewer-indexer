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
package de.intranda.digiverso.presentation.solr;

import java.awt.Dimension;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Element;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.ocr.alto.model.structureclasses.lineelements.Word;
import de.intranda.digiverso.ocr.alto.model.structureclasses.logical.AltoDocument;
import de.intranda.digiverso.presentation.solr.helper.Configuration;
import de.intranda.digiverso.presentation.solr.helper.Hotfolder;
import de.intranda.digiverso.presentation.solr.helper.JDomXP;
import de.intranda.digiverso.presentation.solr.helper.MetadataHelper;
import de.intranda.digiverso.presentation.solr.helper.Utils;
import de.intranda.digiverso.presentation.solr.model.SolrConstants;
import de.intranda.digiverso.presentation.solr.model.SolrConstants.DocType;
import de.intranda.digiverso.presentation.solr.model.datarepository.DataRepository;
import de.intranda.digiverso.presentation.solr.model.datarepository.strategy.IDataRepositoryStrategy;
import de.intranda.digiverso.presentation.solr.model.datarepository.strategy.SingleRepositoryStrategy;
import de.intranda.digiverso.presentation.solr.model.writestrategy.ISolrWriteStrategy;
import de.intranda.digiverso.presentation.solr.model.writestrategy.LazySolrWriteStrategy;

public class MetsIndexerTest extends AbstractSolrEnabledTest {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(MetsIndexerTest.class);

    private static final String PI = "PPN517154005";
    private static final String PI2 = "H030001";

    private static Hotfolder hotfolder;

    private Path metsFile;
    private Path metsFile2;
    private Path metsFile3;
    private Path metsFileVol1;
    private Path metsFileVol2;
    private Path metsFileAnchor1;
    private Path metsFileAnchor2;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        hotfolder = new Hotfolder("resources/test/indexerconfig_solr_test.xml", server);

        metsFile = Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        Assert.assertTrue(Files.isRegularFile(metsFile));
        metsFile2 = Paths.get("resources/test/METS/H030001_mets.xml");
        Assert.assertTrue(Files.isRegularFile(metsFile2));
        metsFile3 = Paths.get("resources/test/METS/AC06736966.xml");
        Assert.assertTrue(Files.isRegularFile(metsFile3));
        metsFileVol1 = Paths.get("resources/test/METS/baltst_559838239/baltst_559838239_NF_75.xml");
        Assert.assertTrue(Files.isRegularFile(metsFileVol1));
        metsFileVol2 = Paths.get("resources/test/METS/baltst_559838239/baltst_559838239_NF_78.xml");
        Assert.assertTrue(Files.isRegularFile(metsFileVol2));
        metsFileAnchor1 = Paths.get("resources/test/METS/baltst_559838239/baltst_559838239_NF_75_anchor.xml");
        Assert.assertTrue(Files.isRegularFile(metsFileAnchor1));
        metsFileAnchor2 = Paths.get("resources/test/METS/baltst_559838239/baltst_559838239_NF_78_anchor.xml");
        Assert.assertTrue(Files.isRegularFile(metsFileAnchor2));
    }

    /**
     * @see MetsIndexer#MetsIndexer(Hotfolder)
     * @verifies set attributes correctly
     */
    @Test
    public void MetsIndexer_shouldSetAttributesCorrectly() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        Assert.assertEquals(hotfolder, indexer.hotfolder);
    }

    /**
     * @see MetsIndexer#index(File,ISolrWriteStrategy,boolean,Map)
     * @verifies index record correctly
     */
    @SuppressWarnings("unchecked")
    @Test
    public void index_shouldIndexRecordCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        dataFolders.put(DataRepository.PARAM_OVERVIEW, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_overview"));
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, false, dataFolders, null, 1);
        Assert.assertEquals(PI + ".xml", ret[0]);
        Assert.assertNull(ret[1]);

        Map<String, Boolean> iddocMap = new HashMap<>();
        String iddoc;

        // Top document
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + PI, null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            {
                Collection<Object> values = doc.getFieldValues(SolrConstants.ACCESSCONDITION);
                Assert.assertNotNull(values);
                Assert.assertEquals(1, values.size());
                Assert.assertEquals("OPENACCESS", values.iterator().next());
            }
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            Assert.assertNotNull(doc.getFieldValues(SolrConstants.DATEUPDATED));
            Assert.assertEquals(1, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
            Assert.assertEquals(doc.getFieldValue(SolrConstants.DATECREATED), doc.getFirstValue(SolrConstants.DATEUPDATED));
            Assert.assertEquals(DocType.DOCSTRCT.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
            Assert.assertEquals("Monograph", doc.getFieldValue(SolrConstants.DOCSTRCT));
            {
                List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
                Assert.assertNotNull(mdList);
                Assert.assertEquals(2, mdList.size());
                Assert.assertEquals("varia", mdList.get(0));
                Assert.assertEquals("digiwunschbuch", mdList.get(1));
            }
            Assert.assertTrue(doc.containsKey(SolrConstants.DEFAULT));
            // Assert.assertTrue(doc.containsKey(SolrConstants.SUPERDEFAULT));
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            Assert.assertNotNull(iddoc);
            iddocMap.put(iddoc, true);
            Assert.assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
            Assert.assertEquals(true, doc.getFieldValue(SolrConstants.ISWORK));
            Assert.assertEquals("Universität und Technische Hochschule", doc.getFieldValue(SolrConstants.LABEL));
            Assert.assertEquals("LOG_0000", doc.getFieldValue(SolrConstants.LOGID));
            Assert.assertEquals(16, doc.getFieldValue(SolrConstants.NUMPAGES));
            Assert.assertEquals(PI, doc.getFieldValue(SolrConstants.PI));
            Assert.assertEquals(PI, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            Assert.assertEquals(SolrConstants._METS, doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
            Assert.assertEquals("00000002.tif", doc.getFieldValue(SolrConstants.THUMBNAIL)); // representative image is set
            Assert.assertEquals("00000002.tif", doc.getFieldValue(SolrConstants.THUMBNAILREPRESENT)); // not really used
            Assert.assertEquals(2, doc.getFieldValue(SolrConstants.THUMBPAGENO)); // representative image should not affect the number
            Assert.assertEquals(" - ", doc.getFieldValue(SolrConstants.THUMBPAGENOLABEL));
            Assert.assertEquals("urn:nbn:de:hebis:66:fuldig-1946", doc.getFieldValue(SolrConstants.URN));
            Assert.assertNull(doc.getFieldValue(SolrConstants.IMAGEURN_OAI)); // only docs representing deleted records should have this field
            Assert.assertEquals("http://opac.sub.uni-goettingen.de/DB=1/PPN?PPN=517154005", doc.getFieldValue(SolrConstants.OPACURL));
            Assert.assertEquals(true, doc.getFieldValue(SolrConstants.FULLTEXTAVAILABLE));
            {
                List<String> mdList = (List<String>) doc.getFieldValue("MD_AUTHOR");
                Assert.assertNotNull(mdList);
                Assert.assertEquals(1, mdList.size());
                Assert.assertEquals("Klein, Felix", mdList.get(0));
            }
            {
                List<String> mdList = (List<String>) doc.getFieldValue("MD_AUTHOR" + SolrConstants._UNTOKENIZED);
                Assert.assertNotNull(mdList);
                Assert.assertEquals(1, mdList.size());
                Assert.assertEquals("Klein, Felix", mdList.get(0));
            }
            Assert.assertEquals("Klein, Felix", doc.getFieldValue("SORT_AUTHOR"));

            {
                List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.YEAR);
                Assert.assertNotNull(mdList);
                Assert.assertEquals(1, mdList.size());
                Assert.assertEquals(Long.valueOf(1980), mdList.get(0));
                Assert.assertEquals(Long.valueOf(1980), doc.getFieldValue(SolrConstants.SORTNUM_ + SolrConstants.YEAR));
            }
            {
                List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.YEARMONTH);
                Assert.assertNotNull(mdList);
                Assert.assertEquals(1, mdList.size());
                Assert.assertEquals(Long.valueOf(198007), mdList.get(0));
                Assert.assertEquals(Long.valueOf(198007), doc.getFieldValue(SolrConstants.SORTNUM_ + SolrConstants.YEARMONTH));
            }
            {
                List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.YEARMONTHDAY);
                Assert.assertNotNull(mdList);
                Assert.assertEquals(1, mdList.size());
                Assert.assertEquals(Long.valueOf(19800710), mdList.get(0));
                Assert.assertEquals(Long.valueOf(19800710), doc.getFieldValue(SolrConstants.SORTNUM_ + SolrConstants.YEARMONTHDAY));
            }
            {
                List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.CENTURY);
                Assert.assertNotNull(mdList);
                Assert.assertEquals(1, mdList.size());
                Assert.assertEquals(Long.valueOf(20), mdList.get(0));
                Assert.assertEquals(Long.valueOf(20), doc.getFieldValue(SolrConstants.SORTNUM_ + SolrConstants.CENTURY));
            }

            // TODO techMD
            // TODO 
        }

        // Child docstructs
        {
            SolrDocumentList docList = hotfolder.getSolrHelper()
                    .search(new StringBuilder(SolrConstants.PI_TOPSTRUCT).append(":")
                            .append(PI)
                            .append(" AND ")
                            .append(SolrConstants.IDDOC_PARENT)
                            .append(":*")
                            .toString(), null);
            Assert.assertEquals(3, docList.size());

            Map<String, Boolean> logIdMap = new HashMap<>();

            for (SolrDocument doc : docList) {
                {
                    Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                    Assert.assertNotNull(values);
                    Assert.assertEquals(1, values.size());
                    Assert.assertEquals("OPENACCESS", values.iterator().next());
                }
                {
                    //                    Collection<Object> values = doc.getFieldValues(SolrConstants.DC);
                    //                    Assert.assertNotNull(values);
                    //                    Assert.assertEquals(2, values.size());
                }
                Assert.assertTrue(doc.containsKey(SolrConstants.DEFAULT));
                Assert.assertEquals(DocType.DOCSTRCT.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
                Assert.assertNotNull(doc.getFieldValue(SolrConstants.DOCSTRCT));
                {
                    List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
                    Assert.assertNotNull(mdList);
                    Assert.assertEquals(2, mdList.size());
                    Assert.assertEquals("varia", mdList.get(0));
                    Assert.assertEquals("digiwunschbuch", mdList.get(1));
                }
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assert.assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    Assert.assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
                Assert.assertEquals(iddoc, doc.getFieldValue(SolrConstants.IDDOC_TOPSTRUCT));
                {
                    String value = (String) doc.getFieldValue(SolrConstants.LOGID);
                    Assert.assertNotNull(value);
                    Assert.assertNull(logIdMap.get(value));
                    logIdMap.put(value, true);
                }
                Assert.assertNotNull(doc.getFieldValue(SolrConstants.THUMBNAIL));
                Assert.assertNotNull(doc.getFieldValue(SolrConstants.THUMBPAGENO));
                Assert.assertNotNull(doc.getFieldValue(SolrConstants.THUMBPAGENOLABEL));
                Assert.assertNotNull(doc.getFieldValue(SolrConstants.NUMPAGES));
                Assert.assertNotNull(doc.getFieldValue(SolrConstants.URN));
                Assert.assertNotNull(doc.getFieldValues(SolrConstants.DATEUPDATED));
            }
        }

        // Pages
        {
            SolrDocumentList docList = hotfolder.getSolrHelper()
                    .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.DOCTYPE + ":" + DocType.PAGE, null);
            Assert.assertEquals(16, docList.size());

            for (SolrDocument doc : docList) {
                {
                    Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                    Assert.assertNotNull(values);
                    Assert.assertEquals(1, values.size());
                    Assert.assertEquals("OPENACCESS", values.iterator().next());
                }
                {
                    Collection<Object> values = doc.getFieldValues(SolrConstants.DC);
                    Assert.assertNotNull(values);
                    Assert.assertEquals(2, values.size());
                }
                Integer order = (Integer) doc.getFieldValue(SolrConstants.ORDER);
                Assert.assertNotNull(order);
                String fileName = (String) doc.getFieldValue(SolrConstants.FILENAME);
                Assert.assertNotNull(fileName);
                Assert.assertNotNull(doc.getFieldValue(SolrConstants.DOCSTRCT));
                Assert.assertNotNull(doc.getFieldValue(SolrConstants.IMAGEURN));
                Assert.assertNotNull(doc.getFieldValue(SolrConstants.FILEIDROOT));
                {
                    String value = (String) doc.getFieldValue(SolrConstants.FILENAME_FULLTEXT);
                    Assert.assertNotNull(value);
                    Assert.assertEquals("fulltext/PPN517154005/" + FilenameUtils.getBaseName(fileName) + AbstractIndexer.TXT_EXTENSION, value);
                    Assert.assertEquals(true, doc.getFieldValue(SolrConstants.FULLTEXTAVAILABLE));
                }
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assert.assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    Assert.assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
                Assert.assertNotNull(doc.getFieldValue(SolrConstants.IDDOC_OWNER));
                // DATEUPDATED from the top docstruct
                Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATEUPDATED));
                // SORT_* fields from the direct owner docstruct
                if (order == 2) {
                    Assert.assertEquals("Universität und Technische Hochschule", doc.getFieldValue("SORT_TITLE"));
                }
            }
        }
    }

    /**
     * @see MetsIndexer#index(File,ISolrWriteStrategy,boolean,Map)
     * @verifies index aggregated metadata correctly
     */
    @SuppressWarnings("unchecked")
    @Test
    public void index_shouldIndexAggregatedMetadataCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        String[] ret = new MetsIndexer(hotfolder).index(metsFile2, false, dataFolders, null, 1);
        Assert.assertEquals(PI2 + ".xml", ret[0]);
        Assert.assertNull(ret[1]);

        Map<String, Boolean> iddocMap = new HashMap<>();
        String iddoc;

        // Top document
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + PI2, null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            {
                Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                Assert.assertNotNull(values);
                Assert.assertEquals(1, values.size());
                Assert.assertEquals("OPENACCESS", values.iterator().next());
            }
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATEUPDATED));
            Assert.assertEquals(DocType.DOCSTRCT.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DOCSTRCT));
            Assert.assertTrue(doc.getFieldValues(SolrConstants.DC) != null && doc.getFieldValues(SolrConstants.DC).size() == 1);
            Assert.assertEquals("casualia", doc.getFieldValues(SolrConstants.DC).iterator().next());
            Assert.assertTrue(doc.containsKey(SolrConstants.DEFAULT));
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            Assert.assertNotNull(iddoc);
            iddocMap.put(iddoc, true);
            Assert.assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
            Assert.assertEquals(true, doc.getFieldValue(SolrConstants.ISWORK));
            Assert.assertNull(doc.getFieldValue(SolrConstants.NUMPAGES));
            Assert.assertEquals(PI2, doc.getFieldValue(SolrConstants.PI));
            Assert.assertEquals(PI2, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            Assert.assertEquals(SolrConstants._METS, doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
        }

        // Grouped metadata
        {
            SolrDocumentList docList = hotfolder.getSolrHelper()
                    .search(new StringBuilder(SolrConstants.DOCTYPE).append(":")
                            .append(DocType.METADATA.name())
                            .append(" AND ")
                            .append(SolrConstants.IDDOC_OWNER)
                            .append(":")
                            .append(iddoc)
                            .toString(), null);
            Assert.assertEquals(4, docList.size());

            for (SolrDocument doc : docList) {
                Assert.assertTrue("MD_CREATOR".equals(doc.getFieldValue(SolrConstants.LABEL))
                        || "MD_PHYSICALCOPY".equals(doc.getFieldValue(SolrConstants.LABEL)));
                Assert.assertNotNull(doc.getFieldValue(SolrConstants.GROUPFIELD));
                if ("MD_CREATOR".equals(doc.getFieldValue(SolrConstants.LABEL))) {
                    Assert.assertNotNull(doc.getFieldValue("MD_VALUE"));
                    Assert.assertNotNull(doc.getFieldValue("MD_FIRSTNAME"));
                    Assert.assertNotNull(doc.getFieldValue("MD_LASTNAME"));
                    Assert.assertNotNull(doc.getFieldValue("NORM_URI"));
                    Assert.assertNotNull(doc.getFieldValue(SolrConstants.DEFAULT));
                } else if ("MD_PHYSICALCOPY".equals(doc.getFieldValue(SolrConstants.LABEL))) {
                    Assert.assertNotNull(doc.getFieldValue("MD_LOCATION"));
                    Assert.assertNotNull(doc.getFieldValue("MD_SHELFMARK"));
                    //                    Assert.assertNotNull(doc.getFieldValue(SolrConstants.NORMDATATERMS));
                }
            }
        }

        // Pages
        {
            SolrDocumentList docList =
                    hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + PI2 + " AND " + SolrConstants.FILENAME + ":*", null);
            Assert.assertEquals(0, docList.size());
        }
    }

    /**
     * @see MetsIndexer#index(File,ISolrWriteStrategy,boolean,Map)
     * @verifies index multi volume records correctly
     */
    @Test
    public void index_shouldIndexMultiVolumeRecordsCorrectly() throws Exception {
        String piVol1 = "PPN612054551";
        String piVol2 = "PPN612060039";
        String piAnchor = "PPN559838239";

        // Index first volume
        Map<String, Path> dataFolders = new HashMap<>();
        String[] ret = new MetsIndexer(hotfolder).index(metsFileVol1, false, dataFolders, null, 1);
        Assert.assertEquals(piVol1 + ".xml", ret[0]);
        Assert.assertNull(ret[1]);

        SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + piVol1, null);
        Assert.assertEquals(1, docList.size());
        SolrDocument doc = docList.get(0);
        Assert.assertEquals(piAnchor, doc.getFieldValue(SolrConstants.PI_PARENT));
        Assert.assertEquals(piAnchor, doc.getFieldValue(SolrConstants.PI_ANCHOR));

        // All child docstructs should have the PI_ANCHOR field
        SolrDocumentList vol1Chidlren = hotfolder.getSolrHelper()
                .search(SolrConstants.PI_TOPSTRUCT + ":" + piVol1 + " AND " + SolrConstants.PI_ANCHOR + ":" + piAnchor, null);
        Assert.assertEquals(95, vol1Chidlren.size());

        // Index anchor
        String anchorIddoc;
        {
            ret = new MetsIndexer(hotfolder).index(metsFileAnchor1, false, dataFolders, null, 1);
            Assert.assertEquals(piAnchor + ".xml", ret[0]);
            Assert.assertNull(ret[1]);
            docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + piAnchor, null);
            Assert.assertEquals(1, docList.size());
            doc = docList.get(0);
            anchorIddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            Assert.assertNotNull(anchorIddoc);
            Assert.assertEquals(anchorIddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
        }

        // Re-index first volume
        {
            ret = new MetsIndexer(hotfolder).index(metsFileVol1, false, dataFolders, null, 1);
            Assert.assertNull(ret[1]);
            docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + piVol1, null);
            Assert.assertEquals(1, docList.size());
            doc = docList.get(0);
            Assert.assertEquals(anchorIddoc, doc.getFieldValue(SolrConstants.IDDOC_PARENT));
        }

        // Index second volume
        {
            ret = new MetsIndexer(hotfolder).index(metsFileVol2, false, dataFolders, null, 1);
            Assert.assertNull(ret[1]);
            docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + piVol2, null);
            Assert.assertEquals(1, docList.size());
            doc = docList.get(0);
            Assert.assertEquals(anchorIddoc, doc.getFieldValue(SolrConstants.IDDOC_PARENT));
        }

        Assert.assertEquals(1, hotfolder.getReindexQueue().size());

        ret = new MetsIndexer(hotfolder).index(hotfolder.getReindexQueue().poll(), true, dataFolders, null, 1);
        Assert.assertEquals(piAnchor + ".xml", ret[0]);
        Assert.assertNull(ret[1]);
    }

    /**
     * @see MetsIndexer#index(File,ISolrWriteStrategy,boolean,Map)
     * @verifies update record correctly
     */
    @Test
    public void index_shouldUpdateRecordCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, false, dataFolders, null, 1);
        Assert.assertEquals(PI + ".xml", ret[0]);
        Assert.assertNull(ret[1]);

        Map<String, Boolean> iddocMap = new HashMap<>();
        String iddoc;
        long dateCreated;
        Collection<Object> dateUpdated;

        // Top document
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + PI, null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            dateCreated = (Long) doc.getFieldValue(SolrConstants.DATECREATED);
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATEUPDATED));
            dateUpdated = doc.getFieldValues(SolrConstants.DATEUPDATED);
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            Assert.assertNotNull(iddoc);
            iddocMap.put(iddoc, true);
            Assert.assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
        }

        // Child docstructs
        {
            SolrDocumentList docList =
                    hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.IDDOC_PARENT + ":*", null);
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assert.assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    Assert.assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
            }
        }

        // Pages
        {
            SolrDocumentList docList =
                    hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.FILENAME + ":*", null);
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assert.assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    Assert.assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
            }
        }

        // Re-index
        ret = new MetsIndexer(hotfolder).index(metsFile, false, dataFolders, null, 1);
        Assert.assertEquals(PI + ".xml", ret[0]);
        Assert.assertNull(ret[1]);

        // Top document
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + PI, null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            Assert.assertEquals(dateCreated, doc.getFieldValue(SolrConstants.DATECREATED));
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATEUPDATED));
            Assert.assertEquals(dateUpdated.size() + 1, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            Assert.assertNotNull(iddoc);
            Assert.assertNull(iddocMap.get(iddoc));
            iddocMap.put(iddoc, true);
            Assert.assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
        }

        // Child docstructs
        {
            SolrDocumentList docList =
                    hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.IDDOC_PARENT + ":*", null);
            Assert.assertEquals(3, docList.size());
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assert.assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    Assert.assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
            }
        }

        // Pages
        {
            SolrDocumentList docList = hotfolder.getSolrHelper()
                    .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.DOCTYPE + ":" + DocType.PAGE.name(), null);
            Assert.assertEquals(16, docList.size());
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assert.assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    Assert.assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
            }
        }
    }

    /**
     * @see MetsIndexer#index(Path,boolean,Map,ISolrWriteStrategy,int)
     * @verifies set access conditions correctly
     */
    @Test
    public void index_shouldSetAccessConditionsCorrectly() throws Exception {
        String pi = "AC06736966";
        Map<String, Path> dataFolders = new HashMap<>();
        String[] ret = new MetsIndexer(hotfolder).index(metsFile3, false, dataFolders, null, 1);
        Assert.assertEquals(pi + ".xml", ret[0]);
        Assert.assertNull(ret[1]);

        // Top document
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + pi, null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Collection<Object> values = doc.getFieldValues(SolrConstants.ACCESSCONDITION);
            Assert.assertNotNull(values);
            Assert.assertEquals(1, values.size());
            Assert.assertEquals("OPENACCESS", values.iterator().next());
        }

        // Child docstructs
        {
            SolrDocumentList docList = hotfolder.getSolrHelper()
                    .search(new StringBuilder(SolrConstants.PI_TOPSTRUCT).append(":")
                            .append(pi)
                            .append(" AND ")
                            .append(SolrConstants.LOGID)
                            .append(":LOG_0035")
                            .toString(), null);
            Assert.assertEquals(1, docList.size());
            for (SolrDocument doc : docList) {
                Collection<Object> values = doc.getFieldValues(SolrConstants.ACCESSCONDITION);
                Assert.assertNotNull(values);
                Assert.assertEquals(1, values.size());
                Assert.assertEquals("bib_network", values.iterator().next());
            }
        }

        // Pages
        {
            SolrDocumentList docList =
                    hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + pi + " AND " + SolrConstants.PHYSID + ":PHYS_0032", null);
            Assert.assertEquals(1, docList.size());
            for (SolrDocument doc : docList) {
                Collection<Object> values = doc.getFieldValues(SolrConstants.ACCESSCONDITION);
                Assert.assertNotNull(values);
                Assert.assertEquals(1, values.size());
                Assert.assertEquals("bib_network", values.iterator().next());
            }
        }
    }

    /**
     * @see MetsIndexer#index(Path,boolean,Map,ISolrWriteStrategy,int)
     * @verifies write overview page texts into index
     */
    @Test
    public void index_shouldWriteOverviewPageTextsIntoIndex() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_OVERVIEW, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_overview"));
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, false, dataFolders, null, 1);

        {
            // Overview page description is not stored and must be searched for (and should have any HTML tags removed)
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.OVERVIEWPAGE_DESCRIPTION + ":\"test description\"", null);
            Assert.assertEquals(1, docList.size());
        }
        {
            // Overview page publication text is not stored and must be searched for (and should have any HTML tags removed)
            SolrDocumentList docList =
                    hotfolder.getSolrHelper().search(SolrConstants.OVERVIEWPAGE_PUBLICATIONTEXT + ":\"test publication text\"", null);
            Assert.assertEquals(1, docList.size());
        }
    }

    /**
     * @see MetsIndexer#generatePageDocuments(JDomXP)
     * @verifies create documents for all mapped pages
     */
    @Test
    public void generatePageDocuments_shouldCreateDocumentsForAllMappedPages() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        ISolrWriteStrategy writeStrategy = new LazySolrWriteStrategy(solrHelper);
        IDataRepositoryStrategy dataRepositoryStrategy = new SingleRepositoryStrategy(Configuration.getInstance());
        indexer.generatePageDocuments(writeStrategy, null, dataRepositoryStrategy.selectDataRepository(PI, metsFile, null, solrHelper)[0], PI, 1);
        Assert.assertEquals(16, writeStrategy.getPageDocsSize());
    }

    /**
     * @see MetsIndexer#generatePageDocuments(JDomXP)
     * @verifies set correct ORDER values
     */
    @Test
    public void generatePageDocuments_shouldSetCorrectORDERValues() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        ISolrWriteStrategy writeStrategy = new LazySolrWriteStrategy(solrHelper);
        IDataRepositoryStrategy dataRepositoryStrategy = new SingleRepositoryStrategy(Configuration.getInstance());
        indexer.generatePageDocuments(writeStrategy, null, dataRepositoryStrategy.selectDataRepository(PI, metsFile, null, solrHelper)[0], PI, 1);
        for (int i = 1; i <= writeStrategy.getPageDocsSize(); ++i) {
            SolrInputDocument doc = writeStrategy.getPageDocForOrder(i);
            Assert.assertEquals(i, doc.getFieldValue(SolrConstants.ORDER));
        }
    }

    //    @Test
    @Deprecated
    public void deskewAlto_handleMissingFilename() throws Exception {

        //        String filename = "AC04987957_00000124";
        String[] filenames = { "00000005.tif", "00225231.png" };

        int[] imageWidth = { 4966, 2794 };

        MetsIndexer indexer = new MetsIndexer(hotfolder);
        File dataFolder = new File("resources/test/alto_deskew");

        int i = 0;
        for (String filename : filenames) {

            String baseFilename = FilenameUtils.getBaseName(filename);

            File altoFile = new File(dataFolder, baseFilename + ".xml");
            String origAltoString = FileUtils.readFileToString(altoFile);
            File outputFolder = new File(dataFolder, "output");
            if (outputFolder.isDirectory()) {
                FileUtils.deleteDirectory(outputFolder);
            }
            outputFolder.mkdir();

            Map<String, Path> dataFolders = new HashMap<>();
            dataFolders.put(DataRepository.PARAM_MEDIA, Paths.get(dataFolder.getAbsolutePath()));

            SolrInputDocument doc = new SolrInputDocument();
            doc.setField(SolrConstants.ALTO, origAltoString);
            doc.setField(SolrConstants.WIDTH, "" + imageWidth[i]);
            //            doc.setField(SolrConstants.FILENAME, filename);

            MetsIndexer.deskewAlto(dataFolders, doc);
            String deskewedAltoString = (String) doc.getFieldValue(SolrConstants.ALTO);
            AltoDocument deskewedDoc = AltoDocument.getDocumentFromString(deskewedAltoString);
            FileUtils.writeStringToFile(new File(outputFolder, filename + ".xml"), deskewedAltoString, false);
            Word testWord = (Word) deskewedDoc.getFirstPage().getAllWordsAsList().get(1);
            Assert.assertNotNull(testWord);
            if (filename.equals("00000005.tif")) {
                Assert.assertEquals("Name", testWord.getContent());
                //                Assert.assertEquals(new Rectangle2D.Float(327, 765, 228, 54), testWord.getRect());
                Assert.assertEquals("Tag0", testWord.getAttributeValue("TAGREFS"));
                Assert.assertEquals("Tag0", deskewedDoc.getTags().getTags().getChild("NamedEntityTag", null).getAttributeValue("ID"));
            }

            i++;
        }
    }

    @Test
    public void testGetSize() throws Exception {

        //        String filename = "AC04987957_00000124";
        String[] filenames = { "00000005.tif", "00225231.png" };

        Dimension[] imageSizes = { new Dimension(4678, 6205), new Dimension(2794, 3838) };

        MetsIndexer indexer = new MetsIndexer(hotfolder);
        File dataFolder = new File("resources/test/image_size");

        int i = 0;
        File outputFolder = new File(dataFolder, "output");
        try {
            for (String filename : filenames) {
                if (outputFolder.isDirectory()) {
                    FileUtils.deleteDirectory(outputFolder);
                }
                outputFolder.mkdirs();

                Map<String, Path> dataFolders = new HashMap<>();
                dataFolders.put(DataRepository.PARAM_MEDIA, Paths.get(dataFolder.getAbsolutePath()));

                SolrInputDocument doc = new SolrInputDocument();
                doc.setField(SolrConstants.FILENAME, filename);

                Optional<Dimension> dim = MetsIndexer.getSize(dataFolders, doc);
                Assert.assertTrue(dim.isPresent());
                Assert.assertEquals(imageSizes[i], dim.get());

                i++;
            }
        } finally {
            if (outputFolder.isDirectory()) {
                FileUtils.deleteDirectory(outputFolder);
            }
        }
    }

    /**
     * @see MetsIndexer#getMetsCreateDate()
     * @verifies return CREATEDATE value
     */
    @Test
    public void getMetsCreateDate_shouldReturnCREATEDATEValue() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile2);
        Date dateCreated = indexer.getMetsCreateDate();
        Assert.assertNotNull(dateCreated);
        Assert.assertEquals("2013-07-02T15:03:37", MetadataHelper.formatterISO8601Full.print(dateCreated.getTime()));

    }

    /**
     * @see MetsIndexer#getMetsCreateDate()
     * @verifies return null if date does not exist in METS
     */
    @Test
    public void getMetsCreateDate_shouldReturnNullIfDateDoesNotExistInMETS() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        Date dateCreated = indexer.getMetsCreateDate();
        Assert.assertNull(dateCreated);
    }

    /**
     * @see MetsIndexer#generatePageDocument(Element,String,Integer,ISolrWriteStrategy,Map)
     * @verifies add FILENAME_HTML-SANDBOXED field for url paths
     */
    @Test
    public void generatePageDocument_shouldAddFILENAME_HTMLSANDBOXEDFieldForUrlPaths() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(Paths.get("resources/test/METS/ppn750544996.dv.mets.xml"));
        ISolrWriteStrategy writeStrategy = new LazySolrWriteStrategy(solrHelper);
        String xpath = "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div";
        List<Element> eleStructMapPhysicalList = indexer.xp.evaluateToElements(xpath, null);
        Assert.assertNotNull(eleStructMapPhysicalList);
        Assert.assertFalse(eleStructMapPhysicalList.isEmpty());

        int page = 1;
        IDataRepositoryStrategy dataRepositoryStrategy = new SingleRepositoryStrategy(Configuration.getInstance());
        Assert.assertTrue(indexer.generatePageDocument(eleStructMapPhysicalList.get(page - 1),
                String.valueOf(MetsIndexer.getNextIddoc(hotfolder.getSolrHelper())), "ppn750544996", page, writeStrategy, null,
                dataRepositoryStrategy.selectDataRepository("ppn750542047", metsFile, null, solrHelper)[0]));
        SolrInputDocument doc = writeStrategy.getPageDocForOrder(page);
        Assert.assertNotNull(doc);
        Assert.assertEquals("http://rosdok.uni-rostock.de/iiif/image-api/rosdok%252Fppn750544996%252Fphys_0001/full/full/0/native.jpg",
                doc.getFieldValue("FILENAME_HTML-SANDBOXED"));
        Assert.assertEquals("http://rosdok.uni-rostock.de/iiif/image-api/rosdok%252Fppn750544996%252Fphys_0001/full/full/0/native.jpg",
                doc.getFieldValue(SolrConstants.FILENAME));
    }

//    /**
//     * @see MetsIndexer#generatePageDocument(Element,String,Integer,ISolrWriteStrategy,Map)
//     * @verifies create ALTO file from fileId if none provided in data folders
//     */
//    @Test
//    public void generatePageDocument_shouldCreateALTOFileFromFileIdIfNoneProvidedInDataFolders() throws Exception {
//        MetsIndexer indexer = new MetsIndexer(hotfolder);
//        indexer.initJDomXP(Paths.get("resources/test/METS/ppn750542047_intrandatest.dv.mets.xml"));
//        ISolrWriteStrategy writeStrategy = new LazySolrWriteStrategy(solrHelper);
//        String xpath = "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div";
//        List<Element> eleStructMapPhysicalList = indexer.xp.evaluateToElements(xpath, null);
//        Assert.assertNotNull(eleStructMapPhysicalList);
//        Assert.assertFalse(eleStructMapPhysicalList.isEmpty());
//
//        Map<String, Path> dataFolders = new HashMap<>();
//        Path altoPath = Paths.get("build/viewer/alto/750542047");
//        Utils.checkAndCreateDirectory(altoPath);
//        Assert.assertTrue(Files.isDirectory(altoPath));
//        dataFolders.put(DataRepository.PARAM_ALTO_CONVERTED, altoPath);
//
//        int page = 5;
//        IDataRepositoryStrategy dataRepositoryStrategy = new SingleRepositoryStrategy(Configuration.getInstance());
//        Assert.assertTrue(indexer.generatePageDocument(eleStructMapPhysicalList.get(page - 1),
//                String.valueOf(MetsIndexer.getNextIddoc(hotfolder.getSolrHelper())), "ppn750542047", page, writeStrategy, dataFolders,
//                dataRepositoryStrategy.selectDataRepository("ppn750542047", metsFile, dataFolders, solrHelper)[0]));
//        SolrInputDocument doc = writeStrategy.getPageDocForOrder(page);
//        Assert.assertNotNull(doc);
//        Assert.assertTrue(Files.isRegularFile(Paths.get(altoPath.toAbsolutePath().toString(), "00000005.xml")));
//    }

    /**
     * @see MetsIndexer#generatePageDocument(Element,String,Integer,ISolrWriteStrategy,Map)
     * @verifies create ALTO file from TEI correctly
     */
    @Test
    public void generatePageDocument_shouldCreateALTOFileFromTEICorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        Path altoPath = Paths.get("build/viewer/alto/PPN517154005");
        Utils.checkAndCreateDirectory(altoPath);
        Assert.assertTrue(Files.isDirectory(altoPath));
        dataFolders.put(DataRepository.PARAM_ALTO_CONVERTED, altoPath);
        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));

        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        String xpath = "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div";
        List<Element> eleStructMapPhysicalList = indexer.xp.evaluateToElements(xpath, null);
        ISolrWriteStrategy writeStrategy = new LazySolrWriteStrategy(solrHelper);
        IDataRepositoryStrategy dataRepositoryStrategy = new SingleRepositoryStrategy(Configuration.getInstance());

        int page = 1;
        Assert.assertTrue(indexer.generatePageDocument(eleStructMapPhysicalList.get(page - 1),
                String.valueOf(MetsIndexer.getNextIddoc(hotfolder.getSolrHelper())), PI, page, writeStrategy, dataFolders,
                dataRepositoryStrategy.selectDataRepository(PI, metsFile, dataFolders, solrHelper)[0]));
        SolrInputDocument doc = writeStrategy.getPageDocForOrder(page);
        Assert.assertNotNull(doc);

        Assert.assertTrue(Files.isRegularFile(Paths.get(altoPath.toAbsolutePath().toString(), "00000001.xml")));
    }

    /**
     * @see MetsIndexer#generatePageDocument(Element,String,Integer,ISolrWriteStrategy,Map)
     * @verifies add fulltext field correctly
     */
    @Test
    public void generatePageDocument_shouldAddFulltextFieldCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));

        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        String xpath = "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div";
        List<Element> eleStructMapPhysicalList = indexer.xp.evaluateToElements(xpath, null);
        ISolrWriteStrategy writeStrategy = new LazySolrWriteStrategy(solrHelper);
        IDataRepositoryStrategy dataRepositoryStrategy = new SingleRepositoryStrategy(Configuration.getInstance());

        int page = 1;
        Assert.assertTrue(indexer.generatePageDocument(eleStructMapPhysicalList.get(page - 1),
                String.valueOf(MetsIndexer.getNextIddoc(hotfolder.getSolrHelper())), PI, page, writeStrategy, dataFolders,
                dataRepositoryStrategy.selectDataRepository(PI, metsFile, dataFolders, solrHelper)[0]));
        SolrInputDocument doc = writeStrategy.getPageDocForOrder(page);
        Assert.assertNotNull(doc);
        Assert.assertEquals("fulltext/" + PI + "/00000001.txt", doc.getFieldValue(SolrConstants.FILENAME_FULLTEXT));
        Assert.assertEquals(true, doc.getFieldValue(SolrConstants.FULLTEXTAVAILABLE));
    }

    /**
     * @see MetsIndexer#generatePageDocument(Element,String,Integer,ISolrWriteStrategy,Map)
     * @verifies add all basic fields
     */
    @Test
    public void generatePageDocument_shouldAddAllBasicFields() throws Exception {
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        String xpath = "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div";
        List<Element> eleStructMapPhysicalList = indexer.xp.evaluateToElements(xpath, null);
        ISolrWriteStrategy writeStrategy = new LazySolrWriteStrategy(solrHelper);
        IDataRepositoryStrategy dataRepositoryStrategy = new SingleRepositoryStrategy(Configuration.getInstance());

        int page = 3;
        Assert.assertTrue(indexer.generatePageDocument(eleStructMapPhysicalList.get(page - 1),
                String.valueOf(MetsIndexer.getNextIddoc(hotfolder.getSolrHelper())), PI, page, writeStrategy, null,
                dataRepositoryStrategy.selectDataRepository(PI, metsFile, null, solrHelper)[0]));
        SolrInputDocument doc = writeStrategy.getPageDocForOrder(page);
        Assert.assertNotNull(doc);
        Assert.assertNotNull(doc.getFieldValue(SolrConstants.IDDOC));
        Assert.assertNotNull(doc.getFieldValue(SolrConstants.GROUPFIELD));
        Assert.assertNotNull(DocType.PAGE.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
        Assert.assertNotNull("image", doc.getFieldValue(SolrConstants.MIMETYPE));
        Assert.assertEquals("PHYS_0003", doc.getFieldValue(SolrConstants.PHYSID));
        Assert.assertEquals(3, doc.getFieldValue(SolrConstants.ORDER));
        Assert.assertEquals("1", doc.getFieldValue(SolrConstants.ORDERLABEL));
        Assert.assertEquals("urn:nbn:de:hebis:66:fuldig-2004", doc.getFieldValue(SolrConstants.IMAGEURN));
        Assert.assertNotNull("00000003.tif", doc.getFieldValue(SolrConstants.FILENAME));
    }

    /**
     * @see MetsIndexer#superupdate(Path,Path,DataRepository)
     * @verifies copy new METS file correctly
     */
    @Test
    public void superupdate_shouldCopyNewMETSFileCorrectly() throws Exception {
        Path viewerRootFolder = Paths.get("build/viewer");
        Assert.assertTrue(Files.isDirectory(viewerRootFolder));
        Path updatedMetsFolder = Paths.get(viewerRootFolder.toAbsolutePath().toString(), "updated_mets");
        Assert.assertTrue(Files.isDirectory(updatedMetsFolder));

        Path metsFile = Paths.get(viewerRootFolder.toAbsolutePath().toString(), "PPN123.UPDATED");
        Files.createFile(metsFile);
        Assert.assertTrue(Files.isRegularFile(metsFile));

        DataRepository dataRepository = new DataRepository(viewerRootFolder.toAbsolutePath().toString());
        MetsIndexer.superupdate(metsFile, updatedMetsFolder, dataRepository);

        Path newMetsFile = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), "PPN123.xml");
        Assert.assertTrue(Files.isRegularFile(newMetsFile));
    }

    /**
     * @see MetsIndexer#superupdate(Path,Path,DataRepository)
     * @verifies copy old METS file to updated mets folder if file already exists
     */
    @Test
    public void superupdate_shouldCopyOldMETSFileToUpdatedMetsFolderIfFileAlreadyExists() throws Exception {
        Path viewerRootFolder = Paths.get("build/viewer");
        Assert.assertTrue(Files.isDirectory(viewerRootFolder));
        Path updatedMetsFolder = Paths.get(viewerRootFolder.toAbsolutePath().toString(), "updated_mets");
        Assert.assertTrue(Files.isDirectory(updatedMetsFolder));

        DataRepository dataRepository = new DataRepository(viewerRootFolder.toAbsolutePath().toString());
        Path metsFile = Paths.get(viewerRootFolder.toAbsolutePath().toString(), "PPN123.UPDATED");
        Files.createFile(metsFile);
        Assert.assertTrue(Files.isRegularFile(metsFile));
        MetsIndexer.superupdate(metsFile, updatedMetsFolder, dataRepository);

        Path newMetsFile = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), "PPN123.xml");
        Assert.assertTrue(Files.isRegularFile(newMetsFile));

        metsFile = Paths.get(viewerRootFolder.toAbsolutePath().toString(), "PPN123.UPDATED");
        Files.createFile(metsFile);
        Assert.assertTrue(Files.isRegularFile(metsFile));
        MetsIndexer.superupdate(metsFile, updatedMetsFolder, dataRepository);

        Path oldMetsFile = Paths.get(updatedMetsFolder.toAbsolutePath().toString(), "PPN123.xml");
        String[] files = updatedMetsFolder.toFile().list();
        Assert.assertNotNull(files);
        Assert.assertEquals(1, files.length);
        Assert.assertTrue(files[0].startsWith("PPN123"));
        Assert.assertTrue(files[0].endsWith(".xml"));
    }
}
