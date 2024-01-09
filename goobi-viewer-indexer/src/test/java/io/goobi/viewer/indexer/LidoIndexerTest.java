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

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.jdom2.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

class LidoIndexerTest extends AbstractSolrEnabledTest {

    /** Logger for this class. */
    //    private static final Logger logger = LogManager.getLogger(LidoIndexerTest.class);

    private static final String PI = "V0011127";

    private File lidoFile;

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());

        lidoFile = new File("src/test/resources/LIDO/khm_lido_export.xml");
        Assertions.assertTrue(lidoFile.isFile());
    }

    /**
     * @see LidoIndexer#LidoIndexer(Hotfolder)
     * @verifies set attributes correctly
     */
    @Test
    void LidoIndexer_shouldSDataRepositoryetAttributesCorrectly() throws Exception {
        LidoIndexer indexer = new LidoIndexer(hotfolder);
        Assertions.assertEquals(hotfolder, indexer.hotfolder);
    }

    @Test
    void testIndexMimeType() throws Exception {
        File lidoVideoFile = new File("src/test/resources/LIDO/1292624.xml");
        File lidoVideoMediaFolder = new File("src/test/resources/LIDO/1292624_media");
        String videoPI = "1292624";
        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoVideoFile);
        Assertions.assertEquals(1, lidoDocs.size());

        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_MEDIA, lidoVideoMediaFolder.getAbsoluteFile().toPath());
        for (Document lidoDoc : lidoDocs) {
            String[] ret = new LidoIndexer(hotfolder).index(lidoDoc, dataFolders, null, 1,
                    SolrIndexerDaemon.getInstance().getConfiguration().getStringList("init.lido.imageXPath"), false, false);
            Assertions.assertNotEquals("ERROR", ret[0]);
        }

        SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":*", null);
        Assertions.assertEquals(1, docList.size());
        SolrDocument doc = docList.get(0);
        String docPi = (String) doc.getFieldValue(SolrConstants.PI);
        Assertions.assertEquals("Document PI was expected to be " + videoPI + ", but was " + docPi, videoPI, docPi);

        docList = SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search(SolrConstants.PI_TOPSTRUCT + ":" + videoPI + " AND " + SolrConstants.FILENAME + ":*", null);
        Assertions.assertEquals(1, docList.size(), "video page not in doclist. Total page hits: "
                + SolrIndexerDaemon.getInstance().getSearchIndex().getNumHits(SolrConstants.FILENAME + ":*"));
        doc = docList.get(0);

        Assertions.assertEquals("video/mp4", doc.getFieldValue(SolrConstants.MIMETYPE));
        Assertions.assertEquals("Film77.mp4", doc.getFieldValue(SolrConstants.FILENAME + "_MP4"));
    }

    /**
     * @see LidoIndexer#index(Document,File,File)
     * @verifies index record correctly
     */
    @SuppressWarnings("unchecked")
    @Test
    void index_shouldIndexRecordCorrectly() throws Exception {
        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile);
        Assertions.assertEquals(30, lidoDocs.size());

        Map<String, Path> dataFolders = new HashMap<>();
        for (Document lidoDoc : lidoDocs) {
            String[] ret = new LidoIndexer(hotfolder).index(lidoDoc, dataFolders, null, 1,
                    SolrIndexerDaemon.getInstance().getConfiguration().getStringList("init.lido.imageXPath"), false, false);
            Assertions.assertNotEquals("ERROR", ret[0]);
        }

        Map<String, Boolean> iddocMap = new HashMap<>();
        String iddoc;

        // Top document
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + PI, null);
            Assertions.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            {
                Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                Assertions.assertNotNull(values);
                Assertions.assertEquals(1, values.size());
                Assertions.assertEquals("OPENACCESS", values.iterator().next());
            }
            Assertions.assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            Assertions.assertNotNull(doc.getFieldValues(SolrConstants.DATEUPDATED));
            {
                List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
                Assertions.assertNotNull(mdList);
                Assertions.assertEquals(1, mdList.size());
                Assertions.assertEquals("mmmsammlungen.200khmrostock.0750fotografien", mdList.get(0));
            }
            Assertions.assertEquals("Abzug", doc.getFieldValue(SolrConstants.DOCSTRCT));
            Assertions.assertTrue(doc.containsKey(SolrConstants.DEFAULT));
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            Assertions.assertNotNull(iddoc);
            iddocMap.put(iddoc, true);
            Assertions.assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
            Assertions.assertEquals(true, doc.getFieldValue(SolrConstants.ISWORK));
            //            Assertions.assertEquals("", doc.getFieldValue(SolrConstants.LABEL));
            Assertions.assertEquals(1, doc.getFieldValue(SolrConstants.NUMPAGES));
            Assertions.assertEquals(PI, doc.getFieldValue(SolrConstants.PI));
            Assertions.assertEquals(PI, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            Assertions.assertEquals(FileFormat.LIDO.name(), doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
            Assertions.assertEquals("PH_1.jpg", doc.getFieldValue(SolrConstants.THUMBNAIL));
            Assertions.assertEquals(1, doc.getFieldValue(SolrConstants.THUMBPAGENO));
            Assertions.assertEquals("X", doc.getFieldValue(SolrConstants.THUMBPAGENOLABEL));
            {
                List<String> mdList = (List<String>) doc.getFieldValue("MD_TITLE");
                Assertions.assertNotNull(mdList);
                Assertions.assertEquals(1, mdList.size());
                Assertions.assertEquals("Feldseite Kröpeliner Tor, Kinder im Vordergrund", mdList.get(0));
            }
            {
                List<String> mdList = (List<String>) doc.getFieldValue("MD_TITLE" + SolrConstants.SUFFIX_UNTOKENIZED);
                Assertions.assertNotNull(mdList);
                Assertions.assertEquals(1, mdList.size());
                Assertions.assertEquals("Feldseite Kröpeliner Tor, Kinder im Vordergrund", mdList.get(0));
            }
            Assertions.assertEquals("Feldseite Kröpeliner Tor, Kinder im Vordergrund", doc.getFieldValue("SORT_TITLE"));
            Assertions.assertEquals("1900", doc.getFieldValue("SORT_" + SolrConstants.EVENTDATESTART));
            Assertions.assertFalse((boolean) doc.getFieldValue(SolrConstants.FULLTEXTAVAILABLE));
        }

        // Pages
        {
            SolrDocumentList docList =
                    SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.FILENAME + ":*", null);
            Assertions.assertEquals(1, docList.size());

            Map<String, Boolean> filenameMap = new HashMap<>();
            Map<Integer, Boolean> orderMap = new HashMap<>();

            for (SolrDocument doc : docList) {
                {
                    Integer value = (Integer) doc.getFieldValue(SolrConstants.ORDER);
                    Assertions.assertNotNull(value);
                    Assertions.assertNull(orderMap.get(value));
                    orderMap.put(value, true);
                }
                {
                    Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                    Assertions.assertNotNull(values);
                    Assertions.assertEquals(1, values.size());
                    Assertions.assertEquals("OPENACCESS", values.iterator().next());
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
                    Assertions.assertNotNull(mdList);
                    Assertions.assertEquals(1, mdList.size(), "Page " + doc.getFieldValue(SolrConstants.ORDER) + ": " + mdList.toString());
                    Assertions.assertEquals("mmmsammlungen.200khmrostock.0750fotografien", mdList.get(0));
                }
                {
                    String value = (String) doc.getFieldValue(SolrConstants.FILENAME);
                    Assertions.assertEquals("PH_1.jpg", value);
                    Assertions.assertNull(filenameMap.get(value));
                    filenameMap.put(value, true);
                }
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assertions.assertNotNull(value);
                    Assertions.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                }
                Assertions.assertEquals(iddoc, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
                Assertions.assertEquals("image/jpeg", doc.getFieldValue(SolrConstants.MIMETYPE));
                Assertions.assertEquals("Abzug", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
            }
        }

        // Events
        {
            // TODO event grouped metadata (docs exists with the event doc as parent; fields do not exist with the docstruct doc as parent)

            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(SolrConstants.IDDOC_OWNER + ":" + iddoc + " AND " + SolrConstants.DOCTYPE + ":" + DocType.EVENT, null);
            Assertions.assertEquals(1, docList.size());

            for (SolrDocument doc : docList) {
                String eventType = (String) doc.getFieldValue(SolrConstants.EVENTTYPE);
                Assertions.assertNotNull(eventType);
                String defaultValue = (String) doc.getFieldValue(SolrConstants.DEFAULT);
                Assertions.assertNotNull(defaultValue);
                Assertions.assertTrue(defaultValue.startsWith(eventType));
                {
                    List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
                    Assertions.assertNotNull(mdList);
                    Assertions.assertEquals(1, mdList.size());
                    Assertions.assertEquals("mmmsammlungen.200khmrostock.0750fotografien", mdList.get(0));
                }
                {
                    List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.CENTURY);
                    Assertions.assertNotNull(mdList);
                    Assertions.assertEquals(1, mdList.size());
                    Assertions.assertEquals(Long.valueOf(20), mdList.get(0));
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.EVENTDATE);
                    Assertions.assertNotNull(mdList);
                    Assertions.assertEquals(1, mdList.size());
                    Assertions.assertEquals("um 1910", mdList.get(0));
                }
                Assertions.assertEquals("1920", doc.getFieldValue(SolrConstants.EVENTDATEEND));
                Assertions.assertEquals("1900", doc.getFieldValue(SolrConstants.EVENTDATESTART));
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assertions.assertNotNull(value);
                    Assertions.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue("MD_EVENTNAME");
                    Assertions.assertNotNull(mdList);
                    Assertions.assertEquals(1, mdList.size());
                    Assertions.assertEquals("Herstellung", mdList.get(0));
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue("MD_EVENTNAME" + SolrConstants.SUFFIX_UNTOKENIZED);
                    Assertions.assertNotNull(mdList);
                    Assertions.assertEquals(1, mdList.size());
                    Assertions.assertEquals("Herstellung", mdList.get(0));
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue("MD_MATERIAL");
                    Assertions.assertNotNull(mdList);
                    Assertions.assertEquals(1, mdList.size());
                    Assertions.assertEquals("Papier, schwarz-weiß", mdList.get(0));
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue("MD_MATERIAL" + SolrConstants.SUFFIX_UNTOKENIZED);
                    Assertions.assertNotNull(mdList);
                    Assertions.assertEquals(1, mdList.size());
                    Assertions.assertEquals("Papier, schwarz-weiß", mdList.get(0));
                }
                {
                    List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.YEAR);
                    Assertions.assertNotNull(mdList);
                    Assertions.assertEquals(3, mdList.size());
                    Assertions.assertNotNull(doc.getFieldValue(SolrConstants.PREFIX_SORTNUM + SolrConstants.YEAR));
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                    Assertions.assertNotNull(mdList);
                    Assertions.assertEquals(1, mdList.size());
                    Assertions.assertEquals("OPENACCESS", mdList.get(0));
                }
                Assertions.assertEquals("Abzug", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
            }
        }
    }

    /**
     * @see LidoIndexer#index(Document,File,File)
     * @verifies update record correctly
     */
    @Test
    void index_shouldUpdateRecordCorrectly() throws Exception {
        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile);
        Assertions.assertEquals(30, lidoDocs.size());

        Map<String, Path> dataFolders = new HashMap<>();
        for (Document lidoDoc : lidoDocs) {
            String[] ret = new LidoIndexer(hotfolder).index(lidoDoc, dataFolders, null, 1,
                    SolrIndexerDaemon.getInstance().getConfiguration().getStringList("init.lido.imageXPath"), false, false);
            Assertions.assertNotEquals("ERROR", ret[0]);
        }

        Map<String, Boolean> iddocMap = new HashMap<>();
        String iddoc;
        long dateCreated;
        Collection<Object> dateUpdated;

        // Top document
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + PI, null);
            Assertions.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assertions.assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            dateCreated = (Long) doc.getFieldValue(SolrConstants.DATECREATED);
            Assertions.assertNotNull(doc.getFieldValue(SolrConstants.DATEUPDATED));
            dateUpdated = doc.getFieldValues(SolrConstants.DATEUPDATED);
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            Assertions.assertNotNull(iddoc);
            iddocMap.put(iddoc, true);
            Assertions.assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
            Assertions.assertFalse((boolean) doc.getFieldValue(SolrConstants.FULLTEXTAVAILABLE));
            Assertions.assertEquals(Boolean.FALSE, doc.getFieldValue(MetadataHelper.FIELD_HAS_WKT_COORDS));
        }

        // Pages
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.DOCTYPE + ":" + DocType.PAGE, null);
            Assertions.assertEquals(1, docList.size());
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assertions.assertNotNull(value);
                    Assertions.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                }
                Assertions.assertEquals(iddoc, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            }
        }

        // Events
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(SolrConstants.IDDOC_OWNER + ":" + iddoc + " AND " + SolrConstants.DOCTYPE + ":" + DocType.EVENT, null);
            Assertions.assertEquals(1, docList.size());
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assertions.assertNotNull(value);
                    Assertions.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                }
            }
        }

        // Re-index
        String[] ret = new LidoIndexer(hotfolder).index(lidoDocs.get(0), dataFolders, null, 1,
                SolrIndexerDaemon.getInstance().getConfiguration().getStringList("init.lido.imageXPath"), false, false);
        Assertions.assertNotEquals("ERROR", ret[0]);

        String newIddoc;

        // Top document
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + PI, null);
            Assertions.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assertions.assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            Assertions.assertEquals(dateCreated, doc.getFieldValue(SolrConstants.DATECREATED));
            Assertions.assertNotNull(doc.getFieldValues(SolrConstants.DATEUPDATED));
            Assertions.assertEquals(dateUpdated.size() + 1, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
            newIddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            Assertions.assertNotNull(newIddoc);
            Assertions.assertNull(iddocMap.get(newIddoc));
            Assertions.assertEquals(Boolean.FALSE, doc.getFieldValue(MetadataHelper.FIELD_HAS_WKT_COORDS));
            iddocMap.put(newIddoc, true);
        }

        // Pages
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.DOCTYPE + ":" + DocType.PAGE, null);
            Assertions.assertEquals(1, docList.size());

            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assertions.assertNotNull(value);
                    Assertions.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                }
                Assertions.assertEquals(newIddoc, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            }
        }

        // Events
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(SolrConstants.IDDOC_OWNER + ":" + newIddoc + " AND " + SolrConstants.DOCTYPE + ":" + DocType.EVENT, null);
            Assertions.assertEquals(1, docList.size());
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assertions.assertNotNull(value);
                    Assertions.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                }
            }
        }
    }
}
