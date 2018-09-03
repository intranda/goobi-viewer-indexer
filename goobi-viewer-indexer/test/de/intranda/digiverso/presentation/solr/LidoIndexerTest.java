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

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.jdom2.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.helper.Configuration;
import de.intranda.digiverso.presentation.solr.helper.Hotfolder;
import de.intranda.digiverso.presentation.solr.helper.JDomXP;
import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;
import de.intranda.digiverso.presentation.solr.model.SolrConstants;
import de.intranda.digiverso.presentation.solr.model.SolrConstants.DocType;
import de.intranda.digiverso.presentation.solr.model.datarepository.DataRepository;

public class LidoIndexerTest extends AbstractSolrEnabledTest {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(LidoIndexerTest.class);

    private static final String PI = "V0011127";

    private static Hotfolder hotfolder;

    private File lidoFile;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        hotfolder = new Hotfolder("resources/test/indexerconfig_solr_test.xml", server);

        lidoFile = new File("resources/test/LIDO/khm_lido_export.xml");
        Assert.assertTrue(lidoFile.isFile());
    }

    /**
     * @see LidoIndexer#LidoIndexer(Hotfolder)
     * @verifies set attributes correctly
     */
    @Test
    public void LidoIndexer_shouldSDataRepositoryetAttributesCorrectly() throws Exception {
        LidoIndexer indexer = new LidoIndexer(hotfolder);
        Assert.assertEquals(hotfolder, indexer.hotfolder);
    }

    @Test
    public void testIndexMimeType() throws Exception {
        File lidoVideoFile = new File("resources/test/LIDO/1292624.xml");
        File lidoVideoMediaFolder = new File("resources/test/LIDO/1292624_media");
        String videoPI = "1292624";
        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoVideoFile);
        Assert.assertEquals(1, lidoDocs.size());
        
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_MEDIA, lidoVideoMediaFolder.getAbsoluteFile().toPath());
        for (Document lidoDoc : lidoDocs) {
            String[] ret = new LidoIndexer(hotfolder).index(lidoDoc, dataFolders, null, 1, Configuration.getInstance().getList(
                    "init.lido.imageXPath"), false);
            Assert.assertNotEquals("ERROR", ret[0]);
        }
        
        SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + videoPI, null);
        Assert.assertEquals("video PI not in doclist. Total pi hits: " + hotfolder.getSolrHelper().getNumHits(SolrConstants.PI + ":*"), 1, docList.size());
        
        docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":"
                + videoPI + " AND " + SolrConstants.FILENAME + ":*", null);
        Assert.assertEquals("video page not in doclist. Total page hits: " + hotfolder.getSolrHelper().getNumHits(SolrConstants.FILENAME + ":*"),1, docList.size());
        SolrDocument doc = docList.get(0);
        
        Assert.assertEquals("video", doc.getFieldValue(SolrConstants.MIMETYPE));
    }
    
    /**
     * @see LidoIndexer#index(Document,File,File)
     * @verifies index record correctly
     */
    @SuppressWarnings("unchecked")
    @Test
    public void index_shouldIndexRecordCorrectly() throws Exception {
        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile);
        Assert.assertEquals(30, lidoDocs.size());

        Map<String, Path> dataFolders = new HashMap<>();
        for (Document lidoDoc : lidoDocs) {
            String[] ret = new LidoIndexer(hotfolder).index(lidoDoc, dataFolders, null, 1, Configuration.getInstance().getList(
                    "init.lido.imageXPath"), false);
            Assert.assertNotEquals("ERROR", ret[0]);
        }

        Map<String, Boolean> iddocMap = new HashMap<>();
        String iddoc;

        // Top document
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + PI, null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            {
                Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                Assert.assertNotNull(values);
                Assert.assertEquals(1, values.size());
                Assert.assertEquals("OPENACCESS", values.iterator().next());
            }
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            Assert.assertNotNull(doc.getFieldValues(SolrConstants.DATEUPDATED));
            {
                List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
                Assert.assertNotNull(mdList);
                Assert.assertEquals(1, mdList.size());
                Assert.assertEquals("mmmsammlungen.200khmrostock.0750fotografien", mdList.get(0));
            }
            Assert.assertEquals("Abzug", doc.getFieldValue(SolrConstants.DOCSTRCT));
            Assert.assertTrue(doc.containsKey(SolrConstants.DEFAULT));
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            Assert.assertNotNull(iddoc);
            iddocMap.put(iddoc, true);
            Assert.assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
            Assert.assertEquals(true, doc.getFieldValue(SolrConstants.ISWORK));
            //            Assert.assertEquals("", doc.getFieldValue(SolrConstants.LABEL));
            Assert.assertEquals(1, doc.getFieldValue(SolrConstants.NUMPAGES));
            Assert.assertEquals(PI, doc.getFieldValue(SolrConstants.PI));
            Assert.assertEquals(PI, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            Assert.assertEquals(SolrConstants._LIDO, doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
            Assert.assertEquals("PH_1.jpg", doc.getFieldValue(SolrConstants.THUMBNAIL));
            Assert.assertEquals(1, doc.getFieldValue(SolrConstants.THUMBPAGENO));
            Assert.assertEquals(" - ", doc.getFieldValue(SolrConstants.THUMBPAGENOLABEL));
            {
                List<String> mdList = (List<String>) doc.getFieldValue("MD_TITLE");
                Assert.assertNotNull(mdList);
                Assert.assertEquals(1, mdList.size());
                Assert.assertEquals("Feldseite Kröpeliner Tor, Kinder im Vordergrund", mdList.get(0));
            }
            {
                List<String> mdList = (List<String>) doc.getFieldValue("MD_TITLE" + SolrConstants._UNTOKENIZED);
                Assert.assertNotNull(mdList);
                Assert.assertEquals(1, mdList.size());
                Assert.assertEquals("Feldseite Kröpeliner Tor, Kinder im Vordergrund", mdList.get(0));
            }
            Assert.assertEquals("Feldseite Kröpeliner Tor, Kinder im Vordergrund", doc.getFieldValue("SORT_TITLE"));

            // TODO PARTNERID
        }

        // Pages
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.FILENAME
                    + ":*", null);
            Assert.assertEquals(1, docList.size());

            Map<String, Boolean> filenameMap = new HashMap<>();
            Map<Integer, Boolean> orderMap = new HashMap<>();
            Map<String, Boolean> physIdMap = new HashMap<>();

            for (SolrDocument doc : docList) {
                {
                    Integer value = (Integer) doc.getFieldValue(SolrConstants.ORDER);
                    Assert.assertNotNull(value);
                    Assert.assertNull(orderMap.get(value));
                    orderMap.put(value, true);
                }
                {
                    Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                    Assert.assertNotNull(values);
                    Assert.assertEquals(1, values.size());
                    Assert.assertEquals("OPENACCESS", values.iterator().next());
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
                    Assert.assertNotNull(mdList);
                    Assert.assertEquals("Page " + doc.getFieldValue(SolrConstants.ORDER) + ": " + mdList.toString(), 1, mdList.size());
                    Assert.assertEquals("mmmsammlungen.200khmrostock.0750fotografien", mdList.get(0));
                }
                {
                    String value = (String) doc.getFieldValue(SolrConstants.FILENAME);
                    Assert.assertEquals("PH_1.jpg", value);
                    Assert.assertNull(filenameMap.get(value));
                    filenameMap.put(value, true);
                }
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assert.assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                }
                Assert.assertEquals(iddoc, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
                Assert.assertEquals("image", doc.getFieldValue(SolrConstants.MIMETYPE));
            }
        }

        // Events
        {
            // TODO event grouped metadata (docs exists with the event doc as parent; fields do not exist with the docstruct doc as parent)

            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.IDDOC_OWNER + ":" + iddoc + " AND " + SolrConstants.DOCTYPE
                    + ":" + DocType.EVENT, null);
            Assert.assertEquals(1, docList.size());

            Map<String, Boolean> filenameMap = new HashMap<>();
            Map<Integer, Boolean> orderMap = new HashMap<>();
            Map<String, Boolean> physIdMap = new HashMap<>();

            for (SolrDocument doc : docList) {
                String eventType = (String) doc.getFieldValue(SolrConstants.EVENTTYPE);
                Assert.assertNotNull(eventType);
                String defaultValue = (String) doc.getFieldValue(SolrConstants.DEFAULT);
                Assert.assertNotNull(defaultValue);
                Assert.assertTrue(defaultValue.startsWith(eventType));
                {
                    List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
                    Assert.assertNotNull(mdList);
                    Assert.assertEquals(1, mdList.size());
                    Assert.assertEquals("mmmsammlungen.200khmrostock.0750fotografien", mdList.get(0));
                }
                {
                    List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.CENTURY);
                    Assert.assertNotNull(mdList);
                    Assert.assertEquals(1, mdList.size());
                    Assert.assertEquals(Long.valueOf(20), mdList.get(0));
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.EVENTDATE);
                    Assert.assertNotNull(mdList);
                    Assert.assertEquals(1, mdList.size());
                    Assert.assertEquals("um 1910", mdList.get(0));
                }
                Assert.assertEquals("1920", doc.getFieldValue(SolrConstants.EVENTDATEEND));
                Assert.assertEquals("1900", doc.getFieldValue(SolrConstants.EVENTDATESTART));
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assert.assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue("MD_EVENTNAME");
                    Assert.assertNotNull(mdList);
                    Assert.assertEquals(1, mdList.size());
                    Assert.assertEquals("Herstellung", mdList.get(0));
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue("MD_EVENTNAME" + SolrConstants._UNTOKENIZED);
                    Assert.assertNotNull(mdList);
                    Assert.assertEquals(1, mdList.size());
                    Assert.assertEquals("Herstellung", mdList.get(0));
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue("MD_MATERIAL");
                    Assert.assertNotNull(mdList);
                    Assert.assertEquals(1, mdList.size());
                    Assert.assertEquals("Papier, schwarz-weiß", mdList.get(0));
                }
                {
                    List<String> mdList = (List<String>) doc.getFieldValue("MD_MATERIAL" + SolrConstants._UNTOKENIZED);
                    Assert.assertNotNull(mdList);
                    Assert.assertEquals(1, mdList.size());
                    Assert.assertEquals("Papier, schwarz-weiß", mdList.get(0));
                }
                {
                    List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.YEAR);
                    Assert.assertNotNull(mdList);
                    Assert.assertEquals(3, mdList.size());
                    Assert.assertNotNull(doc.getFieldValue(SolrConstants.SORTNUM_ + SolrConstants.YEAR));
                }
            }
        }
    }

    /**
     * @see LidoIndexer#index(Document,File,File)
     * @verifies update record correctly
     */
    @Test
    public void index_shouldUpdateRecordCorrectly() throws Exception {
        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile);
        Assert.assertEquals(30, lidoDocs.size());

        Map<String, Path> dataFolders = new HashMap<>();
        for (Document lidoDoc : lidoDocs) {
            @SuppressWarnings("unchecked")
            String[] ret = new LidoIndexer(hotfolder).index(lidoDoc, dataFolders, null, 1, Configuration.getInstance().getList(
                    "init.lido.imageXPath"), false);
            Assert.assertNotEquals("ERROR", ret[0]);
        }

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

        // Pages
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.DOCTYPE + ":"
                    + DocType.PAGE, null);
            Assert.assertEquals(1, docList.size());

            Map<String, Boolean> filenameMap = new HashMap<>();
            Map<Integer, Boolean> orderMap = new HashMap<>();
            Map<String, Boolean> physIdMap = new HashMap<>();

            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assert.assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                }
                Assert.assertEquals(iddoc, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            }
        }

        // Events
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.IDDOC_OWNER + ":" + iddoc + " AND " + SolrConstants.DOCTYPE
                    + ":" + DocType.EVENT, null);
            Assert.assertEquals(1, docList.size());
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assert.assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                }
            }
        }

        // Re-index
        @SuppressWarnings("unchecked")
        String[] ret = new LidoIndexer(hotfolder).index(lidoDocs.get(0), dataFolders, null, 1, Configuration.getInstance().getList(
                "init.lido.imageXPath"), false);
        Assert.assertNotEquals("ERROR", ret[0]);

        String newIddoc;

        // Top document
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + PI, null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            Assert.assertEquals(dateCreated, doc.getFieldValue(SolrConstants.DATECREATED));
            Assert.assertNotNull(doc.getFieldValues(SolrConstants.DATEUPDATED));
            Assert.assertEquals(dateUpdated.size() + 1, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
            newIddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            Assert.assertNotNull(newIddoc);
            Assert.assertNull(iddocMap.get(newIddoc));
            iddocMap.put(newIddoc, true);
        }

        // Pages
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.DOCTYPE + ":"
                    + DocType.PAGE, null);
            Assert.assertEquals(1, docList.size());

            Map<String, Boolean> filenameMap = new HashMap<>();
            Map<Integer, Boolean> orderMap = new HashMap<>();
            Map<String, Boolean> physIdMap = new HashMap<>();

            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assert.assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                }
                Assert.assertEquals(newIddoc, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            }
        }

        // Events
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.IDDOC_OWNER + ":" + newIddoc + " AND " + SolrConstants.DOCTYPE
                    + ":" + DocType.EVENT, null);
            Assert.assertEquals(1, docList.size());
            for (SolrDocument doc : docList) {
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    Assert.assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                }
            }
        }
    }
}
