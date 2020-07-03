package io.goobi.viewer.indexer;

import java.nio.file.Path;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.goobi.viewer.indexer.WorldViewsIndexer;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

public class WorldViewsIndexerTest extends AbstractSolrEnabledTest {

    @SuppressWarnings("unused")
    private Hotfolder hotfolder;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        hotfolder = new Hotfolder("src/test/resources/indexerconfig_solr_test_worldviews.xml", client);
    }

    /**
     * @see WorldViewsIndexer#index(Path,boolean,Map,ISolrWriteStrategy,int)
     * @verifies index record correctly
     */
//    @SuppressWarnings("unchecked")
    @Test
    public void index_shouldIndexRecordCorrectly() throws Exception {
        //        String pi = "gei_test_sthe_quelle_01";
        //        Map<String, Path> dataFolders = new HashMap<>();
        //        dataFolders.put(DataRepository.PARAM_TEIMETADATA, Paths.get("src/test/resources/WorldViews/gei_test_sthe_quelle_01_tei"));
        //
        //        Path file = Paths.get("src/test/resources/WorldViews/gei_test_sthe_quelle_01.xml");
        //        String[] ret = new WorldViewsIndexer(hotfolder).index(file, false, dataFolders, null, 1);
        //        Assert.assertEquals(pi + ".xml", ret[0]);
        //        Assert.assertNull(ret[1]);
        //
        //        Map<String, Boolean> iddocMap = new HashMap<>();
        //        String iddoc;
        //
        //        // Top document
        //        {
        //            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + pi + " AND " + SolrConstants.FULLTEXT + ":*", null);
        //            Assert.assertEquals(1, docList.size());
        //            SolrDocument doc = docList.get(0);
        //            {
        //                Collection<Object> values = doc.getFieldValues(SolrConstants.ACCESSCONDITION);
        //                Assert.assertNotNull(values);
        //                Assert.assertEquals(1, values.size());
        //                Assert.assertEquals("OPENACCESS", values.iterator().next());
        //            }
        //            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
        //            Assert.assertNotNull(doc.getFieldValues(SolrConstants.DATEUPDATED));
        //            Assert.assertEquals(1, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
        //            Assert.assertEquals(doc.getFieldValue(SolrConstants.DATECREATED), doc.getFirstValue(SolrConstants.DATEUPDATED));
        //            Assert.assertEquals(DocType.DOCSTRCT.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
        //            Assert.assertEquals("Monograph", doc.getFieldValue(SolrConstants.DOCSTRCT));
        //            {
        //                List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
        //                Assert.assertNotNull(mdList);
        //                Assert.assertEquals(2, mdList.size());
        //                Assert.assertEquals("col1", mdList.get(0));
        //                Assert.assertEquals("col2", mdList.get(1));
        //            }
        //            Assert.assertTrue(doc.containsKey(SolrConstants.DEFAULT));
        //            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
        //            Assert.assertNotNull(iddoc);
        //            iddocMap.put(iddoc, true);
        //            Assert.assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
        //            Assert.assertEquals(true, doc.getFieldValue(SolrConstants.ISWORK));
        //            // Assert.assertEquals("Weltkrieg mit Worten", doc.getFieldValue(SolrConstants.LABEL));
        //            Assert.assertEquals("LOG_0000", doc.getFieldValue(SolrConstants.LOGID));
        //            Assert.assertEquals(8, doc.getFieldValue(SolrConstants.NUMPAGES));
        //            Assert.assertEquals(pi, doc.getFieldValue(SolrConstants.PI));
        //            Assert.assertEquals(pi, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
        //             Assert.assertEquals(FileFormat.WORLDVIEWS.name(), doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
        //            Assert.assertEquals("Q8 Content 2.jpg", doc.getFieldValue(SolrConstants.THUMBNAIL)); // representative image is set
        //            Assert.assertEquals("Q8 Content 2.jpg", doc.getFieldValue(SolrConstants.THUMBNAILREPRESENT)); // not really used
        //            Assert.assertEquals(1, doc.getFieldValue(SolrConstants.THUMBPAGENO)); // representative image should not affect the number
        //            Assert.assertEquals("X", doc.getFieldValue(SolrConstants.THUMBPAGENOLABEL));
        //            Assert.assertNull(doc.getFieldValue(SolrConstants.IMAGEURN_OAI)); // only docs representing deleted records should have this field
        //            // Assert.assertEquals("", doc.getFieldValue(SolrConstants.OPACURL));
        //            // Assert.assertEquals(true, doc.getFieldValue(SolrConstants.FULLTEXTAVAILABLE));
        //
        //            // Language codes
        //            {
        //                List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.LANGUAGE);
        //                Assert.assertNotNull(mdList);
        //                String s = "";
        //                for (String md : mdList) {
        //                    s += md + ", ";
        //                }
        //                Assert.assertEquals(s, 2, mdList.size());
        //                Assert.assertEquals("de", mdList.get(0));
        //                Assert.assertEquals("en", mdList.get(1));
        //            }
        //            // Title languages
        //            {
        //                List<String> mdList = (List<String>) doc.getFieldValue("MD_TITLE_LANG_DE");
        //                Assert.assertNotNull(mdList);
        //                Assert.assertEquals(1, mdList.size());
        //                Assert.assertEquals("Fences and their victims (deutsch)", mdList.get(0));
        //            }
        //            {
        //                List<String> mdList = (List<String>) doc.getFieldValue("MD_TITLE_LANG_EN");
        //                Assert.assertNotNull(mdList);
        //                Assert.assertEquals(1, mdList.size());
        //                Assert.assertEquals("Fences and their victims", mdList.get(0));
        //            }
        //            // Text
        //            {
        //                Assert.assertEquals("Quelle_Grossstadtdokumente_ger.xml", doc.getFieldValue(SolrConstants.FILENAME_TEI + SolrConstants._LANG_
        //                        + "DE"));
        //                Assert.assertEquals("Quelle_Grossstadtdokumente_eng.xml", doc.getFieldValue(SolrConstants.FILENAME_TEI + SolrConstants._LANG_
        //                        + "EN"));
        //            }
        //            {
        //                List<String> mdList = (List<String>) doc.getFieldValue("MD_AUTHOR");
        //                Assert.assertNotNull(mdList);
        //                Assert.assertEquals(3, mdList.size());
        //                Assert.assertEquals("Ostwald, Hans", mdList.get(0));
        //                Assert.assertEquals("Stahn, Lena-Luise", mdList.get(1));
        //                Assert.assertEquals("Hohlbein, Wolfgang", mdList.get(2));
        //            }
        //            {
        //                List<String> mdList = (List<String>) doc.getFieldValue("MD_AUTHOR" + SolrConstants._UNTOKENIZED);
        //                Assert.assertNotNull(mdList);
        //                Assert.assertEquals(3, mdList.size());
        //                Assert.assertEquals("Ostwald, Hans", mdList.get(0));
        //                Assert.assertEquals("Stahn, Lena-Luise", mdList.get(1));
        //                Assert.assertEquals("Hohlbein, Wolfgang", mdList.get(2));
        //            }
        //            Assert.assertEquals("Ostwald, Hans", doc.getFieldValue("SORT_AUTHOR"));
        //
        //            {
        //                List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.YEAR);
        //                Assert.assertNotNull(mdList);
        //                Assert.assertEquals(1, mdList.size());
        //                Assert.assertEquals(Long.valueOf(2016), mdList.get(0));
        //                Assert.assertEquals(Long.valueOf(2016), doc.getFieldValue(SolrConstants.SORTNUM_ + SolrConstants.YEAR));
        //            }
        //            {
        //                List<Long> mdList = (List<Long>) doc.getFieldValue(SolrConstants.CENTURY);
        //                Assert.assertNotNull(mdList);
        //                Assert.assertEquals(1, mdList.size());
        //                Assert.assertEquals(Long.valueOf(21), mdList.get(0));
        //                Assert.assertEquals(Long.valueOf(21), doc.getFieldValue(SolrConstants.SORTNUM_ + SolrConstants.CENTURY));
        //            }
        //        }

        // Grouped metadata
    }
}
