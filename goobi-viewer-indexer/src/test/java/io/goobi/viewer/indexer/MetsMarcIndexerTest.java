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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

public class MetsMarcIndexerTest extends AbstractSolrEnabledTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
    }

    /**
     * @see MetsMarcIndexer#index(Path,boolean,Map,ISolrWriteStrategy,int,boolean)
     * @verifies index record correctly
     */
    @SuppressWarnings("unchecked")
    @Test
    public void index_shouldIndexRecordCorrectly() throws Exception {
        Path metsFile = Paths.get("src/test/resources/METS/VoorbeeldMETS_9940609919905131.xml");
        assertTrue(Files.isRegularFile(metsFile));

        String pi = "9940609919905131";
        Map<String, Path> dataFolders = new HashMap<>();
        String[] ret = new MetsMarcIndexer(hotfolder).index(metsFile, false, dataFolders, null, 1, false);
        assertEquals(pi + ".xml", ret[0]);
        Assert.assertNull(ret[1]);

        Map<String, Boolean> iddocMap = new HashMap<>();
        String iddoc;

        // Top document
        {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + pi, null);
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
            assertNotNull(doc.getFieldValues(SolrConstants.DATEINDEXED));
            assertEquals(1, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
            assertEquals(doc.getFieldValue(SolrConstants.DATECREATED), doc.getFirstValue(SolrConstants.DATEUPDATED));
            assertEquals(DocType.DOCSTRCT.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
            assertEquals("Monograph", doc.getFieldValue(SolrConstants.DOCSTRCT));
            assertEquals("Monograph", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
            {
//                List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
//                assertNotNull(mdList);
//                assertEquals(2, mdList.size());
//                assertEquals("varia", mdList.get(0));
//                assertEquals("digiwunschbuch", mdList.get(1));
            }
            Assert.assertTrue(doc.containsKey(SolrConstants.DEFAULT));
            // Assert.assertTrue(doc.containsKey(SolrConstants.SUPERDEFAULT));
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            assertNotNull(iddoc);
            iddocMap.put(iddoc, true);
            assertEquals(iddoc, doc.getFieldValue(SolrConstants.GROUPFIELD));
            assertEquals(true, doc.getFieldValue(SolrConstants.ISWORK));
            assertEquals("De secretis antimonii, das ist, von der grossen heymligkeit des Antimonij, zu Teutschem Spiesglas genant, die Artzney betreffend /", doc.getFieldValue(SolrConstants.LABEL));
            assertEquals("LOG_0000", doc.getFieldValue(SolrConstants.LOGID));
            assertEquals(70, doc.getFieldValue(SolrConstants.NUMPAGES));
            assertEquals(pi, doc.getFieldValue(SolrConstants.PI));
            assertEquals(pi, doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            assertEquals(FileFormat.METS_MARC.name(), doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
            assertEquals("ph418_b__bph_ok_962_(2)_002.tif", doc.getFieldValue(SolrConstants.THUMBNAIL)); // representative image is set
            assertEquals("ph418_b__bph_ok_962_(2)_002.tif", doc.getFieldValue(SolrConstants.THUMBNAILREPRESENT)); // not really used
            assertEquals(2, doc.getFieldValue(SolrConstants.THUMBPAGENO)); // representative image should not affect the number
            assertEquals("X", doc.getFieldValue(SolrConstants.THUMBPAGENOLABEL));
            Assert.assertNull(doc.getFieldValue(SolrConstants.IMAGEURN_OAI)); // only docs representing deleted records should have this field
            assertEquals(false, doc.getFieldValue(SolrConstants.FULLTEXTAVAILABLE));
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
                            .append(pi)
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
                Assert.assertTrue(doc.containsKey(SolrConstants.DEFAULT));
                assertEquals(DocType.DOCSTRCT.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
                assertNotNull(doc.getFieldValue(SolrConstants.DOCSTRCT));
                assertEquals(doc.getFieldValue(SolrConstants.DOCSTRCT), doc.getFieldValue(SolrConstants.DOCSTRCT_SUB));
                assertEquals("Monograph", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
                {
//                    List<String> mdList = (List<String>) doc.getFieldValue(SolrConstants.DC);
//                    assertNotNull(mdList);
//                    assertEquals(2, mdList.size());
//                    assertEquals("varia", mdList.get(0));
//                    assertEquals("digiwunschbuch", mdList.get(1));
                }
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
                    iddocMap.put(value, true);
                    assertEquals(value, doc.getFieldValue(SolrConstants.GROUPFIELD));
                }
                assertEquals(iddoc, doc.getFieldValue(SolrConstants.IDDOC_TOPSTRUCT));
                {
                    String value = (String) doc.getFieldValue(SolrConstants.LOGID);
                    assertNotNull(value);
                    Assert.assertNull(logIdMap.get(value));
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
                    .search(SolrConstants.PI_TOPSTRUCT + ":" + pi + " AND " + SolrConstants.DOCTYPE + ":" + DocType.PAGE, null);
            assertEquals(70, docList.size());

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
                    assertEquals("fulltext/" + pi + "/" + FilenameUtils.getBaseName(fileName) + FileTools.TXT_EXTENSION, value);
                    assertEquals(true, doc.getFieldValue(SolrConstants.FULLTEXTAVAILABLE));
                }
                {
                    String value = (String) doc.getFieldValue(SolrConstants.IDDOC);
                    assertNotNull(value);
                    Assert.assertNull(iddocMap.get(value));
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
}