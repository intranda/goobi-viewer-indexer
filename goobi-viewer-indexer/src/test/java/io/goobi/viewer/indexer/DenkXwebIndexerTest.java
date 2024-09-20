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
import java.util.Collections;
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
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

class DenkXwebIndexerTest extends AbstractSolrEnabledTest {

    /** Logger for this class. */
    //    private static final Logger logger = LogManager.getLogger(DenkXwebIndexerTest.class);

    private static final String PI = "30596824";
    private static final String PI2 = "10973880";

    private File denkxwebFile;

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());

        denkxwebFile = new File("src/test/resources/DenkXweb/denkxweb_30596824_short.xml");
        Assertions.assertTrue(denkxwebFile.isFile());
    }

    /**
     * @see DenkXwebIndexer#addToIndex(Path,boolean,Map)
     * @verifies throw IllegalArgumentException if denkxwebFile null
     */
    @Test
    void addToIndex_shouldThrowIllegalArgumentExceptionIfDenkxwebFileNull() {
        DenkXwebIndexer indexer = new DenkXwebIndexer(hotfolder);
        Assertions.assertThrows(IllegalArgumentException.class, () -> indexer.addToIndex(null, Collections.emptyMap()));
    }

    /**
     * @see DenkXwebIndexer#index(Document,Map,ISolrWriteStrategy,int)
     * @verifies index record correctly
     */
    @SuppressWarnings("unchecked")
    @Test
    void index_shouldIndexRecordCorrectly() throws Exception {
        List<Document> recordDocs = JDomXP.splitDenkXwebFile(denkxwebFile);
        Assertions.assertEquals(2, recordDocs.size());

        Map<String, Path> dataFolders = new HashMap<>();
        for (Document recordDoc : recordDocs) {
            String[] ret = new DenkXwebIndexer(hotfolder).index(recordDoc, dataFolders, null, 1, false);
            Assertions.assertNotEquals(ret[1], "ERROR", ret[0]);
        }

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
            Assertions.assertEquals(FileFormat.DENKXWEB.name(), doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
            Assertions.assertEquals("Baudenkmal", doc.getFieldValue(SolrConstants.DOCSTRCT));
            Assertions.assertEquals("Baudenkmal", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
        }
    }

    /**
     * @see DenkXwebIndexer#generatePageDocuments(ISolrWriteStrategy,Map,int,boolean)
     * @verifies generate pages correctly
     */
    @SuppressWarnings("unchecked")
    @Test
    void generatePageDocuments_shouldGeneratePagesCorrectly() throws Exception {
        File denkxwebFile = new File("src/test/resources/DenkXweb/" + PI2 + ".xml");
        Assertions.assertTrue(denkxwebFile.isFile());
        List<Document> recordDocs = JDomXP.splitDenkXwebFile(denkxwebFile);
        Assertions.assertEquals(1, recordDocs.size());

        Map<String, Path> dataFolders = new HashMap<>();
        for (Document recordDoc : recordDocs) {
            String[] ret = new DenkXwebIndexer(hotfolder).index(recordDoc, dataFolders, null, 1, false);
            Assertions.assertNotEquals(ret[1], "ERROR", ret[0]);
        }

        // Top document
        SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + PI2, null);
        Assertions.assertEquals(1, docList.size());
        SolrDocument topDoc = docList.get(0);
        {
            Collection<Object> values = (Collection<Object>) topDoc.getFieldValue(SolrConstants.ACCESSCONDITION);
            Assertions.assertNotNull(values);
            Assertions.assertEquals(1, values.size());
            Assertions.assertEquals("OPENACCESS", values.iterator().next());
        }
        Assertions.assertNotNull(topDoc.getFieldValue(SolrConstants.DATECREATED));
        Assertions.assertNotNull(topDoc.getFieldValues(SolrConstants.DATEUPDATED));
        Assertions.assertEquals("https://example.com/10973880_2.jpg", topDoc.getFieldValue(SolrConstants.THUMBNAIL));

        // Pages
        {
            SolrDocumentList pageDocList =
                    SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search(" +" + SolrConstants.PI_TOPSTRUCT + ":" + PI2 + " +" + SolrConstants.DOCTYPE + ":PAGE", null);
            Assertions.assertEquals(2, pageDocList.size());
            SolrDocument doc = pageDocList.get(0);
            {
                Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                Assertions.assertNotNull(values);
                Assertions.assertEquals(1, values.size());
                Assertions.assertEquals("OPENACCESS", values.iterator().next());
            }
            Assertions.assertEquals("https://example.com/10973880_1.jpg", doc.getFieldValue(SolrConstants.FILENAME + "_HTML-SANDBOXED"));
            Assertions.assertEquals("image/jpeg", doc.getFieldValue(SolrConstants.MIMETYPE));
            Assertions.assertEquals("foo bar", SolrSearchIndex.getSingleFieldStringValue(doc, "MD_DESCRIPTION"));
            Assertions.assertEquals(topDoc.getFieldValue(SolrConstants.IDDOC), doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            Assertions.assertEquals("Flächendenkmal", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
        }
    }
}
