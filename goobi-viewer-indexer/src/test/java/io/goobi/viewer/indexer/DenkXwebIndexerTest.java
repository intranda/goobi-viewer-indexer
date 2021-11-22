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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

public class DenkXwebIndexerTest extends AbstractSolrEnabledTest {

    /** Logger for this class. */
    //    private static final Logger logger = LoggerFactory.getLogger(DenkXwebIndexerTest.class);

    private static final String PI = "30596824";
    private static final String PI2 = "10973880";

    private File denkxwebFile;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        
        hotfolder = new Hotfolder(TEST_CONFIG_PATH, client);

        denkxwebFile = new File("src/test/resources/DenkXweb/denkxweb_30596824_short.xml");
        Assert.assertTrue(denkxwebFile.isFile());
    }

    /**
     * @see DenkXwebIndexer#index(Document,Map,ISolrWriteStrategy,int)
     * @verifies index record correctly
     */
    @SuppressWarnings("unchecked")
    @Test
    public void index_shouldIndexRecordCorrectly() throws Exception {
        List<Document> recordDocs = JDomXP.splitDenkXwebFile(denkxwebFile);
        Assert.assertEquals(2, recordDocs.size());

        Map<String, Path> dataFolders = new HashMap<>();
        for (Document recordDoc : recordDocs) {
            String[] ret = new DenkXwebIndexer(hotfolder).index(recordDoc, dataFolders, null, 1, false);
            Assert.assertNotEquals(ret[1], "ERROR", ret[0]);
        }

        Map<String, Boolean> iddocMap = new HashMap<>();
        String iddoc;

        // Top document
        {
            SolrDocumentList docList = hotfolder.getSearchIndex().search(SolrConstants.PI + ":" + PI, null);
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
            Assert.assertEquals(FileFormat.DENKXWEB.name(), doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
            Assert.assertEquals("Baudenkmal", doc.getFieldValue(SolrConstants.DOCSTRCT));
            Assert.assertEquals("Baudenkmal", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
        }
    }

    /**
     * @see DenkXwebIndexer#generatePageDocuments(ISolrWriteStrategy,Map,int,boolean)
     * @verifies generate pages correctly
     */
    @SuppressWarnings("unchecked")
    @Test
    public void generatePageDocuments_shouldGeneratePagesCorrectly() throws Exception {
        File denkxwebFile = new File("src/test/resources/DenkXweb/" + PI2 + ".xml");
        Assert.assertTrue(denkxwebFile.isFile());
        List<Document> recordDocs = JDomXP.splitDenkXwebFile(denkxwebFile);
        Assert.assertEquals(1, recordDocs.size());

        Map<String, Path> dataFolders = new HashMap<>();
        for (Document recordDoc : recordDocs) {
            String[] ret = new DenkXwebIndexer(hotfolder).index(recordDoc, dataFolders, null, 1, false);
            Assert.assertNotEquals(ret[1], "ERROR", ret[0]);
        }

        // Top document
        SolrDocumentList docList = hotfolder.getSearchIndex().search(SolrConstants.PI + ":" + PI2, null);
        Assert.assertEquals(1, docList.size());
        SolrDocument topDoc = docList.get(0);
        {
            Collection<Object> values = (Collection<Object>) topDoc.getFieldValue(SolrConstants.ACCESSCONDITION);
            Assert.assertNotNull(values);
            Assert.assertEquals(1, values.size());
            Assert.assertEquals("OPENACCESS", values.iterator().next());
        }
        Assert.assertNotNull(topDoc.getFieldValue(SolrConstants.DATECREATED));
        Assert.assertNotNull(topDoc.getFieldValues(SolrConstants.DATEUPDATED));
        Assert.assertEquals("https://example.com/10973880_2.jpg", topDoc.getFieldValue(SolrConstants.THUMBNAIL));

        // Pages
        {
            SolrDocumentList pageDocList =
                    hotfolder.getSearchIndex().search(" +" + SolrConstants.PI_TOPSTRUCT + ":" + PI2 + " +" + SolrConstants.DOCTYPE + ":PAGE", null);
            Assert.assertEquals(2, pageDocList.size());
            SolrDocument doc = pageDocList.get(0);
            {
                Collection<Object> values = (Collection<Object>) doc.getFieldValue(SolrConstants.ACCESSCONDITION);
                Assert.assertNotNull(values);
                Assert.assertEquals(1, values.size());
                Assert.assertEquals("OPENACCESS", values.iterator().next());
            }
            Assert.assertEquals("https://example.com/10973880_1.jpg", doc.getFieldValue(SolrConstants.FILENAME + "_HTML-SANDBOXED"));
            Assert.assertEquals("image", doc.getFieldValue(SolrConstants.MIMETYPE));
            Assert.assertEquals("foo bar", SolrSearchIndex.getSingleFieldStringValue(doc, "MD_DESCRIPTION"));
            Assert.assertEquals(topDoc.getFieldValue(SolrConstants.IDDOC), doc.getFieldValue(SolrConstants.IDDOC_OWNER));
            Assert.assertEquals("Flächendenkmal", doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
        }
    }
}
