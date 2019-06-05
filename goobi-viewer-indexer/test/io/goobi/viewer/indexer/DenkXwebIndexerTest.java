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

import io.goobi.viewer.indexer.DenkXwebIndexer;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

public class DenkXwebIndexerTest extends AbstractSolrEnabledTest {

    /** Logger for this class. */
    //    private static final Logger logger = LoggerFactory.getLogger(DenkXwebIndexerTest.class);

    private static Hotfolder hotfolder;

    private static final String PI = "30596824";

    private File denkxwebFile;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        hotfolder = new Hotfolder("resources/test/indexerconfig_solr_test.xml", server);

        denkxwebFile = new File("resources/test/DenkXweb/denkxweb_30596824_short.xml");
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
            String[] ret = new DenkXwebIndexer(hotfolder).index(recordDoc, dataFolders, null, 1);
            Assert.assertNotEquals(ret[1], "ERROR", ret[0]);
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

        }
    }
}