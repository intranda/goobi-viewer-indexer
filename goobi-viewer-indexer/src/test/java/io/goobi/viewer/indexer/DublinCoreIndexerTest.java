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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.model.SolrConstants;

public class DublinCoreIndexerTest extends AbstractSolrEnabledTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
    }

    /**
     * @see DublinCoreIndexer#DublinCoreIndexer(Hotfolder)
     * @verifies set attributes correctly
     */
    @Test
    public void DublinCoreIndexer_shouldSetAttributesCorrectly() throws Exception {
        Assert.assertNotNull(hotfolder);
        Indexer indexer = new DublinCoreIndexer(hotfolder);
        Assert.assertEquals(hotfolder, indexer.hotfolder);
    }

    /**
     * @see DublinCoreIndexer#addToIndex(Path,boolean,Map)
     * @verifies add record to index correctly
     */
    @Test
    public void addToIndex_shouldAddRecordToIndexCorrectly() throws Exception {
        Path dcFile = Paths.get("src/test/resources/DC/record.xml");
        Assert.assertTrue(Files.isRegularFile(dcFile));

        Indexer indexer = new DublinCoreIndexer(hotfolder);
        indexer.addToIndex(dcFile, false, new HashMap<>());

        SolrDocumentList result =
                SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":123e4567-e89b-12d3-a456-556642440000", null);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("123e4567-e89b-12d3-a456-556642440000", result.get(0).getFieldValue(SolrConstants.PI));
    }
}