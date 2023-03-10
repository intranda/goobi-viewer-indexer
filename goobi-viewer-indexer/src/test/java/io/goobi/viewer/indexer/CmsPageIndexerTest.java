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

import static org.junit.Assert.assertTrue;

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

public class CmsPageIndexerTest extends AbstractSolrEnabledTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(TEST_CONFIG_PATH, client);
    }

    /**
     * @see CmsPageIndexer#addToIndex(Path,boolean,Map)
     * @verifies add record to index correctly
     */
    @Test
    public void addToIndex_shouldAddRecordToIndexCorrectly() throws Exception {
        Path cmsFile = Paths.get("src/test/resources/indexed_cms/CMS123.xml");
        assertTrue(Files.isRegularFile(cmsFile));

        Indexer indexer = new CmsPageIndexer(hotfolder);
        indexer.addToIndex(cmsFile, false, new HashMap<>());

        SolrDocumentList result = hotfolder.getSearchIndex().search(SolrConstants.PI + ":CMS123", null);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("CMS123", result.get(0).getFieldValue(SolrConstants.PI));
    }
}