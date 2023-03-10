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
import java.util.Map;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.model.SolrConstants;

public class CmsPageIndexerTest extends AbstractSolrEnabledTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(TEST_CONFIG_PATH, client);
    }

    /**
     * @see CmsPageIndexer#CmsPageIndexer(Hotfolder)
     * @verifies set attributes correctly
     */
    @Test
    public void CmsPageIndexer_shouldSetAttributesCorrectly() throws Exception {
        Assert.assertNotNull(hotfolder);
        Indexer indexer = new CmsPageIndexer(hotfolder);
        Assert.assertEquals(hotfolder, indexer.hotfolder);
    }

    /**
     * @see CmsPageIndexer#addToIndex(Path,boolean,Map)
     * @verifies add record to index correctly
     */
    @Test
    public void addToIndex_shouldAddRecordToIndexCorrectly() throws Exception {
        Path cmsFile = Paths.get("src/test/resources/indexed_cms/CMS123.xml");
        assertTrue(Files.isRegularFile(cmsFile));
        Path indexFile = Paths.get(hotfolder.getHotfolderPath().toString(), "CMS123.xml");
        Files.copy(cmsFile, indexFile);
        assertTrue(Files.isRegularFile(indexFile));

        Indexer indexer = new CmsPageIndexer(hotfolder);
        indexer.addToIndex(indexFile, false, new HashMap<>());

        SolrDocumentList result = hotfolder.getSearchIndex().search(SolrConstants.PI + ":CMS123", null);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        SolrDocument doc = result.get(0);

        assertEquals("CMS123", doc.getFieldValue(SolrConstants.PI));
        assertEquals("CMS Page Title", doc.getFieldValue(SolrConstants.LABEL));
        assertEquals("CMS Page Title", SolrSearchIndex.getSingleFieldStringValue(doc, "MD_TITLE"));

        Collection<Object> categories = doc.getFieldValues("MD_CATEGORY");
        assertNotNull(categories);
        assertEquals(2, categories.size());
        assertTrue(categories.contains("cat1"));
        assertTrue(categories.contains("cat2"));

        assertEquals("one two three", SolrSearchIndex.getSingleFieldStringValue(doc, "MD_TEXT_LANG_EN"));
        assertEquals("eins zwei drei", SolrSearchIndex.getSingleFieldStringValue(doc, "MD_TEXT_LANG_DE"));
    }
}