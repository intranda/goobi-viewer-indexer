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
package de.intranda.digiverso.presentation.solr.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.AbstractSolrEnabledTest;
import de.intranda.digiverso.presentation.solr.model.SolrConstants;
import de.intranda.digiverso.presentation.solr.model.SolrConstants.DocType;

public class SolrHelperTest extends AbstractSolrEnabledTest {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(SolrHelperTest.class);

    /**
     * @see SolrHelper#deleteDocuments(List)
     * @verifies return false if id list empty
     */
    @Test
    public void deleteDocuments_shouldReturnFalseIfIdListEmpty() throws Exception {
        SolrHelper sh = new SolrHelper(server);
        Assert.assertFalse(sh.deleteDocuments(new ArrayList<String>()));
    }

    /**
     * @see SolrHelper#checkAndCreateGroupDoc(String,String,long)
     * @verifies create new document with all values if none exists
     */
    @Test
    public void checkAndCreateGroupDoc_shouldCreateNewDocumentWithAllValuesIfNoneExists() throws Exception {
        Map<String, String> moreMetadata = new HashMap<>();
        moreMetadata.put("MD_SHELFMARK", "shelfmark");
        moreMetadata.put("MD_TITLE", "title");
        SolrInputDocument doc = solrHelper.checkAndCreateGroupDoc(SolrConstants.GROUPID_ + "TEST", "id10T", moreMetadata, 123456L);
        Assert.assertNotNull(doc);
        Assert.assertEquals("123456", doc.getFieldValue(SolrConstants.IDDOC));
        Long timestamp = (Long) doc.getFieldValue(SolrConstants.DATECREATED);
        Assert.assertNotNull(timestamp);
        Assert.assertEquals(timestamp, doc.getFieldValue(SolrConstants.DATEUPDATED));
        Assert.assertEquals(DocType.GROUP.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
        Assert.assertEquals("id10T", doc.getFieldValue(SolrConstants.PI));
        Assert.assertEquals(SolrConstants.GROUPID_ + "TEST", doc.getFieldValue(SolrConstants.GROUPTYPE));
        Assert.assertEquals("shelfmark", doc.getFieldValue("MD_SHELFMARK"));
        Assert.assertEquals("title", doc.getFieldValue("MD_TITLE"));
    }

    /**
     * @see SolrHelper#checkAndCreateGroupDoc(String,String,long)
     * @verifies create updated document with all values if one already exists
     */
    @Test
    public void checkAndCreateGroupDoc_shouldCreateUpdatedDocumentWithAllValuesIfOneAlreadyExists() throws Exception {
        Map<String, String> moreMetadata = new HashMap<>();
        moreMetadata.put("MD_SHELFMARK", "old_shelfmark");
        moreMetadata.put("MD_TITLE", "old_title");
        SolrInputDocument doc = solrHelper.checkAndCreateGroupDoc(SolrConstants.GROUPID_ + "TEST", "id10T", moreMetadata, 123456L);
        Assert.assertNotNull(doc);
        solrHelper.writeToIndex(doc);
        solrHelper.commit(false);

        moreMetadata = new HashMap<>();
        moreMetadata.put("MD_SHELFMARK", "new_shelfmark");
        moreMetadata.put("MD_TITLE", "new_title");
        SolrInputDocument doc2 = solrHelper.checkAndCreateGroupDoc(SolrConstants.GROUPID_ + "TEST", "id10T", moreMetadata, 123456L);
        Assert.assertNotNull(doc2);
        Assert.assertEquals(doc.getFieldValue(SolrConstants.IDDOC), doc2.getFieldValue(SolrConstants.IDDOC));
        Assert.assertEquals(doc.getFieldValue(SolrConstants.DATECREATED), doc2.getFieldValue(SolrConstants.DATECREATED));
        Assert.assertNotEquals(doc.getFieldValue(SolrConstants.DATEUPDATED), doc2.getFieldValue(SolrConstants.DATEUPDATED));
        Assert.assertEquals(DocType.GROUP.name(), doc2.getFieldValue(SolrConstants.DOCTYPE));
        Assert.assertEquals("id10T", doc2.getFieldValue(SolrConstants.PI));
        Assert.assertEquals(SolrConstants.GROUPID_ + "TEST", doc2.getFieldValue(SolrConstants.GROUPTYPE));
        Assert.assertEquals("new_shelfmark", doc2.getFieldValue("MD_SHELFMARK"));
        Assert.assertEquals("new_title", doc2.getFieldValue("MD_TITLE"));
    }

    /**
     * @see SolrHelper#updateDoc(SolrDocument,Map)
     * @verifies update doc correctly
     */
    @Test
    public void updateDoc_shouldUpdateDocCorrectly() throws Exception {
        String iddoc = "12345";

        {
            // Create original doc
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField(SolrConstants.IDDOC, iddoc);
            doc.addField(SolrConstants.GROUPFIELD, iddoc);
            doc.addField(SolrConstants.FULLTEXT, "fulltext");
            doc.addField("MD_FULLTEXT", "fulltext");
            doc.addField(SolrConstants.DATEUPDATED, System.currentTimeMillis());
            doc.addField(SolrConstants.PI_TOPSTRUCT, "PPN123");
            Assert.assertTrue(solrHelper.writeToIndex(doc));
            solrHelper.commit(false);
        }
        {
            // Update doc
            SolrDocumentList ret = solrHelper.search(SolrConstants.IDDOC + ":" + iddoc, null);
            Assert.assertNotNull(ret);
            Assert.assertFalse(ret.isEmpty());
            SolrDocument doc = ret.get(0);
            Assert.assertEquals(iddoc, doc.getFieldValue(SolrConstants.IDDOC));
            Assert.assertEquals(1, doc.getFieldValues(SolrConstants.DATEUPDATED).size());

            Map<String, Map<String, Object>> partialUpdates = new HashMap<>();
            {
                Map<String, Object> update = new HashMap<>();
                update.put("set", "new fulltext");
                partialUpdates.put(SolrConstants.FULLTEXT, update);
                partialUpdates.put("MD_FULLTEXT", update);
            }
            {
                Map<String, Object> update = new HashMap<>();
                update.put("add", System.currentTimeMillis());
                partialUpdates.put(SolrConstants.DATEUPDATED, update);
            }
            Assert.assertTrue(solrHelper.updateDoc(doc, partialUpdates));
        }
        {
            // Fetch and check updated doc
            SolrDocumentList ret = solrHelper.search(SolrConstants.IDDOC + ":" + iddoc, null);
            Assert.assertNotNull(ret);
            Assert.assertFalse(ret.isEmpty());
            SolrDocument doc = ret.get(0);
            Assert.assertEquals(iddoc, doc.getFieldValue(SolrConstants.IDDOC));
            Assert.assertEquals("new fulltext", doc.getFieldValues("MD_FULLTEXT").iterator().next());
            Assert.assertEquals(2, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
            Assert.assertEquals("PPN123", doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
        }
    }
}