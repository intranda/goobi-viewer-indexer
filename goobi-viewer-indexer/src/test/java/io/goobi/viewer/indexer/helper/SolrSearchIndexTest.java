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
package io.goobi.viewer.indexer.helper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import io.goobi.viewer.indexer.AbstractSolrEnabledTest;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;

public class SolrSearchIndexTest extends AbstractSolrEnabledTest {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(SolrSearchIndexTest.class);

    private SolrSearchIndex searchIndex;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        searchIndex = new SolrSearchIndex(client);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        {
            Path indexerFolder = Paths.get("target/indexer");
            if (Files.isDirectory(indexerFolder)) {
                logger.info("Deleting {}...", indexerFolder);
                FileUtils.deleteDirectory(indexerFolder.toFile());
            }
            Assert.assertFalse(Files.isDirectory(indexerFolder));
        }
        {
            Path viewerRootFolder = Paths.get("target/viewer");
            if (Files.isDirectory(viewerRootFolder)) {
                logger.info("Deleting {}...", viewerRootFolder);
                FileUtils.deleteDirectory(viewerRootFolder.toFile());
                Assert.assertFalse(Files.isDirectory(viewerRootFolder));
            }
            Assert.assertFalse(Files.isDirectory(viewerRootFolder));
        }

        // Delete all data after every test
        if (searchIndex != null && searchIndex.deleteByQuery("*:*")) {
            searchIndex.commit(false);
            logger.debug("Index cleared");
        }

        if (client != null) {
            client.close();
        }
    }

    /**
     * @see SolrSearchIndex#deleteDocuments(List)
     * @verifies return false if id list empty
     */
    @Test
    public void deleteDocuments_shouldReturnFalseIfIdListEmpty() throws Exception {
        SolrSearchIndex sh = new SolrSearchIndex(client);
        Assert.assertFalse(sh.deleteDocuments(Collections.emptyList()));
    }

    /**
     * @see SolrSearchIndex#checkAndCreateGroupDoc(String,String,long)
     * @verifies create new document with all values if none exists
     */
    @Test
    public void checkAndCreateGroupDoc_shouldCreateNewDocumentWithAllValuesIfNoneExists() throws Exception {
        Map<String, String> moreMetadata = new HashMap<>();
        moreMetadata.put("MD_SHELFMARK", "shelfmark");
        moreMetadata.put("MD_TITLE", "title");
        SolrInputDocument doc = searchIndex.checkAndCreateGroupDoc(SolrConstants.PREFIX_GROUPID + "TEST", "id10T", moreMetadata, 123456L);
        Assert.assertNotNull(doc);
        Assert.assertEquals("123456", doc.getFieldValue(SolrConstants.IDDOC));
        Long timestamp = (Long) doc.getFieldValue(SolrConstants.DATECREATED);
        Assert.assertNotNull(timestamp);
        Assert.assertEquals(timestamp, doc.getFieldValue(SolrConstants.DATEUPDATED));
        Assert.assertEquals(DocType.GROUP.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
        Assert.assertEquals("id10T", doc.getFieldValue(SolrConstants.PI));
        Assert.assertEquals(SolrConstants.PREFIX_GROUPID + "TEST", doc.getFieldValue(SolrConstants.GROUPTYPE));
        Assert.assertEquals("shelfmark", doc.getFieldValue("MD_SHELFMARK"));
        Assert.assertEquals("title", doc.getFieldValue("MD_TITLE"));
    }

    /**
     * @see SolrSearchIndex#checkAndCreateGroupDoc(String,String,long)
     * @verifies create updated document with all values if one already exists
     */
    @Test
    public void checkAndCreateGroupDoc_shouldCreateUpdatedDocumentWithAllValuesIfOneAlreadyExists() throws Exception {
        Map<String, String> moreMetadata = new HashMap<>();
        moreMetadata.put("MD_SHELFMARK", "old_shelfmark");
        moreMetadata.put("MD_TITLE", "old_title");
        SolrInputDocument doc = searchIndex.checkAndCreateGroupDoc(SolrConstants.PREFIX_GROUPID + "TEST", "id10T", moreMetadata, 123456L);
        Assert.assertNotNull(doc);
        searchIndex.writeToIndex(doc);
        searchIndex.commit(false);

        moreMetadata = new HashMap<>();
        moreMetadata.put("MD_SHELFMARK", "new_shelfmark");
        moreMetadata.put("MD_TITLE", "new_title");
        SolrInputDocument doc2 = searchIndex.checkAndCreateGroupDoc(SolrConstants.PREFIX_GROUPID + "TEST", "id10T", moreMetadata, 123456L);
        Assert.assertNotNull(doc2);
        Assert.assertEquals(doc.getFieldValue(SolrConstants.IDDOC), doc2.getFieldValue(SolrConstants.IDDOC));
        Assert.assertEquals(doc.getFieldValue(SolrConstants.DATECREATED), doc2.getFieldValue(SolrConstants.DATECREATED));
        Assert.assertNotEquals(doc.getFieldValue(SolrConstants.DATEUPDATED), doc2.getFieldValue(SolrConstants.DATEUPDATED));
        Assert.assertEquals(DocType.GROUP.name(), doc2.getFieldValue(SolrConstants.DOCTYPE));
        Assert.assertEquals("id10T", doc2.getFieldValue(SolrConstants.PI));
        Assert.assertEquals(SolrConstants.PREFIX_GROUPID + "TEST", doc2.getFieldValue(SolrConstants.GROUPTYPE));
        Assert.assertEquals("new_shelfmark", doc2.getFieldValue("MD_SHELFMARK"));
        Assert.assertEquals("new_title", doc2.getFieldValue("MD_TITLE"));
    }

    /**
     * @see SolrSearchIndex#checkAndCreateGroupDoc(String,String,Map,long)
     * @verifies add default field
     */
    @Test
    public void checkAndCreateGroupDoc_shouldAddDefaultField() throws Exception {
        Map<String, String> moreMetadata = new HashMap<>();
        moreMetadata.put("MD_SHELFMARK", "shelfmark");
        moreMetadata.put("MD_TITLE", "title");
        SolrInputDocument doc = searchIndex.checkAndCreateGroupDoc(SolrConstants.PREFIX_GROUPID + "TEST", "id10T", moreMetadata, 123456L);
        Assert.assertNotNull(doc);
        Assert.assertEquals(DocType.GROUP.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
        String defaultValue = (String) doc.getFieldValue(SolrConstants.DEFAULT);
        Assert.assertNotNull(defaultValue);
        Assert.assertTrue(defaultValue.contains("id10T"));
        Assert.assertTrue(defaultValue.contains("shelfmark"));
        Assert.assertTrue(defaultValue.contains("title"));
    }

    /**
     * @see SolrSearchIndex#checkAndCreateGroupDoc(String,String,Map,long)
     * @verifies add access conditions
     */
    @Test
    public void checkAndCreateGroupDoc_shouldAddAccessConditions() throws Exception {
        SolrInputDocument doc = searchIndex.checkAndCreateGroupDoc(SolrConstants.PREFIX_GROUPID + "TEST", "id10T", null, 123456L);
        Assert.assertNotNull(doc);
        Assert.assertEquals(SolrConstants.OPEN_ACCESS_VALUE, doc.getFieldValue(SolrConstants.ACCESSCONDITION));
    }

    /**
     * @see SolrSearchIndex#updateDoc(SolrDocument,Map)
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
            searchIndex.writeToIndex(doc);
            searchIndex.commit(false);
        }
        {
            // Update doc
            SolrDocumentList ret = searchIndex.search(SolrConstants.IDDOC + ":" + iddoc, null);
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
            searchIndex.updateDoc(doc, partialUpdates);
        }
        {
            // Fetch and check updated doc
            SolrDocumentList ret = searchIndex.search(SolrConstants.IDDOC + ":" + iddoc, null);
            Assert.assertNotNull(ret);
            Assert.assertFalse(ret.isEmpty());
            SolrDocument doc = ret.get(0);
            Assert.assertEquals(iddoc, doc.getFieldValue(SolrConstants.IDDOC));
            Assert.assertEquals("new fulltext", doc.getFieldValues("MD_FULLTEXT").iterator().next());
            Assert.assertEquals(2, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
            Assert.assertEquals("PPN123", doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
        }
    }

    /**
     * @see SolrSearchIndex#createDocument(List)
     * @verifies skip fields correctly
     */
    @Test
    public void createDocument_shouldSkipFieldsCorrectly() throws Exception {
        List<LuceneField> luceneFields = new ArrayList<>(2);
        luceneFields.add(new LuceneField("foo", "bar"));
        luceneFields.add(new LuceneField("skip", "me"));
        luceneFields.get(1).setSkip(true);

        SolrInputDocument doc = SolrSearchIndex.createDocument(luceneFields);
        Assert.assertNotNull(doc);
        Assert.assertEquals("bar", doc.getFieldValue("foo"));
        Assert.assertFalse(doc.containsKey("skip"));
    }

    /**
     * @see SolrSearchIndex#getBooleanFieldName(String)
     * @verifies boolify field correctly
     */
    @Test
    public void getBooleanFieldName_shouldBoolifyFieldCorrectly() throws Exception {
        Assert.assertEquals("BOOL_FOO", SolrSearchIndex.getBooleanFieldName("FOO"));
        Assert.assertEquals("BOOL_FOO", SolrSearchIndex.getBooleanFieldName("MD_FOO"));
        Assert.assertEquals("BOOL_FOO", SolrSearchIndex.getBooleanFieldName("MDNUM_FOO"));
        Assert.assertEquals("BOOL_FOO", SolrSearchIndex.getBooleanFieldName("SORT_FOO"));
    }
}