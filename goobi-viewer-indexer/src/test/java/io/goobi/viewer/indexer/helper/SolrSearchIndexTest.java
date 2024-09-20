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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractSolrEnabledTest;
import io.goobi.viewer.indexer.MetsIndexer;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;

class SolrSearchIndexTest extends AbstractSolrEnabledTest {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(SolrSearchIndexTest.class);

    private SolrClient client;

    private SolrSearchIndex searchIndex;

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        String solrUrl = SolrIndexerDaemon.getInstance().getConfiguration().getConfiguration("solrUrl");
        client = SolrSearchIndex.getNewHttpSolrClient(solrUrl, true);
        searchIndex = new SolrSearchIndex(client);
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        {
            Path indexerFolder = Paths.get("target/indexer");
            if (Files.isDirectory(indexerFolder)) {
                logger.info("Deleting {}...", indexerFolder);
                FileUtils.deleteDirectory(indexerFolder.toFile());
            }
            Assertions.assertFalse(Files.isDirectory(indexerFolder));
        }
        {
            Path viewerRootFolder = Paths.get("target/viewer");
            if (Files.isDirectory(viewerRootFolder)) {
                logger.info("Deleting {}...", viewerRootFolder);
                FileUtils.deleteDirectory(viewerRootFolder.toFile());
                Assertions.assertFalse(Files.isDirectory(viewerRootFolder));
            }
            Assertions.assertFalse(Files.isDirectory(viewerRootFolder));
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
    void deleteDocuments_shouldReturnFalseIfIdListEmpty() throws Exception {
        SolrSearchIndex sh = new SolrSearchIndex(client);
        Assertions.assertFalse(sh.deleteDocuments(Collections.emptyList()));
    }

    /**
     * @see SolrSearchIndex#getSolrSchemaDocument(String)
     * @verifies return schema document correctly
     */
    @Test
    void getSolrSchemaDocument_shouldReturnSchemaDocumentCorrectly() throws Exception {
        org.jdom2.Document doc = SolrSearchIndex.getSolrSchemaDocument(SolrIndexerDaemon.getInstance().getConfiguration().getSolrUrl());
        Assertions.assertNotNull(doc);
        Assertions.assertEquals("schema", doc.getRootElement().getName());
    }

    /**
     * @see SolrSearchIndex#checkAndCreateGroupDoc(String,String,long)
     * @verifies create new document with all values if none exists
     */
    @Test
    void checkAndCreateGroupDoc_shouldCreateNewDocumentWithAllValuesIfNoneExists() {
        Map<String, String> moreMetadata = new HashMap<>();
        moreMetadata.put("MD_SHELFMARK", "shelfmark");
        moreMetadata.put("MD_TITLE", "title");
        SolrInputDocument doc = searchIndex.checkAndCreateGroupDoc(SolrConstants.PREFIX_GROUPID + "TEST", "id10T", moreMetadata, 123456L);
        Assertions.assertNotNull(doc);
        Assertions.assertEquals("123456", doc.getFieldValue(SolrConstants.IDDOC));
        Long timestamp = (Long) doc.getFieldValue(SolrConstants.DATECREATED);
        Assertions.assertNotNull(timestamp);
        Assertions.assertEquals(timestamp, doc.getFieldValue(SolrConstants.DATEUPDATED));
        Assertions.assertEquals(DocType.GROUP.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
        Assertions.assertEquals("id10T", doc.getFieldValue(SolrConstants.PI));
        Assertions.assertEquals(SolrConstants.PREFIX_GROUPID + "TEST", doc.getFieldValue(SolrConstants.GROUPTYPE));
        Assertions.assertEquals("shelfmark", doc.getFieldValue("MD_SHELFMARK"));
        Assertions.assertEquals("title", doc.getFieldValue("MD_TITLE"));
    }

    /**
     * @see SolrSearchIndex#checkAndCreateGroupDoc(String,String,long)
     * @verifies create updated document with all values if one already exists
     */
    @Test
    void checkAndCreateGroupDoc_shouldCreateUpdatedDocumentWithAllValuesIfOneAlreadyExists() throws Exception {
        Map<String, String> moreMetadata = new HashMap<>();
        moreMetadata.put("MD_SHELFMARK", "old_shelfmark");
        moreMetadata.put("MD_TITLE", "old_title");
        SolrInputDocument doc = searchIndex.checkAndCreateGroupDoc(SolrConstants.PREFIX_GROUPID + "TEST", "id10T", moreMetadata, 123456L);
        Assertions.assertNotNull(doc);
        searchIndex.writeToIndex(doc);
        searchIndex.commit(false);

        moreMetadata = new HashMap<>();
        moreMetadata.put("MD_SHELFMARK", "new_shelfmark");
        moreMetadata.put("MD_TITLE", "new_title");
        SolrInputDocument doc2 = searchIndex.checkAndCreateGroupDoc(SolrConstants.PREFIX_GROUPID + "TEST", "id10T", moreMetadata, 123456L);
        Assertions.assertNotNull(doc2);
        Assertions.assertEquals(doc.getFieldValue(SolrConstants.IDDOC), doc2.getFieldValue(SolrConstants.IDDOC));
        Assertions.assertEquals(doc.getFieldValue(SolrConstants.DATECREATED), doc2.getFieldValue(SolrConstants.DATECREATED));
        Assertions.assertNotEquals(doc.getFieldValue(SolrConstants.DATEUPDATED), doc2.getFieldValue(SolrConstants.DATEUPDATED));
        Assertions.assertEquals(DocType.GROUP.name(), doc2.getFieldValue(SolrConstants.DOCTYPE));
        Assertions.assertEquals("id10T", doc2.getFieldValue(SolrConstants.PI));
        Assertions.assertEquals(SolrConstants.PREFIX_GROUPID + "TEST", doc2.getFieldValue(SolrConstants.GROUPTYPE));
        Assertions.assertEquals("new_shelfmark", doc2.getFieldValue("MD_SHELFMARK"));
        Assertions.assertEquals("new_title", doc2.getFieldValue("MD_TITLE"));
    }

    /**
     * @see SolrSearchIndex#checkAndCreateGroupDoc(String,String,Map,long)
     * @verifies add default field
     */
    @Test
    void checkAndCreateGroupDoc_shouldAddDefaultField() {
        Map<String, String> moreMetadata = new HashMap<>();
        moreMetadata.put("MD_SHELFMARK", "shelfmark");
        moreMetadata.put("MD_TITLE", "title");
        SolrInputDocument doc = searchIndex.checkAndCreateGroupDoc(SolrConstants.PREFIX_GROUPID + "TEST", "id10T", moreMetadata, 123456L);
        Assertions.assertNotNull(doc);
        Assertions.assertEquals(DocType.GROUP.name(), doc.getFieldValue(SolrConstants.DOCTYPE));
        String defaultValue = (String) doc.getFieldValue(SolrConstants.DEFAULT);
        Assertions.assertNotNull(defaultValue);
        Assertions.assertTrue(defaultValue.contains("id10T"));
        Assertions.assertTrue(defaultValue.contains("shelfmark"));
        Assertions.assertTrue(defaultValue.contains("title"));
    }

    /**
     * @see SolrSearchIndex#checkAndCreateGroupDoc(String,String,Map,long)
     * @verifies add access conditions
     */
    @Test
    void checkAndCreateGroupDoc_shouldAddAccessConditions() {
        SolrInputDocument doc = searchIndex.checkAndCreateGroupDoc(SolrConstants.PREFIX_GROUPID + "TEST", "id10T", null, 123456L);
        Assertions.assertNotNull(doc);
        Assertions.assertEquals(SolrConstants.OPEN_ACCESS_VALUE, doc.getFieldValue(SolrConstants.ACCESSCONDITION));
    }

    /**
     * @see SolrSearchIndex#updateDoc(SolrDocument,Map)
     * @verifies update doc correctly
     */
    @Test
    void updateDoc_shouldUpdateDocCorrectly() throws Exception {
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
            Assertions.assertNotNull(ret);
            Assertions.assertFalse(ret.isEmpty());
            SolrDocument doc = ret.get(0);
            Assertions.assertEquals(iddoc, doc.getFieldValue(SolrConstants.IDDOC));
            Assertions.assertEquals(1, doc.getFieldValues(SolrConstants.DATEUPDATED).size());

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
            Assertions.assertNotNull(ret);
            Assertions.assertFalse(ret.isEmpty());
            SolrDocument doc = ret.get(0);
            Assertions.assertEquals(iddoc, doc.getFieldValue(SolrConstants.IDDOC));
            Assertions.assertEquals("new fulltext", doc.getFieldValues("MD_FULLTEXT").iterator().next());
            Assertions.assertEquals(2, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
            Assertions.assertEquals("PPN123", doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
        }
    }

    /**
     * @see SolrSearchIndex#createDocument(List)
     * @verifies skip fields correctly
     */
    @Test
    void createDocument_shouldSkipFieldsCorrectly() {
        List<LuceneField> luceneFields = new ArrayList<>(2);
        luceneFields.add(new LuceneField("foo", "bar"));
        luceneFields.add(new LuceneField("skip", "me"));
        luceneFields.get(1).setSkip(true);

        SolrInputDocument doc = SolrSearchIndex.createDocument(luceneFields);
        Assertions.assertNotNull(doc);
        Assertions.assertEquals("bar", doc.getFieldValue("foo"));
        Assertions.assertFalse(doc.containsKey("skip"));
    }

    /**
     * @see SolrSearchIndex#getBooleanFieldName(String)
     * @verifies boolify field correctly
     */
    @Test
    void getBooleanFieldName_shouldBoolifyFieldCorrectly() {
        Assertions.assertEquals("BOOL_FOO", SolrSearchIndex.getBooleanFieldName("FOO"));
        Assertions.assertEquals("BOOL_FOO", SolrSearchIndex.getBooleanFieldName("MD_FOO"));
        Assertions.assertEquals("BOOL_FOO", SolrSearchIndex.getBooleanFieldName("MDNUM_FOO"));
        Assertions.assertEquals("BOOL_FOO", SolrSearchIndex.getBooleanFieldName("SORT_FOO"));
    }

    /**
     * @see SolrSearchIndex#checkDuplicateFieldValues(List,List)
     * @verifies return correct identifiers
     */
    @Test
    void checkDuplicateFieldValues_shouldReturnCorrectIdentifiers() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());

        String[] ret = new MetsIndexer(hotfolder).index(Paths.get("src/test/resources/METS/H030001_mets.xml"), new HashMap<>(), null, 1, false);
        Assertions.assertNull(ret[1]);
        ret = new MetsIndexer(hotfolder).index(Paths.get("src/test/resources/METS/AC06736966.xml"), new HashMap<>(), null, 1, false);
        Assertions.assertNull(ret[1]);
        Set<String> result = SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .checkDuplicateFieldValues(Collections.singletonList(SolrConstants.PI_TOPSTRUCT), Arrays.asList("AC06736966", "H030001"), null);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.contains("H030001"));
        Assertions.assertTrue(result.contains("AC06736966"));
    }

    /**
     * @see SolrSearchIndex#checkDuplicateFieldValues(List,List,String)
     * @verifies ignore records that match skipPi
     */
    @Test
    void checkDuplicateFieldValues_shouldIgnoreRecordsThatMatchSkipPi() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());

        String[] ret = new MetsIndexer(hotfolder).index(Paths.get("src/test/resources/METS/H030001_mets.xml"), new HashMap<>(), null, 1, false);
        Assertions.assertNull(ret[1]);
        ret = new MetsIndexer(hotfolder).index(Paths.get("src/test/resources/METS/AC06736966.xml"), new HashMap<>(), null, 1, false);
        Assertions.assertNull(ret[1]);
        Set<String> result = SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .checkDuplicateFieldValues(Collections.singletonList(SolrConstants.PI_TOPSTRUCT), Arrays.asList("AC06736966", "H030001"),
                        "AC06736966");
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains("H030001"));
    }
}