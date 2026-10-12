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
package io.goobi.viewer.indexer.model.writestrategy;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractSolrEnabledTest;
import io.goobi.viewer.indexer.EadIndexer;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.model.IndexingResult;
import io.goobi.viewer.indexer.model.SolrConstants;

class StreamingSolrWriteStrategyTest extends AbstractSolrEnabledTest {

    private static final String PI = "STREAMING_TEST";
    private static final int NUM_CHILD_DOCS = 5;

    private Path eadFile = Paths.get("src/test/resources/EAD/Akte_Koch.xml");

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
    }

    /**
     * @see StreamingSolrWriteStrategy#addDocs(java.util.List)
     * @verifies send full batches while docs are added
     */
    @Test
    void addDocs_shouldSendFullBatchesWhileDocsAreAdded() throws Exception {
        StreamingSolrWriteStrategy strategy = new StreamingSolrWriteStrategy(SolrIndexerDaemon.getInstance().getSearchIndex(), 2);
        try {
            addChildDocs(strategy);
            // 5 docs with batch size 2: two full batches sent, one doc still buffered
            Assertions.assertEquals(4, strategy.getSentDocsCount());
        } finally {
            strategy.cleanup();
        }
    }

    /**
     * @see StreamingSolrWriteStrategy#writeDocs(boolean)
     * @verifies write all docs and aggregate super fields into root doc
     */
    @Test
    void writeDocs_shouldWriteAllDocsAndAggregateSuperFieldsIntoRootDoc() throws Exception {
        SolrSearchIndex searchIndex = SolrIndexerDaemon.getInstance().getSearchIndex();
        StreamingSolrWriteStrategy strategy = new StreamingSolrWriteStrategy(searchIndex, 2);
        SolrInputDocument rootDoc = createRootDoc();
        try {
            addChildDocs(strategy);
            strategy.setRootDoc(rootDoc);
            strategy.writeDocs(true);
        } finally {
            strategy.cleanup();
        }
        searchIndex.commit(false);

        // cleanup() after a complete record must not delete anything
        Assertions.assertEquals(NUM_CHILD_DOCS + 1L, searchIndex.getNumHits(SolrConstants.PI_TOPSTRUCT + ":" + PI));
        // SUPER* values of all streamed child docs (plus the root doc's own) end up in the root doc
        Assertions.assertNotNull(rootDoc.getFieldValues(SolrConstants.SUPERSEARCHTERMS_ARCHIVE));
        Assertions.assertEquals(NUM_CHILD_DOCS + 1, rootDoc.getFieldValues(SolrConstants.SUPERSEARCHTERMS_ARCHIVE).size());
    }

    /**
     * @see StreamingSolrWriteStrategy#cleanup()
     * @verifies delete sent docs if record incomplete
     */
    @Test
    void cleanup_shouldDeleteSentDocsIfRecordIncomplete() throws Exception {
        SolrSearchIndex searchIndex = SolrIndexerDaemon.getInstance().getSearchIndex();
        StreamingSolrWriteStrategy strategy = new StreamingSolrWriteStrategy(searchIndex, 2);
        addChildDocs(strategy);
        Assertions.assertEquals(4, strategy.getSentDocsCount());

        // Simulate a failure before writeDocs(): the docs already sent must not survive the next commit
        strategy.cleanup();
        searchIndex.commit(false);

        Assertions.assertEquals(0, searchIndex.getNumHits(SolrConstants.PI_TOPSTRUCT + ":" + PI));
    }

    /**
     * @see AbstractWriteStrategy#create(Path,java.util.Map,Hotfolder,boolean)
     * @verifies return streaming strategy for large file if allowed and enabled
     */
    @Test
    void create_shouldReturnStreamingStrategyForLargeFileIfAllowedAndEnabled() throws Exception {
        Configuration config = SolrIndexerDaemon.getInstance().getConfiguration();
        config.overrideValue("performance.metsFileSizeThreshold", 1);
        config.overrideValue("performance.streamingWriteStrategy", true);
        try {
            Hotfolder hf = new Hotfolder(config.getHotfolderPath());
            Assertions.assertInstanceOf(StreamingSolrWriteStrategy.class, AbstractWriteStrategy.create(eadFile, new HashMap<>(), hf, true));
        } finally {
            config.overrideValue("performance.metsFileSizeThreshold", 10485760);
            config.overrideValue("performance.streamingWriteStrategy", false);
        }
    }

    /**
     * @see AbstractWriteStrategy#create(Path,java.util.Map,Hotfolder,boolean)
     * @verifies return serializing strategy for large file if streaming not allowed
     */
    @Test
    void create_shouldReturnSerializingStrategyForLargeFileIfStreamingNotAllowed() throws Exception {
        Configuration config = SolrIndexerDaemon.getInstance().getConfiguration();
        config.overrideValue("performance.metsFileSizeThreshold", 1);
        config.overrideValue("performance.streamingWriteStrategy", true);
        try {
            Hotfolder hf = new Hotfolder(config.getHotfolderPath());
            Assertions.assertInstanceOf(SerializingSolrWriteStrategy.class, AbstractWriteStrategy.create(eadFile, new HashMap<>(), hf, false));
            // Disabled in the configuration
            config.overrideValue("performance.streamingWriteStrategy", false);
            Assertions.assertInstanceOf(SerializingSolrWriteStrategy.class, AbstractWriteStrategy.create(eadFile, new HashMap<>(), hf, true));
        } finally {
            config.overrideValue("performance.metsFileSizeThreshold", 10485760);
            config.overrideValue("performance.streamingWriteStrategy", false);
        }
    }

    /**
     * @see AbstractWriteStrategy#create(Path,java.util.Map,Hotfolder,boolean)
     * @verifies return lazy strategy for small file
     */
    @Test
    void create_shouldReturnLazyStrategyForSmallFile() throws Exception {
        ISolrWriteStrategy strategy = AbstractWriteStrategy.create(eadFile, new HashMap<>(), hotfolder, true);
        Assertions.assertEquals(LazySolrWriteStrategy.class, strategy.getClass());
    }

    /**
     * @see StreamingSolrWriteStrategy#writeDocs(boolean)
     * @verifies index EAD record with the same docs as lazy strategy
     */
    @Test
    void writeDocs_shouldIndexEADRecordWithTheSameDocsAsLazyStrategy() throws Exception {
        SolrSearchIndex searchIndex = SolrIndexerDaemon.getInstance().getSearchIndex();
        String query = SolrConstants.PI_TOPSTRUCT + ":Akte_Koch";

        // Reference: in-memory strategy
        IndexingResult lazyResult = new EadIndexer(hotfolder).index(eadFile, new HashMap<>(), new LazySolrWriteStrategy(searchIndex));
        Assertions.assertNull(lazyResult.getError());
        searchIndex.commit(false);
        long lazyCount = searchIndex.getNumHits(query);
        Assertions.assertTrue(lazyCount > 3, "test record must have enough docs to span several batches");
        Set<String> lazyIddocs = getIddocs(searchIndex, query);

        // Re-index with the streaming strategy and a small batch size
        IndexingResult streamingResult = new EadIndexer(hotfolder).index(eadFile, new HashMap<>(), new StreamingSolrWriteStrategy(searchIndex, 3));
        Assertions.assertNull(streamingResult.getError());
        searchIndex.commit(false);

        // Same number of docs, and the previous instance has been replaced
        Assertions.assertEquals(lazyCount, searchIndex.getNumHits(query));
        Set<String> streamingIddocs = getIddocs(searchIndex, query);
        Assertions.assertTrue(Collections.disjoint(lazyIddocs, streamingIddocs));
    }

    /**
     * Adds {@link #NUM_CHILD_DOCS} structure docs to the given strategy.
     *
     * @param strategy
     */
    private static void addChildDocs(ISolrWriteStrategy strategy) {
        for (int i = 0; i < NUM_CHILD_DOCS; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField(SolrConstants.IDDOC, PI + "_child_" + i);
            doc.addField(SolrConstants.PI_TOPSTRUCT, PI);
            doc.addField(SolrConstants.GROUPFIELD, PI);
            doc.addField(SolrConstants.DOCTYPE, SolrConstants.DocType.DOCSTRCT.name());
            doc.addField(SolrConstants.SEARCHTERMS_ARCHIVE, "term" + i);
            strategy.addDoc(doc);
        }
    }

    /**
     * @return Root doc for {@link #PI}
     */
    private static SolrInputDocument createRootDoc() {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(SolrConstants.IDDOC, PI + "_root");
        doc.addField(SolrConstants.PI, PI);
        doc.addField(SolrConstants.PI_TOPSTRUCT, PI);
        doc.addField(SolrConstants.GROUPFIELD, PI);
        doc.addField(SolrConstants.DOCTYPE, SolrConstants.DocType.DOCSTRCT.name());
        doc.addField(SolrConstants.ISWORK, true);
        doc.addField(SolrConstants.SEARCHTERMS_ARCHIVE, "rootterm");
        return doc;
    }

    /**
     * @param searchIndex
     * @param query
     * @return IDDOC values of all docs matching query
     * @throws Exception
     */
    private static Set<String> getIddocs(SolrSearchIndex searchIndex, String query) throws Exception {
        Set<String> ret = new HashSet<>();
        for (SolrDocument doc : searchIndex.search(query, Collections.singletonList(SolrConstants.IDDOC), 10000)) {
            ret.add(String.valueOf(doc.getFieldValue(SolrConstants.IDDOC)));
        }
        return ret;
    }
}
