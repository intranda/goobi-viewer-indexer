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
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

class EadIndexerTest extends AbstractSolrEnabledTest {

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
    }

    /**
     * @see EadIndexer#addToIndex(Path,boolean,Map)
     * @verifies add record to index correctly
     */
    @Test
    void addToIndex_shouldAddRecordToIndexCorrectly(@TempDir Path tempDir) throws Exception {
        Path eadFile = Paths.get("src/test/resources/EAD/Akte_Koch.xml");
        Assertions.assertTrue(Files.isRegularFile(eadFile));

        Path eadFileCopy = Paths.get(tempDir.toAbsolutePath().toString(), "Akte_Koch.xml");
        Files.copy(eadFile, eadFileCopy, StandardCopyOption.REPLACE_EXISTING);
        Assertions.assertTrue(Files.isRegularFile(eadFileCopy));

        Indexer indexer = new EadIndexer(hotfolder);
        List<String> identifiers = indexer.addToIndex(eadFileCopy, new HashMap<>());
        Assertions.assertNotNull(identifiers);
        Assertions.assertEquals(1, identifiers.size());
        Assertions.assertEquals("Akte_Koch", identifiers.get(0));

        SolrDocumentList result =
                SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":Akte_Koch", null);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("Akte_Koch", result.get(0).getFieldValue(SolrConstants.PI));
        Assertions.assertNotNull(result.get(0).getFieldValue(SolrConstants.SEARCHTERMS_ARCHIVE));
        Assertions.assertTrue(Files.isRegularFile(eadFile)); // Original file didn't get deleted
    }

    /**
     * @see EadIndexer#indexAllChildren(IndexObject,int,ISolrWriteStrategy,boolean)
     * @verifies assign contiguous sibling order to all children in parallel
     */
    @Test
    void indexAllChildren_shouldAssignContiguousSiblingOrderToAllChildrenInParallel(@TempDir Path tempDir) throws Exception {
        Configuration config = SolrIndexerDaemon.getInstance().getConfiguration();
        int originalThreads = config.getThreads();
        // Force the parallel branch in EadIndexer.indexAllChildren (default test config uses a single thread)
        int threads = 4;
        config.overrideValue("performance.threads", threads);
        try {
            Path eadFile = Paths.get("src/test/resources/EAD/Akte_Koch.xml");
            Assertions.assertTrue(Files.isRegularFile(eadFile));

            Path eadFileCopy = Paths.get(tempDir.toAbsolutePath().toString(), "Akte_Koch.xml");
            Files.copy(eadFile, eadFileCopy, StandardCopyOption.REPLACE_EXISTING);

            Indexer indexer = new EadIndexer(hotfolder);
            List<String> identifiers = indexer.addToIndex(eadFileCopy, new HashMap<>());
            Assertions.assertEquals(1, identifiers.size());

            // Every child docstruct carries SORTNUM_ARCHIVE_ORDER (its sibling index) and IDDOC_PARENT
            String orderField = SolrConstants.PREFIX_SORTNUM + "ARCHIVE_ORDER";
            SolrDocumentList docs = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(SolrConstants.PI_TOPSTRUCT + ":Akte_Koch AND " + orderField + ":[* TO *]",
                            Arrays.asList(SolrConstants.IDDOC_PARENT, orderField));
            Assertions.assertFalse(docs.isEmpty());

            // Group each child's sibling order by its parent
            Map<String, List<Integer>> ordersByParent = new HashMap<>();
            for (SolrDocument doc : docs) {
                Object parent = doc.getFieldValue(SolrConstants.IDDOC_PARENT);
                Assertions.assertNotNull(parent, "child docstruct must have IDDOC_PARENT");
                int order = Integer.parseInt(String.valueOf(doc.getFieldValue(orderField)));
                ordersByParent.computeIfAbsent(String.valueOf(parent), k -> new ArrayList<>()).add(order);
            }

            // At least one sibling group must be large enough to have taken the parallel branch
            Assertions.assertTrue(ordersByParent.values().stream().anyMatch(orders -> orders.size() >= threads),
                    "expected a sibling group of at least " + threads + " children to exercise the parallel branch");

            // Each sibling group must carry exactly the indices 0..n-1: no gaps, no duplicates, regardless of completion order
            for (Map.Entry<String, List<Integer>> entry : ordersByParent.entrySet()) {
                List<Integer> actual = new ArrayList<>(entry.getValue());
                Collections.sort(actual);
                List<Integer> expected = IntStream.range(0, actual.size()).boxed().collect(Collectors.toList());
                Assertions.assertEquals(expected, actual,
                        "children of parent " + entry.getKey() + " must have contiguous sibling order 0.." + (actual.size() - 1));
            }
        } finally {
            config.overrideValue("performance.threads", originalThreads);
        }
    }
}