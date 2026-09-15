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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrDocumentList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

class Ead3IndexerTest extends AbstractSolrEnabledTest {

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
    }

    /**
     * @see Ead3Indexer#addToIndex(Path,boolean,Map)
     * @verifies add record to index correctly
     */
    @Test
    void addToIndex_shouldAddRecordToIndexCorrectly(@TempDir Path tempDir) throws Exception {
        Path eadFile = Paths.get("src/test/resources/EAD/EAD3_example.xml");
        Assertions.assertTrue(Files.isRegularFile(eadFile));

        Path eadFileCopy = Paths.get(tempDir.toAbsolutePath().toString(), "EAD3_example.xml");
        Files.copy(eadFile, eadFileCopy, StandardCopyOption.REPLACE_EXISTING);
        Assertions.assertTrue(Files.isRegularFile(eadFileCopy));

        Indexer indexer = new Ead3Indexer(hotfolder);
        indexer.addToIndex(eadFileCopy, new HashMap<>());

        SolrDocumentList result =
                SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":EAD3_example", null);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("EAD3_example", result.get(0).getFieldValue(SolrConstants.PI));
        Assertions.assertNotNull(result.get(0).getFieldValue(SolrConstants.SEARCHTERMS_ARCHIVE));
        Assertions.assertTrue(Files.isRegularFile(eadFile)); // Original file didn't get deleted
    }

    /**
     * @see EadIndexer#indexAllChildren(IndexObject,int,ISolrWriteStrategy,boolean)
     * @verifies index ead3 namespace children correctly
     */
    @Test
    void indexAllChildren_shouldIndexEad3NamespaceChildrenCorrectly(@TempDir Path tempDir) throws Exception {
        Path eadFile = Paths.get("src/test/resources/EAD/EAD3_example.xml");
        Assertions.assertTrue(Files.isRegularFile(eadFile));

        Path eadFileCopy = Paths.get(tempDir.toAbsolutePath().toString(), "EAD3_example.xml");
        Files.copy(eadFile, eadFileCopy, StandardCopyOption.REPLACE_EXISTING);

        Indexer indexer = new Ead3Indexer(hotfolder);
        List<String> identifiers = indexer.addToIndex(eadFileCopy, new HashMap<>());
        Assertions.assertEquals(1, identifiers.size());

        // The EAD3 record nests <ead:c> elements across several levels. Each must be resolved via the EAD3
        // namespace (not left unresolved) and indexed as a child docstruct carrying the sibling order field.
        String orderField = SolrConstants.PREFIX_SORTNUM + "ARCHIVE_ORDER";
        SolrDocumentList children = SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search(SolrConstants.PI_TOPSTRUCT + ":EAD3_example AND " + orderField + ":[* TO *]", null);
        Assertions.assertNotNull(children);
        // Non-empty proves top-level resolution; more than one proves recursion into nested EAD3-namespace children.
        Assertions.assertTrue(children.size() >= 2, "expected nested EAD3 <ead:c> children to be indexed");
    }
}