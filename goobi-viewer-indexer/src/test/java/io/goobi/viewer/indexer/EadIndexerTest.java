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
import java.util.Map;

import org.apache.solr.common.SolrDocumentList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.model.SolrConstants;

class EadIndexerTest extends AbstractSolrEnabledTest {

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
    }

    /**
     * @see DublinCoreIndexer#DublinCoreIndexer(Hotfolder)
     * @verifies set attributes correctly
     */
    @Test
    void DublinCoreIndexer_shouldSetAttributesCorrectly() throws Exception {
        Assertions.assertNotNull(hotfolder);
        Indexer indexer = new DublinCoreIndexer(hotfolder);
        Assertions.assertEquals(hotfolder, indexer.hotfolder);
    }

    /**
     * @see DublinCoreIndexer#addToIndex(Path,boolean,Map)
     * @verifies add record to index correctly
     */
    @Test
    void addToIndex_shouldAddRecordToIndexCorrectly(@TempDir Path tempDir) throws Exception {
        Path eadFile = Paths.get("src/test/resources/EAD/Akte_Koch.xml");
        Assertions.assertTrue(Files.isRegularFile(eadFile));
        
        Path eadFileCopy = Paths.get(tempDir.toAbsolutePath().toString(), "record.xml");
        Files.copy(eadFile, eadFileCopy, StandardCopyOption.REPLACE_EXISTING);
        Assertions.assertTrue(Files.isRegularFile(eadFileCopy));

        Indexer indexer = new EadIndexer(hotfolder);
        indexer.addToIndex(eadFileCopy, false, new HashMap<>());

        SolrDocumentList result =
                SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":Akte_Koch", null);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("Akte_Koch", result.get(0).getFieldValue(SolrConstants.PI));
        Assertions.assertNotNull(result.get(0).getFieldValue(SolrConstants.SEARCHTERMS_ARCHIVE));
        Assertions.assertTrue(Files.isRegularFile(eadFile)); // Original file didn't get deleted
    }
}