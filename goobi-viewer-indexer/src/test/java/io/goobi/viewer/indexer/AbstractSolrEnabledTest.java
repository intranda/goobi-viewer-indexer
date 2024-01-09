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

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;

/**
 * JUnit test classes that extend this class can use the embedded Solr server setup with an empty index.
 */
public abstract class AbstractSolrEnabledTest extends AbstractTest {

    /** Logger for this class. */
    private static Logger logger = LogManager.getLogger(AbstractSolrEnabledTest.class);

    protected static Hotfolder hotfolder;

    protected SolrClient client;

    @BeforeAll
    public static void setUpClass() throws Exception {
        AbstractTest.setUpClass();

        logger = LogManager.getLogger(AbstractSolrEnabledTest.class);
    }

    @BeforeEach
    public void setUp() throws Exception {
        String solrUrl = SolrIndexerDaemon.getInstance().getConfiguration().getConfiguration("solrUrl");

        // Only allow localhost and default indexer test URL to avoid erasing production indexes
        Assertions.assertTrue(
                solrUrl.startsWith("http://localhost:") || solrUrl.equals("https://viewer-testing-index.goobi.io/solr/indexer-testing"),
                "Only default or localhost Solr URLs are allowed for testing.");

        SolrIndexerDaemon.getInstance().injectSearchIndex(new SolrSearchIndex(client));
    }

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
        if (SolrIndexerDaemon.getInstance().getSearchIndex() != null && SolrIndexerDaemon.getInstance().getSearchIndex().deleteByQuery("*:*")) {
            SolrIndexerDaemon.getInstance().getSearchIndex().commit(false);
            logger.debug("Index cleared");
        }

        if (client != null) {
            client.close();
        }
    }
}