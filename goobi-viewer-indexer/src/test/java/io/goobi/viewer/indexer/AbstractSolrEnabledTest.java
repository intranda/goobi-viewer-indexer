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
import org.apache.solr.client.solrj.SolrClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;

/**
 * JUnit test classes that extend this class can use the embedded Solr server setup with an empty index.
 */
public abstract class AbstractSolrEnabledTest extends AbstractTest {

    /** Logger for this class. */
    private static Logger logger;

    protected SolrClient server;
    protected SolrSearchIndex searchIndex;

    @BeforeClass
    public static void setUpClass() throws Exception {
        AbstractTest.setUpClass();

        logger = LoggerFactory.getLogger(AbstractSolrEnabledTest.class);
    }

    @Before
    public void setUp() throws Exception {
        server = SolrSearchIndex.getNewHttpSolrServer(Configuration.getInstance("indexerconfig_solr_test.xml").getConfiguration("solrUrl"), 30000,
                30000);
        searchIndex = new SolrSearchIndex(server);
    }

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
        if (searchIndex.deleteByQuery("*:*")) {
            searchIndex.commit(false);
            logger.debug("Index cleared");
        }

        server.close();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        //        if (coreContainer != null) {
        //            coreContainer.shutdown();
        //        }
        //
        //        logger.info("Deleting index...");
        //        File dataDir = new File(solrPath + CORE_NAME + File.separator + "data");
        //        FileUtils.deleteDirectory(dataDir);
    }
}