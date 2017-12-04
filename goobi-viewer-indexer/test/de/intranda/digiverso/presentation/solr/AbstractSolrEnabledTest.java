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
package de.intranda.digiverso.presentation.solr;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.helper.SolrHelper;

/**
 * JUnit test classes that extend this class can use the embedded Solr server setup with an empty index.
 */
public abstract class AbstractSolrEnabledTest {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(AbstractSolrEnabledTest.class);

    private static final String CORE_NAME = "test-indexer";

    private static String solrPath = "/opt/digiverso/viewer/apache-solr/";
    
    private CoreContainer coreContainer;
    protected EmbeddedSolrServer server;
    protected SolrHelper solrHelper;

    @BeforeClass
    public static void setUpClass() throws Exception {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.indexOf("win") >= 0) {
            solrPath = "C:/digiverso/viewer/apache-solr-test/";
        }
        Path solrDir = Paths.get(solrPath);
        Assert.assertTrue("Solr folder not found in " + solrPath, Files.isDirectory(solrDir));
    }

    @Before
    public void setUp() throws Exception {
        coreContainer = new CoreContainer(solrPath);
        coreContainer.load();

        Assert.assertTrue(coreContainer.isLoaded(CORE_NAME));
        server = new EmbeddedSolrServer(coreContainer, CORE_NAME);
        Assert.assertNotNull(server);
        solrHelper = new SolrHelper(server);
    }

    @After
    public void tearDown() throws Exception {
        {
            Path indexerFolder = Paths.get("build/indexer");
            if (Files.isDirectory(indexerFolder)) {
                logger.info("Deleting {}...", indexerFolder);
                FileUtils.deleteDirectory(indexerFolder.toFile());
            }
            Assert.assertFalse(Files.isDirectory(indexerFolder));
        }
        {
            Path viewerRootFolder = Paths.get("build/viewer");
            if (Files.isDirectory(viewerRootFolder)) {
                logger.info("Deleting {}...", viewerRootFolder);
                FileUtils.deleteDirectory(viewerRootFolder.toFile());
                Assert.assertFalse(Files.isDirectory(viewerRootFolder));
            }
            Assert.assertFalse(Files.isDirectory(viewerRootFolder));
        }

        if (coreContainer != null && !coreContainer.isShutDown()) {
            coreContainer.shutdown();
        }

        logger.debug("Deleting index...");
        File dataDir = new File(solrPath + CORE_NAME + File.separator + "data");
        FileUtils.deleteDirectory(dataDir);
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