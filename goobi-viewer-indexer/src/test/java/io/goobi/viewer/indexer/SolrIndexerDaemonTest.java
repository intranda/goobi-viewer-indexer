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

import org.jdom2.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;

class SolrIndexerDaemonTest extends AbstractTest {

    @BeforeAll
    public static void setUpClass() throws Exception {
        AbstractTest.setUpClass();
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Reset config before every test
        SolrIndexerDaemon.getInstance().injectConfiguration(new Configuration(TEST_CONFIG_PATH));
    }

    /**
     * @see SolrIndexerDaemon#init()
     * @verifies throw FatalIndexerException if solr schema name could not be checked
     */
    @Test
    void init_shouldThrowFatalIndexerExceptionIfSolrSchemaNameCouldNotBeChecked() throws Exception {
        SolrIndexerDaemon instance = SolrIndexerDaemon.getInstance();
        instance.getConfiguration().overrideValue("init.solrUrl", "https://foo.bar/schema.xml");
        Assertions.assertThrows(FatalIndexerException.class, () -> instance.init());
    }

    /**
     * @see SolrIndexerDaemon#stop()
     * @verifies set running to false
     */
    @Test
    void stop_shouldSetRunningToFalse() throws Exception {
        SolrIndexerDaemon instance = SolrIndexerDaemon.getInstance();
        instance.setRunning(true);
        instance.stop();
        Assertions.assertFalse(instance.isRunning());
    }

    /**
     * @see SolrIndexerDaemon#checkSolrSchemaName(Document)
     * @verifies return false if doc null
     */
    @Test
    void checkSolrSchemaName_shouldReturnFalseIfDocNull() throws Exception {
        Assertions.assertFalse(SolrIndexerDaemon.checkSolrSchemaName(null));
    }

    /**
     * @see SolrIndexerDaemon#checkSolrSchemaName(Document)
     * @verifies return true if schema compatible
     */
    @Test
    void checkSolrSchemaName_shouldReturnTrueIfSchemaCompatible() throws Exception {
        org.jdom2.Document doc = SolrSearchIndex.getSolrSchemaDocument(SolrIndexerDaemon.getInstance().getConfiguration().getSolrUrl());
        Assertions.assertNotNull(doc);
        Assertions.assertTrue(SolrIndexerDaemon.checkSolrSchemaName(doc));
    }

    /**
     * @see SolrIndexerDaemon#setConfFileName(String)
     * @verifies set confFileName correctly
     */
    @Test
    void setConfFileName_shouldSetConfFileNameCorrectly() throws Exception {
        SolrIndexerDaemon instance = SolrIndexerDaemon.getInstance().setConfFileName("config_new.xml");
        Assertions.assertEquals("config_new.xml", instance.getConfFileName());
    }

    /**
     * @see SolrIndexerDaemon#getSearchIndex()
     * @verifies create new instance if none exists
     */
    @Test
    void getSearchIndex_shouldCreateNewInstanceIfNoneExists() throws Exception {
        SolrIndexerDaemon instance = SolrIndexerDaemon.getInstance();
        instance.injectSearchIndex(null);
        Assertions.assertNotNull(instance.getSearchIndex());
    }
}