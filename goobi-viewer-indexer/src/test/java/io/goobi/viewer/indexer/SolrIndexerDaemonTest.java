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

import org.junit.Assert;
import org.junit.Test;

public class SolrIndexerDaemonTest {

    /**
     * @see SolrIndexerDaemon#stop()
     * @verifies set running to false
     */
    @Test
    public void stop_shouldSetRunningToFalse() throws Exception {
        SolrIndexerDaemon instance = SolrIndexerDaemon.getInstance();
        instance.running = true;
        instance.stop();
        Assert.assertFalse(instance.running);
    }

    /**
     * @see SolrIndexerDaemon#checkSolrSchemaName(Document)
     * @verifies return false if doc null
     */
    @Test
    public void checkSolrSchemaName_shouldReturnFalseIfDocNull() throws Exception {
        Assert.assertFalse(SolrIndexerDaemon.checkSolrSchemaName(null));
    }

    /**
     * @see SolrIndexerDaemon#setConfFileName(String)
     * @verifies set confFileName correctly
     */
    @Test
    public void setConfFileName_shouldSetConfFileNameCorrectly() throws Exception {
        SolrIndexerDaemon instance = SolrIndexerDaemon.getInstance().setConfFileName("config_new.xml");
        Assert.assertEquals("config_new.xml", instance.confFileName);
    }

    /**
     * @see SolrIndexerDaemon#getSearchIndex()
     * @verifies created new instance if none exists
     */
    @Test
    public void getSearchIndex_shouldCreatedNewInstanceIfNoneExists() throws Exception {
        Assert.assertNotNull(SolrIndexerDaemon.getInstance().getSearchIndex());
    }
}