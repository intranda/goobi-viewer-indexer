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
package io.goobi.viewer.indexer.model.config;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.helper.Configuration;

class ConfigurationTest extends AbstractTest {

    /**
     * @see Configuration#getEmailRecipients()
     * @verifies return correct values
     */
    @Test
    void getEmailRecipients_shouldReturnCorrectValues() {
        List<String> result = SolrIndexerDaemon.getInstance().getConfiguration().getEmailRecipients();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("foo@example.com", result.get(0));
    }
}