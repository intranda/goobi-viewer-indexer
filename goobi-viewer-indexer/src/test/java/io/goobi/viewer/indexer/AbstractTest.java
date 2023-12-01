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

import java.io.File;

import org.junit.BeforeClass;

import io.goobi.viewer.indexer.helper.Configuration;

/**
 * JUnit test classes that extend this class will have test-specific logging configurations.
 */
public abstract class AbstractTest {

    public static final String TEST_CONFIG_PATH = new File("src/test/resources/config_indexer.test.xml").getAbsolutePath();
    public static final String TEST_LOG_CONFIG_PATH = new File("src/test/resources/log4j2.test.xml").getAbsolutePath();

    @BeforeClass
    public static void setUpClass() throws Exception {
        System.setProperty("log4j.configurationFile", TEST_LOG_CONFIG_PATH);
        SolrIndexerDaemon.getInstance().injectConfiguration(new Configuration(TEST_CONFIG_PATH));
    }
}
