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

import org.junit.Assert;
import org.junit.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.helper.Configuration;

public class MetadataConfigurationManagerTest extends AbstractTest {

    /**
     * @see MetadataConfigurationManager#getLanguageMapping(String)
     * @verifies return correct mapping
     */
    @Test
    public void getLanguageMapping_shouldReturnCorrectMapping() throws Exception {
        Assert.assertEquals("en", MetadataConfigurationManager.getLanguageMapping("eng"));
        Assert.assertEquals("de", MetadataConfigurationManager.getLanguageMapping("ger"));
    }

    /**
     * @see MetadataConfigurationManager#getLanguageMapping(String)
     * @verifies return null if code not configured
     */
    @Test
    public void getLanguageMapping_shouldReturnNullIfCodeNotConfigured() throws Exception {
        Assert.assertNull(MetadataConfigurationManager.getLanguageMapping("epo"));
    }

    /**
     * @see MetadataConfigurationManager#getConfigurationListForField(String)
     * @verifies return correct FieldConfig
     */
    @Test
    public void getConfigurationListForField_shouldReturnCorrectFieldConfig() throws Exception {
        List<FieldConfig> result =
                Configuration.getInstance().getMetadataConfigurationManager().getConfigurationListForField("MD_TESTFIELD");
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("MD_TESTFIELD", result.get(0).getFieldname());
    }
}