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
import io.goobi.viewer.indexer.model.SolrConstants.MetadataGroupType;

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

    /**
     * @see MetadataConfigurationManager#loadFieldConfiguration(XMLConfiguration)
     * @verifies load nested group entities correctly
     */
    @Test
    public void loadFieldConfiguration_shouldLoadNestedGroupEntitiesCorrectly() throws Exception {
        List<FieldConfig> configItems = Configuration.getInstance().getMetadataConfigurationManager().getConfigurationListForField("MD_EVENT");
        Assert.assertNotNull(configItems);
        Assert.assertEquals(1, configItems.size());
        FieldConfig fieldConfig = configItems.get(0);
        Assert.assertTrue(fieldConfig.isGroupEntity());

        Assert.assertNotNull(fieldConfig.getGroupEntity());
        Assert.assertEquals(3, fieldConfig.getGroupEntity().getSubfields().size());

        Assert.assertEquals(1, fieldConfig.getGroupEntity().getChildren().size());
        GroupEntity child = fieldConfig.getGroupEntity().getChildren().get(0);
        Assert.assertEquals("MD_ARTIST", child.getName());
        Assert.assertEquals(MetadataGroupType.PERSON, child.getType());
        Assert.assertEquals("intranda:actor[intranda:role='Künstler/-in']", child.getXpath());
        Assert.assertEquals(8, child.getSubfields().size());
    }
}