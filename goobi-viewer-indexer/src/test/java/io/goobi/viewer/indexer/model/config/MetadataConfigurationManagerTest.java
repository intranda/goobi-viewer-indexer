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

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.model.SolrConstants.MetadataGroupType;

class MetadataConfigurationManagerTest extends AbstractTest {

    /**
     * @see MetadataConfigurationManager#getLanguageMapping(String)
     * @verifies return correct mapping
     */
    @Test
    void getLanguageMapping_shouldReturnCorrectMapping() throws Exception {
        Assertions.assertEquals("en", MetadataConfigurationManager.getLanguageMapping("eng"));
        Assertions.assertEquals("de", MetadataConfigurationManager.getLanguageMapping("ger"));
    }

    /**
     * @see MetadataConfigurationManager#getLanguageMapping(String)
     * @verifies return null if code not configured
     */
    @Test
    void getLanguageMapping_shouldReturnNullIfCodeNotConfigured() throws Exception {
        Assertions.assertNull(MetadataConfigurationManager.getLanguageMapping("epo"));
    }

    /**
     * @see MetadataConfigurationManager#getConfigurationListForField(String)
     * @verifies return correct FieldConfig
     */
    @Test
    void getConfigurationListForField_shouldReturnCorrectFieldConfig() throws Exception {
        List<FieldConfig> result =
                SolrIndexerDaemon.getInstance().getConfiguration().getMetadataConfigurationManager().getConfigurationListForField("MD_TESTFIELD");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("MD_TESTFIELD", result.get(0).getFieldname());
    }

    /**
     * @see MetadataConfigurationManager#loadFieldConfiguration(XMLConfiguration)
     * @verifies load all field configs correctly
     */
    @Test
    void loadFieldConfiguration_shouldLoadAllFieldConfigsCorrectly() throws Exception {
        List<FieldConfig> configItems =
                SolrIndexerDaemon.getInstance().getConfiguration().getMetadataConfigurationManager().getConfigurationListForField("MD_TESTFIELD");
        Assertions.assertNotNull(configItems);
        Assertions.assertEquals(1, configItems.size());

        Assertions.assertTrue(SolrIndexerDaemon.getInstance()
                .getConfiguration()
                .getMetadataConfigurationManager()
                .getFieldsToAddToParents()
                .contains("!MD_TESTFIELD"));
    }

    /**
     * @see MetadataConfigurationManager#loadFieldConfiguration(XMLConfiguration)
     * @verifies load nested group entities correctly
     */
    @Test
    void loadFieldConfiguration_shouldLoadNestedGroupEntitiesCorrectly() throws Exception {
        List<FieldConfig> configItems =
                SolrIndexerDaemon.getInstance().getConfiguration().getMetadataConfigurationManager().getConfigurationListForField("MD_EVENT");
        Assertions.assertNotNull(configItems);
        Assertions.assertEquals(1, configItems.size());
        FieldConfig fieldConfig = configItems.get(0);
        Assertions.assertTrue(fieldConfig.isGroupEntity());

        Assertions.assertNotNull(fieldConfig.getGroupEntity());
        Assertions.assertEquals(3, fieldConfig.getGroupEntity().getSubfields().size());

        Assertions.assertEquals(3, fieldConfig.getGroupEntity().getChildren().size());
        GroupEntity child = fieldConfig.getGroupEntity().getChildren().get(0);
        Assertions.assertEquals("MD_ARTIST", child.getName());
        Assertions.assertEquals(MetadataGroupType.PERSON, child.getType());
        Assertions.assertEquals("intranda:actor[intranda:role='Künstler/-in']", child.getXpath());
        Assertions.assertEquals(8, child.getSubfields().size());
    }

    /**
     * @see MetadataConfigurationManager#readGroupEntity(HierarchicalConfiguration)
     * @verifies read group entity correctly
     */
    @Test
    void readGroupEntity_shouldReadGroupEntityCorrectly() throws Exception {
        List<FieldConfig> configItems =
                SolrIndexerDaemon.getInstance()
                        .getConfiguration()
                        .getMetadataConfigurationManager()
                        .getConfigurationListForField("MD_ACCESSLOCATIONS");
        Assertions.assertNotNull(configItems);
        Assertions.assertEquals(1, configItems.size());
        FieldConfig fieldConfig = configItems.get(0);
        Assertions.assertTrue(fieldConfig.isGroupEntity());
        Assertions.assertEquals(4, fieldConfig.getGroupEntity().getSubfields().size());
        Assertions.assertTrue(fieldConfig.getGroupEntity().isAddAuthorityDataToDocstruct());
        Assertions.assertTrue(fieldConfig.getGroupEntity().isAddCoordsToDocstruct());
    }

    /**
     * @see MetadataConfigurationManager#readGroupEntity(HierarchicalConfiguration)
     * @verifies recursively read child group entities
     */
    @Test
    void readGroupEntity_shouldRecursivelyReadChildGroupEntities() throws Exception {
        List<FieldConfig> configItems =
                SolrIndexerDaemon.getInstance().getConfiguration().getMetadataConfigurationManager().getConfigurationListForField("MD_EVENT");
        Assertions.assertNotNull(configItems);
        Assertions.assertEquals(1, configItems.size());
        FieldConfig fieldConfig = configItems.get(0);
        Assertions.assertTrue(fieldConfig.isGroupEntity());

        Assertions.assertNotNull(fieldConfig.getGroupEntity());
        Assertions.assertEquals(3, fieldConfig.getGroupEntity().getSubfields().size());

        Assertions.assertEquals(3, fieldConfig.getGroupEntity().getChildren().size());
        GroupEntity child = fieldConfig.getGroupEntity().getChildren().get(0);
        Assertions.assertEquals("MD_ARTIST", child.getName());
        Assertions.assertEquals(MetadataGroupType.PERSON, child.getType());
        Assertions.assertEquals("intranda:actor[intranda:role='Künstler/-in']", child.getXpath());
        Assertions.assertEquals(8, child.getSubfields().size());
    }
}