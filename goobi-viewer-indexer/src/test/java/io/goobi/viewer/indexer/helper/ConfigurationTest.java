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
package io.goobi.viewer.indexer.helper;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.model.SolrConstants.MetadataGroupType;
import io.goobi.viewer.indexer.model.config.FieldConfig;
import io.goobi.viewer.indexer.model.config.GroupEntity;
import io.goobi.viewer.indexer.model.config.NonSortConfiguration;
import io.goobi.viewer.indexer.model.config.SubfieldConfig;
import io.goobi.viewer.indexer.model.config.ValueNormalizer.ValueNormalizerPosition;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

public class ConfigurationTest extends AbstractTest {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(ConfigurationTest.class);

    private static Hotfolder hotfolder;

    @BeforeClass
    public static void setUpClass() throws Exception {
        AbstractTest.setUpClass();

        hotfolder = new Hotfolder(TEST_CONFIG_PATH, null, null);
    }

    @Test
    public void folderTest() throws Exception {
        Assert.assertTrue(Files.isDirectory(hotfolder.getHotfolderPath()));
        Assert.assertTrue(new File(Configuration.getInstance().getString("init.viewerHome")).isDirectory());
        Assert.assertTrue(Files.isDirectory(hotfolder.getSuccessFolder()));
        Assert.assertTrue(Files.isDirectory(hotfolder.getUpdatedMets()));
        Assert.assertTrue(Files.isDirectory(hotfolder.getDeletedMets()));
        Assert.assertTrue(Files.isDirectory(hotfolder.getErrorMets()));
    }

    @Test
    public void configItemTest() throws Exception {
        List<String> fieldNames = Configuration.getInstance().getMetadataConfigurationManager().getListWithAllFieldNames();
        Assert.assertEquals(122, fieldNames.size());
        List<FieldConfig> fieldConfigList =
                Configuration.getInstance().getMetadataConfigurationManager().getConfigurationListForField("MD_TESTFIELD");
        Assert.assertNotNull(fieldConfigList);
        Assert.assertEquals(1, fieldConfigList.size());
        FieldConfig fieldConfig = fieldConfigList.get(0);

        Assert.assertNotNull(fieldConfig.getxPathConfigurations());
        Assert.assertEquals(2, fieldConfig.getxPathConfigurations().size());
        Assert.assertEquals("all", fieldConfig.getParents());
        Assert.assertEquals("false", fieldConfig.getChild());
        Assert.assertEquals("first", fieldConfig.getNode());
        Assert.assertTrue(fieldConfig.isOneToken());
        Assert.assertTrue(fieldConfig.isOneField());
        Assert.assertEquals(" , ", fieldConfig.getOneFieldSeparator());
        Assert.assertEquals("XXX", fieldConfig.getConstantValue());
        Assert.assertTrue(fieldConfig.isLowercase());
        Assert.assertTrue(fieldConfig.isAddToDefault());
        Assert.assertFalse(fieldConfig.isAddUntokenizedVersion());
        Assert.assertTrue(fieldConfig.isAddSortField());
        Assert.assertTrue(fieldConfig.isAddSortFieldToTopstruct());
        Assert.assertTrue(fieldConfig.isAddExistenceBoolean());
        Assert.assertEquals("#", fieldConfig.getSplittingCharacter());
        Assert.assertTrue(fieldConfig.isNormalizeYear());
        Assert.assertEquals(2, fieldConfig.getNormalizeYearMinDigits());
        Assert.assertTrue(fieldConfig.isGroupEntity());
        Assert.assertTrue(fieldConfig.isAllowDuplicateValues());

        GroupEntity groupEntity = fieldConfig.getGroupEntity();
        Assert.assertNotNull(groupEntity);
        Assert.assertEquals(MetadataGroupType.OTHER, groupEntity.getType());
        Assert.assertEquals("https://example.com?param1=value1&param2=value2", groupEntity.getUrl());
        {
            SubfieldConfig fieldSubconfig = groupEntity.getSubfields().get("field1");
            Assert.assertNotNull(fieldSubconfig);
            Assert.assertEquals(2, fieldSubconfig.getXpaths().size());
            Assert.assertEquals("xpath1", fieldSubconfig.getXpaths().get(0));
            Assert.assertEquals("xpath2", fieldSubconfig.getXpaths().get(1));
            Assert.assertTrue(fieldSubconfig.isMultivalued());
            Assert.assertTrue(fieldSubconfig.isAddSortField());
            Assert.assertEquals("def", fieldSubconfig.getDefaultValues().get("xpath2"));
        }
        {
            SubfieldConfig fieldSubconfig = groupEntity.getSubfields().get("field2");
            Assert.assertNotNull(fieldSubconfig);
            Assert.assertEquals(1, fieldSubconfig.getXpaths().size());
            Assert.assertEquals("xpath3", fieldSubconfig.getXpaths().get(0));
            Assert.assertFalse(fieldSubconfig.isMultivalued());
        }

        Map<Object, String> replaceRules = fieldConfig.getReplaceRules();
        Assert.assertNotNull(replaceRules);
        Assert.assertEquals(2, replaceRules.size());
        logger.info(replaceRules.keySet().toString());
        Assert.assertEquals("replace1 ", replaceRules.get("stringToReplace1 "));
        Assert.assertEquals("replace2", replaceRules.get("REGEX:[ ]*stringToReplace2[ ]*"));

        List<NonSortConfiguration> nonSortConfigurations = fieldConfig.getNonSortConfigurations();
        Assert.assertNotNull(nonSortConfigurations);
        Assert.assertEquals(1, nonSortConfigurations.size());
        Assert.assertEquals("nonSortPrefix", nonSortConfigurations.get(0).getPrefix());
        Assert.assertEquals("nonSortSuffix", nonSortConfigurations.get(0).getSuffix());
    }

    @Test
    public void metadataConfigTest() throws Exception {
        Assert.assertEquals(122, Configuration.getInstance().getMetadataConfigurationManager().getListWithAllFieldNames().size());
        List<FieldConfig> configItems = Configuration.getInstance().getMetadataConfigurationManager().getConfigurationListForField("MD_TESTFIELD");
        Assert.assertNotNull(configItems);
        Assert.assertEquals(1, configItems.size());

        FieldConfig configItem = configItems.get(0);
        Assert.assertEquals("MD_TESTFIELD", configItem.getFieldname());
        Assert.assertEquals(2, configItem.getxPathConfigurations().size());
        Assert.assertEquals("first", configItem.getNode());
        Assert.assertEquals("all", configItem.getParents());
        Assert.assertEquals("false", configItem.getChild());
        Assert.assertTrue(configItem.isOneToken());
        Assert.assertTrue(configItem.isOneField());
        Assert.assertEquals("XXX", configItem.getConstantValue());
        Assert.assertTrue(configItem.isLowercase());
        Assert.assertTrue(configItem.isAddToDefault());
        Assert.assertFalse(configItem.isAddUntokenizedVersion());
        Assert.assertTrue(configItem.isAddSortField());
        Assert.assertTrue(configItem.isAddSortFieldToTopstruct());
        Assert.assertEquals("#", configItem.getSplittingCharacter());
        Assert.assertTrue(configItem.isNormalizeYear());
        Assert.assertTrue(configItem.isInterpolateYears());
        Assert.assertEquals(2, configItem.getReplaceRules().size());
        Assert.assertEquals("replace1 ", configItem.getReplaceRules().get("stringToReplace1 "));
        Assert.assertEquals("replace2", configItem.getReplaceRules().get("REGEX:[ ]*stringToReplace2[ ]*"));
        Assert.assertEquals(1, configItem.getNonSortConfigurations().size());
        Assert.assertEquals("nonSortPrefix", configItem.getNonSortConfigurations().get(0).getPrefix());
        Assert.assertEquals("nonSortSuffix", configItem.getNonSortConfigurations().get(0).getSuffix());

        // Value normalizers
        Assert.assertNotNull(configItem.getValueNormalizers());
        Assert.assertEquals(2, configItem.getValueNormalizers().size());
        Assert.assertTrue(configItem.getValueNormalizers().get(0).isConvertRoman());
        Assert.assertEquals("foo ([C|I|M|V|X]+) .*$", configItem.getValueNormalizers().get(0).getRegex());
        Assert.assertEquals(5, configItem.getValueNormalizers().get(1).getTargetLength());
        Assert.assertEquals('a', configItem.getValueNormalizers().get(1).getFiller());
        Assert.assertEquals(ValueNormalizerPosition.FRONT, configItem.getValueNormalizers().get(1).getPosition());
        Assert.assertEquals("foo ([0-9]+) .*$", configItem.getValueNormalizers().get(1).getRegex());

        Assert.assertEquals("mods:coordinates/point", configItem.getGeoJSONSource());
        Assert.assertEquals(" / ", configItem.getGeoJSONSourceSeparator());
    }

    /**
     * @see Configuration#getViewerUrl()
     * @verifies return correct value
     */
    @Test
    public void getViewerUrl_shouldReturnCorrectValue() throws Exception {
        Assert.assertEquals("http://localhost:8080/viewer", Configuration.getInstance().getViewerUrl());
    }

    /**
     * @see Configuration#getPageCountStart()
     * @verifies return correct value
     */
    @Test
    public void getPageCountStart_shouldReturnCorrectValue() throws Exception {
        Assert.assertEquals(0, Configuration.getInstance("src/test/resources/config_indexer.test.xml").getPageCountStart());
    }

    /**
     * @see Configuration#initNamespaces()
     * @verifies add custom namespaces correctly
     */
    @Test
    public void initNamespaces_shouldAddCustomNamespacesCorrectly() throws Exception {
        Configuration.getInstance().initNamespaces();
        Assert.assertEquals(17, Configuration.getInstance().getNamespaces().size());
        Assert.assertNotNull(Configuration.getInstance().getNamespaces().get("intranda"));
    }

    /**
     * @see Configuration#getViewerHome()
     * @verifies return correct value
     */
    @Test
    public void getViewerHome_shouldReturnCorrectValue() throws Exception {
        Assert.assertEquals("target/viewer/", Configuration.getInstance().getViewerHome());
    }

    /**
     * @see Configuration#getEmptyOrderLabelReplacement()
     * @verifies return correct value
     */
    @Test
    public void getEmptyOrderLabelReplacement_shouldReturnCorrectValue() throws Exception {
        Assert.assertEquals("X", Configuration.getInstance().getEmptyOrderLabelReplacement());
    }

    /**
     * @see Configuration#getDataRepositoryConfigurations()
     * @verifies return all items
     */
    @Test
    public void getDataRepositoryConfigurations_shouldReturnAllItems() throws Exception {
        List<DataRepository> resp = Configuration.getInstance().getDataRepositoryConfigurations();
        Assert.assertEquals(3, resp.size());
        Assert.assertEquals("target/viewer/data/1", resp.get(0).getPath());
        Assert.assertEquals("target/viewer/data/2", resp.get(1).getPath());
        Assert.assertEquals("target/viewer/data/3", resp.get(2).getPath());
        Assert.assertEquals(10737418240L, resp.get(0).getBuffer());
        Assert.assertEquals(104857600L, resp.get(1).getBuffer());
        Assert.assertEquals(1000L, resp.get(2).getBuffer());
    }

    /**
     * @see Configuration#getViewerAuthorizationToken()
     * @verifies return correct value
     */
    @Test
    public void getViewerAuthorizationToken_shouldReturnCorrectValue() throws Exception {
        Assert.assertEquals("test", Configuration.getInstance().getViewerAuthorizationToken());
    }

    /**
     * @see Configuration#isCountHotfolderFiles()
     * @verifies return correct value
     */
    @Test
    public void isCountHotfolderFiles_shouldReturnCorrectValue() throws Exception {
        Assert.assertFalse(Configuration.getInstance().isCountHotfolderFiles());
    }

    /**
     * @see Configuration#isAuthorityDataCacheEnabled()
     * @verifies return correct value
     */
    @Test
    public void isAuthorityDataCacheEnabled_shouldReturnCorrectValue() throws Exception {
        Assert.assertFalse(Configuration.getInstance().isAuthorityDataCacheEnabled());
    }

    /**
     * @see Configuration#getAuthorityDataCacheSizeWarningThreshold()
     * @verifies return correct value
     */
    @Test
    public void getAuthorityDataCacheSizeWarningThreshold_shouldReturnCorrectValue() throws Exception {
        Assert.assertEquals(100, Configuration.getInstance().getAuthorityDataCacheSizeWarningThreshold());
    }

    /**
     * @see Configuration#getAuthorityDataCacheRecordTTL()
     * @verifies return correct value
     */
    @Test
    public void getAuthorityDataCacheRecordTTL_shouldReturnCorrectValue() throws Exception {
        Assert.assertEquals(12, Configuration.getInstance().getAuthorityDataCacheRecordTTL());
    }

    /**
     * @see Configuration#isProxyEnabled()
     * @verifies return correct value
     */
    @Test
    public void isProxyEnabled_shouldReturnCorrectValue() throws Exception {
        Assert.assertTrue(Configuration.getInstance().isProxyEnabled());
    }

    /**
     * @see Configuration#getProxyUrl()
     * @verifies return correct value
     */
    @Test
    public void getProxyUrl_shouldReturnCorrectValue() throws Exception {
        Assert.assertEquals("my.proxy", Configuration.getInstance().getProxyUrl());
    }

    /**
     * @see Configuration#getProxyPort()
     * @verifies return correct value
     */
    @Test
    public void getProxyPort_shouldReturnCorrectValue() throws Exception {
        Assert.assertEquals(9999, Configuration.getInstance().getProxyPort());
    }

    /**
     * @see Configuration#isHostProxyWhitelisted(String)
     * @verifies return true if host whitelisted
     */
    @Test
    public void isHostProxyWhitelistedd_shouldReturnTrueIfHostWhitelisted() throws Exception {
        Assert.assertTrue(Configuration.getInstance().isHostProxyWhitelisted("http://localhost:1234"));
    }
}
