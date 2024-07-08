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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.SolrConstants.MetadataGroupType;
import io.goobi.viewer.indexer.model.config.FieldConfig;
import io.goobi.viewer.indexer.model.config.GroupEntity;
import io.goobi.viewer.indexer.model.config.NonSortConfiguration;
import io.goobi.viewer.indexer.model.config.SubfieldConfig;
import io.goobi.viewer.indexer.model.config.ValueNormalizer.ValueNormalizerPosition;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

class ConfigurationTest extends AbstractTest {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(ConfigurationTest.class);

    private static Hotfolder hotfolder;

    @BeforeAll
    public static void setUpClass() throws Exception {
        AbstractTest.setUpClass();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
    }

    @Test
    void folderTest() throws Exception {
        assertTrue(Files.isDirectory(hotfolder.getHotfolderPath()));
        assertTrue(new File(SolrIndexerDaemon.getInstance().getConfiguration().getString("init.viewerHome")).isDirectory());
        assertTrue(Files.isDirectory(hotfolder.getSuccessFolder()));
        assertTrue(Files.isDirectory(hotfolder.getUpdatedMets()));
        assertTrue(Files.isDirectory(hotfolder.getDeletedMets()));
        assertTrue(Files.isDirectory(hotfolder.getErrorMets()));
    }

    @Test
    void configItemTest() throws Exception {
        List<String> fieldNames =
                SolrIndexerDaemon.getInstance().getConfiguration().getMetadataConfigurationManager().getListWithAllFieldNames(FileFormat.METS);
        assertEquals(106, fieldNames.size());
        List<FieldConfig> fieldConfigList =
                SolrIndexerDaemon.getInstance().getConfiguration().getMetadataConfigurationManager().getConfigurationListForField("MD_TESTFIELD");
        Assertions.assertNotNull(fieldConfigList);
        assertEquals(1, fieldConfigList.size());
        FieldConfig fieldConfig = fieldConfigList.get(0);

        Assertions.assertNotNull(fieldConfig.getxPathConfigurations());
        assertEquals(2, fieldConfig.getxPathConfigurations().size());
        assertEquals("all", fieldConfig.getParents());
        assertEquals("false", fieldConfig.getChild());
        assertEquals("first", fieldConfig.getNode());
        assertTrue(fieldConfig.isOneToken());
        assertTrue(fieldConfig.isOneField());
        assertEquals(" , ", fieldConfig.getOneFieldSeparator());
        assertEquals("XXX", fieldConfig.getConstantValue());
        assertTrue(fieldConfig.isLowercase());
        assertTrue(fieldConfig.isAddToDefault());
        assertFalse(fieldConfig.isAddUntokenizedVersion());
        assertTrue(fieldConfig.isAddSortField());
        assertTrue(fieldConfig.isAddSortFieldToTopstruct());
        assertTrue(fieldConfig.isAddExistenceBoolean());
        assertEquals("#", fieldConfig.getSplittingCharacter());
        assertTrue(fieldConfig.isNormalizeYear());
        assertEquals(2, fieldConfig.getNormalizeYearMinDigits());
        assertTrue(fieldConfig.isGroupEntity());
        assertTrue(fieldConfig.isAllowDuplicateValues());

        GroupEntity groupEntity = fieldConfig.getGroupEntity();
        Assertions.assertNotNull(groupEntity);
        assertEquals(MetadataGroupType.OTHER, groupEntity.getType());
        assertEquals("https://example.com?param1=value1&param2=value2", groupEntity.getUrl());
        {
            SubfieldConfig fieldSubconfig = groupEntity.getSubfields().get("field1");
            Assertions.assertNotNull(fieldSubconfig);
            assertEquals(2, fieldSubconfig.getXpaths().size());
            assertEquals("xpath1", fieldSubconfig.getXpaths().get(0));
            assertEquals("xpath2", fieldSubconfig.getXpaths().get(1));
            assertTrue(fieldSubconfig.isMultivalued());
            assertTrue(fieldSubconfig.isAddSortField());
            assertEquals("def", fieldSubconfig.getDefaultValues().get("xpath2"));
        }
        {
            SubfieldConfig fieldSubconfig = groupEntity.getSubfields().get("field2");
            Assertions.assertNotNull(fieldSubconfig);
            assertEquals(1, fieldSubconfig.getXpaths().size());
            assertEquals("xpath3", fieldSubconfig.getXpaths().get(0));
            assertFalse(fieldSubconfig.isMultivalued());
        }

        Map<Object, String> replaceRules = fieldConfig.getReplaceRules();
        Assertions.assertNotNull(replaceRules);
        assertEquals(2, replaceRules.size());
        logger.info(replaceRules.keySet().toString());
        assertEquals("replace1 ", replaceRules.get("stringToReplace1 "));
        assertEquals("replace2", replaceRules.get("REGEX:[ ]*stringToReplace2[ ]*"));

        List<NonSortConfiguration> nonSortConfigurations = fieldConfig.getNonSortConfigurations();
        Assertions.assertNotNull(nonSortConfigurations);
        assertEquals(1, nonSortConfigurations.size());
        assertEquals("nonSortPrefix", nonSortConfigurations.get(0).getPrefix());
        assertEquals("nonSortSuffix", nonSortConfigurations.get(0).getSuffix());
    }

    @Test
    void metadataConfigTest() throws Exception {
        assertEquals(106,
                SolrIndexerDaemon.getInstance()
                        .getConfiguration()
                        .getMetadataConfigurationManager()
                        .getListWithAllFieldNames(FileFormat.METS)
                        .size());
        List<FieldConfig> configItems =
                SolrIndexerDaemon.getInstance().getConfiguration().getMetadataConfigurationManager().getConfigurationListForField("MD_TESTFIELD");
        Assertions.assertNotNull(configItems);
        assertEquals(1, configItems.size());

        FieldConfig configItem = configItems.get(0);
        assertEquals("MD_TESTFIELD", configItem.getFieldname());
        assertEquals(2, configItem.getxPathConfigurations().size());
        assertEquals("first", configItem.getNode());
        assertEquals("all", configItem.getParents());
        assertEquals("false", configItem.getChild());
        assertTrue(configItem.isOneToken());
        assertTrue(configItem.isOneField());
        assertEquals("XXX", configItem.getConstantValue());
        assertTrue(configItem.isLowercase());
        assertTrue(configItem.isAddToDefault());
        assertFalse(configItem.isAddUntokenizedVersion());
        assertTrue(configItem.isAddSortField());
        assertTrue(configItem.isAddSortFieldToTopstruct());
        assertEquals("#", configItem.getSplittingCharacter());
        assertTrue(configItem.isNormalizeYear());
        assertTrue(configItem.isInterpolateYears());
        assertEquals(2, configItem.getReplaceRules().size());
        assertEquals("replace1 ", configItem.getReplaceRules().get("stringToReplace1 "));
        assertEquals("replace2", configItem.getReplaceRules().get("REGEX:[ ]*stringToReplace2[ ]*"));
        assertEquals(1, configItem.getNonSortConfigurations().size());
        assertEquals("nonSortPrefix", configItem.getNonSortConfigurations().get(0).getPrefix());
        assertEquals("nonSortSuffix", configItem.getNonSortConfigurations().get(0).getSuffix());

        // Value normalizers
        Assertions.assertNotNull(configItem.getValueNormalizers());
        assertEquals(2, configItem.getValueNormalizers().size());
        assertTrue(configItem.getValueNormalizers().get(0).isConvertRoman());
        assertEquals("foo ([C|I|M|V|X]+) .*$", configItem.getValueNormalizers().get(0).getRegex());
        assertEquals(5, configItem.getValueNormalizers().get(1).getTargetLength());
        assertEquals('a', configItem.getValueNormalizers().get(1).getFiller());
        assertEquals(ValueNormalizerPosition.FRONT, configItem.getValueNormalizers().get(1).getPosition());
        assertEquals("foo ([0-9]+) .*$", configItem.getValueNormalizers().get(1).getRegex());

        assertEquals("mods:coordinates/point", configItem.getGeoJSONSource());
        assertEquals(" / ", configItem.getGeoJSONSourceSeparator());
    }

    /**
     * @see Configuration#getViewerUrl()
     * @verifies return correct value
     */
    @Test
    void getViewerUrl_shouldReturnCorrectValue() throws Exception {
        assertEquals("http://localhost:8080/viewer", SolrIndexerDaemon.getInstance().getConfiguration().getViewerUrl());
    }

    /**
     * @see Configuration#getPageCountStart()
     * @verifies return correct value
     */
    @Test
    void getPageCountStart_shouldReturnCorrectValue() throws Exception {
        assertEquals(0, SolrIndexerDaemon.getInstance().getConfiguration().getPageCountStart());
    }

    /**
     * @see Configuration#initNamespaces()
     * @verifies add custom namespaces correctly
     */
    @Test
    void initNamespaces_shouldAddCustomNamespacesCorrectly() throws Exception {
        SolrIndexerDaemon.getInstance().getConfiguration().initNamespaces();
        assertEquals(20, SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().size());
        Assertions.assertNotNull(SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("intranda"));
    }

    /**
     * @see Configuration#getViewerHome()
     * @verifies return correct value
     */
    @Test
    void getViewerHome_shouldReturnCorrectValue() throws Exception {
        assertEquals("target/viewer/", SolrIndexerDaemon.getInstance().getConfiguration().getViewerHome());
    }

    /**
     * @see Configuration#getEmptyOrderLabelReplacement()
     * @verifies return correct value
     */
    @Test
    void getEmptyOrderLabelReplacement_shouldReturnCorrectValue() throws Exception {
        assertEquals("X", SolrIndexerDaemon.getInstance().getConfiguration().getEmptyOrderLabelReplacement());
    }

    /**
     * @see Configuration#getDataRepositoryConfigurations()
     * @verifies return all items
     */
    @Test
    void getDataRepositoryConfigurations_shouldReturnAllItems() throws Exception {
        List<DataRepository> resp = SolrIndexerDaemon.getInstance().getConfiguration().getDataRepositoryConfigurations();
        assertEquals(3, resp.size());
        assertEquals("target/viewer/data/1", resp.get(0).getPath());
        assertEquals("target/viewer/data/2", resp.get(1).getPath());
        assertEquals("target/viewer/data/3", resp.get(2).getPath());
        assertEquals(10737418240L, resp.get(0).getBuffer());
        assertEquals(104857600L, resp.get(1).getBuffer());
        assertEquals(1000L, resp.get(2).getBuffer());
    }

    /**
     * @see Configuration#getViewerAuthorizationToken()
     * @verifies return correct value
     */
    @Test
    void getViewerAuthorizationToken_shouldReturnCorrectValue() throws Exception {
        assertEquals("test", SolrIndexerDaemon.getInstance().getConfiguration().getViewerAuthorizationToken());
    }

    /**
     * @see Configuration#isCountHotfolderFiles()
     * @verifies return correct value
     */
    @Test
    void isCountHotfolderFiles_shouldReturnCorrectValue() throws Exception {
        assertFalse(SolrIndexerDaemon.getInstance().getConfiguration().isCountHotfolderFiles());
    }

    /**
     * @see Configuration#isAuthorityDataCacheEnabled()
     * @verifies return correct value
     */
    @Test
    void isAuthorityDataCacheEnabled_shouldReturnCorrectValue() throws Exception {
        assertFalse(SolrIndexerDaemon.getInstance().getConfiguration().isAuthorityDataCacheEnabled());
    }

    /**
     * @see Configuration#getAuthorityDataCacheSizeWarningThreshold()
     * @verifies return correct value
     */
    @Test
    void getAuthorityDataCacheSizeWarningThreshold_shouldReturnCorrectValue() throws Exception {
        assertEquals(100, SolrIndexerDaemon.getInstance().getConfiguration().getAuthorityDataCacheSizeWarningThreshold());
    }

    /**
     * @see Configuration#getAuthorityDataCacheRecordTTL()
     * @verifies return correct value
     */
    @Test
    void getAuthorityDataCacheRecordTTL_shouldReturnCorrectValue() throws Exception {
        assertEquals(12, SolrIndexerDaemon.getInstance().getConfiguration().getAuthorityDataCacheRecordTTL());
    }

    /**
     * @see Configuration#isProxyEnabled()
     * @verifies return correct value
     */
    @Test
    void isProxyEnabled_shouldReturnCorrectValue() throws Exception {
        assertTrue(SolrIndexerDaemon.getInstance().getConfiguration().isProxyEnabled());
    }

    /**
     * @see Configuration#getProxyUrl()
     * @verifies return correct value
     */
    @Test
    void getProxyUrl_shouldReturnCorrectValue() throws Exception {
        assertEquals("my.proxy", SolrIndexerDaemon.getInstance().getConfiguration().getProxyUrl());
    }

    /**
     * @see Configuration#getProxyPort()
     * @verifies return correct value
     */
    @Test
    void getProxyPort_shouldReturnCorrectValue() throws Exception {
        assertEquals(9999, SolrIndexerDaemon.getInstance().getConfiguration().getProxyPort());
    }

    /**
     * @see Configuration#isHostProxyWhitelisted(String)
     * @verifies return true if host whitelisted
     */
    @Test
    void isHostProxyWhitelistedd_shouldReturnTrueIfHostWhitelisted() throws Exception {
        assertTrue(SolrIndexerDaemon.getInstance().getConfiguration().isHostProxyWhitelisted("http://localhost:1234"));
    }

    /**
     * @see Configuration#getMetsPreferredImageFileGroups()
     * @verifies return configured values
     */
    @Test
    void getMetsPreferredImageFileGroups_shouldReturnConfiguredValues() throws Exception {
        List<String> resp = SolrIndexerDaemon.getInstance().getConfiguration().getMetsPreferredImageFileGroups();
        assertEquals(2, resp.size());
        assertEquals("BOOKVIEWER", resp.get(0));
        assertEquals("ZOOMIFY", resp.get(1));
    }

    /**
     * @see Configuration#getMetsAllowedPhysicalTypes()
     * @verifies return configured values
     */
    @Test
    void getMetsAllowedPhysicalTypes_shouldReturnConfiguredValues() throws Exception {
        List<String> resp = SolrIndexerDaemon.getInstance().getConfiguration().getMetsAllowedPhysicalTypes();
        assertEquals(3, resp.size());
        assertEquals("object", resp.get(0));
        assertEquals("audio", resp.get(1));
        assertEquals("video", resp.get(2));
    }

    /**
     * @see Configuration#getMetsVolumeCheckXPath()
     * @verifies return correct value
     */
    @Test
    void getMetsVolumeCheckXPath_shouldReturnCorrectValue() throws Exception {
        assertEquals(
                "/mets:mets/mets:dmdSec[1]/mets:mdWrap[@MDTYPE='MODS']/mets:xmlData/mods:mods/mods:relatedItem[@type='host']"
                        + "[not(@otherType) or @otherType='hierarchical']/mods:recordInfo/mods:recordIdentifier",
                SolrIndexerDaemon.getInstance().getConfiguration().getMetsVolumeCheckXPath());
    }

    /**
     * @see Configuration#checkEmailConfiguration()
     * @verifies return false until all values configured
     */
    @Test
    void checkEmailConfiguration_shouldReturnFalseUntilAllValuesConfigured() throws Exception {
        assertFalse(SolrIndexerDaemon.getInstance().getConfiguration().checkEmailConfiguration());
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.recipients", "recipient@example.com");
        assertFalse(SolrIndexerDaemon.getInstance().getConfiguration().checkEmailConfiguration());
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpServer", "smtp.example.com");
        assertFalse(SolrIndexerDaemon.getInstance().getConfiguration().checkEmailConfiguration());
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpSenderAddress", "sender@example.com");
        assertFalse(SolrIndexerDaemon.getInstance().getConfiguration().checkEmailConfiguration());
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpSenderName", "Sender");
        assertFalse(SolrIndexerDaemon.getInstance().getConfiguration().checkEmailConfiguration());
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpSecurity", "NONE");
        assertTrue(SolrIndexerDaemon.getInstance().getConfiguration().checkEmailConfiguration());
    }

    /**
     * @see Configuration#getHotfolderPath()
     * @verifies return correct value
     */
    @Test
    void getHotfolderPath_shouldReturnCorrectValue() throws Exception {
        assertEquals("target/viewer/hotfolder/", SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
    }

    /**
     * @see Configuration#getHotfolderPaths()
     * @verifies return all values
     */
    @Test
    void getHotfolderPaths_shouldReturnAllValues() throws Exception {
        assertEquals(2, SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPaths().size());
    }

    /**
     * @see Configuration#getOldSolrUrl()
     * @verifies return correct value
     */
    @Test
    void getOldSolrUrl_shouldReturnCorrectValue() throws Exception {
        assertEquals("https://viewer-testing-index.goobi.io/solr/indexer-testing", SolrIndexerDaemon.getInstance().getConfiguration().getSolrUrl());
    }
}
