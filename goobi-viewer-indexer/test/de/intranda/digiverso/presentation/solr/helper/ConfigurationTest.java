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
package de.intranda.digiverso.presentation.solr.helper;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MultiMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.model.NonSortConfiguration;
import de.intranda.digiverso.presentation.solr.model.config.FieldConfig;

public class ConfigurationTest {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationTest.class);

    private static Hotfolder hotfolder;

    @BeforeClass
    public static void setUpClass() throws Exception {
        hotfolder = new Hotfolder("resources/test/indexerconfig_solr_test.xml", null);
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Test
    public void folderTest() throws Exception {
        Assert.assertTrue(Files.isDirectory(hotfolder.getHotfolder()));
        Assert.assertTrue(new File(Configuration.getInstance().getString("init.viewerHome")).isDirectory());
        Assert.assertTrue(Files.isDirectory(hotfolder.getDataRepositoriesHomePath()));
        Assert.assertTrue(Files.isDirectory(hotfolder.getSuccess()));
        Assert.assertTrue(Files.isDirectory(hotfolder.getUpdatedMets()));
        Assert.assertTrue(Files.isDirectory(hotfolder.getDeletedMets()));
        Assert.assertTrue(Files.isDirectory(hotfolder.getErrorMets()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void configItemTest() throws Exception {
        Map<String, List<Map<String, Object>>> fieldConfigurations = Configuration.getInstance().getFieldConfiguration();
        Assert.assertEquals(120, fieldConfigurations.size());
        List<Map<String, Object>> fieldInformation = fieldConfigurations.get("MD_TESTFIELD");
        Assert.assertNotNull(fieldInformation);
        Assert.assertEquals(1, fieldInformation.size());
        Map<String, Object> fieldValues = fieldInformation.get(0);
        Assert.assertEquals(19, fieldValues.size());

        List<String> xpath = (List<String>) fieldValues.get("xpath");
        Assert.assertNotNull(xpath);
        Assert.assertEquals(2, xpath.size());

        Assert.assertNotNull(fieldValues.get("getparents"));
        Assert.assertEquals("all", fieldValues.get("getparents"));

        Assert.assertNotNull(fieldValues.get("getchildren"));
        Assert.assertEquals("false", fieldValues.get("getchildren"));

        Assert.assertNotNull(fieldValues.get("getnode"));
        Assert.assertEquals("first", fieldValues.get("getnode"));

        Assert.assertNotNull(fieldValues.get("onetoken"));
        Assert.assertTrue(Boolean.valueOf((String) fieldValues.get("onetoken")));

        Assert.assertNotNull(fieldValues.get("onefield"));
        Assert.assertTrue(Boolean.valueOf((String) fieldValues.get("onefield")));

        Assert.assertNotNull(fieldValues.get("constantValue"));
        Assert.assertEquals("XXX", fieldValues.get("constantValue"));

        Assert.assertNotNull(fieldValues.get("lowercase"));
        Assert.assertTrue(Boolean.valueOf((String) fieldValues.get("lowercase")));

        Assert.assertNotNull(fieldValues.get("addToDefault"));
        Assert.assertTrue(Boolean.valueOf((String) fieldValues.get("addToDefault")));

        Assert.assertNotNull(fieldValues.get("addUntokenizedVersion"));
        Assert.assertFalse(Boolean.valueOf((String) fieldValues.get("addUntokenizedVersion")));

        Assert.assertNotNull(fieldValues.get("addSortField"));
        Assert.assertTrue(Boolean.valueOf((String) fieldValues.get("addSortField")));

        Assert.assertNotNull(fieldValues.get("splittingCharacter"));
        Assert.assertEquals("#", fieldValues.get("splittingCharacter"));

        Assert.assertNotNull(fieldValues.get("normalizeYear"));
        Assert.assertTrue(Boolean.valueOf((String) fieldValues.get("normalizeYear")));

        Assert.assertNotNull(fieldValues.get("normalizeYearMinDigits"));
        Assert.assertEquals(2, (int) fieldValues.get("normalizeYearMinDigits"));

        Assert.assertNotNull(fieldValues.get("aggregateEntity"));
        Assert.assertTrue(Boolean.valueOf((String) fieldValues.get("aggregateEntity")));

        MultiMap groupEntity = (MultiMap) fieldValues.get("groupEntity");
        Assert.assertNotNull(groupEntity);
        Assert.assertEquals(3, groupEntity.size());
        List<String> typeList = (List<String>) groupEntity.get("type");
        Assert.assertNotNull(typeList);
        Assert.assertFalse(typeList.isEmpty());
        Assert.assertEquals("TYPE", typeList.get(0));
        List<String> xpathList = (List<String>) groupEntity.get("field1");
        Assert.assertNotNull(xpathList);
        Assert.assertEquals(2, xpathList.size());
        Assert.assertEquals("xpath1", xpathList.get(0));
        Assert.assertEquals("xpath2", xpathList.get(1));

        Map<Object, String> replaceRules = (Map<Object, String>) fieldValues.get("replaceRules");
        Assert.assertNotNull(replaceRules);
        Assert.assertEquals(2, replaceRules.size());
        Assert.assertEquals("replace1", replaceRules.get("stringToReplace1"));
        Assert.assertEquals("replace2", replaceRules.get("stringToReplace2"));

        List<NonSortConfiguration> nonSortConfigurations = (List<NonSortConfiguration>) fieldValues.get("nonSortConfigurations");
        Assert.assertNotNull(nonSortConfigurations);
        Assert.assertEquals(1, nonSortConfigurations.size());
        Assert.assertEquals("nonSortPrefix", nonSortConfigurations.get(0).getPrefix());
        Assert.assertEquals("nonSortSuffix", nonSortConfigurations.get(0).getSuffix());
    }

    @Test
    public void metadataConfigTest() throws Exception {
        Assert.assertEquals(120, Configuration.getInstance().getMetadataConfigurationManager().getListWithAllFieldNames().size());
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
        Assert.assertEquals("#", configItem.getSplittingCharacter());
        Assert.assertTrue(configItem.isNormalizeYear());
        Assert.assertTrue(configItem.isInterpolateYears());
        Assert.assertEquals(2, configItem.getReplaceRules().size());
        Assert.assertEquals("replace1", configItem.getReplaceRules().get("stringToReplace1"));
        Assert.assertEquals("replace2", configItem.getReplaceRules().get("stringToReplace2"));
        Assert.assertEquals(1, configItem.getNonSortConfigurations().size());
        Assert.assertEquals("nonSortPrefix", configItem.getNonSortConfigurations().get(0).getPrefix());
        Assert.assertEquals("nonSortSuffix", configItem.getNonSortConfigurations().get(0).getSuffix());
    }

    /**
     * @see Configuration#getPageCountStart()
     * @verifies return correct value
     */
    @Test
    public void getPageCountStart_shouldReturnCorrectValue() throws Exception {
        Assert.assertEquals(0, Configuration.getInstance("resources/test/indexerconfig_solr_test.xml").getPageCountStart());
    }

    /**
     * @see Configuration#initNamespaces()
     * @verifies add custom namespaces correctly
     */
    @Test
    public void initNamespaces_shouldAddCustomNamespacesCorrectly() throws Exception {
        Configuration.getInstance().initNamespaces();
        Assert.assertEquals(11, Configuration.getInstance().getNamespaces().size());
        Assert.assertNotNull(Configuration.getInstance().getNamespaces().get("intranda"));
    }

}
