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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jdom2.Document;
import org.jdom2.Element;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import de.intranda.digiverso.normdataimporter.model.NormData;
import de.intranda.digiverso.normdataimporter.model.NormDataValue;
import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.model.GroupedMetadata;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.MetadataGroupType;
import io.goobi.viewer.indexer.model.config.FieldConfig;
import io.goobi.viewer.indexer.model.config.GroupEntity;

public class MetadataHelperTest extends AbstractTest {

    @SuppressWarnings("unused")
    private static Hotfolder hotfolder;

    @BeforeClass
    public static void setUpClass() throws Exception {
        AbstractTest.setUpClass();

        hotfolder = new Hotfolder("src/test/resources/indexerconfig_solr_test.xml", null);
    }

    /**
     * @see MetadataHelper#getCentury(long)
     * @verifies detect positive century correctly
     */
    @Test
    public void getCentury_shouldDetectPositiveCenturyCorrectly() throws Exception {
        Assert.assertEquals(1, MetadataHelper.getCentury(50));
        Assert.assertEquals(9, MetadataHelper.getCentury(865));
        Assert.assertEquals(20, MetadataHelper.getCentury(1901));
    }

    /**
     * @see MetadataHelper#getCentury(long)
     * @verifies detect negative century correctly
     */
    @Test
    public void getCentury_shouldDetectNegativeCenturyCorrectly() throws Exception {
        Assert.assertEquals(-1, MetadataHelper.getCentury(-50));
        Assert.assertEquals(-2, MetadataHelper.getCentury(-150));
    }

    /**
     * @see MetadataHelper#getCentury(long)
     * @verifies detect first century correctly
     */
    @Test
    public void getCentury_shouldDetectFirstCenturyCorrectly() throws Exception {
        Assert.assertEquals(1, MetadataHelper.getCentury(7));
    }

    /**
     * @see MetadataHelper#applyIdentifierModifications(String)
     * @verifies trim identifier
     */
    @Test
    public void applyIdentifierModifications_shouldTrimIdentifier() throws Exception {
        Assert.assertEquals("id", MetadataHelper.applyIdentifierModifications(" id "));
    }

    /**
     * @see MetadataHelper#applyIdentifierModifications(String)
     * @verifies replace spaces with underscores
     */
    @Test
    public void applyIdentifierModifications_shouldReplaceSpacesWithUnderscores() throws Exception {
        Assert.assertEquals("ID_10t", MetadataHelper.applyIdentifierModifications("ID 10t"));
    }

    /**
     * @see MetadataHelper#applyIdentifierModifications(String)
     * @verifies apply replace rules
     */
    @Test
    public void applyIdentifierModifications_shouldApplyReplaceRules() throws Exception {
        Assert.assertEquals("ID_10t", MetadataHelper.applyIdentifierModifications("replaceme/ID,10t/replacemetoo"));
    }

    /**
     * @see MetadataHelper#applyIdentifierModifications(String)
     * @verifies replace commas with underscores
     */
    @Test
    public void applyIdentifierModifications_shouldReplaceCommasWithUnderscores() throws Exception {
        Assert.assertEquals("ID_10t", MetadataHelper.applyIdentifierModifications("ID,10t"));
    }

    /**
     * @see MetadataHelper#getGroupedMetadata(Element,GroupEntity,String)
     * @verifies group correctly
     */
    @Test
    public void getGroupedMetadata_shouldGroupCorrectly() throws Exception {
        List<FieldConfig> fieldConfigurations =
                Configuration.getInstance().getMetadataConfigurationManager().getConfigurationListForField("MD_AUTHOR");
        Assert.assertNotNull(fieldConfigurations);
        Assert.assertEquals(1, fieldConfigurations.size());
        FieldConfig fieldConfig = fieldConfigurations.get(0);
        Assert.assertNotNull(fieldConfig.getGroupEntity());

        Document docMods = JDomXP.readXmlFile("src/test/resources/METS/aggregation_mods_test.xml");
        Assert.assertNotNull(docMods);
        Assert.assertNotNull(docMods.getRootElement());

        Element eleName = docMods.getRootElement().getChild("name", Configuration.getInstance().getNamespaces().get("mods"));
        Assert.assertNotNull(eleName);
        GroupedMetadata gmd = MetadataHelper.getGroupedMetadata(eleName, fieldConfig.getGroupEntity(), fieldConfig, "label", new StringBuilder(),
                new ArrayList<>());
        Assert.assertFalse(gmd.getFields().isEmpty());
        Assert.assertEquals("label", gmd.getLabel());
        Assert.assertEquals("display_form", gmd.getMainValue());
        String label = null;
        String metadataType = null;
        String corporation = null;
        String lastName = null;
        List<String> givenNameList = new ArrayList<>(2);
        String displayForm = null;
        String groupField = null;
        String date = null;
        String termsOfAddress = null;
        String link = null;
        for (LuceneField field : gmd.getFields()) {
            switch (field.getField()) {
                case SolrConstants.METADATATYPE:
                    metadataType = field.getValue();
                    break;
                case "LABEL":
                    label = field.getValue();
                    break;
                case "MD_CORPORATION":
                    corporation = field.getValue();
                    break;
                case "MD_LASTNAME":
                    lastName = field.getValue();
                    break;
                case "MD_FIRSTNAME":
                    givenNameList.add(field.getValue());
                    break;
                case "MD_DISPLAYFORM":
                    displayForm = field.getValue();
                    break;
                case SolrConstants.GROUPFIELD:
                    groupField = field.getValue();
                    break;
                case "MD_LIFEPERIOD":
                    date = field.getValue();
                    break;
                case "MD_TERMSOFADDRESS":
                    termsOfAddress = field.getValue();
                    break;
                case "MD_LINK":
                    link = field.getValue();
                    break;
            }
        }
        Assert.assertEquals(MetadataGroupType.PERSON.name(), metadataType);
        Assert.assertEquals("corporate_name", corporation);
        Assert.assertEquals("last", lastName);
        Assert.assertEquals(2, givenNameList.size());
        Assert.assertEquals("first", givenNameList.get(0));
        Assert.assertEquals("second", givenNameList.get(1));
        Assert.assertEquals("display_form", displayForm);
        Assert.assertEquals("date", date);
        Assert.assertEquals("terms_of_address", termsOfAddress);
        Assert.assertEquals("xlink", link);
        Assert.assertEquals("label_display_form", groupField);
    }

    /**
     * @see MetadataHelper#completeCenturies(List)
     * @verifies complete centuries correctly
     */
    @Test
    public void completeCenturies_shouldCompleteCenturiesCorrectly() throws Exception {
        List<LuceneField> centuries = new ArrayList<>();
        centuries.add(new LuceneField(SolrConstants.CENTURY, "-2"));
        centuries.add(new LuceneField(SolrConstants.CENTURY, "3"));
        List<LuceneField> newCenturies = MetadataHelper.completeCenturies(centuries);
        Assert.assertEquals(3, newCenturies.size());
        Assert.assertEquals(SolrConstants.CENTURY, newCenturies.get(0).getField());
        Assert.assertEquals("-1", newCenturies.get(0).getValue());
        Assert.assertEquals("1", newCenturies.get(1).getValue());
        Assert.assertEquals("2", newCenturies.get(2).getValue());
    }

    /**
     * @see MetadataHelper#completeYears(List)
     * @verifies complete years correctly
     */
    @Test
    public void completeYears_shouldCompleteYearsCorrectly() throws Exception {
        List<LuceneField> years = new ArrayList<>();
        years.add(new LuceneField(SolrConstants.YEAR, "1990"));
        years.add(new LuceneField(SolrConstants.YEAR, "1993"));
        List<LuceneField> newYears = MetadataHelper.completeYears(years);
        Assert.assertEquals(2, newYears.size());
        Assert.assertEquals(SolrConstants.YEAR, newYears.get(0).getField());
        Assert.assertEquals("1991", newYears.get(0).getValue());
        Assert.assertEquals("1992", newYears.get(1).getValue());
    }

    /**
     * @see MetadataHelper#applyReplaceRules(String,Map)
     * @verifies apply rules correctly
     */
    @Test
    public void applyReplaceRules_shouldApplyRulesCorrectly() throws Exception {
        Map<Object, String> replaceRules = new HashMap<>();
        replaceRules.put('<', "");
        replaceRules.put(">", "s");
        replaceRules.put("REGEX:[ ]*100[ ]*", "");
        Assert.assertEquals("vase", MetadataHelper.applyReplaceRules(" 100 v<a>e", replaceRules));
    }

    /**
     * @see MetadataHelper#applyReplaceRules(String,Map)
     * @verifies throw IllegalArgumentException if value is null
     */
    @Test(expected = IllegalArgumentException.class)
    public void applyReplaceRules_shouldThrowIllegalArgumentExceptionIfValueIsNull() throws Exception {
        Map<Object, String> replaceRules = new HashMap<>();
        MetadataHelper.applyReplaceRules(null, replaceRules);
    }

    /**
     * @see MetadataHelper#applyReplaceRules(String,Map)
     * @verifies return unmodified value if replaceRules is null
     */
    @Test
    public void applyReplaceRules_shouldReturnUnmodifiedValueIfReplaceRulesIsNull() throws Exception {
        Assert.assertEquals("v<a>e", MetadataHelper.applyReplaceRules("v<a>e", null));
    }

    /**
     * @see MetadataHelper#getConcatenatedValue(String)
     * @verifies concatenate value terms correctly
     */
    @Test
    public void getConcatenatedValue_shouldConcatenateValueTermsCorrectly() throws Exception {
        Assert.assertEquals("foobar", MetadataHelper.getConcatenatedValue("foo-bar"));
    }

    /**
     * @see MetadataHelper#addValueToDefault(String,StringBuilder)
     * @verifies add value correctly
     */
    @Test
    public void addValueToDefault_shouldAddValueCorrectly() throws Exception {
        StringBuilder sb = new StringBuilder(" bla blup ");
        MetadataHelper.addValueToDefault("foo bar", sb);
        Assert.assertEquals(" bla blup  foo bar ", sb.toString());
    }

    /**
     * @see MetadataHelper#addValueToDefault(String,StringBuilder)
     * @verifies add concatenated value correctly
     */
    @Test
    public void addValueToDefault_shouldAddConcatenatedValueCorrectly() throws Exception {
        StringBuilder sb = new StringBuilder(" bla blup ");
        MetadataHelper.addValueToDefault("foo-bar", sb);
        //        Assert.assertEquals(" bla blup  foo-bar  foobar ", sb.toString());
        Assert.assertEquals(" bla blup  foo-bar ", sb.toString());
    }

    /**
     * @see MetadataHelper#addValueToDefault(String,StringBuilder)
     * @verifies throw IllegalArgumentException if value is null
     */
    @Test(expected = IllegalArgumentException.class)
    public void addValueToDefault_shouldThrowIllegalArgumentExceptionIfValueIsNull() throws Exception {
        MetadataHelper.addValueToDefault(null, new StringBuilder());
    }

    /**
     * @see MetadataHelper#addValueToDefault(String,StringBuilder)
     * @verifies throw IllegalArgumentException if sbDefaultMetadataValues is null
     */
    @Test(expected = IllegalArgumentException.class)
    public void addValueToDefault_shouldThrowIllegalArgumentExceptionIfSbDefaultMetadataValuesIsNull() throws Exception {
        MetadataHelper.addValueToDefault("foo-bar", null);
    }

    /**
     * @see MetadataHelper#addSortField(String,String,boolean,List,List)
     * @verifies add regular sort fields correctly
     */
    @Test
    public void addSortField_shouldAddRegularSortFieldsCorrectly() throws Exception {
        List<LuceneField> result = new ArrayList<>(1);
        MetadataHelper.addSortField("MD_TITLE", "Title", SolrConstants.SORT_, null, null, result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(SolrConstants.SORT_ + "TITLE", result.get(0).getField());
        Assert.assertEquals("Title", result.get(0).getValue());
    }

    /**
     * @see MetadataHelper#addSortField(String,String,boolean,List,List)
     * @verifies add numerical sort fields correctly
     */
    @Test
    public void addSortField_shouldAddNumericalSortFieldsCorrectly() throws Exception {
        List<LuceneField> result = new ArrayList<>(1);
        MetadataHelper.addSortField(SolrConstants.YEAR, "-100", SolrConstants.SORTNUM_, null, null, result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(SolrConstants.SORTNUM_ + SolrConstants.YEAR, result.get(0).getField());
        Assert.assertEquals("-100", result.get(0).getValue());
    }

    /**
     * @see MetadataHelper#addSortField(String,String,boolean,List,List)
     * @verifies not add existing fields
     */
    @Test
    public void addSortField_shouldNotAddExistingFields() throws Exception {
        List<LuceneField> result = new ArrayList<>(1);
        result.add(new LuceneField(SolrConstants.SORT_ + "TITLE", "other title"));
        MetadataHelper.addSortField("MD_TITLE", "Title", SolrConstants.SORT_, null, null, result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(SolrConstants.SORT_ + "TITLE", result.get(0).getField());
        Assert.assertEquals("other title", result.get(0).getValue());
    }

    /**
     * @see MetadataHelper#extractLanguageCodeFromMetadataField(String)
     * @verifies extract language code correctly
     */
    @Test
    public void extractLanguageCodeFromMetadataField_shouldExtractLanguageCodeCorrectly() throws Exception {
        Assert.assertEquals("en", MetadataHelper.extractLanguageCodeFromMetadataField("MD_TITLE_LANG_EN"));
    }

    /**
     * @see MetadataHelper#extractLanguageCodeFromMetadataField(String)
     * @verifies ignore any suffixes longer than two chars
     */
    @Test
    public void extractLanguageCodeFromMetadataField_shouldIgnoreAnySuffixesLongerThanTwoChars() throws Exception {
        Assert.assertNull(MetadataHelper.extractLanguageCodeFromMetadataField("MD_TITLE_LANG_EN_UNTOKENIZED"));
    }

    /**
     * @see MetadataHelper#cleanUpName(String)
     * @verifies remove leading comma
     */
    @Test
    public void cleanUpName_shouldRemoveLeadingComma() throws Exception {
        Assert.assertEquals("foo", MetadataHelper.cleanUpName(", foo"));
        Assert.assertEquals("foo", MetadataHelper.cleanUpName(",foo"));
    }

    /**
     * @see MetadataHelper#cleanUpName(String)
     * @verifies remove trailing comma
     */
    @Test
    public void cleanUpName_shouldRemoveTrailingComma() throws Exception {
        Assert.assertEquals("foo", MetadataHelper.cleanUpName("foo,"));
        Assert.assertEquals("foo", MetadataHelper.cleanUpName("foo, "));
    }

    /**
     * @see MetadataHelper#processTEIMetadataFiles(IndexObject,Path)
     * @verifies append fulltext from all files
     */
    @Test
    public void processTEIMetadataFiles_shouldAppendFulltextFromAllFiles() throws Exception {
        IndexObject obj = new IndexObject(1L);
        Path teiFolder = Paths.get("src/test/resources/WorldViews/gei_test_sthe_quelle_01_tei");
        Assert.assertTrue(Files.isDirectory(teiFolder));
        MetadataHelper.processTEIMetadataFiles(obj, teiFolder);
        Assert.assertNotNull(obj.getLuceneFieldWithName(SolrConstants.FULLTEXT));
        String fulltext = obj.getLuceneFieldWithName(SolrConstants.FULLTEXT).getValue();
        Assert.assertNotNull(fulltext);
        Assert.assertTrue(fulltext.contains("ENGLISH"));
        Assert.assertTrue(fulltext.contains("FRENCH"));
        Assert.assertTrue(fulltext.contains("Systematische Übersicht über die Elemente für die Auszeichnung von Quellen"));
    }

    /**
     * @see MetadataHelper#parseDatesAndCenturies(Set,String,int)
     * @verifies parse centuries and dates correctly
     */
    @Test
    public void parseDatesAndCenturies_shouldParseCenturiesAndDatesCorrectly() throws Exception {
        String date = "2019-03-18";
        Set<Integer> centuries = new HashSet<>(1);
        List<LuceneField> result = MetadataHelper.parseDatesAndCenturies(centuries, date, 4);
        Assert.assertEquals(4, result.size());
        result.get(0).getField().equals(SolrConstants.YEAR);
        result.get(0).getValue().equals("2019");
        result.get(1).getField().equals(SolrConstants.YEARMONTH);
        result.get(1).getValue().equals("201903");
        result.get(2).getField().equals(SolrConstants.YEARMONTHDAY);
        result.get(2).getValue().equals("20190318");
        result.get(3).getField().equals(SolrConstants.CENTURY);
        result.get(3).getValue().equals("21");
        Assert.assertTrue(centuries.contains(21));
    }

    /**
     * @see MetadataHelper#parseDatesAndCenturies(Set,String,int)
     * @verifies normalize year digits
     */
    @Test
    public void parseDatesAndCenturies_shouldNormalizeYearDigits() throws Exception {
        String date = "0190-03-18";
        Set<Integer> centuries = new HashSet<>(1);
        List<LuceneField> result = MetadataHelper.parseDatesAndCenturies(centuries, date, 4);
        Assert.assertEquals(4, result.size());
        result.get(0).getField().equals(SolrConstants.YEAR);
        result.get(0).getValue().equals("0190");
        result.get(1).getField().equals(SolrConstants.YEARMONTH);
        result.get(1).getValue().equals("019003");
        result.get(2).getField().equals(SolrConstants.YEARMONTHDAY);
        result.get(2).getValue().equals("01900318");
    }

    /**
     * @see MetadataHelper#getPIFromXML(String,JDomXP)
     * @verifies extract DenkXweb PI correctly
     */
    @Test
    public void getPIFromXML_shouldExtractDenkXwebPICorrectly() throws Exception {
        Path path = Paths.get("src/test/resources/DenkXweb/denkxweb_30596824_short.xml");
        Assert.assertTrue(Files.isRegularFile(path));
        List<Document> docs = JDomXP.splitDenkXwebFile(path.toFile());
        String pi = MetadataHelper.getPIFromXML("", new JDomXP(docs.get(0)));
        Assert.assertEquals("30596824", pi);
    }

    /**
     * @see MetadataHelper#parseAuthorityMetadata(List,StringBuilder,StringBuilder,List,Map,String)
     * @verifies add name search field correctly
     */
    @Test
    public void parseAuthorityMetadata_shouldAddNameSearchFieldCorrectly() throws Exception {
        List<NormData> authorityDataList = new ArrayList<>(2);
        authorityDataList.add(new NormData("NORM_NAME", new NormDataValue("one", null, null)));
        authorityDataList.add(new NormData("NORM_OFFICIALNAME", new NormDataValue("two", null, null)));
        authorityDataList.add(new NormData("NORM_ALTNAME", new NormDataValue("three", null, null)));
        List<LuceneField> result = MetadataHelper.parseAuthorityMetadata(authorityDataList, null, null, null, null, "MD_FIELD");
        Assert.assertEquals(12, result.size());
        Assert.assertEquals("MD_FIELD_NAME_SEARCH", result.get(2).getField());
        Assert.assertEquals("one", result.get(2).getValue());
        Assert.assertEquals("MD_FIELD_NAME_SEARCH", result.get(6).getField());
        Assert.assertEquals("two", result.get(6).getValue());
        Assert.assertEquals("MD_FIELD_NAME_SEARCH", result.get(10).getField());
        Assert.assertEquals("three", result.get(10).getValue());
    }
}
