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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.jdom2.Document;
import org.jdom2.Element;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import de.intranda.digiverso.normdataimporter.model.NormData;
import de.intranda.digiverso.normdataimporter.model.NormDataValue;
import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.model.GroupedMetadata;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.MetadataGroupType;
import io.goobi.viewer.indexer.model.config.FieldConfig;
import io.goobi.viewer.indexer.model.config.GroupEntity;

class MetadataHelperTest extends AbstractTest {

    @SuppressWarnings("unused")
    private static Hotfolder hotfolder;

    @BeforeAll
    public static void setUpClass() throws Exception {
        AbstractTest.setUpClass();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
    }

    /**
     * @see MetadataHelper#getCentury(long)
     * @verifies detect positive century correctly
     */
    @Test
    void getCentury_shouldDetectPositiveCenturyCorrectly() {
        assertEquals(1, MetadataHelper.getCentury(50));
        assertEquals(9, MetadataHelper.getCentury(865));
        assertEquals(20, MetadataHelper.getCentury(1901));
    }

    /**
     * @see MetadataHelper#getCentury(long)
     * @verifies detect negative century correctly
     */
    @Test
    void getCentury_shouldDetectNegativeCenturyCorrectly() {
        assertEquals(-1, MetadataHelper.getCentury(-50));
        assertEquals(-2, MetadataHelper.getCentury(-150));
    }

    /**
     * @see MetadataHelper#getCentury(long)
     * @verifies detect first century correctly
     */
    @Test
    void getCentury_shouldDetectFirstCenturyCorrectly() {
        assertEquals(1, MetadataHelper.getCentury(7));
    }

    /**
     * @see MetadataHelper#applyIdentifierModifications(String)
     * @verifies trim identifier
     */
    @Test
    void applyIdentifierModifications_shouldTrimIdentifier() {
        assertEquals("id", MetadataHelper.applyIdentifierModifications(" id "));
    }

    /**
     * @see MetadataHelper#applyIdentifierModifications(String)
     * @verifies replace illegal characters with underscores
     */
    @Test
    void applyIdentifierModifications_shouldReplaceIllegalCharactersWithUnderscores() {
        assertEquals("ID_10t", MetadataHelper.applyIdentifierModifications("ID 10t"));
        assertEquals("ID_10t", MetadataHelper.applyIdentifierModifications("ID,10t"));
        assertEquals("ID_10t_", MetadataHelper.applyIdentifierModifications("ID(10t)"));
    }

    /**
     * @see MetadataHelper#applyIdentifierModifications(String)
     * @verifies apply replace rules
     */
    @Test
    void applyIdentifierModifications_shouldApplyReplaceRules() {
        assertEquals("ID_10t", MetadataHelper.applyIdentifierModifications("replaceme/ID,10t/replacemetoo"));
    }

    /**
     * @see MetadataHelper#getGroupedMetadata(Element,GroupEntity,String)
     * @verifies group correctly
     */
    @Test
    void getGroupedMetadata_shouldGroupCorrectly() throws Exception {
        List<FieldConfig> fieldConfigurations =
                SolrIndexerDaemon.getInstance().getConfiguration().getMetadataConfigurationManager().getConfigurationListForField("MD_AUTHOR");
        assertNotNull(fieldConfigurations);
        assertEquals(1, fieldConfigurations.size());
        FieldConfig fieldConfig = fieldConfigurations.get(0);
        assertNotNull(fieldConfig.getGroupEntity());

        Document docMods = JDomXP.readXmlFile("src/test/resources/METS/aggregation_mods_test.xml");
        assertNotNull(docMods);
        assertNotNull(docMods.getRootElement());

        Element eleName = docMods.getRootElement().getChild("name", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mods"));
        assertNotNull(eleName);
        GroupedMetadata gmd = MetadataHelper.getGroupedMetadata(eleName, fieldConfig.getGroupEntity(), fieldConfig, "MD_AUTHOR", new StringBuilder(),
                new ArrayList<>());
        Assertions.assertFalse(gmd.getFields().isEmpty());
        assertEquals("Display_Form", gmd.getMainValue());
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
                case SolrConstants.LABEL:
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
                default:
                    break;
            }
        }
        assertEquals("MD_AUTHOR", label);
        assertEquals(MetadataGroupType.PERSON.name(), metadataType);
        assertEquals("Corporate_Name", corporation);
        assertEquals("Last", lastName);
        assertEquals(2, givenNameList.size());
        assertEquals("First", givenNameList.get(0));
        assertEquals("Second", givenNameList.get(1));
        assertEquals("Display_Form", displayForm);
        assertEquals("DATE", date);
        assertEquals("Terms_Of_Address", termsOfAddress);
        assertEquals("xlink", link);
        assertEquals("MD_AUTHOR_Display_Form", groupField);
    }

    /**
     * @see MetadataHelper#getGroupedMetadata(Element,GroupEntity,FieldConfig,String,StringBuilder,List)
     * @verifies not lowercase certain fields
     */
    @Test
    void getGroupedMetadata_shouldNotLowercaseCertainFields() throws Exception {
        List<FieldConfig> fieldConfigurations =
                SolrIndexerDaemon.getInstance().getConfiguration().getMetadataConfigurationManager().getConfigurationListForField("MD_AUTHOR");
        assertNotNull(fieldConfigurations);
        assertEquals(1, fieldConfigurations.size());
        FieldConfig fieldConfig = fieldConfigurations.get(0);
        assertNotNull(fieldConfig.getGroupEntity());
        fieldConfig.setLowercase(true);

        Document docMods = JDomXP.readXmlFile("src/test/resources/METS/aggregation_mods_test.xml");
        assertNotNull(docMods);
        assertNotNull(docMods.getRootElement());

        Element eleName = docMods.getRootElement().getChild("name", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mods"));
        assertNotNull(eleName);
        GroupedMetadata gmd = MetadataHelper.getGroupedMetadata(eleName, fieldConfig.getGroupEntity(), fieldConfig, "MD_AUTHOR", new StringBuilder(),
                new ArrayList<>());
        Assertions.assertFalse(gmd.getFields().isEmpty());
        assertEquals("display_form", gmd.getMainValue());
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
                default:
                    break;
            }
        }
        assertEquals("MD_AUTHOR", label);
        assertEquals(MetadataGroupType.PERSON.name(), metadataType);
        assertEquals("corporate_name", corporation);
        assertEquals("last", lastName);
        assertEquals(2, givenNameList.size());
        assertEquals("first", givenNameList.get(0));
        assertEquals("second", givenNameList.get(1));
        assertEquals("display_form", displayForm);
        assertEquals("date", date);
        assertEquals("terms_of_address", termsOfAddress);
        assertEquals("xlink", link);
        assertEquals("MD_AUTHOR_Display_Form", groupField);
    }

    /**
     * @see MetadataHelper#completeCenturies(List)
     * @verifies complete centuries correctly
     */
    @Test
    void completeCenturies_shouldCompleteCenturiesCorrectly() {
        List<LuceneField> centuries = new ArrayList<>();
        centuries.add(new LuceneField(SolrConstants.CENTURY, "-2"));
        centuries.add(new LuceneField(SolrConstants.CENTURY, "3"));
        List<LuceneField> newCenturies = MetadataHelper.completeCenturies(centuries);
        assertEquals(3, newCenturies.size());
        assertEquals(SolrConstants.CENTURY, newCenturies.get(0).getField());
        assertEquals("-1", newCenturies.get(0).getValue());
        assertEquals("1", newCenturies.get(1).getValue());
        assertEquals("2", newCenturies.get(2).getValue());
    }

    /**
     * @see MetadataHelper#completeYears(List)
     * @verifies complete years correctly
     */
    @Test
    void completeYears_shouldCompleteYearsCorrectly() {
        List<LuceneField> years = new ArrayList<>();
        years.add(new LuceneField(SolrConstants.YEAR, "1990"));
        years.add(new LuceneField(SolrConstants.YEAR, "1993"));
        List<LuceneField> newYears = MetadataHelper.completeYears(years, SolrConstants.YEAR);
        assertEquals(2, newYears.size());
        assertEquals(SolrConstants.YEAR, newYears.get(0).getField());
        assertEquals("1991", newYears.get(0).getValue());
        assertEquals("1992", newYears.get(1).getValue());
    }

    /**
     * @see MetadataHelper#applyReplaceRules(String,Map)
     * @verifies apply rules correctly
     */
    @Test
    void applyReplaceRules_shouldApplyRulesCorrectly() {
        Map<Object, String> replaceRules = new HashMap<>();
        replaceRules.put('<', "");
        replaceRules.put(">", "s");
        replaceRules.put("REGEX:[ ]*100[ ]*", "");
        assertEquals("vase", MetadataHelper.applyReplaceRules(" 100 v<a>e", replaceRules));
    }

    /**
     * @see MetadataHelper#applyReplaceRules(String,Map)
     * @verifies throw IllegalArgumentException if value is null
     */
    @Test
    void applyReplaceRules_shouldThrowIllegalArgumentExceptionIfValueIsNull() {
        Map<Object, String> replaceRules = new HashMap<>();
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetadataHelper.applyReplaceRules(null, replaceRules));
    }

    /**
     * @see MetadataHelper#applyReplaceRules(String,Map)
     * @verifies return unmodified value if replaceRules is null
     */
    @Test
    void applyReplaceRules_shouldReturnUnmodifiedValueIfReplaceRulesIsNull() {
        assertEquals("v<a>e", MetadataHelper.applyReplaceRules("v<a>e", null));
    }

    /**
     * @see MetadataHelper#getConcatenatedValue(String)
     * @verifies concatenate value terms correctly
     */
    @Test
    void getConcatenatedValue_shouldConcatenateValueTermsCorrectly() {
        assertEquals("foobar", MetadataHelper.getConcatenatedValue("foo-bar"));
    }

    /**
     * @see MetadataHelper#addValueToDefault(String,StringBuilder)
     * @verifies add value correctly
     */
    @Test
    void addValueToDefault_shouldAddValueCorrectly() {
        StringBuilder sb = new StringBuilder(" bla blup ");
        MetadataHelper.addValueToDefault("foo bar", sb);
        assertEquals(" bla blup  foo bar ", sb.toString());
    }

    /**
     * @see MetadataHelper#addValueToDefault(String,StringBuilder)
     * @verifies add concatenated value correctly
     */
    @Test
    void addValueToDefault_shouldAddConcatenatedValueCorrectly() {
        StringBuilder sb = new StringBuilder(" bla blup ");
        MetadataHelper.addValueToDefault("foo-bar", sb);
        assertEquals(" bla blup  foo-bar ", sb.toString());
    }

    /**
     * @see MetadataHelper#addValueToDefault(String,StringBuilder)
     * @verifies throw IllegalArgumentException if value is null
     */
    @Test
    void addValueToDefault_shouldThrowIllegalArgumentExceptionIfValueIsNull() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetadataHelper.addValueToDefault(null, new StringBuilder()));
    }

    /**
     * @see MetadataHelper#addValueToDefault(String,StringBuilder)
     * @verifies throw IllegalArgumentException if sbDefaultMetadataValues is null
     */
    @Test
    void addValueToDefault_shouldThrowIllegalArgumentExceptionIfSbDefaultMetadataValuesIsNull() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetadataHelper.addValueToDefault("foo-bar", null));
    }

    /**
     * @see MetadataHelper#addSortField(String,String,boolean,List,List)
     * @verifies add regular sort fields correctly
     */
    @Test
    void addSortField_shouldAddRegularSortFieldsCorrectly() {
        List<LuceneField> result = new ArrayList<>(1);
        MetadataHelper.addSortField("MD_TITLE", "Title", SolrConstants.PREFIX_SORT, null, null, result);
        assertEquals(1, result.size());
        assertEquals(SolrConstants.PREFIX_SORT + "TITLE", result.get(0).getField());
        assertEquals("Title", result.get(0).getValue());
    }

    /**
     * @see MetadataHelper#addSortField(String,String,boolean,List,List)
     * @verifies add numerical sort fields correctly
     */
    @Test
    void addSortField_shouldAddNumericalSortFieldsCorrectly() {
        List<LuceneField> result = new ArrayList<>(1);
        MetadataHelper.addSortField(SolrConstants.YEAR, "-100", SolrConstants.PREFIX_SORTNUM, null, null, result);
        assertEquals(1, result.size());
        assertEquals(SolrConstants.PREFIX_SORTNUM + SolrConstants.YEAR, result.get(0).getField());
        assertEquals("-100", result.get(0).getValue());
    }

    /**
     * @see MetadataHelper#addSortField(String,String,boolean,List,List)
     * @verifies not add existing fields
     */
    @Test
    void addSortField_shouldNotAddExistingFields() {
        List<LuceneField> result = new ArrayList<>(1);
        result.add(new LuceneField(SolrConstants.PREFIX_SORT + "TITLE", "other title"));
        MetadataHelper.addSortField("MD_TITLE", "Title", SolrConstants.PREFIX_SORT, null, null, result);
        assertEquals(1, result.size());
        assertEquals(SolrConstants.PREFIX_SORT + "TITLE", result.get(0).getField());
        assertEquals("other title", result.get(0).getValue());
    }

    /**
     * @see MetadataHelper#extractLanguageCodeFromMetadataField(String)
     * @verifies extract language code correctly
     */
    @Test
    void extractLanguageCodeFromMetadataField_shouldExtractLanguageCodeCorrectly() {
        assertEquals("en", MetadataHelper.extractLanguageCodeFromMetadataField("MD_TITLE_LANG_EN"));
    }

    /**
     * @see MetadataHelper#extractLanguageCodeFromMetadataField(String)
     * @verifies ignore any suffixes longer than two chars
     */
    @Test
    void extractLanguageCodeFromMetadataField_shouldIgnoreAnySuffixesLongerThanTwoChars() {
        Assertions.assertNull(MetadataHelper.extractLanguageCodeFromMetadataField("MD_TITLE_LANG_EN_UNTOKENIZED"));
    }

    /**
     * @see MetadataHelper#cleanUpName(String)
     * @verifies remove leading comma
     */
    @Test
    void cleanUpName_shouldRemoveLeadingComma() {
        assertEquals("foo", MetadataHelper.cleanUpName(", foo"));
        assertEquals("foo", MetadataHelper.cleanUpName(",foo"));
    }

    /**
     * @see MetadataHelper#cleanUpName(String)
     * @verifies remove trailing comma
     */
    @Test
    void cleanUpName_shouldRemoveTrailingComma() {
        assertEquals("foo", MetadataHelper.cleanUpName("foo,"));
        assertEquals("foo", MetadataHelper.cleanUpName("foo, "));
    }

    /**
     * @see MetadataHelper#processTEIMetadataFiles(IndexObject,Path)
     * @verifies append fulltext from all files
     */
    @Test
    void processTEIMetadataFiles_shouldAppendFulltextFromAllFiles() throws Exception {
        IndexObject obj = new IndexObject(UUID.randomUUID().toString());
        Path teiFolder = Paths.get("src/test/resources/WorldViews/gei_test_sthe_quelle_01_tei");
        assertTrue(Files.isDirectory(teiFolder));
        MetadataHelper.processTEIMetadataFiles(obj, teiFolder);
        assertNotNull(obj.getLuceneFieldWithName(SolrConstants.FULLTEXT));
        String fulltext = obj.getLuceneFieldWithName(SolrConstants.FULLTEXT).getValue();
        assertNotNull(fulltext);
        assertTrue(fulltext.contains("ENGLISH"));
        assertTrue(fulltext.contains("FRENCH"));
        assertTrue(fulltext.contains("Systematische Übersicht über die Elemente für die Auszeichnung von Quellen"));
    }

    /**
     * @see MetadataHelper#parseDatesAndCenturies(Set,String,int)
     * @verifies parse centuries and dates correctly
     */
    @Test
    void parseDatesAndCenturies_shouldParseCenturiesAndDatesCorrectly() {
        String date = "2019-03-18";
        Set<Integer> centuries = new HashSet<>(1);
        List<LuceneField> result = MetadataHelper.parseDatesAndCenturies(centuries, date, 4, null);
        assertEquals(5, result.size());
        result.get(0).getField().equals(SolrConstants.YEAR);
        result.get(0).getValue().equals("2019");
        result.get(1).getField().equals(SolrConstants.YEARMONTH);
        result.get(1).getValue().equals("201903");
        result.get(2).getField().equals(SolrConstants.YEARMONTHDAY);
        result.get(2).getValue().equals("20190318");
        result.get(3).getField().equals(SolrConstants.MONTHDAY);
        result.get(3).getValue().equals("0318");
        result.get(4).getField().equals(SolrConstants.CENTURY);
        result.get(4).getValue().equals("21");
        assertTrue(centuries.contains(21));
    }

    /**
     * @see MetadataHelper#parseDatesAndCenturies(Set,String,int)
     * @verifies normalize year digits
     */
    @Test
    void parseDatesAndCenturies_shouldNormalizeYearDigits() {
        String date = "0190-03-18";
        Set<Integer> centuries = new HashSet<>(1);
        List<LuceneField> result = MetadataHelper.parseDatesAndCenturies(centuries, date, 4, null);
        assertEquals(5, result.size());
        result.get(0).getField().equals(SolrConstants.YEAR);
        result.get(0).getValue().equals("0190");
        result.get(1).getField().equals(SolrConstants.YEARMONTH);
        result.get(1).getValue().equals("019003");
        result.get(2).getField().equals(SolrConstants.YEARMONTHDAY);
        result.get(2).getValue().equals("01900318");
        result.get(3).getField().equals(SolrConstants.MONTHDAY);
        result.get(3).getValue().equals("0318");
    }

    /**
     * @see MetadataHelper#parseDatesAndCenturies(Set,String,int,String)
     * @verifies add custom field correctly
     */
    @Test
    void parseDatesAndCenturies_shouldAddCustomFieldCorrectly() {
        String date = "2023-01-09";
        Set<Integer> centuries = new HashSet<>(1);
        List<LuceneField> result = MetadataHelper.parseDatesAndCenturies(centuries, date, 4, "MDNUM_CUSTOMYEAR");
        assertEquals(6, result.size());
        result.get(0).getField().equals(SolrConstants.YEAR);
        result.get(0).getValue().equals("2019");
        result.get(1).getField().equals("MDNUM_CUSTOMYEAR");
        result.get(1).getValue().equals("2019");
    }

    /**
     * @see MetadataHelper#getPIFromXML(String,JDomXP)
     * @verifies extract DenkXweb PI correctly
     */
    @Test
    void getPIFromXML_shouldExtractDenkXwebPICorrectly() {
        Path path = Paths.get("src/test/resources/DenkXweb/denkxweb_30596824_short.xml");
        assertTrue(Files.isRegularFile(path));
        List<Document> docs = JDomXP.splitDenkXwebFile(path.toFile());
        String pi = MetadataHelper.getPIFromXML("", new JDomXP(docs.get(0)));
        assertEquals("30596824", pi);
    }

    /**
     * @see MetadataHelper#parseAuthorityMetadata(List,StringBuilder,StringBuilder,List,Map,String)
     * @verifies add name search field correctly
     */
    @Test
    void parseAuthorityMetadata_shouldAddNameSearchFieldCorrectly() {
        List<NormData> authorityDataList = new ArrayList<>(2);
        authorityDataList.add(new NormData("NORM_NAME", new NormDataValue("one", null, null)));
        authorityDataList.add(new NormData("NORM_OFFICIALNAME", new NormDataValue("two", null, null)));
        authorityDataList.add(new NormData("NORM_ALTNAME", new NormDataValue("three", null, null)));
        List<LuceneField> result = MetadataHelper.parseAuthorityMetadata(authorityDataList, null, null, null, null, "MD_FIELD");
        assertEquals(13, result.size());
        assertEquals("MD_FIELD_NAME_SEARCH", result.get(2).getField());
        assertEquals("one", result.get(2).getValue());
        assertEquals("MD_FIELD_NAME_SEARCH", result.get(6).getField());
        assertEquals("two", result.get(6).getValue());
        assertEquals("MD_FIELD_NAME_SEARCH", result.get(10).getField());
        assertEquals("three", result.get(10).getValue());
        assertEquals(MetadataHelper.FIELD_HAS_WKT_COORDS, result.get(12).getField());
        assertEquals("false", result.get(12).getValue());
    }

    /**
     * @see MetadataHelper#toOneToken(String,String)
     * @verifies remove non-alphanumerical characters
     */
    @Test
    void toOneToken_shouldRemoveNonalphanumericalCharacters() {
        assertEquals("LoremIpsum", MetadataHelper.toOneToken("Lorem Ipsum!", null));
    }

    /**
     * @see MetadataHelper#toOneToken(String,String)
     * @verifies replace splitting char
     */
    @Test
    void toOneToken_shouldReplaceSplittingChar() {
        assertEquals("foo.bar", MetadataHelper.toOneToken("foo#bar", "#"));
    }
}
