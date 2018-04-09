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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;

import org.apache.commons.collections.MultiMap;
import org.jdom2.Document;
import org.jdom2.Element;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import de.intranda.digiverso.presentation.solr.helper.MetadataHelper.PrimitiveDate;
import de.intranda.digiverso.presentation.solr.model.GroupedMetadata;
import de.intranda.digiverso.presentation.solr.model.IndexObject;
import de.intranda.digiverso.presentation.solr.model.LuceneField;
import de.intranda.digiverso.presentation.solr.model.SolrConstants;
import de.intranda.digiverso.presentation.solr.model.SolrConstants.MetadataGroupType;

public class MetadataHelperTest {

    @SuppressWarnings("unused")
    private static Hotfolder hotfolder;

    @BeforeClass
    public static void setUpClass() throws Exception {
        hotfolder = new Hotfolder("resources/test/indexerconfig_solr_test.xml", null);
    }

    /**
     * @see MetadataHelper#normalizeDate(String)
     * @verifies parse german date formats correctly
     */
    @Test
    public void normalizeDate_shouldParseGermanDateFormatsCorrectly() throws Exception {
        List<PrimitiveDate> ret = MetadataHelper.normalizeDate("05.08.2014", 3);
        Assert.assertEquals(1, ret.size());
        Assert.assertEquals(Integer.valueOf(2014), ret.get(0).getYear());
        Assert.assertEquals(Integer.valueOf(8), ret.get(0).getMonth());
        Assert.assertEquals(Integer.valueOf(5), ret.get(0).getDay());
    }

    /**
     * @see MetadataHelper#normalizeDate(String)
     * @verifies parse rfc date formats correctly
     */
    @Test
    public void normalizeDate_shouldParseRfcDateFormatsCorrectly() throws Exception {
        List<PrimitiveDate> ret = MetadataHelper.normalizeDate("2014-08-05", 3);
        Assert.assertEquals(1, ret.size());
        Assert.assertEquals(Integer.valueOf(2014), ret.get(0).getYear());
        Assert.assertEquals(Integer.valueOf(8), ret.get(0).getMonth());
        Assert.assertEquals(Integer.valueOf(5), ret.get(0).getDay());

        ret = MetadataHelper.normalizeDate("2-05", 3);
        Assert.assertEquals(1, ret.size());
        Assert.assertEquals(Integer.valueOf(2), ret.get(0).getYear());
        Assert.assertEquals(Integer.valueOf(5), ret.get(0).getMonth());
    }

    /**
     * @see MetadataHelper#normalizeDate(String)
     * @verifies parse american date formats correctly
     */
    @Test
    public void normalizeDate_shouldParseAmericanDateFormatsCorrectly() throws Exception {
        List<PrimitiveDate> ret = MetadataHelper.normalizeDate("08/05/2014", 3);
        Assert.assertEquals(1, ret.size());
        Assert.assertEquals(Integer.valueOf(2014), ret.get(0).getYear());
        Assert.assertEquals(Integer.valueOf(8), ret.get(0).getMonth());
        Assert.assertEquals(Integer.valueOf(5), ret.get(0).getDay());
    }

    /**
     * @see MetadataHelper#normalizeDate(String)
     * @verifies parse chinese date formats correctly
     */
    @Test
    public void normalizeDate_shouldParseChineseDateFormatsCorrectly() throws Exception {
        List<PrimitiveDate> ret = MetadataHelper.normalizeDate("2014.08.05", 3);
        Assert.assertEquals(1, ret.size());
        Assert.assertEquals(Integer.valueOf(2014), ret.get(0).getYear());
        Assert.assertEquals(Integer.valueOf(8), ret.get(0).getMonth());
        Assert.assertEquals(Integer.valueOf(5), ret.get(0).getDay());
    }

    /**
     * @see MetadataHelper#normalizeDate(String)
     * @verifies parse japanese date formats correctly
     */
    @Test
    public void normalizeDate_shouldParseJapaneseDateFormatsCorrectly() throws Exception {
        List<PrimitiveDate> ret = MetadataHelper.normalizeDate("2014/08/05", 3);
        Assert.assertEquals(1, ret.size());
        Assert.assertEquals(Integer.valueOf(2014), ret.get(0).getYear());
        Assert.assertEquals(Integer.valueOf(8), ret.get(0).getMonth());
        Assert.assertEquals(Integer.valueOf(5), ret.get(0).getDay());
    }

    /**
     * @see MetadataHelper#normalizeDate(String)
     * @verifies parse year ranges correctly
     */
    @Test
    public void normalizeDate_shouldParseYearRangesCorrectly() throws Exception {
        List<PrimitiveDate> ret = MetadataHelper.normalizeDate("2010-2014", 3);
        Assert.assertEquals(2, ret.size());
        Assert.assertEquals(Integer.valueOf(2010), ret.get(0).getYear());
        Assert.assertEquals(Integer.valueOf(2014), ret.get(1).getYear());

        ret = MetadataHelper.normalizeDate("10-20", 2);
        Assert.assertEquals(2, ret.size());
        Assert.assertEquals(Integer.valueOf(10), ret.get(0).getYear());
        Assert.assertEquals(Integer.valueOf(20), ret.get(1).getYear());
    }

    /**
     * @see MetadataHelper#normalizeDate(String)
     * @verifies parse single years correctly
     */
    @Test
    public void normalizeDate_shouldParseSingleYearsCorrectly() throws Exception {
        List<PrimitiveDate> ret = MetadataHelper.normalizeDate("-300 -30 -3", 2);
        Assert.assertEquals(2, ret.size());
        Assert.assertEquals(Integer.valueOf(-300), ret.get(0).getYear());
        Assert.assertEquals(Integer.valueOf(-30), ret.get(1).getYear());

        ret = MetadataHelper.normalizeDate("300 30 3", 3);
        Assert.assertEquals(1, ret.size());
        Assert.assertEquals(Integer.valueOf(300), ret.get(0).getYear());

        ret = MetadataHelper.normalizeDate("300 30 3", 1);
        Assert.assertEquals(3, ret.size());
        Assert.assertEquals(Integer.valueOf(300), ret.get(0).getYear());
        Assert.assertEquals(Integer.valueOf(30), ret.get(1).getYear());
        Assert.assertEquals(Integer.valueOf(3), ret.get(2).getYear());
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
     * @see MetadataHelper#getGroupedMetadata(Element,MultiMap,String)
     * @verifies group correctly
     */
    @Test
    public void getGroupedMetadata_shouldGroupCorrectly() throws Exception {
        Map<String, List<Map<String, Object>>> fieldConfigurations = Configuration.getInstance().getFieldConfiguration();
        List<Map<String, Object>> fieldInformation = fieldConfigurations.get("MD_AUTHOR");
        Assert.assertNotNull(fieldInformation);
        Assert.assertEquals(1, fieldInformation.size());
        Map<String, Object> fieldValues = fieldInformation.get(0);
        MultiMap groupEntity = (MultiMap) fieldValues.get("groupEntity");
        Assert.assertNotNull(groupEntity);

        Document docMods = JDomXP.readXmlFile("resources/test/METS/aggregation_mods_test.xml");
        Assert.assertNotNull(docMods);
        Assert.assertNotNull(docMods.getRootElement());

        Element eleName = docMods.getRootElement().getChild("name", Configuration.getInstance().getNamespaces().get("mods"));
        Assert.assertNotNull(eleName);
        GroupedMetadata gmd = MetadataHelper.getGroupedMetadata(eleName, groupEntity, "label");
        Assert.assertFalse(gmd.getFields().isEmpty());
        Assert.assertEquals("label", gmd.getLabel());
        Assert.assertEquals("display_form", gmd.getMainValue());
        String label = null;
        String metadataType = null;
        String corporation = null;
        String lastName = null;
        String firstName = null;
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
                    firstName = field.getValue();
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
        Assert.assertEquals("first", firstName);
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
     * @verifies throw IllegalArgumentException if replaceRules is null
     */
    @Test(expected = IllegalArgumentException.class)
    public void applyReplaceRules_shouldThrowIllegalArgumentExceptionIfReplaceRulesIsNull() throws Exception {
        MetadataHelper.applyReplaceRules("v<a>e", null);
    }

    /**
     * @see MetadataHelper#normalizeDate(String,int)
     * @verifies throw IllegalArgumentException if normalizeYearMinDigits less than 1
     */
    @Test(expected = IllegalArgumentException.class)
    public void normalizeDate_shouldThrowIllegalArgumentExceptionIfNormalizeYearMinDigitsLessThan1() throws Exception {
        MetadataHelper.normalizeDate("05.08.2014", 0);
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
        Assert.assertEquals(" bla blup  foo-bar  foobar ", sb.toString());
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
     * @see MetadataHelper#convertDateStringForSolrField(String)
     * @verifies convert date correctly
     */
    @Test
    public void convertDateStringForSolrField_shouldConvertDateCorrectly() throws Exception {
        Assert.assertEquals("2016-11-02T00:00:00+0100", MetadataHelper.convertDateStringForSolrField("2016-11-02"));
        Assert.assertEquals("2016-11-01T00:00:00+0100", MetadataHelper.convertDateStringForSolrField("2016-11"));
        Assert.assertEquals("2016-01-01T00:00:00+0100", MetadataHelper.convertDateStringForSolrField("2016"));
    }

    /**
     * @see MetadataHelper#addSortField(String,String,boolean,List,List)
     * @verifies add regular sort fields correctly
     */
    @Test
    public void addSortField_shouldAddRegularSortFieldsCorrectly() throws Exception {
        List<LuceneField> result = new ArrayList<>(1);
        MetadataHelper.addSortField("MD_TITLE", "Title", SolrConstants.SORT_, null, result);
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
        MetadataHelper.addSortField(SolrConstants.YEAR, "-100", SolrConstants.SORTNUM_, null, result);
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
        MetadataHelper.addSortField("MD_TITLE", "Title", SolrConstants.SORT_, null, result);
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
        Path teiFolder = Paths.get("resources/test/WorldViews/gei_test_sthe_quelle_01_tei");
        Assert.assertTrue(Files.isDirectory(teiFolder));
        MetadataHelper.processTEIMetadataFiles(obj, teiFolder);
        Assert.assertNotNull(obj.getLuceneFieldWithName(SolrConstants.FULLTEXT));
        String fulltext = obj.getLuceneFieldWithName(SolrConstants.FULLTEXT).getValue();
        Assert.assertNotNull(fulltext);
        Assert.assertTrue(fulltext.contains("ENGLISH"));
        Assert.assertTrue(fulltext.contains("FRENCH"));
        Assert.assertTrue(fulltext.contains("Systematische Übersicht über die Elemente für die Auszeichnung von Quellen"));
    }
}