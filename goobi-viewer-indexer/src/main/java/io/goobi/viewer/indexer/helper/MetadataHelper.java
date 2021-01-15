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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.jdom2.Attribute;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.normdataimporter.NormDataImporter;
import de.intranda.digiverso.normdataimporter.model.GeoNamesRecord;
import de.intranda.digiverso.normdataimporter.model.NormData;
import de.intranda.digiverso.normdataimporter.model.NormDataValue;
import de.intranda.digiverso.normdataimporter.model.Record;
import io.goobi.viewer.indexer.helper.language.LanguageHelper;
import io.goobi.viewer.indexer.model.FatalIndexerException;
import io.goobi.viewer.indexer.model.GeoCoords;
import io.goobi.viewer.indexer.model.GroupedMetadata;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.PrimitiveDate;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.config.FieldConfig;
import io.goobi.viewer.indexer.model.config.NonSortConfiguration;
import io.goobi.viewer.indexer.model.config.SubfieldConfig;
import io.goobi.viewer.indexer.model.config.ValueNormalizer;
import io.goobi.viewer.indexer.model.config.XPathConfig;

/**
 * <p>
 * MetadataHelper class.
 * </p>
 *
 */
public class MetadataHelper {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(MetadataHelper.class);

    private static final String XPATH_ROOT_PLACEHOLDER = "{{{ROOT}}}";
    public static final String FIELD_WKT_COORDS = "WKT_COORDS";
    public static final String FIELD_HAS_WKT_COORDS = "BOOL_WKT_COORDS";

    /** Constant <code>FORMAT_TWO_DIGITS</code> */
    public static final ThreadLocal<DecimalFormat> FORMAT_TWO_DIGITS = new ThreadLocal<DecimalFormat>() {

        @Override
        protected DecimalFormat initialValue() {
            return new DecimalFormat("00");
        }
    };
    /** Constant <code>FORMAT_FOUR_DIGITS</code> */
    public static final ThreadLocal<DecimalFormat> FORMAT_FOUR_DIGITS = new ThreadLocal<DecimalFormat>() {

        @Override
        protected DecimalFormat initialValue() {
            return new DecimalFormat("0000");
        }
    };
    /** Constant <code>FORMAT_EIGHT_DIGITS</code> */
    public static final ThreadLocal<DecimalFormat> FORMAT_EIGHT_DIGITS = new ThreadLocal<DecimalFormat>() {

        @Override
        protected DecimalFormat initialValue() {
            return new DecimalFormat("00000000");
        }
    };

    /** Constant <code>formatterISO8601Full</code> */
    public static DateTimeFormatter formatterISO8601Full = DateTimeFormatter.ISO_LOCAL_DATE_TIME; // yyyy-MM-dd'T'HH:mm:ss
    /** Constant <code>formatterISO8601DateTimeInstant</code> */
    public static DateTimeFormatter formatterISO8601DateTimeInstant = DateTimeFormatter.ISO_INSTANT; // yyyy-MM-dd'T'HH:mm:ssZ
    /** Constant <code>formatterISO8601DateTimeWithOffset</code> */
    public static DateTimeFormatter formatterISO8601DateTimeWithOffset = DateTimeFormatter.ISO_OFFSET_DATE_TIME; // yyyy-MM-dd'T'HH:mm:ss+01:00
    /** Constant <code>formatterISO8601Date</code> */
    public static DateTimeFormatter formatterISO8601Date = DateTimeFormatter.ISO_LOCAL_DATE; // yyyy-MM-dd
    /** Constant <code>formatterISO8601DateTimeNoSeconds</code> */
    public static DateTimeFormatter formatterISO8601DateTimeNoSeconds = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    /** Constant <code>formatterISO8601YearMonth</code> */
    public static DateTimeFormatter formatterISO8601YearMonth = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM")
            .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
            .toFormatter();
    /** Constant <code>formatterDEDate</code> */
    public static DateTimeFormatter formatterDEDate = DateTimeFormatter.ofPattern("dd.MM.yyyy");
    /** Constant <code>formatterUSDate</code> */
    public static DateTimeFormatter formatterUSDate = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    /** Constant <code>formatterCNDate</code> */
    public static DateTimeFormatter formatterCNDate = DateTimeFormatter.ofPattern("yyyy.MM.dd");
    /** Constant <code>formatterJPDate</code> */
    public static DateTimeFormatter formatterJPDate = DateTimeFormatter.ofPattern("yyyy/MM/dd");;
    /** Constant <code>formatterBasicDateTime</code> */
    public static DateTimeFormatter formatterBasicDateTime = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    /** Constant <code>addNormDataFieldsToDefault</code> */
    public static List<String> addAuthorityDataFieldsToDefault;

    /**
     * Retrieves configured metadata fields from the given XML node. Written for LIDO events, but should theoretically work for any METS or LIDO node.
     *
     * @param element a {@link org.jdom2.Element} object.
     * @param queryPrefix a {@link java.lang.String} object.
     * @param indexObj The IndexObject is needed to alter its DEFAULT field value.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @param xp a {@link io.goobi.viewer.indexer.helper.JDomXP} object.
     * @return a {@link java.util.List} object.
     */
    @SuppressWarnings("rawtypes")
    public static List<LuceneField> retrieveElementMetadata(Element element, String queryPrefix, IndexObject indexObj, JDomXP xp)
            throws FatalIndexerException {
        List<LuceneField> ret = new ArrayList<>();

        Set<Integer> centuries = new HashSet<>();
        List<String> fieldNamesList = Configuration.getInstance().getMetadataConfigurationManager().getListWithAllFieldNames();
        StringBuilder sbDefaultMetadataValues = new StringBuilder();
        if (indexObj.getDefaultValue() != null) {
            sbDefaultMetadataValues.append(indexObj.getDefaultValue());
        }
        for (String fieldName : fieldNamesList) {
            List<FieldConfig> configurationItemList =
                    Configuration.getInstance().getMetadataConfigurationManager().getConfigurationListForField(fieldName);
            if (configurationItemList == null || configurationItemList.isEmpty()) {
                continue;
            }
            for (FieldConfig configurationItem : configurationItemList) {
                // Constant value instead of XPath
                if (configurationItem.getConstantValue() != null) {
                    StringBuilder sbValue = new StringBuilder(configurationItem.getConstantValue());
                    if (configurationItem.getValuepostfix().length() > 0) {
                        sbValue.append(configurationItem.getValuepostfix());
                    }
                    String value = sbValue.toString().trim();
                    if (configurationItem.isOneToken()) {
                        value = toOneToken(value, configurationItem.getSplittingCharacter());
                    }
                    LuceneField luceneField = new LuceneField(configurationItem.getFieldname(), value);
                    ret.add(luceneField);
                    if (configurationItem.isAddToDefault()) {
                        addValueToDefault(configurationItem.getConstantValue(), sbDefaultMetadataValues);
                    }
                    continue;
                }

                List<Element> elementsToIterateOver = new ArrayList<>();
                elementsToIterateOver.add(element);

                // If a field needs child and/or parent values, add those elements to the iteration list
                Set<String> childrenAndAncestors = new HashSet<>();
                // children
                if (configurationItem.getChild().equals("all")) {
                    List<Attribute> childrenNodeList = xp.evaluateToAttributes("mets:div/attribute::DMDID", indexObj.getRootStructNode());
                    if (childrenNodeList != null) {
                        for (Attribute attribute : childrenNodeList) {
                            String d = attribute.getValue();
                            if (StringUtils.isNotEmpty(d)) {
                                childrenAndAncestors.add(d);
                            }
                        }
                    }
                }
                // ancestors
                IndexObject parent = indexObj.getParent();
                if (parent != null && !parent.isAnchor()) {
                    switch (configurationItem.getParents()) {
                        case "first":
                            if (parent.getDmdid() != null) {
                                childrenAndAncestors.add(parent.getDmdid());
                            } else {
                                logger.warn("DMDID for Parent element '{}' not found.", parent.getLogId());
                            }
                            break;
                        case "all":
                            if (parent.getDmdid() != null) {
                                childrenAndAncestors.add(parent.getDmdid());
                            } else {
                                logger.warn("DMDID for Parent element '{}' not found.", parent.getLogId());
                            }
                            while (parent.getParent() != null && !parent.getParent().isAnchor()) {
                                parent = parent.getParent();
                                if (parent.getDmdid() != null) {
                                    childrenAndAncestors.add(parent.getDmdid());
                                } else {
                                    logger.warn("DMDID for Parent element '{}' not found.", parent.getLogId());
                                }
                            }
                            break;
                        default: // nothing
                    }
                }
                for (String dmdId : childrenAndAncestors) {
                    Element eleMdWrap = xp.getMdWrap(dmdId);
                    if (eleMdWrap != null) {
                        elementsToIterateOver.add(eleMdWrap);
                    } else {
                        logger.warn("Field {}: mets:mdWrap section not found for DMDID {}", fieldName, dmdId);
                    }
                }

                boolean breakfirst = false;
                if (configurationItem.getNode().equalsIgnoreCase("first")) {
                    breakfirst = true;
                }

                // Collect values from XPath expressions
                List<String> fieldValues = new ArrayList<>();
                for (XPathConfig xpathConfig : configurationItem.getxPathConfigurations()) {
                    if (xpathConfig == null) {
                        logger.error("An XPath expression for {} is null.", configurationItem.getFieldname());
                        continue;
                    }
                    String xpath = xpathConfig.getxPath();
                    String fieldValue = "";
                    String query;
                    // Cut off "displayForm" if the field is to be grouped
                    if (configurationItem.isGroupEntity() && xpath.endsWith("/mods:displayForm")) {
                        xpath = xpath.substring(0, xpath.length() - 17);
                        logger.debug("new xpath: {}", xpath);
                    }
                    if (xpath.contains(XPATH_ROOT_PLACEHOLDER)) {
                        // Replace the placeholder with the prefix (e.g. in expressions using concat())
                        query = xpath.replace(XPATH_ROOT_PLACEHOLDER, queryPrefix);
                    } else {
                        // User prefix as prefix
                        query = queryPrefix + xpath;
                    }
                    for (Element currentElement : elementsToIterateOver) {
                        List list = xp.evaluate(query, currentElement);
                        if (list == null || list.isEmpty()) {
                            continue;
                        }
                        for (Object xpathAnswerObject : list) {
                            if (configurationItem.isGroupEntity()) {
                                // Grouped metadata
                                logger.trace(xpath);
                                Element eleMods = (Element) xpathAnswerObject;
                                GroupedMetadata gmd =
                                        getGroupedMetadata(eleMods, configurationItem.getGroupEntityFields(), configurationItem.getFieldname());
                                // Allow duplicate values for this GMD, if the configuration item allows it
                                gmd.setAllowDuplicateValues(configurationItem.isAllowDuplicateValues());
                                List<LuceneField> authorityData = new ArrayList<>();
                                boolean groupFieldAlreadyReplaced = false;
                                String authorityIdentifier = null;
                                StringBuilder sbAuthorityDataTerms = new StringBuilder();

                                // Add the relevant value as a non-grouped metadata value (for term browsing, etc.)
                                if (gmd.getMainValue() != null) {
                                    fieldValue = gmd.getMainValue();
                                    // Apply XPath prefix
                                    if (StringUtils.isNotEmpty(xpathConfig.getPrefix())) {
                                        fieldValue = xpathConfig.getPrefix() + fieldValue;
                                    }
                                    // Apply XPath suffix
                                    if (StringUtils.isNotEmpty(xpathConfig.getSuffix())) {
                                        fieldValue = fieldValue + xpathConfig.getSuffix();
                                    }
                                    fieldValues.add(fieldValue);
                                }

                                // Retrieve authority data
                                if (gmd.getAuthorityURI() != null) {
                                    authorityData.addAll(retrieveAuthorityData(gmd.getAuthorityURI(), sbDefaultMetadataValues,
                                            sbAuthorityDataTerms, addAuthorityDataFieldsToDefault, configurationItem.getReplaceRules(),
                                            configurationItem.getFieldname()));
                                    // Add default authority data name to the docstruct doc so that it can be searched
                                    for (LuceneField authorityField : authorityData) {
                                        switch (authorityField.getField()) {
                                            case "NORM_NAME":
                                                // Add NORM_NAME as MD_*_UNTOKENIZED and to DEFAULT to the docstruct
                                                if (StringUtils.isNotBlank(authorityField.getValue())) {
                                                    // fieldValues.add(normField.getValue());
                                                    if (configurationItem.isAddToDefault()) {
                                                        // Add norm value to DEFAULT
                                                        addValueToDefault(authorityField.getValue(), sbDefaultMetadataValues);
                                                    }
                                                    if (configurationItem.isAddUntokenizedVersion() || fieldName.startsWith("MD_")) {
                                                        ret.add(new LuceneField(
                                                                new StringBuilder(fieldName).append(SolrConstants._UNTOKENIZED).toString(),
                                                                authorityField.getValue()));
                                                    }
                                                }
                                                break;
                                            case "NORM_IDENTIFIER":
                                                // If a NORM_IDENTIFIER exists for this metadata group, use it to replace the value of GROUPFIELD
                                                for (LuceneField groupField : gmd.getFields()) {
                                                    if (groupField.getField().equals(SolrConstants.GROUPFIELD)) {
                                                        groupField.setValue(authorityField.getValue());
                                                        groupFieldAlreadyReplaced = true;
                                                        break;
                                                    }
                                                    authorityIdentifier = authorityField.getValue();
                                                }
                                                break;
                                            default: // nothing
                                        }
                                    }
                                }
                                for (LuceneField field : gmd.getFields()) {
                                    // Apply modifications configured for the main field to all the group field values
                                    String moddedValue = applyAllModifications(configurationItem, field.getValue());

                                    // Convert to geoJSON
                                    if (configurationItem.getGeoJSONSource() != null && field.getField().equals("MD_VALUE")) {
                                        try {
                                            GeoCoords coords = GeoJSONTools.convert(moddedValue, configurationItem.getGeoJSONSource(),
                                                    configurationItem.getGeoJSONSourceSeparator());
                                            if (coords.getGeoJSON() != null) {
                                                moddedValue = coords.getGeoJSON();
                                            }
                                            // Add WKT search field
                                            if (configurationItem.isGeoJSONAddSearchField() && coords.getWKT() != null) {
                                                ret.add(new LuceneField(FIELD_WKT_COORDS, coords.getWKT()));
                                                ret.add(new LuceneField(FIELD_HAS_WKT_COORDS, "true"));
                                            }
                                        } catch (NumberFormatException e) {
                                            logger.error("Cannot convert to geoJSON: {}", e.getMessage());
                                        }
                                    }

                                    field.setValue(moddedValue);

                                    if (configurationItem.isAddToDefault()) {
                                        // Add main value to owner doc's DEFAULT field
                                        if (StringUtils.isNotBlank(fieldValue)) {
                                            addValueToDefault(fieldValue, sbDefaultMetadataValues);
                                        }
                                        // Add grouped metadata field to DEFAULT
                                        if (StringUtils.isNotEmpty(field.getValue()) && !moddedValue.equals(fieldValue)) {
                                            switch (field.getField()) {
                                                case SolrConstants.LABEL:
                                                case SolrConstants.METADATATYPE:
                                                    // skip
                                                    break;
                                                default:
                                                    addValueToDefault(moddedValue, sbDefaultMetadataValues);
                                            }
                                        }
                                    }
                                }

                                // If there was no existing GROUPFIELD in the group metadata, add one now using the norm identifier
                                if (!groupFieldAlreadyReplaced && StringUtils.isNotEmpty(authorityIdentifier)) {
                                    gmd.getFields().add(new LuceneField(SolrConstants.GROUPFIELD, authorityIdentifier));
                                }
                                // Add MD_VALUE as DEFAULT to the grouped metadata doc
                                if (configurationItem.isAddToDefault() && StringUtils.isNotBlank(gmd.getMainValue())) {
                                    gmd.getFields().add(new LuceneField(SolrConstants.DEFAULT, gmd.getMainValue()));
                                }
                                gmd.getAuthorityDataFields().addAll(authorityData); // Add authority data outside the loop over groupMetadata

                                // Add GMD to index object (if not duplicate or duplicates are allowed for this field)
                                if (!indexObj.getGroupedMetadataFields().contains(gmd) || configurationItem.isAllowDuplicateValues()) {
                                    indexObj.getGroupedMetadataFields().add(gmd);
                                }
                                // NORMDATATERMS is now in the metadata docs, not docstructs
                                if (sbAuthorityDataTerms.length() > 0) {
                                    gmd.getFields().add(new LuceneField(SolrConstants.NORMDATATERMS, sbAuthorityDataTerms.toString()));
                                }
                            } else {
                                // Regular metadata
                                String xpathAnswerString = JDomXP.objectToString(xpathAnswerObject);
                                if (StringUtils.isNotBlank(xpathAnswerString)) {
                                    xpathAnswerString = xpathAnswerString.trim();
                                    xpathAnswerString = new StringBuilder(xpathAnswerString).append(configurationItem.getValuepostfix()).toString();
                                    fieldValue = xpathAnswerString.trim();
                                    if (configurationItem.getSplittingCharacter() != null) {
                                        // Hack to prevent empty concatenated collection names
                                        if (configurationItem.getSplittingCharacter().equals(fieldValue)) {
                                            continue;
                                        }
                                        // Hack to prevent collection names ending with a separator
                                        if (fieldValue.endsWith(configurationItem.getSplittingCharacter())) {
                                            fieldValue = fieldValue.substring(0, fieldValue.length() - 1);
                                        }
                                    }
                                    if (configurationItem.isOneToken()) {
                                        fieldValue = toOneToken(fieldValue, configurationItem.getSplittingCharacter());
                                    }
                                    if (fieldName.startsWith("DATE_")) {
                                        if ((fieldValue = convertDateStringForSolrField(fieldValue, false)) == null) {
                                            continue;
                                        }
                                    }
                                    // Apply XPath prefix
                                    if (StringUtils.isNotEmpty(xpathConfig.getPrefix())) {
                                        fieldValue = xpathConfig.getPrefix() + fieldValue;
                                    }
                                    // Apply XPath suffix
                                    if (StringUtils.isNotEmpty(xpathConfig.getSuffix())) {
                                        fieldValue = fieldValue + xpathConfig.getSuffix();
                                    }
                                    if (configurationItem.isOneField()) {
                                        if (fieldValues.isEmpty()) {
                                            fieldValues.add("");
                                        }
                                        StringBuilder sb = new StringBuilder();
                                        sb.append(fieldValues.get(0));
                                        if (sb.length() > 0) {
                                            sb.append(configurationItem.getOneFieldSeparator());
                                        }
                                        sb.append(fieldValue);
                                        fieldValues.set(0, sb.toString());
                                    } else {
                                        fieldValues.add(fieldValue);
                                    }
                                } else {
                                    logger.debug("Null or empty string value returned for XPath query '{}'.", query);
                                }
                            }
                        }
                    }
                }

                for (String fieldValue : fieldValues) {
                    // CURRENTNOSORT must be an integer value
                    if (fieldName.equals(SolrConstants.CURRENTNOSORT)) {
                        try {
                            fieldValue = String.valueOf(Long.valueOf(fieldValue));
                        } catch (NumberFormatException e) {
                            logger.error("{} cannot be written because it's not an integer value: {}", SolrConstants.CURRENTNOSORT, fieldValue);
                            continue;
                        }
                    }
                    // Apply string modifications configured for this field
                    fieldValue = applyAllModifications(configurationItem, fieldValue);

                    // Convert to geoJSON
                    if (configurationItem.getGeoJSONSource() != null) {
                        GeoCoords coords = GeoJSONTools.convert(fieldValue, configurationItem.getGeoJSONSource(),
                                configurationItem.getGeoJSONSourceSeparator());
                        if (coords.getGeoJSON() != null) {
                            fieldValue = coords.getGeoJSON();
                        }
                        // Add WKT search field
                        if (configurationItem.isGeoJSONAddSearchField() && coords.getWKT() != null) {
                            ret.add(new LuceneField(FIELD_WKT_COORDS, coords.getWKT()));
                        }
                    }

                    // Add value to DEFAULT
                    if (configurationItem.isAddToDefault() && StringUtils.isNotBlank(fieldValue)) {
                        addValueToDefault(fieldValue, sbDefaultMetadataValues);
                    }
                    // Add normalized year
                    if (configurationItem.isNormalizeYear()) {
                        List<LuceneField> normalizedFields =
                                parseDatesAndCenturies(centuries, fieldValue, configurationItem.getNormalizeYearMinDigits());
                        ret.addAll(normalizedFields);
                        // Add sort fields for normalized years, centuries, etc.
                        for (LuceneField normalizedDateField : normalizedFields) {
                            addSortField(normalizedDateField.getField(), normalizedDateField.getValue(), SolrConstants.SORTNUM_,
                                    configurationItem.getNonSortConfigurations(), configurationItem.getValueNormalizer(), ret);
                        }
                    }
                    // Add Solr field
                    LuceneField luceneField = new LuceneField(fieldName, fieldValue);
                    ret.add(luceneField);

                    // Make sure non-sort characters are removed before adding _UNTOKENIZED and SORT_ fields
                    if (configurationItem.isAddSortField()) {
                        addSortField(configurationItem.getFieldname(), fieldValue, SolrConstants.SORT_, configurationItem.getNonSortConfigurations(),
                                configurationItem.getValueNormalizer(), ret);
                    }
                    if (configurationItem.isAddUntokenizedVersion() || fieldName.startsWith("MD_")) {
                        ret.add(new LuceneField(fieldName + SolrConstants._UNTOKENIZED, fieldValue));
                    }

                    // Abort after first value, if so configured
                    if (breakfirst) {
                        break;
                    }
                }
                // Add years in between collected YEAR values, if so configured
                if (configurationItem.isInterpolateYears()) {
                    ret.addAll(completeYears(ret));
                }
            }
        }
        if (sbDefaultMetadataValues.length() > 0) {
            indexObj.setDefaultValue(sbDefaultMetadataValues.toString());
        }
        ret.addAll(completeCenturies(ret));

        return ret;

    }

    /**
     * 
     * @param url URL to query
     * @param sbDefaultMetadataValues StringBuilder for collecting
     * @param sbNormDataTerms
     * @param addToDefaultFields Optional list of fields whose values should be added to DEFAULT
     * @param replaceRules Optional metadata value replace rules
     * @param labelField Field name of the metadata group to which this authority data belongs
     * @return
     */
    private static List<LuceneField> retrieveAuthorityData(String url, StringBuilder sbDefaultMetadataValues, StringBuilder sbNormDataTerms,
            List<String> addToDefaultFields, Map<Object, String> replaceRules, String labelField) {
        logger.info("retrieveAuthorityData: {}", url);
        if (url == null) {
            throw new IllegalArgumentException("url may not be null");
        }

        // If it's just an identifier, assume it's GND
        if (!url.startsWith("http")) {
            url = "https://d-nb.info/gnd/" + url;
        }

        Record record = NormDataImporter.getSingleRecord(url.trim());
        if (record == null) {
            logger.warn("Authority dataset could not be retrieved: {}", url);
            return Collections.emptyList();
        }
        if (record.getNormDataList().isEmpty()) {
            logger.warn("No authority data fields found.");
            return Collections.emptyList();
        }

        return parseAuthorityMetadata(record.getNormDataList(), sbDefaultMetadataValues, sbNormDataTerms, addToDefaultFields,
                replaceRules, labelField);
    }

    /**
     * 
     * @param authorityDataList
     * @param sbDefaultMetadataValues
     * @param sbNormDataTerms
     * @param addToDefaultFields Optional list of fields whose values should be added to DEFAULT
     * @param replaceRules Optional metadata value replace rules
     * @param labelField Field name of the metadata group to which this authority data belongs
     * @should add name search field correctly
     * @return
     */
    static List<LuceneField> parseAuthorityMetadata(List<NormData> authorityDataList, StringBuilder sbDefaultMetadataValues,
            StringBuilder sbNormDataTerms, List<String> addToDefaultFields, Map<Object, String> replaceRules, String labelField) {
        if (authorityDataList == null || authorityDataList.isEmpty()) {
            return Collections.emptyList();
        }

        List<LuceneField> ret = new ArrayList<>(authorityDataList.size());
        Set<String> nameSearchFieldValues = new HashSet<>();
        Set<String> placeSearchFieldValues = new HashSet<>();
        for (NormData authorityDataField : authorityDataList) {
            if (!authorityDataField.getKey().startsWith("NORM_")) {
                continue;
            }
            for (NormDataValue val : authorityDataField.getValues()) {
                // IKFN norm data browsing hack
                if (StringUtils.isBlank(val.getText()) || authorityDataField.getKey().equals("NORM_STATICPAGE")) {
                    continue;
                }
                String textValue = TextHelper.normalizeSequence(val.getText());
                if (replaceRules != null) {
                    textValue = applyReplaceRules(textValue, replaceRules);
                }

                ret.add(new LuceneField(authorityDataField.getKey(), textValue));
                ret.add(new LuceneField(authorityDataField.getKey() + SolrConstants._UNTOKENIZED, textValue));
                String valWithSpaces = new StringBuilder(" ").append(textValue).append(' ').toString();

                // Add to DEFAULT
                if (sbDefaultMetadataValues != null && addToDefaultFields != null && !addToDefaultFields.isEmpty()
                        && addToDefaultFields.contains(authorityDataField.getKey())
                        && !sbDefaultMetadataValues.toString().contains(valWithSpaces)) {
                    addValueToDefault(textValue, sbDefaultMetadataValues);
                    logger.trace("Added to DEFAULT: {}", textValue);
                }

                // Add to the norm data search field NORMDATATERMS
                if (sbNormDataTerms != null && !authorityDataField.getKey().startsWith("NORM_URI")
                        && !sbNormDataTerms.toString().contains(valWithSpaces)) {
                    sbNormDataTerms.append(valWithSpaces);
                    logger.trace("Added to NORMDATATERMS: {}", textValue);
                }

                // Aggregate place fields into the same untokenized field for term browsing
                if ((authorityDataField.getKey().equals("NORM_ALTNAME") || authorityDataField.getKey().equals("NORM_OFFICIALNAME"))
                        && !nameSearchFieldValues.contains(textValue)) {
                    if (StringUtils.isNotEmpty(labelField)) {
                        ret.add(new LuceneField(labelField + "_NAME_SEARCH", textValue));
                    }
                    ret.add(new LuceneField("NORM_NAME" + SolrConstants._UNTOKENIZED, textValue));
                    nameSearchFieldValues.add(textValue);
                } else if (authorityDataField.getKey().startsWith("NORM_PLACE") && !placeSearchFieldValues.contains(textValue)) {
                    if (StringUtils.isNotEmpty(labelField)) {
                        ret.add(new LuceneField(labelField + "_PLACE_SEARCH", textValue));
                    }
                    ret.add(new LuceneField("NORM_PLACE" + SolrConstants._UNTOKENIZED, textValue));
                    placeSearchFieldValues.add(textValue);
                } else if (authorityDataField.getKey().equals("NORM_LIFEPERIOD")) {
                    String[] valueSplit = textValue.split("-");
                    if (valueSplit.length > 0) {
                        for (String date : valueSplit) {
                            if (StringUtils.isNotEmpty(labelField)) {
                                ret.add(new LuceneField(labelField + "_DATE_SEARCH", date.trim()));
                            }
                            ret.add(new LuceneField("NORM_DATE" + SolrConstants._UNTOKENIZED, date.trim()));
                        }
                    }
                } else if (authorityDataField.getKey().equals(GeoNamesRecord.AUTOCOORDS_FIELD)) {
                    // Add searchable WKT lon-lat coordinates
                    String[] textValueSplit = textValue.split(" ");
                    if (textValueSplit.length > 1) {
                        String coords = textValueSplit[0] + " " + textValueSplit[1];
                        ret.add(new LuceneField(FIELD_WKT_COORDS, coords));
                        ret.add(new LuceneField(FIELD_HAS_WKT_COORDS, "true"));
                    }

                    // Add geoJSON
                    String geoJSON = GeoJSONTools.convertCoordinatesToGeoJSONString(textValue, "mods:coordinates/point", " ");
                    if (StringUtils.isNotEmpty(geoJSON)) {
                        ret.add(new LuceneField("NORM_COORDS_GEOJSON", geoJSON));
                    }
                }

            }
        }

        return ret;
    }

    /**
     * Finds and writes metadata to the given IndexObject without considering any child elements.
     *
     * @param indexObj The IndexObject into which the metadata shall be written.
     * @param element The JDOM Element relative to which to search.
     * @param queryPrefix a {@link java.lang.String} object.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @param xp a {@link io.goobi.viewer.indexer.helper.JDomXP} object.
     */
    public static void writeMetadataToObject(IndexObject indexObj, Element element, String queryPrefix, JDomXP xp) throws FatalIndexerException {
        List<LuceneField> fields = retrieveElementMetadata(element, queryPrefix, indexObj, xp);

        for (LuceneField field : fields) {
            if (indexObj.getLuceneFieldWithName(field.getField()) != null) {
                boolean duplicate = false;

                // Do not write duplicate fields (same name + value)
                for (LuceneField f : indexObj.getLuceneFields()) {
                    if (f.getField().equals(field.getField()) && (((f.getValue() != null) && f.getValue().equals(field.getValue()))
                            || field.getField().startsWith(SolrConstants.SORT_))) {
                        duplicate = true;
                        break;
                    }
                }
                if (duplicate) {
                    List<FieldConfig> fieldConfigList =
                            Configuration.getInstance().getMetadataConfigurationManager().getConfigurationListForField(field.getField());
                    if (fieldConfigList == null || fieldConfigList.isEmpty() || !fieldConfigList.get(0).isAllowDuplicateValues()) {
                        logger.debug("Duplicate field found: {}:{}", field.getField(), field.getValue());
                        continue;
                    }
                }
            }

            if (field.getField().equals(SolrConstants.ACCESSCONDITION)) {
                // Add access conditions to a separate list
                indexObj.getAccessConditions().add(field.getValue());
            } else {
                indexObj.addToLucene(field, false);
            }

            indexObj.setDefaultValue(indexObj.getDefaultValue().trim());
        }
    }

    /**
     * 
     * @param configurationItem
     * @param fieldValue
     * @param sbDefaultMetadataValues
     * @return
     * @throws FatalIndexerException
     */
    private static String applyAllModifications(FieldConfig configurationItem, String fieldValue) throws FatalIndexerException {
        if (StringUtils.isEmpty(fieldValue)) {
            return fieldValue;
        }

        fieldValue = applyReplaceRules(fieldValue, configurationItem.getReplaceRules());
        fieldValue = applyValueDefaultModifications(fieldValue);

        if (configurationItem.getFieldname().equals(SolrConstants.PI)) {
            fieldValue = applyIdentifierModifications(fieldValue);
        }

        if (configurationItem.isLowercase()) {
            fieldValue = fieldValue.toLowerCase();
        }

        if (configurationItem.getNonSortConfigurations() != null) {
            for (NonSortConfiguration nonSortConfig : configurationItem.getNonSortConfigurations()) {
                // Remove non-sort characters
                fieldValue = nonSortConfig.apply(fieldValue);
            }
        }
        if (configurationItem.getValueNormalizer() != null) {
            fieldValue = configurationItem.getValueNormalizer().normalize(fieldValue);
        }

        return fieldValue;
    }

    /**
     * <p>
     * applyReplaceRules.
     * </p>
     *
     * @param value a {@link java.lang.String} object.
     * @param replaceRules a {@link java.util.Map} object.
     * @should apply rules correctly
     * @should throw IllegalArgumentException if value is null
     * @should return unmodified value if replaceRules is null
     * @return a {@link java.lang.String} object.
     */
    public static String applyReplaceRules(String value, Map<Object, String> replaceRules) {
        if (value == null) {
            throw new IllegalArgumentException("value may not be null");
        }
        if (replaceRules == null) {
            return value;
        }
        String ret = value;
        for (Object key : replaceRules.keySet()) {
            if (key instanceof Character) {
                StringBuffer sb = new StringBuffer();
                sb.append(key);
                ret = ret.replace(sb.toString(), replaceRules.get(key));
            } else if (key instanceof String) {
                logger.debug("replace rule: {} -> {}", key, replaceRules.get(key));
                if (((String) key).startsWith("REGEX:")) {
                    ret = ret.replaceAll(((String) key).substring(6), replaceRules.get(key));
                } else {
                    ret = ret.replace((String) key, replaceRules.get(key));
                }
            } else {
                logger.error("Unknown replacement key type of '{}: {}", key.toString(), key.getClass().getName());
            }
        }

        return ret;
    }

    /**
     * <p>
     * applyValueDefaultModifications.
     * </p>
     *
     * @param fieldValue a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     */
    public static String applyValueDefaultModifications(String fieldValue) {
        String ret = fieldValue;
        if (StringUtils.isNotEmpty(ret)) {
            // Remove any prior HTML escaping, otherwise strings like '&amp;amp;' might occur
            ret = StringEscapeUtils.unescapeHtml4(ret);
        }

        return ret;
    }

    /**
     * <p>
     * applyIdentifierModifications.
     * </p>
     *
     * @param pi a {@link java.lang.String} object.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should trim identifier
     * @should apply replace rules
     * @should replace spaces with underscores
     * @should replace commas with underscores
     * @return a {@link java.lang.String} object.
     */
    public static String applyIdentifierModifications(String pi) throws FatalIndexerException {
        if (StringUtils.isEmpty(pi)) {
            return pi;
        }
        String ret = pi.trim();
        // Apply replace rules defined for the field PI
        List<FieldConfig> configItems = Configuration.getInstance().getMetadataConfigurationManager().getConfigurationListForField(SolrConstants.PI);
        if (configItems != null && !configItems.isEmpty()) {
            Map<Object, String> replaceRules = configItems.get(0).getReplaceRules();
            if (replaceRules != null && !replaceRules.isEmpty()) {
                ret = MetadataHelper.applyReplaceRules(ret, replaceRules);
            }
        }
        ret = ret.replace(" ", "_");
        ret = ret.replace(",", "_");
        ret = ret.replace(":", "_");

        return ret;
    }

    /**
     * Adds a SORT_* version of the given field, but only if no field by that name exists yet.
     *
     * @param fieldName a {@link java.lang.String} object.
     * @param fieldValue a {@link java.lang.String} object.
     * @param nonSortConfigurations a {@link java.util.List} object.
     * @param retList a {@link java.util.List} object.
     * @should add regular sort fields correctly
     * @should add numerical sort fields correctly
     * @should not add existing fields
     * @param sortFieldPrefix a {@link java.lang.String} object.
     * @param valueNormalizer a {@link io.goobi.viewer.indexer.model.config.ValueNormalizer} object.
     */
    public static void addSortField(String fieldName, String fieldValue, String sortFieldPrefix, List<NonSortConfiguration> nonSortConfigurations,
            ValueNormalizer valueNormalizer, List<LuceneField> retList) {
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName may not be null");
        }
        if (fieldValue == null) {
            throw new IllegalArgumentException("fieldValue may not be null");
        }
        if (sortFieldPrefix == null) {
            throw new IllegalArgumentException("sortFieldPrefix may not be null");
        }

        String sortFieldName = sortFieldPrefix + fieldName.replace("MD_", "");
        for (LuceneField field : retList) {
            if (field.getField().equals(sortFieldName)) {
                // A sort field by that name already exists (only one may be added to a doc)
                return;
            }
        }
        if (nonSortConfigurations != null && !nonSortConfigurations.isEmpty()) {
            for (NonSortConfiguration nonSortConfig : nonSortConfigurations) {
                // remove non-sort characters and anything between them
                fieldValue = nonSortConfig.apply(fieldValue);
            }
        }
        if (valueNormalizer != null) {
            fieldValue = valueNormalizer.normalize(fieldValue);
        }
        logger.debug("Adding sorting field {}: {}", sortFieldName, fieldValue);
        retList.add(new LuceneField(sortFieldName, fieldValue));
    }

    /**
     * Removes any non-alphanumeric characters from 'value'. If 'splittingChar' is given, replace its occurrences in 'value' with periods.
     * 
     * @param inValue String
     * @param splittingChar
     * @return String
     **/
    private static String toOneToken(String inValue, String splittingChar) {
        String value = inValue;
        if (StringUtils.isNotEmpty(splittingChar)) {
            value = value.replaceAll(" ", "");
            value = value.replaceAll("[^\\w|" + splittingChar + "]", "");
            value = value.replace("_", "");
            value = value.replace(splittingChar, ".");
            // logger.info(value);
        } else {
            value = value.replaceAll("[\\W]", "");
        }

        // while (value.contains(replacementChar + replacementChar)) {
        // value = value.replace(replacementChar + replacementChar, replacementChar);
        // // do not do replaceAll() here, it will reduce the string to a single "."
        // }
        return value;
    }

    /**
     * <p>
     * getAnchorPi.
     * </p>
     *
     * @param xp a {@link io.goobi.viewer.indexer.helper.JDomXP} object.
     * @return a {@link java.lang.String} object.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException if any.
     */
    public static String getAnchorPi(JDomXP xp) throws FatalIndexerException {
        String query =
                "/mets:mets/mets:dmdSec/mets:mdWrap[@MDTYPE='MODS']/mets:xmlData/mods:mods/mods:relatedItem[@type='host']/mods:recordInfo/mods:recordIdentifier";
        List<Element> relatedItemList = xp.evaluateToElements(query, null);
        if ((relatedItemList != null) && (!relatedItemList.isEmpty())) {
            return relatedItemList.get(0).getText();
        }

        return null;
    }

    /**
     * Retrieves the PI value from the given METS document object.
     *
     * @param prefix a {@link java.lang.String} object.
     * @param xp a {@link io.goobi.viewer.indexer.helper.JDomXP} object.
     * @return String or null
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should extract DenkXweb PI correctly
     */
    public static String getPIFromXML(String prefix, JDomXP xp) throws FatalIndexerException {
        List<FieldConfig> piConfig = Configuration.getInstance().getMetadataConfigurationManager().getConfigurationListForField(SolrConstants.PI);
        if (piConfig == null) {
            return null;
        }

        List<XPathConfig> xPathConfigurations = piConfig.get(0).getxPathConfigurations();
        for (XPathConfig xPathConfig : xPathConfigurations) {
            String query = prefix + xPathConfig.getxPath();
            query = query.replace("///", "/");
            logger.trace(query);
            String pi = xp.evaluateToString(query, null);
            if (StringUtils.isNotEmpty(pi)) {
                return pi;
            }
        }

        return null;
    }

    /**
     * 
     * @param centuries
     * @param value
     * @param normalizeYearMinDigits
     * @return List of generated LuceneFields.
     * @should parse centuries and dates correctly
     * @should normalize year digits
     */
    static List<LuceneField> parseDatesAndCenturies(Set<Integer> centuries, String value, int normalizeYearMinDigits) {
        if (StringUtils.isEmpty(value)) {
            return Collections.emptyList();
        }

        List<LuceneField> ret = new ArrayList<>();
        for (PrimitiveDate date : normalizeDate(value, normalizeYearMinDigits)) {
            if (date.getYear() == null) {
                continue;
            }
            logger.trace("using PrimitiveDate: {}", date.toString());
            ret.add(new LuceneField(SolrConstants.YEAR, String.valueOf(date.getYear())));
            int century = getCentury(date.getYear());
            if (!centuries.contains(century)) {
                ret.add(new LuceneField(SolrConstants.CENTURY, String.valueOf(century)));
                centuries.add(century);
            }
            if (date.getMonth() != null) {
                String year = FORMAT_FOUR_DIGITS.get().format(date.getYear());
                ret.add(new LuceneField(SolrConstants.YEARMONTH, year + FORMAT_TWO_DIGITS.get().format(date.getMonth())));
                if (date.getDay() != null) {
                    ret.add(new LuceneField(SolrConstants.YEARMONTHDAY,
                            year + FORMAT_TWO_DIGITS.get().format(date.getMonth()) + FORMAT_TWO_DIGITS.get().format(date.getDay())));
                    logger.trace("added YEARMONTHDAY: {}", new LuceneField(SolrConstants.YEARMONTHDAY,
                            year + FORMAT_TWO_DIGITS.get().format(date.getMonth()) + FORMAT_TWO_DIGITS.get().format(date.getDay())).getValue());
                }
            }

        }

        return ret;
    }

    /**
     * Extract dates from the given string and returns them as a list.
     * 
     * @param dateString
     * @param normalizeYearMinDigits
     * @return List of parsed PrimitiveDates.
     * @should parse german date formats correctly
     * @should parse rfc date formats correctly
     * @should parse american date formats correctly
     * @should parse chinese date formats correctly
     * @should parse japanese date formats correctly
     * @should parse year ranges correctly
     * @should parse single years correctly
     * @should throw IllegalArgumentException if normalizeYearMinDigits less than 1
     */
    static List<PrimitiveDate> normalizeDate(String dateString, int normalizeYearMinDigits) {
        if (normalizeYearMinDigits < 1) {
            throw new IllegalArgumentException("normalizeYearMinDigits must be at least 1");
        }
        logger.trace("normalizeDate: {} (min digits: {})", dateString, normalizeYearMinDigits);

        List<PrimitiveDate> ret = new ArrayList<>();

        // Try known date formats first
        try {
            LocalDate date = LocalDate.parse(dateString, formatterISO8601Full);
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (DateTimeParseException e) {
        }
        try {
            LocalDate date = LocalDate.parse(dateString, formatterDEDate);
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (DateTimeParseException e) {
        }
        try {
            LocalDate date = LocalDate.parse(dateString, formatterISO8601Date);
            ret.add(new PrimitiveDate(date));
            logger.trace("parsed date: {} (using format yyyy-MM-dd)", date.toString());
            return ret;
        } catch (DateTimeParseException e) {
        }
        try {
            LocalDate date = LocalDate.parse(dateString, formatterISO8601YearMonth);
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (DateTimeParseException e) {
        }
        try {
            LocalDate date = LocalDate.parse(dateString, formatterUSDate);
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (DateTimeParseException e) {
        }
        try {
            LocalDate date = LocalDate.parse(dateString, formatterCNDate);
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (DateTimeParseException e) {
        }
        try {
            LocalDate date = LocalDate.parse(dateString, formatterJPDate);
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (DateTimeParseException e) {
        }

        // Try parsing year ranges
        if (dateString.contains("-") && dateString.charAt(0) != '-') {
            Pattern p = Pattern.compile("[\\d+]\\d+");
            Matcher m = p.matcher(dateString);
            while (m.find()) {
                try {
                    String sub = dateString.substring(m.start(), m.end());
                    if (sub.length() >= normalizeYearMinDigits) {
                        int year = Integer.valueOf(sub);
                        ret.add(new PrimitiveDate(year, null, null));
                    }
                } catch (NumberFormatException e) {
                    logger.error(e.getMessage());
                }
            }
            return ret;
        }
        // Try parsing remaining numbers
        Pattern p = Pattern.compile("[-]{0,1}\\d+");
        Matcher m = p.matcher(dateString);
        while (m.find()) {
            try {
                String sub = dateString.substring(m.start(), m.end());
                if (sub.length() >= (sub.charAt(0) == '-' ? (normalizeYearMinDigits + 1) : normalizeYearMinDigits)) {
                    int year = Integer.valueOf(sub);
                    ret.add(new PrimitiveDate(year, null, null));
                }
            } catch (NumberFormatException e) {
                logger.error(e.getMessage());
            }
        }

        return ret;
    }

    /**
     * Returns the number of the century to which the given year belongs.
     * 
     * @param year
     * @return
     * @should detect positive century correctly
     * @should detect negative century correctly
     * @should detect first century correctly
     */
    static int getCentury(long year) {
        boolean bc = false;
        String yearString = String.valueOf(year);
        if (yearString.charAt(0) == '-') {
            bc = true;
            yearString = yearString.substring(1);
        }
        if (yearString.length() > 2) {
            yearString = yearString.substring(0, yearString.length() - 2);
        } else {
            // First century years are <= 2
            yearString = "0";
        }
        int century = Integer.valueOf(yearString);
        if (bc) {
            century *= -1;
            century -= 1;
        } else {
            century++;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("year: " + year + ", century: " + century);
        }

        return century;
    }

    /**
     * Adds additional CENTURY fields if there is a gap (e.g. 15th and 17th century implies 16th).
     * 
     * @param fields
     * @return
     * @should complete centuries correctly
     */
    static List<LuceneField> completeCenturies(List<LuceneField> fields) {
        return completeIntegerValues(fields, SolrConstants.CENTURY, new HashSet<>(Collections.singletonList(0)));
    }

    /**
     * Adds additional YEAR fields if there is a gap (e.g. if there is 1990 and 1993, also add 1991 and 1992).
     * 
     * @param fields
     * @return
     * @should complete years correctly
     */
    static List<LuceneField> completeYears(List<LuceneField> fields) {
        return completeIntegerValues(fields, SolrConstants.YEAR, null);
    }

    /**
     * 
     * @param fields
     * @param fieldName
     * @param
     * @return
     */
    private static List<LuceneField> completeIntegerValues(List<LuceneField> fields, String fieldName, Set<Integer> skipValues) {
        List<LuceneField> newValues = new ArrayList<>();
        if (fields != null && !fields.isEmpty()) {
            List<Integer> oldValues = new ArrayList<>();
            for (LuceneField field : fields) {
                if (field.getField().equals(fieldName) && !oldValues.contains(Integer.valueOf(field.getValue()))) {
                    oldValues.add(Integer.valueOf(field.getValue()));
                }
            }
            if (!oldValues.isEmpty()) {
                Collections.sort(oldValues);
                int count = 0;
                for (int i = oldValues.get(0); i < oldValues.get(oldValues.size() - 1); ++i) {
                    if (!oldValues.contains(i)) {
                        if (skipValues != null && skipValues.contains(i)) {
                            continue;
                        }
                        newValues.add(new LuceneField(fieldName, String.valueOf(i)));
                        logger.debug("Added implicit {}: {}", fieldName, i);
                        count++;
                    }
                    //                    if (count == 10) {
                    //                        break;
                    //                    }
                }
            }
        }

        return newValues;
    }

    /**
     * 
     * @param ele
     * @param groupEntityFields
     * @param groupLabel
     * @param xp
     * @return
     * @throws FatalIndexerException
     * @should group correctly
     */
    static GroupedMetadata getGroupedMetadata(Element ele, Map<String, Object> groupEntityFields, String groupLabel) throws FatalIndexerException {
        logger.trace("getGroupedMetadata: {}", groupLabel);
        GroupedMetadata ret = new GroupedMetadata();
        ret.setLabel(groupLabel);
        ret.getFields().add(new LuceneField(SolrConstants.LABEL, groupLabel));

        // Grouped metadata type
        String type = null;
        if (groupEntityFields.get("type") != null) {
            type = (String) groupEntityFields.get("type");
            ret.getFields().add(new LuceneField(SolrConstants.METADATATYPE, type.trim()));
        } else {
            type = "OTHER";
            logger.warn("Attribute groupedMetadata/@type not configured for field '{}', using 'OTHER'.", groupLabel);
        }

        boolean addAuthorityDataToDocstruct = false;
        if (groupEntityFields.get("addAuthorityDataToDocstruct") != null) {
            addAuthorityDataToDocstruct = (boolean) groupEntityFields.get("addAuthorityDataToDocstruct");
        }
        ret.setAddAuthorityDataToDocstruct(addAuthorityDataToDocstruct);

        boolean authorityUriFound = false;
        Map<String, List<String>> collectedValues = new HashMap<>();
        for (Object field : groupEntityFields.keySet()) {
            if ("type".equals(field) || !(groupEntityFields.get(field) instanceof SubfieldConfig)) {
                continue;
            }
            SubfieldConfig subfield = (SubfieldConfig) groupEntityFields.get(field);
            for (String xpath : subfield.getXpaths()) {
                logger.debug("XPath: {}", xpath);
                List<String> values = JDomXP.evaluateToStringListStatic(xpath, ele);
                if (values == null || values.isEmpty()) {
                    // Use default value, if available
                    if (subfield.getDefaultValues().get(xpath) != null) {
                        values = Collections.singletonList(subfield.getDefaultValues().get(xpath));
                    }
                    if (values == null || values.isEmpty()) {
                        continue;
                    }
                }
                // Trim down to the first value if subfield is not multivalued
                if (!subfield.isMultivalued() && values.size() > 1) {
                    logger.info("{} is not multivalued", subfield.getFieldname());
                    values = values.subList(0, 1);
                }
                for (Object val : values) {
                    String fieldValue = JDomXP.objectToString(val);
                    logger.debug("found: {}:{}", subfield.getFieldname(), fieldValue);
                    if (fieldValue == null) {
                        continue;
                    }
                    fieldValue = fieldValue.trim();
                    if (fieldValue.isEmpty()) {
                        continue;
                    }

                    if (subfield.getFieldname().startsWith(NormDataImporter.FIELD_URI)) {
                        // Skip values that probably aren't real identifiers or URIs
                        if (fieldValue.length() < 2) {
                            logger.trace("Authority URI too short: {}", fieldValue);
                            continue;
                        }
                        if (NormDataImporter.FIELD_URI.equals(subfield.getFieldname())) {
                            authorityUriFound = true;
                            ret.setAuthorityURI(fieldValue);
                        }
                        // Add GND URL part, if the value is not a URL
                        if (!fieldValue.startsWith("http")) {
                            fieldValue = "http://d-nb.info/gnd/" + fieldValue;
                        }
                    }

                    ret.getFields().add(new LuceneField(subfield.getFieldname(), fieldValue));
                    if (!collectedValues.containsKey(fieldValue)) {
                        collectedValues.put(subfield.getFieldname(), new ArrayList<>(values.size()));
                    }
                    collectedValues.get(subfield.getFieldname()).add(fieldValue);
                }

            }
        }

        String mdValue = null;
        for (LuceneField field : ret.getFields()) {
            if (field.getField().equals("MD_VALUE") || (field.getField().equals("MD_DISPLAYFORM") && "name".equals(type))
                    || (field.getField().equals("MD_LOCATION") && "location".equals(type))) {
                mdValue = cleanUpName(field.getValue());
                field.setValue(mdValue);
            }
        }
        // if no MD_VALUE field exists, construct one
        if (mdValue == null) {
            StringBuilder sbValue = new StringBuilder();
            switch (type) {
                case "PERSON":
                    if (collectedValues.containsKey("MD_LASTNAME") && !collectedValues.get("MD_LASTNAME").isEmpty()) {
                        sbValue.append(collectedValues.get("MD_LASTNAME").get(0));
                    }
                    if (collectedValues.containsKey("MD_FIRSTNAME") && !collectedValues.get("MD_FIRSTNAME").isEmpty()) {
                        if (sbValue.length() > 0) {
                            sbValue.append(", ");
                        }
                        sbValue.append(collectedValues.get("MD_FIRSTNAME").get(0));
                    }
                    break;
            }
            if (sbValue.length() > 0) {
                mdValue = sbValue.toString();
                ret.getFields().add(new LuceneField("MD_VALUE", mdValue));
            }
        }
        if (mdValue != null) {
            ret.setMainValue(mdValue);
        }

        // Add SORT_DISPLAYFORM so that there is a single-valued field by which to group metadata search hits
        if (mdValue != null) {
            addSortField(SolrConstants.GROUPFIELD, new StringBuilder(groupLabel).append("_").append(mdValue).toString(), "", null, null,
                    ret.getFields());
        }

        // Add authority data URI, if available (GND only)
        if (!authorityUriFound) {
            String authority = ele.getAttributeValue("authority");
            if (authority == null) {
                // Add valueURI without any other specifications
                String valueURI = ele.getAttributeValue("valueURI");
                if (valueURI != null) {
                    ret.getFields().add(new LuceneField(NormDataImporter.FIELD_URI, valueURI));
                    ret.setAuthorityURI(valueURI);
                }
            } else {
                String authorityURI = ele.getAttributeValue("authorityURI");
                String valueURI = ele.getAttributeValue("valueURI");
                switch (authority) {
                    case "gnd":
                        // Skip missing GND identifiers
                        if ("https://d-nb.info/gnd/".equals(valueURI) || "http://d-nb.info/gnd/".equals(valueURI)) {
                            break;
                        }
                    default:
                        if (StringUtils.isNotEmpty(valueURI)) {
                            valueURI = valueURI.trim();
                            if (StringUtils.isNotEmpty(authorityURI) && !valueURI.startsWith(authorityURI)) {
                                ret.getFields().add(new LuceneField(NormDataImporter.FIELD_URI, authorityURI + valueURI));
                            } else {
                                ret.getFields().add(new LuceneField(NormDataImporter.FIELD_URI, valueURI));
                            }
                            ret.setAuthorityURI(valueURI);
                        }
                        break;
                }
            }
        }

        return ret;
    }

    /**
     * 
     * @param value
     * @return
     * @should remove leading comma
     * @should remove trailing comma
     */
    static String cleanUpName(String value) {
        if (value == null) {
            return value;
        }
        value = value.trim();
        // Hack to remove the comma if a person has no first or last name (e.g when using concat() in XPath)
        if (value.endsWith(",") && value.length() > 1) {
            value = value.substring(0, value.length() - 1);
        }
        if (value.startsWith(",") && value.length() > 1) {
            value = value.substring(1).trim();
        }

        return value;
    }

    /**
     * Adds the given value to the given StringBuilder if the value does not yet exist in the buffer, separated by spaces. Also adds a concatenated
     * version of the value if it contains a hyphen.
     * 
     * @param value
     * @param sbDefaultMetadataValues
     * @should add value correctly
     * @should add concatenated value correctly
     * @should throw IllegalArgumentException if value is null
     * @should throw IllegalArgumentException if sbDefaultMetadataValues is null
     */
    static void addValueToDefault(String value, StringBuilder sbDefaultMetadataValues) {
        if (value == null) {
            throw new IllegalArgumentException("value may not be null");
        }
        if (sbDefaultMetadataValues == null) {
            throw new IllegalArgumentException("sbDefaultMetadataValues may not be null");
        }

        String fieldValueTrim = value.trim();
        String defaultValueWithSpaces = new StringBuilder(" ").append(fieldValueTrim).append(' ').toString();
        if (!sbDefaultMetadataValues.toString().contains(defaultValueWithSpaces)) {
            sbDefaultMetadataValues.append(defaultValueWithSpaces);
        }
        String concatValue = getConcatenatedValue(fieldValueTrim);
        if (!concatValue.equals(value)) {
            String concatValueWithSpaces = new StringBuilder(" ").append(concatValue).append(' ').toString();
            if (!sbDefaultMetadataValues.toString().contains(concatValueWithSpaces)) {
                sbDefaultMetadataValues.append(concatValueWithSpaces);
            }
        }
    }

    /**
     * 
     * @param value
     * @return
     * @should concatenate value terms correctly
     */
    static String getConcatenatedValue(String value) {
        StringBuilder sbConcat = new StringBuilder();
        if (value != null && value.contains("-")) {
            String[] constantValueSplit = value.split("-");
            for (String s : constantValueSplit) {
                sbConcat.append(s);
            }
        }

        return sbConcat.toString();
    }

    /**
     * 
     * @param value
     * @param useUTC If true, UTC time zone will be used; default time zone otherwise
     * @return Converted datetime string
     * @should convert date correctly
     */
    static String convertDateStringForSolrField(String value, boolean useUTC) {
        List<PrimitiveDate> dates = normalizeDate(value, 4);
        if (!dates.isEmpty()) {
            PrimitiveDate date = dates.get(0);
            if (date.getYear() != null) {
                ZonedDateTime ld =
                        LocalDateTime.of(date.getYear(), date.getMonth() != null ? date.getMonth() : 1, date.getDay() != null ? date.getDay() : 1, 0,
                                0, 0, 0).atZone(useUTC ? ZoneOffset.UTC : ZoneOffset.systemDefault());
                return ld.format(formatterISO8601DateTimeInstant);
            }
        }

        logger.warn("Could not parse date from value: {}", value);
        return null;
    }

    /**
     * <p>
     * main.
     * </p>
     *
     * @param args an array of {@link java.lang.String} objects.
     */
    public static void main(String[] args) {
        String string = "Vol. 15 xxx";
        Pattern p = Pattern.compile(".*(\\d+).*");
        Matcher m = p.matcher(string);

        if (m.find()) {
            MatchResult mr = m.toMatchResult();
            String value = mr.group(1);
            System.out.println("found: " + value);
        }

        p = Pattern.compile("\\d+");
        m = p.matcher(string);
        while (m.find()) {
            try {
                String sub = string.substring(m.start(), m.end());
                System.out.println("found 2: " + sub);
            } catch (NumberFormatException e) {
                logger.error(e.getMessage());
            }
        }
    }

    /**
     * Extracts the (lowercase) language code from the given field name.
     *
     * @param fieldName a {@link java.lang.String} object.
     * @should extract language code correctly
     * @should ignore any suffixes longer than two chars
     * @return a {@link java.lang.String} object.
     */
    public static String extractLanguageCodeFromMetadataField(String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName may not be null");
        }

        if (fieldName.contains(SolrConstants._LANG_)) {
            int index = fieldName.indexOf(SolrConstants._LANG_) + SolrConstants._LANG_.length();
            if (fieldName.length() == index + 2) {
                return fieldName.substring(index).toLowerCase();
            }
        }

        return null;
    }

    /**
     * Reads TEI files from the given Path and adds metadata and texts to the give index object.
     *
     * @param indexObj a {@link io.goobi.viewer.indexer.model.IndexObject} object.
     * @param teiFolder a {@link java.nio.file.Path} object.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @throws java.io.IOException
     * @throws org.jdom2.JDOMException
     * @should append fulltext from all files
     */
    public static void processTEIMetadataFiles(IndexObject indexObj, Path teiFolder) throws FatalIndexerException, IOException, JDOMException {
        if (indexObj == null) {
            throw new IllegalArgumentException("indexObj may not be null");
        }
        if (teiFolder == null) {
            throw new IllegalArgumentException("teiFolder may not be null");
        }

        StringBuilder sbFulltext = new StringBuilder();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(teiFolder, "*.{xml}")) {
            for (Path path : stream) {
                logger.info("Found TEI file: {}", path.getFileName().toString());
                JDomXP tei = new JDomXP(path.toFile());
                writeMetadataToObject(indexObj, tei.getRootElement(), "", tei);

                // Add text body
                Element eleText = tei.getRootElement().getChild("text", null);
                if (eleText != null && eleText.getChild("body", null) != null) {
                    String language = eleText.getAttributeValue("lang", Configuration.getInstance().getNamespaces().get("xml")); // TODO extract language from a different element? - No, this is the correct element (Florian)
                    String fileFieldName = SolrConstants.FILENAME_TEI;
                    if (language != null) {
                        //                                String isoCode = MetadataConfigurationManager.getLanguageMapping(language);
                        String isoCode = LanguageHelper.getInstance().getLanguage(language).getIsoCodeOld();
                        if (isoCode != null) {
                            fileFieldName += SolrConstants._LANG_ + isoCode.toUpperCase();
                        }
                        indexObj.getLanguages().add(isoCode);
                    }
                    indexObj.addToLucene(fileFieldName, path.getFileName().toString());

                    // Add searchable version of the text
                    Element eleBody = eleText.getChild("body", null);
                    Element eleNewRoot = new Element("tempRoot");
                    for (Element ele : eleBody.getChildren()) {
                        eleNewRoot.addContent(ele.clone());
                    }
                    String body = XmlTools.getStringFromElement(eleNewRoot, null).replace("<tempRoot>", "").replace("</tempRoot>", "").trim();
                    sbFulltext.append(TextHelper.cleanUpHtmlTags(body)).append('\n');
                } else {
                    logger.warn("No text body found in TEI");
                }

            }
        }
        if (sbFulltext.length() > 0) {
            indexObj.addToLucene(SolrConstants.FULLTEXT, sbFulltext.toString());
        }
    }
}
