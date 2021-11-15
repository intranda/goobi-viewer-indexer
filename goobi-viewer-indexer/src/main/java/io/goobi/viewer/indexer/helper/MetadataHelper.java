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
import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.helper.language.LanguageHelper;
import io.goobi.viewer.indexer.model.GeoCoords;
import io.goobi.viewer.indexer.model.GroupedMetadata;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.PrimitiveDate;
import io.goobi.viewer.indexer.model.PrimoDocument;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.MetadataGroupType;
import io.goobi.viewer.indexer.model.config.FieldConfig;
import io.goobi.viewer.indexer.model.config.GroupEntity;
import io.goobi.viewer.indexer.model.config.NonSortConfiguration;
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

    public static Map<String, Record> authorityDataCache = new HashMap<>();

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

    /** Constant <code>addNormDataFieldsToDefault</code> */
    public static List<String> addAuthorityDataFieldsToDefault;

    /**
     * Retrieves configured metadata fields from the given XML node. Written for LIDO events, but should theoretically work for any METS or LIDO node.
     *
     * @param element a {@link org.jdom2.Element} object.
     * @param queryPrefix a {@link java.lang.String} object.
     * @param indexObj The IndexObject is needed to alter its DEFAULT field value.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
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
                                        getGroupedMetadata(eleMods, configurationItem.getGroupEntity(), configurationItem,
                                                configurationItem.getFieldname(), sbDefaultMetadataValues,
                                                ret);

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

                                // Add GMD to index object (if not duplicate or duplicates are allowed for this field)
                                if (!indexObj.getGroupedMetadataFields().contains(gmd) || configurationItem.isAllowDuplicateValues()) {
                                    indexObj.getGroupedMetadataFields().add(gmd);
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
                                        if ((fieldValue = DateTools.convertDateStringForSolrField(fieldValue, false)) == null) {
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
                        logger.trace("Added to DEFAULT: {}", fieldValue);
                    }
                    // Add normalized year
                    if (configurationItem.isNormalizeYear()) {
                        List<LuceneField> normalizedFields =
                                parseDatesAndCenturies(centuries, fieldValue, configurationItem.getNormalizeYearMinDigits());
                        ret.addAll(normalizedFields);
                        // Add sort fields for normalized years, centuries, etc.
                        for (LuceneField normalizedDateField : normalizedFields) {
                            addSortField(normalizedDateField.getField(), normalizedDateField.getValue(), SolrConstants.SORTNUM_,
                                    configurationItem.getNonSortConfigurations(), configurationItem.getValueNormalizers(), ret);
                        }
                    }
                    // Add Solr field
                    LuceneField luceneField = new LuceneField(fieldName, fieldValue);
                    ret.add(luceneField);

                    // Make sure non-sort characters are removed before adding _UNTOKENIZED and SORT_ fields
                    if (configurationItem.isAddSortField()) {
                        addSortField(configurationItem.getFieldname(), fieldValue, SolrConstants.SORT_, configurationItem.getNonSortConfigurations(),
                                configurationItem.getValueNormalizers(), ret);
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
     * @throws FatalIndexerException
     */
    private static List<LuceneField> retrieveAuthorityData(String url, StringBuilder sbDefaultMetadataValues, StringBuilder sbNormDataTerms,
            List<String> addToDefaultFields, Map<Object, String> replaceRules, String labelField) throws FatalIndexerException {
        logger.info("retrieveAuthorityData: {}", url);
        if (url == null) {
            throw new IllegalArgumentException("url may not be null");
        }

        // If it's just an identifier, assume it's GND
        if (!url.startsWith("http")) {
            url = "https://d-nb.info/gnd/" + url;
        }

        url = url.trim();
        boolean authorityDataCacheEnabled = Configuration.getInstance().isAuthorityDataCacheEnabled();

        Record record = authorityDataCache.get(url);
        if (record != null) {
            long cachedRecordAge = (System.currentTimeMillis() - record.getCreationTimestamp()) / 1000 / 60 / 60;
            logger.debug("Cached record age: {}", cachedRecordAge);
            if (cachedRecordAge < Configuration.getInstance().getAuthorityDataCacheRecordTTL()) {
                logger.info("Authority data retrieved from local cache: {}", url);
            } else {
                // Do not use expired record and clear from cache;
                record = null;
                authorityDataCache.remove(url);
            }

        }
        if (record == null) {
            record = NormDataImporter.getSingleRecord(url);
            if (record == null) {
                logger.warn("Authority dataset could not be retrieved: {}", url);
                return Collections.emptyList();
            }
            if (record.getNormDataList().isEmpty()) {
                logger.warn("No authority data fields found.");
                return Collections.emptyList();
            }
            if (authorityDataCacheEnabled) {
                authorityDataCache.put(url.trim(), record);
                if (authorityDataCache.size() > Configuration.getInstance().getAuthorityDataCacheSizeWarningThreshold()) {
                    logger.warn("Authority data cache size: {}, please restart indexer to clear.", authorityDataCache.size());
                }
            }
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
                if (authorityDataField.getKey().equals("NORM_NAME")
                        || (authorityDataField.getKey().equals("NORM_ALTNAME") || authorityDataField.getKey().equals("NORM_OFFICIALNAME"))
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
                    String[] textValueSplit = textValue.split(" ");

                    String type = "mods:coordinates/point";
                    if (textValue.startsWith("E")) {
                        switch (textValueSplit.length) {
                            case 2:
                                type = "sexagesimal:point";
                                break;
                            case 4:
                                type = "sexagesimal:polygon";
                                break;
                        }
                    }

                    GeoCoords coords = GeoJSONTools.convert(textValue, type, " ");

                    // Add searchable WKT lon-lat coordinates
                    //                        String coords = textValueSplit[0] + " " + textValueSplit[1];
                    ret.add(new LuceneField(FIELD_WKT_COORDS, coords.getWKT()));
                    ret.add(new LuceneField(FIELD_HAS_WKT_COORDS, "true"));

                    // Add geoJSON
                    String geoJSON = GeoJSONTools.convertCoordinatesToGeoJSONString(textValue, type, " ");
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
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
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
        if (!configurationItem.getValueNormalizers().isEmpty()) {
            for (ValueNormalizer normalizer : configurationItem.getValueNormalizers()) {
                fieldValue = normalizer.normalize(fieldValue);
                logger.info("normalized value: {}", fieldValue);
            }
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
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
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
            List<ValueNormalizer> valueNormalizers, List<LuceneField> retList) {
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
        if (valueNormalizers != null && !valueNormalizers.isEmpty()) {
            for (ValueNormalizer normalizer : valueNormalizers) {
                fieldValue = normalizer.normalize(fieldValue);
            }
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
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException if any.
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
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
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
        for (PrimitiveDate date : DateTools.normalizeDate(value, normalizeYearMinDigits)) {
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
                // int count = 0;
                for (int i = oldValues.get(0); i < oldValues.get(oldValues.size() - 1); ++i) {
                    if (!oldValues.contains(i)) {
                        if (skipValues != null && skipValues.contains(i)) {
                            continue;
                        }
                        newValues.add(new LuceneField(fieldName, String.valueOf(i)));
                        logger.debug("Added implicit {}: {}", fieldName, i);
                        // count++;
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
     * @param ele Relative JDOM2 root element
     * @param groupEntity {@link GroupEntity} configuration from which to create the {@link GroupedMetadata}
     * @param configurationItem Master field configuration
     * @param groupLabel Main field name
     * @param sbDefaultMetadataValues StringBuilder that collects default values
     * @param luceneFields
     * @return Generated {@link GroupedMetadata}
     * @throws FatalIndexerException
     * @should group correctly
     */
    static GroupedMetadata getGroupedMetadata(Element ele, GroupEntity groupEntity, FieldConfig configurationItem, String groupLabel,
            StringBuilder sbDefaultMetadataValues, List<LuceneField> luceneFields)
            throws FatalIndexerException {
        logger.trace("getGroupedMetadata: {}", groupLabel);
        GroupedMetadata ret = new GroupedMetadata();
        ret.setLabel(groupLabel);
        ret.getFields().add(new LuceneField(SolrConstants.LABEL, groupLabel));
        ret.getFields().add(new LuceneField(SolrConstants.METADATATYPE, groupEntity.getType().name()));
        ret.setAddAuthorityDataToDocstruct(groupEntity.isAddAuthorityDataToDocstruct());
        ret.setAddCoordsToDocstruct(groupEntity.isAddCoordsToDocstruct());
        ret.setAllowDuplicateValues(configurationItem.isAllowDuplicateValues());
        List<LuceneField> authorityData = new ArrayList<>();
        boolean groupFieldAlreadyReplaced = false;
        String authorityIdentifier = null;
        StringBuilder sbAuthorityDataTerms = new StringBuilder();

        Map<String, List<String>> collectedValues = new HashMap<>();
        ret.collectGroupMetadataValues(collectedValues, groupEntity.getSubfields(), ele);

        String mdValue = null;
        for (LuceneField field : ret.getFields()) {
            if (field.getField().equals("MD_VALUE")
                    || (field.getField().equals("MD_DISPLAYFORM") && MetadataGroupType.PERSON.equals(groupEntity.getType()))
                    || (field.getField().equals("MD_LOCATION") && MetadataGroupType.LOCATION.equals(groupEntity.getType()))) {
                mdValue = cleanUpName(field.getValue());
                field.setValue(mdValue);
            }
        }
        // if no MD_VALUE field exists, construct one
        if (mdValue == null) {
            StringBuilder sbValue = new StringBuilder();
            switch (groupEntity.getType()) {
                case PERSON:
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
                default:
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

        // Query citation resource
        if (MetadataGroupType.CITATION.equals(groupEntity.getType())) {
            if (StringUtils.isNotEmpty(groupEntity.getUrl())) {
                try {
                    PrimoDocument primo = new PrimoDocument(groupEntity.getUrl())
                            .prepareURL(collectedValues)
                            .fetch()
                            .build();
                    ret.collectGroupMetadataValues(collectedValues, groupEntity.getSubfields(),
                            primo.getXp().getRootElement());
                } catch (HTTPException | JDOMException | IOException | IllegalStateException e) {
                    logger.error(e.getMessage());
                }
            } else {
                logger.warn("Citation metadata field {} is missing a URL.", groupLabel);
            }
        }

        // Add single-valued field by which to group metadata search hits
        if (mdValue != null) {
            addSortField(SolrConstants.GROUPFIELD, new StringBuilder(groupLabel).append("_").append(mdValue).toString(), "", null, null,
                    ret.getFields());
        }

        // Add authority data URI, if available (GND only)
        if (ret.getAuthorityURI() == null) {
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

        // Retrieve authority data
        if (ret.getAuthorityURI() != null) {
            authorityData.addAll(retrieveAuthorityData(ret.getAuthorityURI(), sbDefaultMetadataValues,
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
                            if (configurationItem.isAddUntokenizedVersion() || groupLabel.startsWith("MD_")) {
                                luceneFields.add(new LuceneField(
                                        new StringBuilder(groupLabel).append(SolrConstants._UNTOKENIZED).toString(),
                                        authorityField.getValue()));
                            }
                        }
                        break;
                    case "NORM_IDENTIFIER":
                        // If a NORM_IDENTIFIER exists for this metadata group, use it to replace the value of GROUPFIELD
                        for (LuceneField groupField : ret.getFields()) {
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

        for (LuceneField field : ret.getFields()) {
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
                        luceneFields.add(new LuceneField(FIELD_WKT_COORDS, coords.getWKT()));
                        luceneFields.add(new LuceneField(FIELD_HAS_WKT_COORDS, "true"));
                    }
                } catch (NumberFormatException e) {
                    logger.error("Cannot convert to geoJSON: {}", e.getMessage());
                }
            }

            field.setValue(moddedValue);

            if (configurationItem.isAddToDefault()) {
                // Add main value to owner doc's DEFAULT field
                if (StringUtils.isNotBlank(ret.getMainValue())) {
                    addValueToDefault(ret.getMainValue(), sbDefaultMetadataValues);
                }
                // Add grouped metadata field to DEFAULT
                if (StringUtils.isNotEmpty(field.getValue()) && !moddedValue.equals(ret.getMainValue())) {
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
            ret.getFields().add(new LuceneField(SolrConstants.GROUPFIELD, authorityIdentifier));
        }
        // Add MD_VALUE as DEFAULT to the grouped metadata doc
        if (configurationItem.isAddToDefault() && StringUtils.isNotBlank(ret.getMainValue())) {
            ret.getFields().add(new LuceneField(SolrConstants.DEFAULT, ret.getMainValue()));
        }
        ret.getAuthorityDataFields().addAll(authorityData); // Add authority data outside the loop over groupMetadata

        // NORMDATATERMS is now in the metadata docs, not docstructs
        if (sbAuthorityDataTerms.length() > 0) {
            ret.getFields().add(new LuceneField(SolrConstants.NORMDATATERMS, sbAuthorityDataTerms.toString()));
        }

        // Nested group entities
        if (!groupEntity.getChildren().isEmpty()) {
            for (GroupEntity childGroupEntity : groupEntity.getChildren()) {
                logger.debug("Processing child config: {}", childGroupEntity.getName());
                List<Element> eleChildList = JDomXP.evaluateToElementsStatic(childGroupEntity.getXpath(), ele);
                if (eleChildList == null) {
                    continue;
                }
                for (Element eleChild : eleChildList) {
                    GroupedMetadata child =
                            getGroupedMetadata(eleChild, childGroupEntity, configurationItem, childGroupEntity.getName(), sbDefaultMetadataValues,
                                    luceneFields);
                    ret.getChildren().add(child);
                    logger.debug("added child: {}", child.getMainValue());
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
        // 2021-10-22: Commented out concatenation to reduce autosuggest noise
        //        String concatValue = getConcatenatedValue(fieldValueTrim);
        //        if (!concatValue.equals(value)) {
        //            String concatValueWithSpaces = new StringBuilder(" ").append(concatValue).append(' ').toString();
        //            if (!sbDefaultMetadataValues.toString().contains(concatValueWithSpaces)) {
        //                sbDefaultMetadataValues.append(concatValueWithSpaces);
        //            }
        //        }
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
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
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
