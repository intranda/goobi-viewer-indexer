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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.jdom2.Attribute;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.filter.Filters;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.normdataimporter.NormDataImporter;
import de.intranda.digiverso.normdataimporter.model.NormData;
import de.intranda.digiverso.normdataimporter.model.NormDataValue;
import de.intranda.digiverso.presentation.solr.helper.language.LanguageHelper;
import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;
import de.intranda.digiverso.presentation.solr.model.GroupedMetadata;
import de.intranda.digiverso.presentation.solr.model.IndexObject;
import de.intranda.digiverso.presentation.solr.model.LuceneField;
import de.intranda.digiverso.presentation.solr.model.NonSortConfiguration;
import de.intranda.digiverso.presentation.solr.model.SolrConstants;
import de.intranda.digiverso.presentation.solr.model.config.FieldConfig;
import de.intranda.digiverso.presentation.solr.model.config.XPathConfig;

public class MetadataHelper {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(MetadataHelper.class);

    private static final String DEFAULT_MULTIVALUE_SEPARATOR = " ; ";
    private static final String XPATH_ROOT_PLACEHOLDER = "{{{ROOT}}}";

    private static String multiValueSeparator = DEFAULT_MULTIVALUE_SEPARATOR;

    public static final ThreadLocal<DecimalFormat> FORMAT_TWO_DIGITS = new ThreadLocal<DecimalFormat>() {

        @Override
        protected DecimalFormat initialValue() {
            return new DecimalFormat("00");
        }
    };
    public static final ThreadLocal<DecimalFormat> FORMAT_FOUR_DIGITS = new ThreadLocal<DecimalFormat>() {

        @Override
        protected DecimalFormat initialValue() {
            return new DecimalFormat("0000");
        }
    };
    public static final ThreadLocal<DecimalFormat> FORMAT_EIGHT_DIGITS = new ThreadLocal<DecimalFormat>() {

        @Override
        protected DecimalFormat initialValue() {
            return new DecimalFormat("00000000");
        }
    };

    public static DateTimeFormatter formatterISO8601Full = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
    public static DateTimeFormatter formatterISO8601Date = ISODateTimeFormat.date(); // yyyy-MM-dd
    public static DateTimeFormatter formatterISO8601YearMonth = DateTimeFormat.forPattern("yyyy-MM");
    public static DateTimeFormatter formatterDEDate = DateTimeFormat.forPattern("dd.MM.yyyy");
    public static DateTimeFormatter formatterUSDate = DateTimeFormat.forPattern("MM/dd/yyyy");
    public static DateTimeFormatter formatterCNDate = DateTimeFormat.forPattern("yyyy.MM.dd");
    public static DateTimeFormatter formatterJPDate = DateTimeFormat.forPattern("yyyy/MM/dd");;
    public static DateTimeFormatter formatterBasicDateTime = DateTimeFormat.forPattern("yyyyMMddHHmmss");
    public static DateTimeFormatter formatterISO8601DateTimeFullWithTimeZone = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");

    public static List<String> addNormDataFieldsToDefault;

    /**
     * Retrieves configured metadata fields from the given XML node. Written for LIDO events, but should theoretically work for any METS or LIDO node.
     * 
     * @param element
     * @param queryPrefix
     * @param indexObj The IndexObject is needed to alter its DEFAULT field value.
     * @return
     * @throws FatalIndexerException
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
                    // Cut off "displayForm" if the field is to be aggregated
                    if (configurationItem.isGroupEntity() && xpath.endsWith("/mods:displayForm")) {
                        xpath = xpath.substring(0, xpath.length() - 17);
                        logger.debug("new xpath: {}", xpath);
                    }
                    if (xpath.contains(XPATH_ROOT_PLACEHOLDER)) {
                        // Replace the placeholder with the prefix (e.g. in expresions using concat())
                        query = xpath.replace(XPATH_ROOT_PLACEHOLDER, queryPrefix);
                    } else {
                        // User prefix as prefix
                        query = queryPrefix + xpath;
                    }
                    if (xpath.startsWith("teiHeader"))
                        logger.info("XPath: {}", query);
                    for (Element currentElement : elementsToIterateOver) {
                        List list = xp.evaluate(query, currentElement);
                        if (list != null) {
                            if (xpath.startsWith("teiHeader"))
                                logger.info("found: {}", list.size());
                            for (Object xpathAnswerObject : list) {
                                if (configurationItem.isGroupEntity()) {
                                    // Aggregated / grouped metadata
                                    Element eleMods = (Element) xpathAnswerObject;
                                    GroupedMetadata gmd =
                                            getGroupedMetadata(eleMods, configurationItem.getGroupEntityFields(), configurationItem.getFieldname());
                                    List<LuceneField> normData = new ArrayList<>();
                                    boolean groupFieldAlreadyReplaced = false;
                                    String normIdentifier = null;
                                    StringBuilder sbNormDataTerms = new StringBuilder();

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

                                    // Retrieve normdata
                                    if (gmd.getNormUri() != null) {
                                        normData.addAll(retrieveNormData(sbDefaultMetadataValues, sbNormDataTerms, gmd.getNormUri(),
                                                addNormDataFieldsToDefault, configurationItem.getReplaceRules()));
                                        // Add default norm data name to the docstruct doc so that it can be searched
                                        for (LuceneField normField : normData) {
                                            switch (normField.getField()) {
                                                case "NORM_NAME":
                                                    // Add NORM_NAME as MD_*_UNTOKENIZED and to DEFAULT to the docstruct
                                                    if (StringUtils.isNotBlank(normField.getValue())) {
                                                        // fieldValues.add(normField.getValue());
                                                        if (configurationItem.isAddToDefault()) {
                                                            // Add norm value to DEFAULT
                                                            addValueToDefault(normField.getValue(), sbDefaultMetadataValues);
                                                        }
                                                        if (configurationItem.isAddUntokenizedVersion() || fieldName.startsWith("MD_")) {
                                                            ret.add(new LuceneField(
                                                                    new StringBuilder(fieldName).append(SolrConstants._UNTOKENIZED).toString(),
                                                                    normField.getValue()));
                                                        }
                                                    }
                                                    break;
                                                case "NORM_IDENTIFIER":
                                                    // If a NORM_IDENTIFIER exists for this metadata group, use it to replace the value of GROUPFIELD
                                                    for (LuceneField groupField : gmd.getFields()) {
                                                        if (groupField.getField().equals(SolrConstants.GROUPFIELD)) {
                                                            groupField.setValue(normField.getValue());
                                                            groupFieldAlreadyReplaced = true;
                                                            break;
                                                        }
                                                        normIdentifier = normField.getValue();
                                                    }
                                                    break;
                                                default: // nothing
                                            }
                                        }
                                    }
                                    for (LuceneField field : gmd.getFields()) {
                                        // Apply modifications configured for the main field to all the group field values
                                        String moddedValue = applyAllModifications(configurationItem, field.getValue());
                                        field.setValue(moddedValue);

                                        if (configurationItem.isAddToDefault()) {
                                            // Add main value to DEFAULT
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
                                    if (!groupFieldAlreadyReplaced && StringUtils.isNotEmpty(normIdentifier)) {
                                        gmd.getFields().add(new LuceneField(SolrConstants.GROUPFIELD, normIdentifier));
                                    }
                                    // Add MD_VALUE as DEFAULT to the metadata doc
                                    if (configurationItem.isAddToDefault() && StringUtils.isNotBlank(fieldValue)) {
                                        gmd.getFields().add(new LuceneField(SolrConstants.DEFAULT, fieldValue));
                                    }
                                    gmd.getFields().addAll(normData); // Add norm data outside the loop over groupMetadata

                                    if (!indexObj.getGroupedMetadataFields().contains(gmd)) {
                                        indexObj.getGroupedMetadataFields().add(gmd);
                                    }
                                    // NORMDATATERMS is now in the metadata docs, not docstructs
                                    if (sbNormDataTerms.length() > 0) {
                                        gmd.getFields().add(new LuceneField(SolrConstants.NORMDATATERMS, sbNormDataTerms.toString()));
                                    }
                                } else {
                                    // Regular metadata
                                    String xpathAnswerString = JDomXP.objectToString(xpathAnswerObject);
                                    if (StringUtils.isNotBlank(xpathAnswerString)) {
                                        xpathAnswerString = xpathAnswerString.trim();
                                        xpathAnswerString =
                                                new StringBuilder(xpathAnswerString).append(configurationItem.getValuepostfix()).toString();
                                        fieldValue = xpathAnswerString.trim();
                                        if (configurationItem.isOneToken()) {
                                            fieldValue = toOneToken(fieldValue, configurationItem.getSplittingCharacter());
                                        }
                                        if (fieldName.startsWith("DATE_")) {
                                            if ((fieldValue = convertDateStringForSolrField(fieldValue)) == null) {
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
                                                sb.append(multiValueSeparator);
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
                }

                for (String fieldValue : fieldValues) {
                    // CURRENTNOSORT must be along  value
                    if (fieldName.equals(SolrConstants.CURRENTNOSORT)) {
                        try {
                            fieldValue = String.valueOf(Integer.valueOf(fieldValue));
                        } catch (NumberFormatException e) {
                            logger.error("{} cannot be written because it is not a integer value: {}", SolrConstants.CURRENTNOSORT, fieldValue);
                            continue;
                        }
                    }
                    // Apply string modifications configured for this field
                    fieldValue = applyAllModifications(configurationItem, fieldValue);
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
                                    configurationItem.getNonSortConfigurations(), ret);
                        }
                    }
                    // Add Solr field
                    LuceneField luceneField = new LuceneField(fieldName, fieldValue);
                    ret.add(luceneField);

                    // Make sure non-sort characters are removed before adding _UNTOKENIZED and SORT_ fields
                    if (configurationItem.isAddSortField()) {
                        addSortField(configurationItem.getFieldname(), fieldValue, SolrConstants.SORT_, configurationItem.getNonSortConfigurations(),
                                ret);
                    }
                    if (configurationItem.isAddUntokenizedVersion() || fieldName.startsWith("MD_")) {
                        ret.add(new LuceneField(new StringBuilder(fieldName).append(SolrConstants._UNTOKENIZED).toString(), fieldValue));
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
        if (sbDefaultMetadataValues.length() > 0)

        {
            indexObj.setDefaultValue(sbDefaultMetadataValues.toString());
        }
        ret.addAll(completeCenturies(ret));

        return ret;

    }

    /**
     * 
     * @param sbDefaultMetadataValues
     * @param sbNormDataTerms
     * @param url
     * @param addToDefaultFields
     * @param replaceRulesaddToDefaultfields
     * @param url URL to query.
     * @return
     */
    private static List<LuceneField> retrieveNormData(StringBuilder sbDefaultMetadataValues, StringBuilder sbNormDataTerms, String url,
            List<String> addToDefaultFields, Map<Object, String> replaceRules) {
        if (sbDefaultMetadataValues == null) {
            throw new IllegalArgumentException("sbDefaultMetadataValues may not be null");
        }
        if (sbNormDataTerms == null) {
            throw new IllegalArgumentException("sbNormDataTerms may not be null");
        }
        if (url == null) {
            throw new IllegalArgumentException("url may not be null");
        }
        List<LuceneField> ret = new ArrayList<>();

        // If it's just an identifier, assume it's GND
        if (!url.startsWith("http")) {
            url = "http://d-nb.info/gnd/" + url;
        }
        Map<String, List<NormData>> normDataListMap = NormDataImporter.importNormData(url);
        for (String key : normDataListMap.keySet()) {
            if (normDataListMap.get(key) == null) {
                logger.warn("No normdata set found.");
                continue;
            }
            for (NormData normData : normDataListMap.get(key)) {
                if (normData.getKey().startsWith("NORM_")) {
                    // IKFN norm data browsing hack
                    for (NormDataValue val : normData.getValues()) {
                        if (StringUtils.isNotBlank(val.getText()) && !normData.getKey().equals("NORM_STATICPAGE")) {
                            String textValue = val.getText();
                            if (replaceRules != null) {
                                textValue = applyReplaceRules(textValue, replaceRules);
                            }
                            // Remove 'start of string' and 'string terminator' chars to prevent values like "Albach, Julius ˜vonœ"
                            //                            if (StringUtils.contains(textValue, '\u0098') && StringUtils.contains(textValue, '\u009C')) {
                            //                                StringUtils.replaceChars(textValue, '\u0098', '\u0000');
                            //                                StringUtils.replaceChars(textValue, '\u009C', '\u0000');
                            //                                logger.info("Cleaned up '{}'", textValue);
                            //                            }

                            ret.add(new LuceneField(normData.getKey(), textValue));
                            ret.add(new LuceneField(normData.getKey() + SolrConstants._UNTOKENIZED, textValue));

                            String valWithSpaces = new StringBuilder(" ").append(textValue).append(' ').toString();

                            // Add to DEFAULT
                            if (addToDefaultFields != null && !addToDefaultFields.isEmpty() && addToDefaultFields.contains(normData.getKey())
                                    && !sbDefaultMetadataValues.toString().contains(valWithSpaces)) {
                                addValueToDefault(textValue, sbDefaultMetadataValues);
                                logger.trace("Added to DEFAULT: {}", textValue);
                            }

                            // Add to the norm data search field NORMDATATERMS
                            if (!normData.getKey().startsWith("NORM_URI") && !sbNormDataTerms.toString().contains(valWithSpaces)) {
                                sbNormDataTerms.append(valWithSpaces);
                                logger.trace("Added to NORMDATATERMS: {}", textValue);
                            }

                            // Add aggregated Aggregate place fields into the same untokenized field for term browsing
                            if (normData.getKey().equals("NORM_ALTNAME")) {
                                ret.add(new LuceneField("NORM_NAME_SEARCH", textValue));
                                ret.add(new LuceneField("NORM_NAME" + SolrConstants._UNTOKENIZED, textValue));
                            } else if (normData.getKey().startsWith("NORM_PLACE")) {
                                ret.add(new LuceneField("NORM_PLACE_SEARCH", textValue));
                                ret.add(new LuceneField("NORM_PLACE" + SolrConstants._UNTOKENIZED, textValue));
                            } else if (normData.getKey().equals("NORM_LIFEPERIOD")) {
                                String[] valueSplit = textValue.split("-");
                                if (valueSplit.length > 0) {
                                    for (String date : valueSplit) {
                                        ret.add(new LuceneField("NORM_DATE_SEARCH", date.trim()));
                                        ret.add(new LuceneField("NORM_DATE" + SolrConstants._UNTOKENIZED, date.trim()));
                                    }
                                }
                            }
                        }
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
     * @param queryPrefix
     * @throws FatalIndexerException
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
                    logger.debug("Duplicate field found: {}:{}", field.getField(), indexObj.getLuceneFieldWithName(field.getField()).getValue());
                    continue;
                }
            }
            if (field.getField().equals(SolrConstants.ACCESSCONDITION)) {
                // Add access conditions to a separate list
                indexObj.getAccessConditions().add(field.getValue());
            } else {
                indexObj.addToLucene(field);
            }
            // Extract language code from the field name and add it to the topstruct indexObj
            //            if (field.getField().startsWith("MD_TEXT_")) {
            //                String language = extractLanguageCodeFromMetadataField(field.getField());
            //                if (StringUtils.isNotEmpty(language)) {
            //                    IndexObject obj = indexObj;
            //                    while (obj.getParent() != null && !obj.getParent().isAnchor()) {
            //                        obj = obj.getParent();
            //                    }
            //                    obj.getLanguages().add(language);
            //                }
            //            }

            // logger.debug("METADATA " + fieldName + " : " + field.getValue());

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
                if (nonSortConfig.getPrefix() != null) {
                    fieldValue = fieldValue.replaceAll(nonSortConfig.getPrefix(), "");
                }
                if (nonSortConfig.getSuffix() != null) {
                    fieldValue = fieldValue.replaceAll(nonSortConfig.getSuffix(), "");
                }
            }
        }

        return fieldValue;
    }

    /**
     * 
     * @param value
     * @param replaceRules
     * @return
     * @should apply rules correctly
     * @should throw IllegalArgumentException if value is null
     * @should throw IllegalArgumentException if replaceRules is null
     */
    public static String applyReplaceRules(String value, Map<Object, String> replaceRules) {
        if (value == null) {
            throw new IllegalArgumentException("value may not be null");
        }
        if (replaceRules == null) {
            throw new IllegalArgumentException("replaceRules may not be null");
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
     * 
     * @param fieldValue
     * @return
     */
    public static String applyValueDefaultModifications(String fieldValue) {
        String ret = fieldValue;
        if (StringUtils.isNotEmpty(ret)) {
            // Remove any prior HTML escaping, otherwise strings like '&amp;amp;' might occur
            ret = StringEscapeUtils.unescapeHtml(ret);
        }

        return ret;
    }

    /**
     * 
     * @param pi
     * @return
     * @throws FatalIndexerException
     * @should trim identifier
     * @should apply replace rules
     * @should replace spaces with underscores
     * @should replace commas with underscores
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
     * @param fieldName
     * @param fieldValue
     * @param numerical If true, SORTNUM_ will be used instead of SORT_
     * @param nonSortConfigurations
     * @param retList
     * @should add regular sort fields correctly
     * @should add numerical sort fields correctly
     * @should not add existing fields
     */
    static void addSortField(String fieldName, String fieldValue, String sortFieldPrefix, List<NonSortConfiguration> nonSortConfigurations,
            List<LuceneField> retList) {
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
                StringBuilder regex = new StringBuilder();
                if (nonSortConfig.getPrefix() != null) {
                    regex.append(nonSortConfig.getPrefix());
                }
                regex.append("[\\w|\\W]*");
                if (nonSortConfig.getSuffix() != null) {
                    regex.append(nonSortConfig.getSuffix());
                }
                fieldValue = fieldValue.replaceAll(regex.toString(), "").trim();
                // logger.info("Non-sort regex: {}", regex);
                // logger.info("fieldValue: {}", fieldValue);
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
     * @param prefix
     * @param xp
     * @return String or null
     * @throws FatalIndexerException
     */
    @SuppressWarnings({ "unchecked" })
    public static String getPIFromXML(String prefix, JDomXP xp) throws FatalIndexerException {
        List<Map<String, Object>> piConfig = Configuration.getInstance().getFieldConfiguration().get(SolrConstants.PI);
        if (piConfig != null) {
            List<XPathConfig> xPathConfigurations = (ArrayList<XPathConfig>) piConfig.get(0).get("xpath");
            for (XPathConfig xPathConfig : xPathConfigurations) {
                String query = prefix + xPathConfig.getxPath();
                List<String> list = new ArrayList<>();
                List<Object> oList = xp.evaluate(query, null);
                if ((oList != null) && (!oList.isEmpty())) {
                    for (Object object : oList) {
                        list.add(JDomXP.objectToString(object));
                    }
                    if (!list.isEmpty() && StringUtils.isNotBlank(list.get(0))) {
                        return list.get(0);
                    }
                }
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
     */
    private static List<LuceneField> parseDatesAndCenturies(Set<Integer> centuries, String value, int normalizeYearMinDigits) {
        List<LuceneField> ret = new ArrayList<>();

        if (StringUtils.isNotEmpty(value)) {
            for (PrimitiveDate date : normalizeDate(value, normalizeYearMinDigits)) {
                if (date.getYear() != null) {
                    ret.add(new LuceneField(SolrConstants.YEAR, String.valueOf(date.getYear())));
                    int century = getCentury(date.getYear());
                    if (!centuries.contains(century)) {
                        ret.add(new LuceneField(SolrConstants.CENTURY, String.valueOf(century)));
                        centuries.add(century);
                    }
                    if (date.getMonth() != null) {
                        ret.add(new LuceneField(SolrConstants.YEARMONTH, date.getYear() + FORMAT_TWO_DIGITS.get().format(date.getMonth())));
                        if (date.getDay() != null) {
                            ret.add(new LuceneField(SolrConstants.YEARMONTHDAY, date.getYear() + FORMAT_TWO_DIGITS.get().format(date.getMonth())
                                    + FORMAT_TWO_DIGITS.get().format(date.getDay())));
                        }
                    }
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
    protected static List<PrimitiveDate> normalizeDate(String dateString, int normalizeYearMinDigits) {
        if (normalizeYearMinDigits < 1) {
            throw new IllegalArgumentException("normalizeYearMinDigits must be at least 1");
        }

        List<PrimitiveDate> ret = new ArrayList<>();

        // Try known date formats first
        try {
            Date date = formatterDEDate.parseDateTime(dateString).toDate();
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (IllegalArgumentException e) {
        }
        try {
            Date date = formatterISO8601Date.parseDateTime(dateString).toDate();
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (IllegalArgumentException e) {
        }
        try {
            Date date = formatterISO8601YearMonth.parseDateTime(dateString).toDate();
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (IllegalArgumentException e) {
        }
        try {
            Date date = formatterUSDate.parseDateTime(dateString).toDate();
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (IllegalArgumentException e) {
        }
        try {
            Date date = formatterCNDate.parseDateTime(dateString).toDate();
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (IllegalArgumentException e) {
        }
        try {
            Date date = formatterJPDate.parseDateTime(dateString).toDate();
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (IllegalArgumentException e) {
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
    protected static int getCentury(long year) {
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
    protected static List<LuceneField> completeCenturies(List<LuceneField> fields) {
        return completeIntegerValues(fields, SolrConstants.CENTURY, new HashSet<>(Collections.singletonList(0)));
    }

    /**
     * Adds additional YEAR fields if there is a gap (e.g. if there is 1990 and 1993, also add 1991 and 1992).
     * 
     * @param fields
     * @return
     * @should complete years correctly
     */
    protected static List<LuceneField> completeYears(List<LuceneField> fields) {
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
                for (int i = oldValues.get(0); i < oldValues.get(oldValues.size() - 1); ++i) {
                    if (!oldValues.contains(i)) {
                        if (skipValues != null && skipValues.contains(i)) {
                            continue;
                        }
                        newValues.add(new LuceneField(fieldName, String.valueOf(i)));
                        logger.debug("Added implicit {}: {}", fieldName, i);
                    }
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
     * @return
     * @throws FatalIndexerException
     * @should group correctly
     */
    @SuppressWarnings("unchecked")
    protected static GroupedMetadata getGroupedMetadata(Element ele, MultiMap groupEntityFields, String groupLabel) throws FatalIndexerException {
        logger.trace("getGroupedMetadata: {}", groupLabel);
        GroupedMetadata ret = new GroupedMetadata();
        ret.setLabel(groupLabel);

        String type = null;
        ret.getFields().add(new LuceneField(SolrConstants.LABEL, groupLabel));
        if (groupEntityFields.get("type") != null) {
            List<String> values = (List<String>) groupEntityFields.get("type");
            if (values != null && !values.isEmpty()) {
                type = values.get(0).trim();
                ret.getFields().add(new LuceneField(SolrConstants.METADATATYPE, type));
            }
        }
        boolean normUriFound = false;
        Map<String, String> collectedValues = new HashMap<>();
        for (Object field : groupEntityFields.keySet()) {
            if ("type".equals(field)) {
                continue;
            }
            List<String> xpathList = (List<String>) groupEntityFields.get(field);
            if (xpathList != null) {
                for (String xpath : xpathList) {
                    //                    logger.info("XPath: {}", xpath);
                    List<Object> values = JDomXP.evaluate(xpath, ele, Filters.fpassthrough());
                    if (values != null && !values.isEmpty()) {
                        String fieldName = (String) field;
                        String fieldValue = JDomXP.objectToString(values.get(0));
                        //                        logger.info("found: {}:{}", fieldName, fieldValue);
                        if (fieldValue != null) {
                            fieldValue = fieldValue.trim();
                        }
                        ret.getFields().add(new LuceneField(fieldName, fieldValue));
                        collectedValues.put(fieldName, fieldValue);

                        if (NormDataImporter.FIELD_URI.equals(field)) {
                            normUriFound = true;
                            ret.setNormUri(fieldValue);
                        }
                    }
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
                    if (collectedValues.containsKey("MD_LASTNAME")) {
                        sbValue.append(collectedValues.get("MD_LASTNAME"));
                    }
                    if (collectedValues.containsKey("MD_FIRSTNAME")) {
                        if (sbValue.length() > 0) {
                            sbValue.append(", ");
                        }
                        sbValue.append(collectedValues.get("MD_FIRSTNAME"));
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
            addSortField(SolrConstants.GROUPFIELD, new StringBuilder(groupLabel).append("_").append(mdValue).toString(), "", null, ret.getFields());
        }

        // Add norm data URI, if available (GND only)
        if (!normUriFound) {
            String authority = ele.getAttributeValue("authority");
            if (authority != null) {
                String authorityURI = ele.getAttributeValue("authorityURI");
                String valueURI = ele.getAttributeValue("valueURI");
                switch (authority) {
                    case "gnd":
                        // Skip missing GND identifiers
                        if ("http://d-nb.info/gnd/".equals(valueURI)) {
                            break;
                        }
                    case "refgeo":
                    case "refbio":
                        if (StringUtils.isNotEmpty(valueURI)) {
                            valueURI = valueURI.trim();
                            if (StringUtils.isNotEmpty(authorityURI) && !valueURI.startsWith(authorityURI)) {
                                ret.getFields().add(new LuceneField(NormDataImporter.FIELD_URI, authorityURI + valueURI));
                                ret.getFields().add(new LuceneField("NORM_IDENTIFIER", valueURI));
                            } else {
                                ret.getFields().add(new LuceneField(NormDataImporter.FIELD_URI, valueURI));
                            }
                            ret.setNormUri(valueURI);
                        }
                        break;
                    default: // nothing
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
     * Date class that can (but is not required to) contain year, month and day values.
     */
    protected static class PrimitiveDate {

        private Integer year;
        private Integer month;
        private Integer day;

        /** Individual values contructor. */
        public PrimitiveDate(Integer year, Integer month, Integer day) {
            this.year = year;
            this.month = month;
            this.day = day;
        }

        /**
         * Date constructor.
         * 
         * @param date
         */
        public PrimitiveDate(Date date) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            this.year = cal.get(Calendar.YEAR);
            this.month = cal.get(Calendar.MONTH) + 1;
            this.day = cal.get(Calendar.DAY_OF_MONTH);
        }

        /**
         * @return the year
         */
        public Integer getYear() {
            return year;
        }

        /**
         * @return the month
         */
        public Integer getMonth() {
            return month;
        }

        /**
         * @return the day
         */
        public Integer getDay() {
            return day;
        }
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
    protected static void addValueToDefault(String value, StringBuilder sbDefaultMetadataValues) {
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
     * @return
     * @should convert date correctly
     */
    static String convertDateStringForSolrField(String value) {
        List<PrimitiveDate> dates = normalizeDate(value, 4);
        if (!dates.isEmpty()) {
            PrimitiveDate date = dates.get(0);
            if (date.getYear() != null) {
                MutableDateTime mdt = new MutableDateTime(date.getYear(), date.getMonth() != null ? date.getMonth() : 1,
                        date.getDay() != null ? date.getDay() : 1, 0, 0, 0, 0);
                return formatterISO8601DateTimeFullWithTimeZone.print(mdt);
            }
        }

        logger.warn("Could not parse date from value: {}", value);
        return null;
    }

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
     * @param fieldName
     * @return
     * @should extract language code correctly
     * @should ignore any suffixes longer than two chars
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
     * @param indexObj
     * @param teiFolder
     * @throws FatalIndexerException
     * @throws IOException
     * @throws JDOMException
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
                    String body = TextHelper.getStringFromElement(eleNewRoot, null).replace("<tempRoot>", "").replace("</tempRoot>", "").trim();
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
