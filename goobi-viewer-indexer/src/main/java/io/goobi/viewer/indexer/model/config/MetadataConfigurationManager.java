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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.model.FatalIndexerException;
import io.goobi.viewer.indexer.model.config.ValueNormalizer.ValueNormalizerPosition;

/**
 * Object holding field configurations.
 */
public final class MetadataConfigurationManager {

    private static final Logger logger = LoggerFactory.getLogger(MetadataConfigurationManager.class);

    private static final String FALSE = "false";
    private static final String SPACE_PLACEHOLDER = "#SPACE#";

    private Map<String, List<FieldConfig>> configurationList = new HashMap<>();
    private Set<String> fieldsToAddToParents = new HashSet<>();
    private Set<String> fieldsToAddToChildren = new HashSet<>();
    private Set<String> fieldsToAddToPages = new HashSet<>();

    /**
     * <p>
     * Constructor for MetadataConfigurationManager.
     * </p>
     *
     * @param config a {@link org.apache.commons.configuration.XMLConfiguration} object.
     */
    public MetadataConfigurationManager(XMLConfiguration config) {
        // Regular fields
        Map<String, List<Map<String, Object>>> fieldMap = loadFieldConfiguration(config);
        // For each field
        for (String fieldName : fieldMap.keySet()) {
            // Load configurations
            List<Map<String, Object>> fieldConfigurationList = fieldMap.get(fieldName);
            for (Map<String, Object> configurationMap : fieldConfigurationList) {
                // Create ConfigurationItem for each field configuration
                FieldConfig configurationItem = generateConfigurationItem(fieldName, configurationMap);
                if (!configurationList.containsKey(fieldName)) {
                    configurationList.put(fieldName, new ArrayList<FieldConfig>());
                }
                configurationList.get(fieldName).add(configurationItem);
            }
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private static Map<String, List<Map<String, Object>>> loadFieldConfiguration(XMLConfiguration config) {
        Map<String, List<Map<String, Object>>> ret = new HashMap<>();
        Iterator<String> fields = config.getKeys("fields");
        List<String> newFields = new ArrayList<>();
        // each field only once
        while (fields.hasNext()) {
            String fieldname = fields.next();
            fieldname = fieldname.substring(7);
            fieldname = fieldname.substring(0, fieldname.indexOf('.'));
            if (!newFields.contains(fieldname)) {
                newFields.add(fieldname);
            }
        }
        for (String fieldname : newFields) {
            int count = config.getMaxIndex("fields." + fieldname + ".list.item");
            List<Map<String, Object>> fieldInformation = new ArrayList<>();
            for (int i = 0; i <= count; i++) {
                Map<String, Object> fieldValues = new HashMap<>();
                List<XPathConfig> xPathConfigurations = new ArrayList<>();

                int items = config.getMaxIndex("fields." + fieldname + ".list.item(" + i + ").xpath.list.item");
                if (items > -1) {
                    // Multiple XPath items
                    for (int j = 0; j <= items; j++) {
                        SubnodeConfiguration xpathNode =
                                config.configurationAt("fields." + fieldname + ".list.item(" + i + ").xpath.list.item(" + j + ")");
                        String xpath = xpathNode.getString(".");
                        if (StringUtils.isEmpty(xpath)) {
                            logger.error("Found empty XPath configuration for field: {}", fieldname);
                            continue;
                        }
                        String prefix = xpathNode.getString("[@prefix]");
                        String suffix = xpathNode.getString("[@suffix]");
                        xPathConfigurations.add(new XPathConfig(xpath, prefix, suffix));
                    }
                } else if (config.getMaxIndex("fields." + fieldname + ".list.item(" + i + ").xpath") > -1) {
                    // Single XPath item
                    SubnodeConfiguration xpathNode = config.configurationAt("fields." + fieldname + ".list.item(" + i + ").xpath");
                    String xpath = xpathNode.getString(".");
                    if (StringUtils.isEmpty(xpath)) {
                        logger.error("Found empty XPath configuration for field: {}", fieldname);
                    } else {
                        String prefix = xpathNode.getString("[@prefix]");
                        String suffix = xpathNode.getString("[@suffix]");
                        xPathConfigurations.add(new XPathConfig(xpath, prefix, suffix));
                    }
                }

                fieldValues.put("xpath", xPathConfigurations);
                fieldValues.put("getparents", config.getString("fields." + fieldname + ".list.item(" + i + ").getparents"));
                fieldValues.put("getchildren", config.getString("fields." + fieldname + ".list.item(" + i + ").getchildren"));
                fieldValues.put("onetoken", config.getString("fields." + fieldname + ".list.item(" + i + ").onetoken"));
                fieldValues.put("onefield", config.getString("fields." + fieldname + ".list.item(" + i + ").onefield"));
                fieldValues.put("onefieldSeparator", config.getString("fields." + fieldname + ".list.item(" + i + ").onefield[@separator]"));
                fieldValues.put("constantValue", config.getString("fields." + fieldname + ".list.item(" + i + ").constantValue"));
                fieldValues.put("splittingCharacter", config.getString("fields." + fieldname + ".list.item(" + i + ").splittingCharacter"));
                fieldValues.put("getnode", config.getString("fields." + fieldname + ".list.item(" + i + ").getnode"));
                fieldValues.put("addToDefault", config.getString("fields." + fieldname + ".list.item(" + i + ").addToDefault"));
                fieldValues.put("addUntokenizedVersion", config.getString("fields." + fieldname + ".list.item(" + i + ").addUntokenizedVersion"));
                fieldValues.put("lowercase", config.getString("fields." + fieldname + ".list.item(" + i + ").lowercase"));
                fieldValues.put("addSortField", config.getString("fields." + fieldname + ".list.item(" + i + ").addSortField"));
                fieldValues.put("addSortFieldToTopstruct", config.getString("fields." + fieldname + ".list.item(" + i + ").addSortFieldToTopstruct"));
                fieldValues.put("addToParents", config.getString("fields." + fieldname + ".list.item(" + i + ").addToParents"));
                fieldValues.put("addToChildren", config.getString("fields." + fieldname + ".list.item(" + i + ").addToChildren"));
                fieldValues.put("addToPages", config.getString("fields." + fieldname + ".list.item(" + i + ").addToPages"));
                fieldValues.put("allowDuplicateValues", config.getString("fields." + fieldname + ".list.item(" + i + ").allowDuplicateValues"));
                fieldValues.put("geoJSONSource", config.getString("fields." + fieldname + ".list.item(" + i + ").geoJSONSource"));
                fieldValues.put("geoJSONSourceSeparator",
                        config.getString("fields." + fieldname + ".list.item(" + i + ").geoJSONSource[@separator]"));
                fieldValues.put("geoJSONAddSearchField",
                        config.getString("fields." + fieldname + ".list.item(" + i + ").geoJSONSource[@addSearchField]"));

                {
                    // Normalize and interpolate years
                    List eleNormalizeYearList = config.configurationsAt("fields." + fieldname + ".list.item(" + i + ").normalizeYear");
                    if (eleNormalizeYearList != null && !eleNormalizeYearList.isEmpty()) {
                        SubnodeConfiguration eleNormalizeYear = (SubnodeConfiguration) eleNormalizeYearList.get(0);
                        fieldValues.put("normalizeYear", eleNormalizeYear.getString(""));
                        fieldValues.put("normalizeYearMinDigits", eleNormalizeYear.getInt("[@minYearDigits]", 3));
                        fieldValues.put("interpolateYears", config.getString("fields." + fieldname + ".list.item(" + i + ").interpolateYears"));
                    }
                }

                // Grouped entity config
                {
                    Map<String, Object> groupedSubfieldConfigurations = new HashMap<>();
                    String type = config.getString("fields." + fieldname + ".list.item(" + i + ").groupEntity[@type]");
                    if (type != null) {
                        groupedSubfieldConfigurations.put("type", type);
                    }
                    String url = config.getString("fields." + fieldname + ".list.item(" + i + ").groupEntity[@url]");
                    if (url != null) {
                        groupedSubfieldConfigurations.put("url", url);
                    }
                    Boolean addAuthorityDataToDocstruct =
                            config.getBoolean("fields." + fieldname + ".list.item(" + i + ").groupEntity[@addAuthorityDataToDocstruct]", null);
                    if (addAuthorityDataToDocstruct != null) {
                        groupedSubfieldConfigurations.put("addAuthorityDataToDocstruct", addAuthorityDataToDocstruct);
                    }
                    List elements = config.configurationsAt("fields." + fieldname + ".list.item(" + i + ").groupEntity.field");
                    if (elements != null) {
                        for (Iterator it = elements.iterator(); it.hasNext();) {
                            HierarchicalConfiguration sub = (HierarchicalConfiguration) it.next();
                            String name = sub.getString("[@name]", null);
                            String defaultValue = sub.getString("[@defaultValue]", null);
                            boolean multivalued = sub.getBoolean("[@multivalued]", true);
                            String xpathExp = sub.getString("");
                            if (StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(xpathExp)) {
                                SubfieldConfig sfc = (SubfieldConfig) groupedSubfieldConfigurations.get(name);
                                if (sfc == null) {
                                    sfc = new SubfieldConfig(name, multivalued);
                                    groupedSubfieldConfigurations.put(name, sfc);
                                }
                                sfc.getXpaths().add(xpathExp);
                                sfc.getDefaultValues().put(xpathExp, defaultValue);
                                logger.debug("Loaded group entity field: {} - {}", name, xpathExp);
                            } else {
                                logger.warn("Found incomplete groupEntity configuration for field '{}', skipping...", fieldname);
                            }
                        }
                        fieldValues.put("groupEntity", groupedSubfieldConfigurations);
                    }
                }

                {
                    List<NonSortConfiguration> nonSortConfigurations = new ArrayList<>();
                    List elements = config.configurationsAt("fields." + fieldname + ".list.item(" + i + ").nonSortCharacters");
                    if (elements != null) {
                        for (Iterator it = elements.iterator(); it.hasNext();) {
                            HierarchicalConfiguration sub = (HierarchicalConfiguration) it.next();
                            String prefix = sub.getString("[@prefix]");
                            String suffix = sub.getString("[@suffix]");
                            nonSortConfigurations.add(new NonSortConfiguration(prefix, suffix));
                        }
                        fieldValues.put("nonSortConfigurations", nonSortConfigurations);
                    }
                }

                {
                    List elements = config.configurationsAt("fields." + fieldname + ".list.item(" + i + ").normalizeValue");
                    if (elements != null && !elements.isEmpty()) {
                        HierarchicalConfiguration sub = (HierarchicalConfiguration) elements.get(0);
                        int length = sub.getInt("[@length]");
                        char filler = sub.getString("[@filler]", "0").charAt(0);
                        String position = sub.getString("[@position]");
                        String relevantPartRegex = sub.getString("[@relevantPartRegex]");
                        ValueNormalizer normalizer =
                                new ValueNormalizer(length, filler, ValueNormalizerPosition.getByName(position), relevantPartRegex);
                        fieldValues.put("valueNormalizer", normalizer);
                    }
                }

                {
                    // A field can only be configured with chars or strings to be replaced, not both!
                    Map<Object, String> replaceRules = new LinkedHashMap<>();
                    List elements = config.configurationsAt("fields." + fieldname + ".list.item(" + i + ").replace");
                    if (elements != null) {
                        for (Iterator it = elements.iterator(); it.hasNext();) {
                            HierarchicalConfiguration sub = (HierarchicalConfiguration) it.next();
                            Character character = null;
                            try {
                                int charIndex = sub.getInt("[@char]");
                                character = (char) charIndex;
                            } catch (NoSuchElementException e) {
                            }
                            String string = null;
                            try {
                                string = sub.getString("[@string]");
                            } catch (NoSuchElementException e) {
                            }
                            String regex = null;
                            try {
                                regex = sub.getString("[@regex]");
                            } catch (NoSuchElementException e) {
                            }
                            String replaceWith = sub.getString("");
                            if (replaceWith == null) {
                                replaceWith = "";
                            }
                            replaceWith = replaceWith.replace(SPACE_PLACEHOLDER, " ");
                            if (character != null) {
                                replaceRules.put(character, replaceWith);
                            } else if (string != null) {
                                replaceRules.put(string.replace(SPACE_PLACEHOLDER, " "), replaceWith);
                            } else if (regex != null) {
                                replaceRules.put("REGEX:" + regex.replace(SPACE_PLACEHOLDER, " "), replaceWith);
                            }
                        }
                        fieldValues.put("replaceRules", replaceRules);
                    }
                }

                fieldInformation.add(fieldValues);
                ret.put(fieldname, fieldInformation);
            }
        }

        logger.info("{} field configurations loaded.", ret.size());
        return ret;
    }

    /**
     * Generates a ConfigurationItem instance for the given field name.
     * 
     * @param fieldName
     * @param configurationMap {@link HashMap}
     * @return {@link FieldConfig}
     * @should generate configuration item correctly
     */
    @SuppressWarnings("unchecked")
    FieldConfig generateConfigurationItem(String fieldName, Map<String, Object> configurationMap) {
        FieldConfig configurationItem = new FieldConfig(fieldName);
        Object[] keys = configurationMap.keySet().toArray();
        for (Object object : keys) {
            if (configurationMap.get(object) == null) {
                configurationMap.remove(object);
            }
        }

        if (configurationMap.containsKey("getnode")) {
            configurationItem.setNode((String) configurationMap.get("getnode"));
        }

        if (configurationMap.containsKey("getchildren")) {
            configurationItem.setChild((String) configurationMap.get("getchildren"));
        }

        if (configurationMap.containsKey("valuepostfix")) {
            configurationItem.setValuepostfix((String) configurationMap.get("valuepostfix"));
        }

        if (configurationMap.containsKey("constantValue")) {
            configurationItem.setConstantValue((String) configurationMap.get("constantValue"));
        }

        if (configurationMap.containsKey("getparents")) {
            configurationItem.setParents((String) configurationMap.get("getparents"));
        }

        if (configurationMap.containsKey("onetoken")) {
            String value = (String) configurationMap.get("onetoken");
            if (FALSE.equals(value)) {
                configurationItem.setOneToken(false);
            } else {
                configurationItem.setOneToken(true);
            }
        }

        if (configurationMap.containsKey("onefield")) {
            String value = (String) configurationMap.get("onefield");
            if (FALSE.equals(value)) {
                configurationItem.setOneField(false);
            } else {
                configurationItem.setOneField(true);
            }
        }

        if (configurationMap.containsKey("onefieldSeparator")) {
            configurationItem.setOneFieldSeparator(((String) configurationMap.get("onefieldSeparator")).replace(SPACE_PLACEHOLDER, " "));
        }

        if (configurationMap.containsKey("splittingCharacter")) {
            configurationItem.setSplittingCharacter((String) configurationMap.get("splittingCharacter"));
        }

        if (configurationMap.containsKey("addToDefault")) {
            if (((String) configurationMap.get("addToDefault")).equals(FALSE)) {
                configurationItem.setAddToDefault(false);
            } else {
                configurationItem.setAddToDefault(true);
            }
        }

        if (configurationMap.containsKey("addUntokenizedVersion")) {
            if (((String) configurationMap.get("addUntokenizedVersion")).equals(FALSE)) {
                configurationItem.setAddUntokenizedVersion(false);
            } else {
                configurationItem.setAddUntokenizedVersion(true);
            }
        }

        if (configurationMap.containsKey("lowercase")) {
            if (((String) configurationMap.get("lowercase")).equals(FALSE)) {
                configurationItem.setLowercase(false);
            } else {
                configurationItem.setLowercase(true);
            }
        }

        if (configurationMap.containsKey("xpath")) {
            Object xpathObject = configurationMap.get("xpath");
            if (xpathObject != null) {
                if (xpathObject instanceof ArrayList) {
                    // List of XPath expressions
                    List<XPathConfig> xPathConfigurations = (ArrayList<XPathConfig>) xpathObject;
                    configurationItem.setxPathConfigurations(xPathConfigurations);
                } else {
                    // Single XPath expression
                    configurationItem.setxPathConfigurations(new ArrayList<XPathConfig>());
                    configurationItem.getxPathConfigurations().add((XPathConfig) xpathObject);
                }
            }
        }

        if (configurationMap.containsKey("addSortField")) {
            if (((String) configurationMap.get("addSortField")).equals(FALSE)) {
                configurationItem.setAddSortField(false);
            } else {
                configurationItem.setAddSortField(true);
            }
        }

        if (configurationMap.containsKey("addSortFieldToTopstruct")) {
            if (((String) configurationMap.get("addSortFieldToTopstruct")).equals(FALSE)) {
                configurationItem.setAddSortFieldToTopstruct(false);
            } else {
                configurationItem.setAddSortFieldToTopstruct(true);
            }
        }

        if (configurationMap.containsKey("normalizeYear")) {
            if (((String) configurationMap.get("normalizeYear")).equals(FALSE)) {
                configurationItem.setNormalizeYear(false);
            } else {
                configurationItem.setNormalizeYear(true);
            }
            if (configurationMap.containsKey("normalizeYearMinDigits")) {
                configurationItem.setNormalizeYearMinDigits((int) configurationMap.get("normalizeYearMinDigits"));
            }
        }

        if (configurationMap.containsKey("interpolateYears")) {
            if (((String) configurationMap.get("interpolateYears")).equals(FALSE)) {
                configurationItem.setInterpolateYears(false);
            } else {
                configurationItem.setInterpolateYears(true);
            }
        }

        if (configurationMap.containsKey("groupEntity")) {
            configurationItem.setGroupEntityFields((Map<String, Object>) configurationMap.get("groupEntity"));
        }

        if (configurationMap.containsKey("replaceRules")) {
            configurationItem.setReplaceRules((Map<Object, String>) configurationMap.get("replaceRules"));
        }

        if (configurationMap.containsKey("nonSortConfigurations")) {
            configurationItem.setNonSortConfigurations((List<NonSortConfiguration>) configurationMap.get("nonSortConfigurations"));
        }

        if (configurationMap.containsKey("addToParents")) {
            if (((String) configurationMap.get("addToParents")).equals("true")) {
                fieldsToAddToParents.add(configurationItem.getFieldname());
            } else {
            }
        }
        if (configurationMap.containsKey("addToChildren")) {
            if (((String) configurationMap.get("addToChildren")).equals("true")) {
                fieldsToAddToChildren.add(configurationItem.getFieldname());
            } else {
            }
        }
        if (configurationMap.containsKey("addToPages")) {
            if (((String) configurationMap.get("addToPages")).equals("true")) {
                configurationItem.setAddToPages(true);
                fieldsToAddToPages.add(configurationItem.getFieldname());
            } else {
                configurationItem.setAddToPages(false);
            }
        }
        if (configurationMap.containsKey("allowDuplicateValues")) {
            if (((String) configurationMap.get("allowDuplicateValues")).equals("true")) {
                configurationItem.setAllowDuplicateValues(true);
                fieldsToAddToPages.add(configurationItem.getFieldname());
            } else {
                configurationItem.setAllowDuplicateValues(false);
            }
        }

        if (configurationMap.containsKey("valueNormalizer")) {
            configurationItem.setValueNormalizer((ValueNormalizer) configurationMap.get("valueNormalizer"));
        }

        if (configurationMap.containsKey("geoJSONSource")) {
            configurationItem.setGeoJSONSource((String) configurationMap.get("geoJSONSource"));
        }

        if (configurationMap.containsKey("geoJSONSourceSeparator")) {
            configurationItem.setGeoJSONSourceSeparator(
                    ((String) configurationMap.get("geoJSONSourceSeparator")).replace(Configuration.SPACE_SPLACEHOLDER, " "));
        }

        if (configurationMap.containsKey("geoJSONAddSearchField")) {
            if (((String) configurationMap.get("geoJSONAddSearchField")).equals("true")) {
                configurationItem.setGeoJSONAddSearchField(true);
            } else {
                configurationItem.setGeoJSONAddSearchField(false);
            }
        }

        return configurationItem;
    }

    /**
     * Returns config object for the given field name.
     *
     * @param fieldname {@link java.lang.String}
     * @return a {@link java.util.List} object.
     * @should return correct FieldConfig
     */
    public List<FieldConfig> getConfigurationListForField(String fieldname) {
        return configurationList.get(fieldname);
    }

    /**
     * Returns a list with all field names in the config files.
     *
     * @return a {@link java.util.List} object.
     */
    public List<String> getListWithAllFieldNames() {
        List<String> retArray = new ArrayList<>();

        Set<String> keys = configurationList.keySet();
        for (String string : keys) {
            retArray.add(string);
        }
        return retArray;
    }

    /**
     * <p>
     * getLanguageMapping.
     * </p>
     *
     * @param code a {@link java.lang.String} object.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should return correct mapping
     * @should return null if code not configured
     * @return a {@link java.lang.String} object.
     */
    public static String getLanguageMapping(String code) throws FatalIndexerException {
        return Configuration.getInstance().getString("languageMapping." + code);
    }

    /**
     * Checks for custom docstruct name mappings to be used in the index instead of the given name.
     *
     * @param string a {@link java.lang.String} object.
     * @return The mapped replacement name, if available; default docstruct name, if configured to be used; the original name (without spaces)
     *         otherwise.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     */
    public static String mapDocStrct(String string) throws FatalIndexerException {
        String ret = string.replace(" ", "_");
        Map<String, String> docStructMap = Configuration.getInstance().getListConfiguration("docstructmapping");
        if (docStructMap.containsKey(ret)) {
            return docStructMap.get(ret);
        } else if (Boolean.valueOf(Configuration.getInstance().getConfiguration("docstructmapping.useDefaultDocstruct"))) {
            return docStructMap.get("_default");
        } else {
            return ret;
        }
    }

    /**
     * <p>
     * Getter for the field <code>fieldsToAddToParents</code>.
     * </p>
     *
     * @return the fieldsToAddToParents
     */
    public Set<String> getFieldsToAddToParents() {
        return fieldsToAddToParents;
    }

    /**
     * <p>
     * Getter for the field <code>fieldsToAddToChildren</code>.
     * </p>
     *
     * @return the fieldsToAddToChildren
     */
    public Set<String> getFieldsToAddToChildren() {
        return fieldsToAddToChildren;
    }

    /**
     * <p>
     * Getter for the field <code>fieldsToAddToPages</code>.
     * </p>
     *
     * @return the fieldsToAddToPages
     */
    public Set<String> getFieldsToAddToPages() {
        return fieldsToAddToPages;
    }

}
