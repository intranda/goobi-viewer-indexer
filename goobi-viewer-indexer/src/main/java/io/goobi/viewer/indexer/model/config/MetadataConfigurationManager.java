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

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.model.SolrConstants.MetadataGroupType;
import io.goobi.viewer.indexer.model.config.ValueNormalizer.ValueNormalizerPosition;

/**
 * Object holding field configurations.
 */
public final class MetadataConfigurationManager {

    private static final Logger logger = LoggerFactory.getLogger(MetadataConfigurationManager.class);

    private static final String SPACE_PLACEHOLDER = "#SPACE#";

    private Map<String, List<FieldConfig>> fieldConfigurations = new HashMap<>();
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
        fieldConfigurations = loadFieldConfiguration(config);
    }

    /**
     * 
     * @param config
     * @return
     * @should load all field configs correctly
     */
    @SuppressWarnings({ "rawtypes" })
    private Map<String, List<FieldConfig>> loadFieldConfiguration(XMLConfiguration config) {
        Map<String, List<FieldConfig>> ret = new HashMap<>();
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
            //            List<Map<String, Object>> fieldInformation = new ArrayList<>();
            for (int i = 0; i <= count; i++) {
                //                Map<String, Object> fieldValues = new HashMap<>();
                FieldConfig fieldConfig = new FieldConfig(fieldname);

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
                        fieldConfig.getxPathConfigurations().add(new XPathConfig(xpath, prefix, suffix));
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
                        fieldConfig.getxPathConfigurations().add(new XPathConfig(xpath, prefix, suffix));
                    }
                }

                fieldConfig.setParents(config.getString("fields." + fieldname + ".list.item(" + i + ").getparents", null));
                fieldConfig.setChild(config.getString("fields." + fieldname + ".list.item(" + i + ").getchildren", null));
                fieldConfig.setOneToken(config.getBoolean("fields." + fieldname + ".list.item(" + i + ").onetoken", false));
                fieldConfig.setOneField(config.getBoolean("fields." + fieldname + ".list.item(" + i + ").onefield", false));
                fieldConfig.setOneFieldSeparator(config.getString("fields." + fieldname + ".list.item(" + i + ").onefield[@separator]",
                        FieldConfig.DEFAULT_MULTIVALUE_SEPARATOR).replace(Configuration.SPACE_SPLACEHOLDER, " "));
                fieldConfig.setConstantValue(config.getString("fields." + fieldname + ".list.item(" + i + ").constantValue", null));
                fieldConfig.setSplittingCharacter(config.getString("fields." + fieldname + ".list.item(" + i + ").splittingCharacter", null));
                fieldConfig.setNode(config.getString("fields." + fieldname + ".list.item(" + i + ").getnode", null));
                fieldConfig.setAddToDefault(config.getBoolean("fields." + fieldname + ".list.item(" + i + ").addToDefault", false));
                fieldConfig.setAddUntokenizedVersion(config.getBoolean("fields." + fieldname + ".list.item(" + i + ").addUntokenizedVersion", true));
                fieldConfig.setLowercase(config.getBoolean("fields." + fieldname + ".list.item(" + i + ").lowercase", false));
                fieldConfig.setAddSortField(config.getBoolean("fields." + fieldname + ".list.item(" + i + ").addSortField", false));
                fieldConfig.setAddSortFieldToTopstruct(
                        config.getBoolean("fields." + fieldname + ".list.item(" + i + ").addSortFieldToTopstruct", false));
                fieldConfig.setAddExistenceBoolean(config.getBoolean("fields." + fieldname + ".list.item(" + i + ").addExistenceBoolean", false));

                if (config.getBoolean("fields." + fieldname + ".list.item(" + i + ").addToParents", false)) {
                    fieldsToAddToParents.add(fieldname);
                }
                if (config.getBoolean("fields." + fieldname + ".list.item(" + i + ").addToChildren", false)) {
                    fieldsToAddToChildren.add(fieldname);
                }
                if (config.getBoolean("fields." + fieldname + ".list.item(" + i + ").addToPages", false)) {
                    fieldsToAddToPages.add(fieldname);
                }

                fieldConfig.setAllowDuplicateValues(config.getBoolean("fields." + fieldname + ".list.item(" + i + ").allowDuplicateValues", false));
                fieldConfig.setGeoJSONSource(config.getString("fields." + fieldname + ".list.item(" + i + ").geoJSONSource", null));
                fieldConfig
                        .setGeoJSONSourceSeparator(config.getString("fields." + fieldname + ".list.item(" + i + ").geoJSONSource[@separator]", "")
                                .replace(Configuration.SPACE_SPLACEHOLDER, " "));
                fieldConfig.setGeoJSONAddSearchField(
                        config.getBoolean("fields." + fieldname + ".list.item(" + i + ").geoJSONSource[@addSearchField]", false));

                // Normalize and interpolate years
                List eleNormalizeYearList = config.configurationsAt("fields." + fieldname + ".list.item(" + i + ").normalizeYear");
                if (eleNormalizeYearList != null && !eleNormalizeYearList.isEmpty()) {
                    SubnodeConfiguration eleNormalizeYear = (SubnodeConfiguration) eleNormalizeYearList.get(0);
                    fieldConfig.setNormalizeYear(eleNormalizeYear.getBoolean("", false));
                    fieldConfig.setNormalizeYearMinDigits(eleNormalizeYear.getInt("[@minYearDigits]", 3));
                    fieldConfig.setInterpolateYears(config.getBoolean("fields." + fieldname + ".list.item(" + i + ").interpolateYears", false));
                }

                // Group entity config
                List<HierarchicalConfiguration> groupEntityList =
                        config.configurationsAt("fields." + fieldname + ".list.item(" + i + ").groupEntity");
                if (groupEntityList != null && !groupEntityList.isEmpty()) {
                    GroupEntity groupEntity = readGroupEntity(groupEntityList.get(0));
                    fieldConfig.setGroupEntity(groupEntity);
                }

                // Non-sort configurations
                List<NonSortConfiguration> nonSortConfigurations = new ArrayList<>();
                List nonSortList = config.configurationsAt("fields." + fieldname + ".list.item(" + i + ").nonSortCharacters");
                if (nonSortList != null) {
                    for (Iterator it = nonSortList.iterator(); it.hasNext();) {
                        HierarchicalConfiguration sub = (HierarchicalConfiguration) it.next();
                        String prefix = sub.getString("[@prefix]");
                        String suffix = sub.getString("[@suffix]");
                        nonSortConfigurations.add(new NonSortConfiguration(prefix, suffix));
                    }
                    fieldConfig.setNonSortConfigurations(nonSortConfigurations);
                }

                // Normalize value
                List<HierarchicalConfiguration> normalizeValueList =
                        config.configurationsAt("fields." + fieldname + ".list.item(" + i + ").normalizeValue");
                if (normalizeValueList != null && !normalizeValueList.isEmpty()) {
                    int length = normalizeValueList.get(0).getInt("[@length]");
                    char filler = normalizeValueList.get(0).getString("[@filler]", "0").charAt(0);
                    String position = normalizeValueList.get(0).getString("[@position]");
                    String relevantPartRegex = normalizeValueList.get(0).getString("[@relevantPartRegex]");
                    ValueNormalizer normalizer =
                            new ValueNormalizer(length, filler, ValueNormalizerPosition.getByName(position), relevantPartRegex);
                    fieldConfig.setValueNormalizer(normalizer);
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
                                //
                            }
                            String string = null;
                            try {
                                string = sub.getString("[@string]");
                            } catch (NoSuchElementException e) {
                                //
                            }
                            String regex = null;
                            try {
                                regex = sub.getString("[@regex]");
                            } catch (NoSuchElementException e) {
                                //
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
                        fieldConfig.setReplaceRules(replaceRules);
                    }
                }

                List<FieldConfig> configs = ret.get(fieldname);
                if (configs == null) {
                    configs = new ArrayList<>(count);
                    ret.put(fieldname, configs);
                }
                configs.add(fieldConfig);
            }
        }

        logger.info("{} field configurations loaded.", ret.size());
        return ret;
    }

    /**
     * 
     * @param config
     * @param fieldname
     * @return
     * @should read group entity correctly
     * @should recursively read child group entities
     */
    static GroupEntity readGroupEntity(HierarchicalConfiguration config) {
        MetadataGroupType type = MetadataGroupType.OTHER;
        String typeName = config.getString("[@type]");
        if (typeName != null) {
            type = MetadataGroupType.getByName(typeName);
        }

        String url = config.getString("[@url]");
        boolean addAuthorityDataToDocstruct = config.getBoolean("@addAuthorityDataToDocstruct", false);
        boolean addCoordsToDocstruct = config.getBoolean("@addCoordsToDocstruct", false);
        GroupEntity ret = new GroupEntity()
                .setType(type)
                .setUrl(url)
                .setAddAuthorityDataToDocstruct(addAuthorityDataToDocstruct)
                .setAddCoordsToDocstruct(addCoordsToDocstruct);

        // Subfield configurations
        {
            List<HierarchicalConfiguration> elements = config.configurationsAt("field");
            if (elements != null) {
                for (Iterator<HierarchicalConfiguration> it = elements.iterator(); it.hasNext();) {
                    HierarchicalConfiguration sub = it.next();
                    SubfieldConfig sfc = readSubfield(sub);
                    if (sfc == null) {
                        continue;
                    }
                    if (!ret.getSubfields().containsKey(sfc.getFieldname())) {
                        ret.getSubfields().put(sfc.getFieldname(), sfc);
                    } else {
                        // If a configuration for this field name already exists, transfer the xpath expressions over
                        SubfieldConfig existing = ret.getSubfields().get(sfc.getFieldname());
                        existing.ingestXpaths(sfc);
                    }
                }
            }
        }

        // Child group entities
        List<HierarchicalConfiguration> children = config.configurationsAt("groupEntity");
        if (children != null && !children.isEmpty()) {
            for (HierarchicalConfiguration child : children) {
                GroupEntity childGroupEntity = readGroupEntity(child);
                if (childGroupEntity != null) {
                    ret.getChildren().add(childGroupEntity);
                }

            }
        }

        return ret;
    }

    /**
     * 
     * @param sub
     * @return
     * @should read subfield config correctly
     */
    static SubfieldConfig readSubfield(HierarchicalConfiguration sub) {
        if (sub == null) {
            return null;
        }

        String fieldName = sub.getString("[@name]", null);
        String defaultValue = sub.getString("[@defaultValue]", null);
        boolean multivalued = sub.getBoolean("[@multivalued]", true);
        String xpathExp = sub.getString("[@xpath]");
        if (xpathExp == null) {
            xpathExp = sub.getString("");
        }
        if (StringUtils.isEmpty(fieldName) || StringUtils.isEmpty(xpathExp)) {
            return null;
        }

        SubfieldConfig ret = new SubfieldConfig(fieldName, multivalued);
        ret.getXpaths().add(xpathExp);
        ret.getDefaultValues().put(xpathExp, defaultValue);
        logger.info("Loaded group entity field: {} - {}", fieldName, xpathExp);

        return ret;
    }

    /**
     * Returns config object for the given field name.
     *
     * @param fieldname {@link java.lang.String}
     * @return a {@link java.util.List} object.
     * @should return correct FieldConfig
     */
    public List<FieldConfig> getConfigurationListForField(String fieldname) {
        return fieldConfigurations.get(fieldname);
    }

    /**
     * Returns a list with all field names in the config files.
     *
     * @return a {@link java.util.List} object.
     */
    public List<String> getListWithAllFieldNames() {
        List<String> retArray = new ArrayList<>();

        Set<String> keys = fieldConfigurations.keySet();
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
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
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
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
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
