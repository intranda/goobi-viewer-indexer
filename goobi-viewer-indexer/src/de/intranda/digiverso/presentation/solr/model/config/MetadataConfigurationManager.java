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
package de.intranda.digiverso.presentation.solr.model.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MultiMap;

import de.intranda.digiverso.presentation.solr.helper.Configuration;
import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;
import de.intranda.digiverso.presentation.solr.model.NonSortConfiguration;

/**
 * Object holding field configurations.
 */
public final class MetadataConfigurationManager {

    private static final String FALSE = "false";

    private Map<String, List<FieldConfig>> configurationList = new HashMap<>();
    private Set<String> fieldsToAddToChildren = new HashSet<>();
    private Set<String> fieldsToAddToPages = new HashSet<>();

    public MetadataConfigurationManager(Configuration config) {
        // Regular fields
        Map<String, List<Map<String, Object>>> fieldMap = config.getFieldConfiguration();
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
            configurationItem.setGroupEntityFields((MultiMap) configurationMap.get("groupEntity"));
        }

        if (configurationMap.containsKey("replaceRules")) {
            configurationItem.setReplaceRules((Map<Object, String>) configurationMap.get("replaceRules"));
        }

        if (configurationMap.containsKey("nonSortConfigurations")) {
            configurationItem.setNonSortConfigurations((List<NonSortConfiguration>) configurationMap.get("nonSortConfigurations"));
        }

        if (configurationMap.containsKey("addToChildren")) {
            if (((String) configurationMap.get("addToChildren")).equals("true")) {
                configurationItem.setAddToChildren(true);
                fieldsToAddToChildren.add(configurationItem.getFieldname());
            } else {
                configurationItem.setAddToChildren(false);
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

        return configurationItem;
    }

    /**
     * Returns config object for the given field name.
     * 
     * @param fieldname {@link String}
     * @return
     */
    public List<FieldConfig> getConfigurationListForField(String fieldname) {
        if (configurationList.containsKey(fieldname)) {
            return configurationList.get(fieldname);
        }
        return null;
    }

    /**
     * Returns a list with all field names in the config files.
     * 
     * @return
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
     * 
     * @param code
     * @return
     * @throws FatalIndexerException
     * @should return correct mapping
     * @should return null if code not configured
     */
    public static String getLanguageMapping(String code) throws FatalIndexerException {
        return Configuration.getInstance().getString("languageMapping." + code);
    }

    /**
     * Checks for custom docstruct name mappings to be used in the index instead of the given name.
     * 
     * @param string
     * @return The mapped replacement name, if available; default docstruct name, if configured to be used; the original name (without spaces)
     *         otherwise.
     * @throws FatalIndexerException
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
     * @return the fieldsToAddToChildren
     */
    public Set<String> getFieldsToAddToChildren() {
        return fieldsToAddToChildren;
    }

    /**
     * @return the fieldsToAddToPages
     */
    public Set<String> getFieldsToAddToPages() {
        return fieldsToAddToPages;
    }

}
