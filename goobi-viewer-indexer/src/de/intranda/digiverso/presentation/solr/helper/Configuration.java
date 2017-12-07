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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.lang.StringUtils;
import org.jdom2.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;
import de.intranda.digiverso.presentation.solr.model.NonSortConfiguration;
import de.intranda.digiverso.presentation.solr.model.config.MetadataConfigurationManager;
import de.intranda.digiverso.presentation.solr.model.config.XPathConfig;

public final class Configuration {
    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    private final XMLConfiguration config;
    private Map<String, List<Map<String, Object>>> fieldConfiguration;
    private final MetadataConfigurationManager metadataConfigurationManager;
    private final Map<String, Namespace> namespaces;

    /* default */
    private static String configPath = "indexerconfig_solr.xml";
    private static Configuration instance = null;

    /**
     * Re-inits the instance with the given config file name.
     * 
     * @param confFilename
     * @return
     * @throws FatalIndexerException
     */
    public static synchronized Configuration getInstance(String confFilename) throws FatalIndexerException {
        if (confFilename != null) {
            Configuration.configPath = confFilename;
            Configuration.instance = null;
        }
        return getInstance();
    }

    /**
     * Do not call this method before the correct config file path has been passed!
     * 
     * @return
     */
    public static synchronized Configuration getInstance() throws FatalIndexerException {
        if (instance == null) {
            try {
                instance = new Configuration();
            } catch (ConfigurationException e) {
                logger.error(e.getMessage(), e);
                throw new FatalIndexerException("Cannot read configuration file '" + configPath + "', shutting down...");
            }
        }
        return instance;
    }

    @SuppressWarnings("deprecation")
    private Configuration() throws ConfigurationException {
        AbstractConfiguration.setDelimiter('&');
        config = new XMLConfiguration(configPath);
        config.setReloadingStrategy(new FileChangedReloadingStrategy());
        metadataConfigurationManager = new MetadataConfigurationManager(this);
        namespaces = new HashMap<>();
        initNamespaces();
    }

    /**
     * Adds relevant XML namespaces to the list of available namespace objects.
     * 
     * @should add custom namespaces correctly
     */
    public void initNamespaces() {
        namespaces.clear();
        namespaces.put("xml", Namespace.getNamespace("xml", "http://www.w3.org/XML/1998/namespace"));
        namespaces.put("mets", Namespace.getNamespace("mets", "http://www.loc.gov/METS/"));
        namespaces.put("mods", Namespace.getNamespace("mods", "http://www.loc.gov/mods/v3"));
        namespaces.put("gdz", Namespace.getNamespace("gdz", "http://gdz.sub.uni-goettingen.de/"));
        namespaces.put("xlink", Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink"));
        namespaces.put("dv", Namespace.getNamespace("dv", "http://dfg-viewer.de/"));
        namespaces.put("lido", Namespace.getNamespace("lido", "http://www.lido-schema.org"));
        namespaces.put("mix", Namespace.getNamespace("mix", "http://www.loc.gov/mix/v20"));
        namespaces.put("mm", Namespace.getNamespace("mm", "http://www.mycore.de/metsmaker/v1"));
        namespaces.put("tei", Namespace.getNamespace("tei", "http://www.tei-c.org/ns/1.0"));

        Map<String, String> additionalNamespaces = getListConfiguration("init.namespaces");
        if (additionalNamespaces != null) {
            for (String key : additionalNamespaces.keySet()) {
                namespaces.put(key, Namespace.getNamespace(key, additionalNamespaces.get(key)));
                logger.info("Added custom namespace '{}'.", key);
            }
        }
    }

    /**
     * Returns param of < init >< elementName > param < /elementName >< /init > if exists otherwise returns question
     * 
     * @param elementName the element name
     * @return param if exists, element name otherwise
     */
    public String getConfiguration(String elementName) {
        String answer = elementName;
        int countInit = config.getMaxIndex("init");
        for (int i = 0; i <= countInit; i++) {
            answer = config.getString("init(" + i + ")." + elementName);
        }
        return answer;
    }

    /**
     * 
     * @param inPath
     * @param defaultValue
     * @return
     */
    private Boolean getBoolean(String inPath, boolean defaultValue) {
        return config.getBoolean(inPath, defaultValue);
    }

    /**
     * 
     * @param inPath
     * @param defaultValue
     * @return
     */
    public Integer getInt(String inPath, int defaultValue) {
        return config.getInt(inPath, defaultValue);
    }

    /**
     * 
     * @param inPath
     * @param defaultValue
     * @return
     */
    public String getString(String inPath, String defaultValue) {
        return config.getString(inPath, defaultValue);
    }

    /**
     * 
     * @param inPath
     * @return
     */
    public String getString(String inPath) {
        return config.getString(inPath);
    }

    /**
     * 
     * @param inPath
     * @return
     */
    @SuppressWarnings({ "rawtypes" })
    public List getList(String inPath) {
        return config.getList(inPath, config.getList(inPath));
    }

    /**
     * 
     * @return
     */
    public String getDataRepositoryStrategy() {
        return getString("init.dataRepositories.strategy", "SingleRepositoryStrategy");
    }

    /**
     * 
     * @return
     */
    public boolean isAddVolumeCollectionsToAnchor() {
        return getBoolean("init.addVolumeCollectionsToAnchor", false);
    }

    /**
     * 
     * @return
     */
    public boolean isAddLabelToChildren() {
        return getBoolean("init.addLabelToChildren", false);
    }

    /**
     * 
     * @return
     */
    public boolean isLabelCleanup() {
        return getBoolean("init.labelCleanup", false);
    }

    /**
     * 
     * @return
     */
    public boolean isAggregateRecords() {
        return getBoolean("init.aggregateRecords", false);
    }

    /**
     * 
     * @return
     */
    public boolean isAutoOptimize() {
        return getBoolean("performance.autoOptimize", false);
    }

    /**
     * 
     * @return
     */
    public int getThreads() {
        return getInt("performance.threads", 1);
    }

    /**
     * 
     * @return
     * @should return correct value
     */
    public int getPageCountStart() {
        return config.getInt("init.pageCountStart", 1);
    }

    /**
     * 
     * @param elementName element name to search for
     * @return HashMap with key and value for each element of 'elementName'
     */
    @SuppressWarnings("unchecked")
    public Map<String, String> getListConfiguration(String elementName) {
        Map<String, String> answerList = new HashMap<>();
        Iterator<String> it = config.getKeys(elementName + ".list");
        while (it.hasNext()) {
            String key = it.next();
            answerList.put(key.substring(key.lastIndexOf('.') + 1), config.getString(key));
        }
        return answerList;
    }

    /**
     * Loads and returns index field configurations.
     * 
     * <li>fields - > ArrayList < HashMap >
     * 
     * <li>HashMap - > HashMap < String, Object >
     * 
     * <li>Object - > String (for store, index, addToDefault, addToMetadata) or ArrayList (for xpath)
     * 
     * 
     * @return HashMap < String, ArrayList < HashMap < String, Object>>> fields
     */
    public Map<String, List<Map<String, Object>>> getFieldConfiguration() {
        if (fieldConfiguration == null) {
            loadFieldConfiguration();
        }

        return fieldConfiguration;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void loadFieldConfiguration() {
        fieldConfiguration = new HashMap<>();
        Iterator<String> fields = config.getKeys("fields");
        ArrayList<String> newFields = new ArrayList<>();
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
                        SubnodeConfiguration xpathNode = config.configurationAt("fields." + fieldname + ".list.item(" + i + ").xpath.list.item(" + j
                                + ")");
                        String xpath = xpathNode.getString(".");
                        if (StringUtils.isEmpty(xpath)) {
                            logger.error("Found empty XPath configuration for field: {}", fieldname);
                            continue;
                        }
                        String prefix = xpathNode.getString("[@prefix]");
                        String suffix = xpathNode.getString("[@suffix]");
                        xPathConfigurations.add(new XPathConfig(xpath, prefix, suffix));
                    }
                } else {
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
                fieldValues.put("constantValue", config.getString("fields." + fieldname + ".list.item(" + i + ").constantValue"));
                fieldValues.put("splittingCharacter", config.getString("fields." + fieldname + ".list.item(" + i + ").splittingCharacter"));
                fieldValues.put("getnode", config.getString("fields." + fieldname + ".list.item(" + i + ").getnode"));
                fieldValues.put("addToDefault", config.getString("fields." + fieldname + ".list.item(" + i + ").addToDefault"));
                fieldValues.put("addUntokenizedVersion", config.getString("fields." + fieldname + ".list.item(" + i + ").addUntokenizedVersion"));
                fieldValues.put("lowercase", config.getString("fields." + fieldname + ".list.item(" + i + ").lowercase"));
                fieldValues.put("addSortField", config.getString("fields." + fieldname + ".list.item(" + i + ").addSortField"));
                fieldValues.put("aggregateEntity", config.getString("fields." + fieldname + ".list.item(" + i + ").aggregateEntity"));
                fieldValues.put("addToChildren", config.getString("fields." + fieldname + ".list.item(" + i + ").addToChildren"));
                fieldValues.put("addToPages", config.getString("fields." + fieldname + ".list.item(" + i + ").addToPages"));

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
                    MultiMap groupedSubfieldConfigurations = new MultiValueMap();
                    String type = config.getString("fields." + fieldname + ".list.item(" + i + ").groupEntity[@type]");
                    if (type != null) {
                        groupedSubfieldConfigurations.put("type", type);
                    }
                    List elements = config.configurationsAt("fields." + fieldname + ".list.item(" + i + ").groupEntity.field");
                    if (elements != null) {
                        for (Iterator it = elements.iterator(); it.hasNext();) {
                            HierarchicalConfiguration sub = (HierarchicalConfiguration) it.next();
                            String name = sub.getString("[@name]", null);
                            String xpathExp = sub.getString("");
                            if (StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(xpathExp)) {
                                groupedSubfieldConfigurations.put(name, xpathExp);
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
                            if (character != null) {
                                replaceRules.put(character, replaceWith);
                            } else if (string != null) {
                                replaceRules.put(string, replaceWith);
                            } else if (regex != null) {
                                replaceRules.put("REGEX:" + regex, replaceWith);
                            }
                        }
                        fieldValues.put("replaceRules", replaceRules);
                    }
                }

                fieldInformation.add(fieldValues);
                fieldConfiguration.put(fieldname, fieldInformation);
            }
        }
    }

    /**
     * @return the metadataConfigurationManager
     */
    public MetadataConfigurationManager getMetadataConfigurationManager() {
        return metadataConfigurationManager;
    }

    /**
     * @return the namespaces
     */
    public Map<String, Namespace> getNamespaces() {
        return namespaces;
    }

}
