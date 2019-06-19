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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.lang.StringUtils;
import org.jdom2.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.model.FatalIndexerException;
import io.goobi.viewer.indexer.model.config.MetadataConfigurationManager;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

public final class Configuration {

    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    private static final Object lock = new Object();

    private final XMLConfiguration config;
    //    private Map<String, List<Map<String, Object>>> fieldConfiguration;
    private MetadataConfigurationManager metadataConfigurationManager;
    private Map<String, Namespace> namespaces;

    /* default */
    private static String configPath = "indexerconfig_solr.xml";
    private static Configuration instance = null;

    /** Timer that checks for changes in the config file and repopulates some configuration objects. */
    private Timer reloadTimer = new Timer();

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
        Configuration conf = instance;
        if (conf == null) {
            synchronized (lock) {
                // Another thread might have initialized instance by now
                conf = instance;
                if (conf == null) {
                    try {
                        conf = new Configuration();
                        instance = conf;
                    } catch (ConfigurationException e) {
                        logger.error(e.getMessage(), e);
                        throw new FatalIndexerException("Cannot read configuration file '" + configPath + "', shutting down...");
                    }
                }
            }
        }

        return instance;
    }

    @SuppressWarnings("deprecation")
    private Configuration() throws ConfigurationException {
        AbstractConfiguration.setDelimiter('&');
        config = new XMLConfiguration(configPath);
        config.setReloadingStrategy(new FileChangedReloadingStrategy());
        reloadConfig(config);

        // Check every 10 seconds for changed config files and refresh maps if necessary
        reloadTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (config != null && config.getReloadingStrategy() != null && config.getReloadingStrategy().reloadingRequired()) {
                    logger.info("Reloading configuration...");
                    reloadConfig(config);
                }
            }
        }, 0, 10000);

    }

    /**
     * 
     * @param config
     */
    private void reloadConfig(XMLConfiguration config) {
        metadataConfigurationManager = new MetadataConfigurationManager(config);
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
        namespaces.put("denkxweb", Namespace.getNamespace("denkxweb", "http://denkxweb.de/"));

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
     * @param inPath
     * @return
     */
    protected List<HierarchicalConfiguration> getLocalConfigurationsAt(String inPath) {
        List<HierarchicalConfiguration> ret = config.configurationsAt(inPath);
        if (ret == null || ret.isEmpty()) {
            ret = config.configurationsAt(inPath);
        }

        return ret;
    }

    /**
     * 
     * @return
     * @should return correct value
     */
    public String getViewerUrl() {
        return getString("init.viewerUrl");
    }

    /**
     * 
     * @return
     * @should return correct value
     */
    public String getViewerHome() {
        return getString("init.viewerHome");
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
     * @return
     * @should return correct value
     */
    public String getEmptyOrderLabelReplacement() {
        return getString("init.emptyOrderLabelReplacement", " - ");
    }

    /**
     * 
     * @param elementName element name to search for
     * @return HashMap with key and value for each element of 'elementName'
     */
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
     * 
     * @return
     * @should return all items
     */
    public List<DataRepository> getDataRepositoryConfigurations() {
        List<HierarchicalConfiguration> elements = getLocalConfigurationsAt("init.dataRepositories.dataRepository");
        if (elements == null) {
            return Collections.emptyList();
        }

        List<DataRepository> ret = new ArrayList<>(elements.size());
        for (Iterator<HierarchicalConfiguration> it2 = elements.iterator(); it2.hasNext();) {
            HierarchicalConfiguration sub = it2.next();
            String path = sub.getString(".");
            long buffer = 0;
            String bufferString = sub.getString("[@buffer]");
            // Parse buffer string
            if (!StringUtils.isEmpty(bufferString)) {
                if (bufferString.endsWith("G")) {
                    bufferString = bufferString.substring(0, bufferString.length() - 1);
                    buffer = Long.valueOf(bufferString) * 1073741824;
                } else if (bufferString.endsWith("M")) {
                    bufferString = bufferString.substring(0, bufferString.length() - 1);
                    buffer = Long.valueOf(bufferString) * 1048576;
                } else if (bufferString.endsWith("B")) {
                    bufferString = bufferString.substring(0, bufferString.length() - 1);
                    buffer = Long.valueOf(bufferString);
                } else {
                    buffer = Long.valueOf(bufferString);
                }
            }
            DataRepository repo = new DataRepository(path);
            repo.setBuffer(buffer);
            ret.add(repo);
        }
        return ret;
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