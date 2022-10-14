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
import java.util.Map.Entry;
import java.util.Timer;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.ConfigurationBuilderEvent;
import org.apache.commons.configuration2.builder.ReloadingFileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.event.Event;
import org.apache.commons.configuration2.event.EventListener;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang3.StringUtils;
import org.jdom2.Namespace;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.model.config.MetadataConfigurationManager;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

/**
 * <p>
 * Configuration class.
 * </p>
 *
 */
public final class Configuration {

    private static final Logger logger = LogManager.getLogger(Configuration.class);

    private static final Object lock = new Object();

    /* default */
    private static String configPath = "config_indexer.xml";
    private static Configuration instance = null;

    private ReloadingFileBasedConfigurationBuilder<XMLConfiguration> builder;
    private MetadataConfigurationManager metadataConfigurationManager;
    private Map<String, Namespace> namespaces;
    private long lastFileReload = -1;
    private long lastConfigManagerReload = -1;

    /** Timer that checks for changes in the config file and repopulates some configuration objects. */
    private Timer reloadTimer = new Timer();

    /**
     * Re-inits the instance with the given config file name.
     *
     * @param confFilename a {@link java.lang.String} object.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @return a {@link io.goobi.viewer.indexer.helper.Configuration} object.
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
     * @return a {@link io.goobi.viewer.indexer.helper.Configuration} object.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException if any.
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

    /**
     * Private constructor.
     * 
     * @throws ConfigurationException
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Configuration() throws ConfigurationException {
        builder =
                new ReloadingFileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
                        .configure(new Parameters().properties()
                                .setFileName(configPath)
                                .setListDelimiterHandler(new DefaultListDelimiterHandler(';'))
                                .setThrowExceptionOnMissing(false));
        lastFileReload = System.currentTimeMillis();
        reloadConfig(builder.getConfiguration());

        // Check every 10 seconds for changed config files and refresh maps if necessary
        builder.addEventListener(ConfigurationBuilderEvent.CONFIGURATION_REQUEST,
                new EventListener() {

                    @Override
                    public void onEvent(Event event) {
                        if (builder.getReloadingController().checkForReloading(null)) {
                            lastFileReload = System.currentTimeMillis();
                        }
                    }
                });
        //        PeriodicReloadingTrigger trigger = new PeriodicReloadingTrigger(builder.getReloadingController(),
        //                null, 10, TimeUnit.SECONDS);
        //        trigger.start();
    }

    private XMLConfiguration getConfig() {
        try {
            XMLConfiguration ret = builder.getConfiguration();
            if (lastFileReload > lastConfigManagerReload) {
                logger.info("Reloading configuration...");
                lastConfigManagerReload = lastFileReload;
                reloadConfig(ret);
            }
            return ret;
        } catch (ConfigurationException e) {
            logger.error(e.getMessage());
        }

        return new XMLConfiguration();
    }

    /**
     * Reloads metadata fields, namespaces, etc. from the configuration object.
     * 
     * @param config
     */
    private void reloadConfig(XMLConfiguration config) {
        metadataConfigurationManager = new MetadataConfigurationManager(config);
        namespaces = new HashMap<>();
        initNamespaces();
    }

    /**
     * <p>
     * killReloadTimer.
     * </p>
     */
    public void killReloadTimer() {
        if (reloadTimer != null) {
            reloadTimer.cancel();
            reloadTimer.purge();
        }
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
        namespaces.put("dc", Namespace.getNamespace("dc", "http://purl.org/dc/elements/1.1/"));
        namespaces.put("pnx", Namespace.getNamespace("pnx", "http://www.exlibrisgroup.com/xsd/primo/primo_nm_bib"));

        Map<String, String> additionalNamespaces = getListConfiguration("init.namespaces");
        for (Entry<String, String> entry : additionalNamespaces.entrySet()) {
            namespaces.put(entry.getKey(), Namespace.getNamespace(entry.getKey(), entry.getValue()));
            logger.info("Added custom namespace '{}'.", entry.getKey());
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
        int countInit = getConfig().getMaxIndex("init");
        for (int i = 0; i <= countInit; i++) {
            answer = getConfig().getString("init(" + i + ")." + elementName);
        }
        return answer;
    }

    /**
     * 
     * @param inPath
     * @param defaultValue
     * @return
     */
    public Boolean getBoolean(String inPath, boolean defaultValue) {
        return getConfig().getBoolean(inPath, defaultValue);
    }

    /**
     * <p>
     * getInt.
     * </p>
     *
     * @param inPath a {@link java.lang.String} object.
     * @param defaultValue a int.
     * @return a {@link java.lang.Integer} object.
     */
    public Integer getInt(String inPath, int defaultValue) {
        return getConfig().getInt(inPath, defaultValue);
    }

    /**
     * <p>
     * getString.
     * </p>
     *
     * @param inPath a {@link java.lang.String} object.
     * @param defaultValue a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     */
    public String getString(String inPath, String defaultValue) {
        return getConfig().getString(inPath, defaultValue);
    }

    /**
     * <p>
     * getString.
     * </p>
     *
     * @param inPath a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     */
    public String getString(String inPath) {
        return getConfig().getString(inPath);
    }

    /**
     * <p>
     * getList.
     * </p>
     *
     * @param inPath a {@link java.lang.String} object.
     * @return a {@link java.util.List} object.
     */
    @SuppressWarnings({ "rawtypes" })
    public List getList(String inPath) {
        return getConfig().getList(inPath, getConfig().getList(inPath));
    }

    /**
     * <p>
     * getLocalConfigurationsAt.
     * </p>
     *
     * @param inPath a {@link java.lang.String} object.
     * @return a {@link java.util.List} object.
     */
    protected List<HierarchicalConfiguration<ImmutableNode>> getLocalConfigurationsAt(String inPath) {
        List<HierarchicalConfiguration<ImmutableNode>> ret = getConfig().configurationsAt(inPath);
        if (ret == null || ret.isEmpty()) {
            ret = getConfig().configurationsAt(inPath);
        }

        return ret;
    }

    /**
     * <p>
     * getViewerUrl.
     * </p>
     *
     * @should return correct value
     * @return a {@link java.lang.String} object.
     */
    public String getViewerUrl() {
        return getString("init.viewerUrl");
    }

    /**
     * <p>
     * getViewerHome.
     * </p>
     *
     * @should return correct value
     * @return a {@link java.lang.String} object.
     */
    public String getViewerHome() {
        return getString("init.viewerHome");
    }

    /**
     * <p>
     * getDataRepositoryStrategy.
     * </p>
     *
     * @return a {@link java.lang.String} object.
     */
    public String getDataRepositoryStrategy() {
        return getString("init.dataRepositories.strategy", "SingleRepositoryStrategy");
    }

    /**
     * <p>
     * isAddVolumeCollectionsToAnchor.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isAddVolumeCollectionsToAnchor() {
        return getBoolean("init.addVolumeCollectionsToAnchor", false);
    }

    /**
     * <p>
     * isAddLabelToChildren.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isAddLabelToChildren() {
        return getBoolean("init.addLabelToChildren", false);
    }

    /**
     * <p>
     * isLabelCleanup.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isLabelCleanup() {
        return getBoolean("init.labelCleanup", false);
    }

    /**
     * <p>
     * isAggregateRecords.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isAggregateRecords() {
        return getBoolean("init.aggregateRecords", false);
    }

    /**
     * <p>
     * isAutoOptimize.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isAutoOptimize() {
        return getBoolean("performance.autoOptimize", false);
    }

    /**
     * <p>
     * getThreads.
     * </p>
     *
     * @return Number of CPU threads to be used for page generation.
     */
    public int getThreads() {
        return getInt("performance.threads", 1);
    }

    /**
     * 
     * @return
     * @should return correct value
     */
    public boolean isCountHotfolderFiles() {
        return getBoolean("performance.countHotfolderFiles", true);
    }

    /**
     * 
     * @return
     * @should return correct value
     */
    public boolean isAuthorityDataCacheEnabled() {
        return getBoolean("performance.authorityDataCache[@enabled]", true);
    }

    /**
     * 
     * @return
     * @should return correct value
     */
    public int getAuthorityDataCacheRecordTTL() {
        return getInt("performance.authorityDataCache.recordTTL", 24);
    }

    /**
     * 
     * @return
     * @should return correct value
     */
    public int getAuthorityDataCacheSizeWarningThreshold() {
        return getInt("performance.authorityDataCache.sizeWarningThreshold", 10000);
    }

    /**
     * <p>
     * getPageCountStart.
     * </p>
     *
     * @should return correct value
     * @return a int.
     */
    public int getPageCountStart() {
        return getConfig().getInt("init.pageCountStart", 1);
    }

    /**
     * <p>
     * getEmptyOrderLabelReplacement.
     * </p>
     *
     * @should return correct value
     * @return a {@link java.lang.String} object.
     */
    public String getEmptyOrderLabelReplacement() {
        return getString("init.emptyOrderLabelReplacement", " - ");
    }

    /**
     * 
     * @return Viewer authorization token string, if configured
     * @should return correct value
     */
    public String getViewerAuthorizationToken() {
        return getConfig().getString("init.viewerAuthorizationToken");
    }

    /**
     * <p>
     * getListConfiguration.
     * </p>
     *
     * @param elementName element name to search for
     * @return HashMap with key and value for each element of 'elementName'
     */
    public Map<String, String> getListConfiguration(String elementName) {
        Map<String, String> answerList = new HashMap<>();
        Iterator<String> it = getConfig().getKeys(elementName + ".list");
        while (it.hasNext()) {
            String key = it.next();
            answerList.put(key.substring(key.lastIndexOf('.') + 1), getConfig().getString(key));
        }
        return answerList;
    }

    /**
     * <p>
     * getDataRepositoryConfigurations.
     * </p>
     *
     * @should return all items
     * @return a {@link java.util.List} object.
     */
    public List<DataRepository> getDataRepositoryConfigurations() {
        List<HierarchicalConfiguration<ImmutableNode>> elements = getLocalConfigurationsAt("init.dataRepositories.dataRepository");
        if (elements == null) {
            return Collections.emptyList();
        }

        List<DataRepository> ret = new ArrayList<>(elements.size());
        for (Iterator<HierarchicalConfiguration<ImmutableNode>> it2 = elements.iterator(); it2.hasNext();) {
            HierarchicalConfiguration<ImmutableNode> sub = it2.next();
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
     * <p>
     * Getter for the field <code>metadataConfigurationManager</code>.
     * </p>
     *
     * @return the metadataConfigurationManager
     */
    public MetadataConfigurationManager getMetadataConfigurationManager() {
        return metadataConfigurationManager;
    }

    /**
     * <p>
     * Getter for the field <code>namespaces</code>.
     * </p>
     *
     * @return the namespaces
     */
    public Map<String, Namespace> getNamespaces() {
        return namespaces;
    }

    /**
     * Overrides values in the config file (for unit test purposes).
     * 
     * @param property Property path (e.g. "accessConditions.fullAccessForLocalhost")
     * @param value New value to set
     */
    public void overrideValue(String property, Object value) {
        getConfig().setProperty(property, value);
    }
}
