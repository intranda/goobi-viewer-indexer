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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdom2.Namespace;

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

    public static final String CONFIG_FILE_NAME = "config_indexer.xml";

    private ReloadingFileBasedConfigurationBuilder<XMLConfiguration> builder;
    private MetadataConfigurationManager metadataConfigurationManager;
    private Map<String, Namespace> namespaces;
    private long lastFileReload = -1;
    private long lastConfigManagerReload = -1;

    /** Timer that checks for changes in the config file and repopulates some configuration objects. */
    private Timer reloadTimer = new Timer();

    /**
     * Private constructor.
     * 
     * @throws ConfigurationException
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Configuration(String configFilePath) {
        builder =
                new ReloadingFileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
                        .configure(new Parameters().properties()
                                .setFileName(configFilePath)
                                .setListDelimiterHandler(new DefaultListDelimiterHandler(';'))
                                .setThrowExceptionOnMissing(false));
        if (builder.getFileHandler().getFile().exists()) {
            lastFileReload = System.currentTimeMillis();
            try {
                reloadConfig(builder.getConfiguration());
                logger.info("Configuration file '{}' loaded.", builder.getFileHandler().getFile().getAbsolutePath());
            } catch (ConfigurationException e) {
                logger.error(e.getMessage(), e);
            }

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
        } else {
            logger.error("Configuration file not found: {}; Base path is {}", builder.getFileHandler().getFile().getAbsoluteFile(),
                    builder.getFileHandler().getBasePath());
        }
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
     * @throws ConfigurationException
     */
    private void reloadConfig(XMLConfiguration config) throws ConfigurationException {
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
        namespaces.put("marc", Namespace.getNamespace("marc", "http://www.loc.gov/MARC21/slim"));
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
        namespaces.put("rdf", Namespace.getNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"));
        namespaces.put("skos", Namespace.getNamespace("skos", "http://www.w3.org/2004/02/skos/core#"));

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
     * @param elementName
     * @return
     */
    public List<String> getConfigurations(String elementName) {
        List<String> ret = new ArrayList<>();
        int countInit = getConfig().getMaxIndex("init." + elementName);
        for (int i = 0; i <= countInit; i++) {
            ret.add(getConfig().getString("init." + elementName + "(" + i + ")"));
        }
        return ret;
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
     * 
     * @param inPath
     * @return
     */
    public List<String> getStringList(String inPath) {
        String[] arr = getConfig().getStringArray(inPath);
        if (arr != null && arr.length > 0) {
            return Arrays.asList(arr);
        }

        return Collections.emptyList();
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
     * 
     * @return
     * @should return correct value
     */
    public String getSolrUrl() {
        return getConfiguration("solrUrl");
    }

    /**
     * 
     * @return
     */
    public String getOldSolrUrl() {
        return getConfiguration("oldSolrUrl");
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
     * 
     * @return
     * @should return correct value
     */
    public String getHotfolderPath() {
        return getHotfolderPaths().get(0);
    }

    /**
     * 
     * @return
     * @should return all values
     */
    public List<String> getHotfolderPaths() {
        return getConfigurations("hotFolder");
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
     * Whether a viewer task should be triggered that creates pdf files for all images of an indexed process
     * 
     * @return Whether a viewer task should be triggered that creates pdf files for all images of an indexed process
     */
    public boolean isPrerenderPdfsEnabled() {
        return getBoolean("init.viewerNotifications.prerenderPdfs[@enabled]", false);
    }

    /**
     * Whether pdfs for record images should be prerendered in any case, even if they already exist
     * 
     * @return Whether pdfs for record images should be prerendered in any case, even if they already exist
     */
    public boolean isForcePrerenderPdfs() {
        return getBoolean("init.viewerNotifications.prerenderPdfs[@force]", false);
    }

    /**
     * The config_contentServer pdf-configuration variant to use when prerendering pdfs for images
     * 
     * @return The config_contentServer pdf-configuration variant to use when prerendering pdfs for images
     */
    public String getPrerenderPdfsConfigVariant() {
        return getString("init.viewerNotifications.prerenderPdfs[@variant]", "default");
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
     * 
     * @return true if enabled; false otherwise
     * @should return correct value
     */
    public boolean isProxyEnabled() {
        return getBoolean("proxy[@enabled]", false);
    }

    /**
     * 
     * @return
     * @should return correct value
     */
    public String getProxyUrl() {
        return getString("proxy.proxyUrl");
    }

    /**
     * 
     * @return Configured port number; 0 if none found
     * @should return correct value
     */
    public int getProxyPort() {
        return getInt("proxy.proxyPort", 0);
    }

    /**
     * 
     * @return
     */
    public List<String> getProxyWhitelist() {
        return getStringList("proxy.whitelist.host");
    }

    /**
     * 
     * @param url
     * @return
     * @throws MalformedURLException
     * @should return true if host whitelisted
     */
    public boolean isHostProxyWhitelisted(String url) throws MalformedURLException {
        URL urlAsURL = new URL(url);
        return getProxyWhitelist().contains(urlAsURL.getHost());
    }

    /**
     * If true, the first page of a document is set as the representative image if no other page is specified in the source document. If this is set
     * to false, and no page is explicitly set as representative, no representative image will be set. Defaults to true
     * 
     * @return whether the first page should be used as representative image per default
     */
    public boolean isUseFirstPageAsDefaultRepresentative() {
        return getBoolean("init.representativeImage.useFirstPageAsDefault", true);
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

    /**
     * 
     * @return true if all email configuration date is complete; false otherwise
     * @throws FatalIndexerException
     * @should return false until all values configured
     */
    boolean checkEmailConfiguration() {
        if (StringUtils.isEmpty(getString("init.email.recipients"))) {
            logger.warn("init.email.recipients not configured, cannot send e-mail report.");
            return false;
        }
        if (StringUtils.isEmpty(getString("init.email.smtpServer"))) {
            logger.warn("init.email.smtpServer not configured, cannot send e-mail report.");
            return false;
        }
        if (StringUtils.isEmpty(getString("init.email.smtpSenderAddress"))) {
            logger.debug("init.email.smtpSenderAddress not configured, cannot send e-mail report.");
            return false;
        }
        if (StringUtils.isEmpty(getString("init.email.smtpSenderName"))) {
            logger.warn("init.email.smtpSenderName not configured, cannot send e-mail report.");
            return false;
        }
        if (StringUtils.isEmpty(getString("init.email.smtpSecurity"))) {
            logger.warn("init.email.smtpSecurity not configured, cannot send e-mail report.");
            return false;
        }

        return true;
    }

}
