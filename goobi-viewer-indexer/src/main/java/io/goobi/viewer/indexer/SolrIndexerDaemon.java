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
package io.goobi.viewer.indexer;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.Utils;

/**
 * Entry Point into Application.
 */
public final class SolrIndexerDaemon {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(SolrIndexerDaemon.class);

    /** Constant <code>VERSION</code> */
    private static final int MIN_SCHEMA_VERSION = 20240625;
    private static final String SCHEMA_VERSION_PREFIX = "goobi_viewer-";
    private static final int DEFAULT_SLEEP_INTERVAL = 1000;

    private static final Object LOCK = new Object();
    private static SolrIndexerDaemon instance = null;

    private String confFileName = "src/main/resources/config_indexer.xml";
    private volatile boolean running = false;
    private boolean initialized = false;

    private Configuration configuration;

    private SolrSearchIndex searchIndex;
    private SolrSearchIndex oldSearchIndex;

    private List<Hotfolder> hotfolders = new ArrayList<>();

    /**
     * <p>
     * Getter for the field <code>instance</code>.
     * </p>
     *
     * @return a {@link io.goobi.viewer.indexer.SolrIndexerDaemon} object.
     */
    public static SolrIndexerDaemon getInstance() {
        SolrIndexerDaemon indexer = instance;
        if (indexer == null) {
            synchronized (LOCK) {
                // Another thread might have initialized instance by now
                indexer = instance;
                if (indexer == null) {
                    indexer = new SolrIndexerDaemon();
                    instance = indexer;
                }
            }
        }

        return indexer;
    }

    /**
     * 
     * @throws FatalIndexerException
     * @return this
     * @should throw FatalIndexerException if solr schema name could not be checked
     */
    public SolrIndexerDaemon init() throws FatalIndexerException {
        if (logger.isInfoEnabled()) {
            logger.info(Version.asString());
        }

        try {
            if (!checkSolrSchemaName(
                    SolrSearchIndex.getSolrSchemaDocument(getConfiguration().getSolrUrl()))) {
                throw new FatalIndexerException("Incompatible Solr schema, exiting..");
            }
        } catch (IOException | JDOMException | FatalIndexerException e) {
            throw new FatalIndexerException("Could not check Solr schema: " + e.getMessage());
        }

        // Init old search index, if configured
        try {
            SolrClient oldClient = SolrSearchIndex.getNewSolrClient(getConfiguration().getOldSolrUrl());
            if (StringUtils.isNotEmpty(getConfiguration().getOldSolrUrl()) && oldClient != null) {
                this.oldSearchIndex = new SolrSearchIndex(oldClient);
                if (logger.isInfoEnabled()) {
                    logger.info("Also using old Solr server at {}", SolrIndexerDaemon.getInstance().getConfiguration().getOldSolrUrl());
                }
            }
        } catch (ConfigurationException e) {
            // oldSolrUrl is optional
        }

        // create hotfolder(s)
        for (String hotfolderPath : getConfiguration().getHotfolderPaths()) {
            Hotfolder hotfolder = new Hotfolder(hotfolderPath);
            hotfolders.add(hotfolder);
            if (hotfolder.getSuccessFolder() == null || !Files.isDirectory(hotfolder.getSuccessFolder())) {
                throw new FatalIndexerException("Configured path for 'successFolder' does not exist, exiting...");
            }
        }

        if (hotfolders.isEmpty()) {
            throw new FatalIndexerException("No hotfolder configuration found, exiting...");
        }

        initialized = true;
        return this;
    }

    /**
     * Removes files that matches the given pi from hotfolders that are prioritized lower than (i.e. listed after) usedHotfolder.
     * 
     * @param pi
     * @param usedHotfolder
     */
    public void removeRecordFileFromLowerPriorityHotfolders(String pi, Hotfolder usedHotfolder) {
        logger.debug("removeRecordFileFromLowerPriorityHotfolders: {}/{}", usedHotfolder.getHotfolderPath().getFileName(), pi);
        int index = hotfolders.indexOf(usedHotfolder);
        if (index == -1) {
            return;
        }
        for (int i = index + 1; i < hotfolders.size(); ++i) {
            Hotfolder hotfolder = hotfolders.get(i);
            try {
                hotfolder.removeSourceFileFromQueue(pi);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    /**
     * <p>
     * main.
     * </p>
     *
     * @param args an array of {@link java.lang.String} objects.
     */
    public static void main(String[] args) {
        boolean cleanupAnchors = false;

        if (args.length > 0) {
            SolrIndexerDaemon.getInstance().confFileName = args[0];
            if (args.length > 1 && args[1].equalsIgnoreCase("-cleanupGrievingAnchors")) {
                cleanupAnchors = true;
            }
        }

        try {
            SolrIndexerDaemon.getInstance().init().start(cleanupAnchors);
        } catch (FatalIndexerException e) {
            logger.error("{}, exiting...", e.getMessage(), e);
            System.exit(-1);
        }
        System.exit(0);
    }

    /**
     * <p>
     * start.
     * </p>
     * 
     * @param cleanupAnchors a boolean.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     */
    public void start(boolean cleanupAnchors) throws FatalIndexerException {
        if (!initialized) {
            init();
        }
        if (running) {
            logger.warn("Indexer is already running");
            return;
        }

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Received shutdown signal.");
                SolrIndexerDaemon.getInstance().stop();
            }
        });

        running = true;

        if (cleanupAnchors) {
            logger.info("GRIEVING ANCHOR CLEANUP MODE");
            logger.info("Removed {} anchor documents with no volumes.", getSearchIndex().removeGrievingAnchors());
            logger.info("Shutting down...");
            running = false;
            return;
        }
        // Set the hotfolder sleep interval
        int sleepInterval = DEFAULT_SLEEP_INTERVAL;
        try {
            sleepInterval = Integer.valueOf(configuration.getConfiguration("sleep"));
            if (sleepInterval < 500) {
                sleepInterval = DEFAULT_SLEEP_INTERVAL;
                logger.warn("Sleep interval must be at lest 500 ms, using default interval of {} ms instead.", DEFAULT_SLEEP_INTERVAL);
            } else {
                logger.info("Sleep interval is {} ms.", sleepInterval);
            }
        } catch (NumberFormatException e) {
            sleepInterval = DEFAULT_SLEEP_INTERVAL;
            logger.warn("<sleep> must contain an numerical value, using default interval of {} ms instead.", DEFAULT_SLEEP_INTERVAL);
        }

        logger.info("Using {} CPU thread(s).", configuration.getThreads());

        Utils.submitDataToViewer(Collections.emptyList(), hotfolders.get(0).countRecordFiles());

        // main loop
        logger.info("Program started, monitoring hotfolder(s)...");
        while (running) {
            for (Hotfolder hotfolder : hotfolders) {
                if (hotfolder.scan()) {
                    break;
                }
            }
            try {
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Stops this instance
     * 
     * @should set running to false
     */
    public void stop() {
        logger.info("Stopping indexer...");
        configuration.killReloadTimer();
        running = false;
    }

    /**
     * 
     * @param doc
     * @return true if schema up to date; false otherwise
     * @should return false if doc null
     * @should return true if schema compatible
     */
    static boolean checkSolrSchemaName(Document doc) {
        if (doc == null) {
            logger.warn("Could not read the Solr schema name.");
            return false;
        }

        Element eleRoot = doc.getRootElement();
        if (eleRoot != null) {
            String schemaName = eleRoot.getAttributeValue("name");
            if (StringUtils.isNotEmpty(schemaName)) {
                try {
                    if (schemaName.length() > SCHEMA_VERSION_PREFIX.length()
                            && Integer.parseInt(schemaName.substring(SCHEMA_VERSION_PREFIX.length())) >= SolrIndexerDaemon.MIN_SCHEMA_VERSION) {
                        return true;
                    }
                } catch (NumberFormatException e) {
                    logger.error("Schema version must contain a number.");
                }
                logger.error("Solr schema is not up to date; required: {}{}, found: {}", SCHEMA_VERSION_PREFIX, MIN_SCHEMA_VERSION, schemaName);
            }
        }

        return false;
    }

    /**
     * <p>
     * Getter for the field <code>configuration</code>.
     * </p>
     *
     * @return the configuration
     * @throws FatalIndexerException
     */
    public Configuration getConfiguration() {
        if (configuration == null) {
            synchronized (LOCK) {
                configuration = new Configuration(confFileName);
            }
        }

        return configuration;
    }

    /**
     * Sets custom Configuration object (used for unit testing).
     *
     * @param configuration a {@link io.goobi.viewer.indexer.helper.Configuration} object.
     */
    public void injectConfiguration(Configuration configuration) {
        if (configuration != null) {
            this.configuration = configuration;
        }
    }

    /**
     * For unit tests only.
     * 
     * @return the running
     */
    boolean isRunning() {
        return running;
    }

    /**
     * For unit tests only.
     * 
     * @param running the running to set
     */
    void setRunning(boolean running) {
        this.running = running;
    }

    /**
     * For unit tests only.
     * 
     * @return the confFileName
     */
    String getConfFileName() {
        return confFileName;
    }

    /**
     * Required for g2g, must be public.
     * 
     * @param confFileName
     * @return this
     * @should set confFileName correctly
     */
    public SolrIndexerDaemon setConfFileName(String confFileName) {
        this.confFileName = confFileName;
        return this;
    }

    /**
     * <p>
     * Getter for the field <code>searchIndex</code>.
     * </p>
     *
     * @return the searchIndex
     * @should create new instance if none exists
     */
    public SolrSearchIndex getSearchIndex() {
        if (this.searchIndex == null) {
            synchronized (LOCK) {
                try {
                    this.searchIndex = new SolrSearchIndex(null);
                    this.searchIndex.setOptimize(configuration.isAutoOptimize());
                    logger.info("Auto-optimize: {}", this.searchIndex.isOptimize());
                } catch (ConfigurationException e) {
                    logger.error(e.getMessage());
                }
            }
        }

        return this.searchIndex;
    }

    /**
     * Sets custom SolrSearchIndex object (used for unit testing).
     *
     * @param searchIndex a {@link io.goobi.viewer.indexer.helper.SolrSearchIndex} object.
     */
    public void injectSearchIndex(SolrSearchIndex searchIndex) {
        this.searchIndex = searchIndex;
    }

    /**
     * @return the oldSearchIndex
     */
    public SolrSearchIndex getOldSearchIndex() {
        return oldSearchIndex;
    }
}
