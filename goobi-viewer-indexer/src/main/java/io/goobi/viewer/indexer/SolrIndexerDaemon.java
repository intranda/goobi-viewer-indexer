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

import java.nio.file.Files;

import org.apache.commons.lang3.StringUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.FatalIndexerException;

/**
 * Entry Point into Application.
 */
public final class SolrIndexerDaemon {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(SolrIndexerDaemon.class);

    /** Constant <code>VERSION</code> */
    private static final int MIN_SCHEMA_VERSION = 20201217;
    private static final String SCHEMA_VERSION_PREFIX = "goobi_viewer-";
    private static final int DEFAULT_SLEEP_INTERVAL = 1000;

    private static final Object lock = new Object();
    private static volatile SolrIndexerDaemon instance = null;

    private String confFilename = "indexerconfig_solr.xml";
    private int sleepInterval = 1000;
    private volatile boolean running = false;

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
            synchronized (lock) {
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
     * <p>
     * main.
     * </p>
     *
     * @param args an array of {@link java.lang.String} objects.
     */
    public static void main(String[] args) {
        String configFileName = null;
        boolean noUpdate = false;
        boolean cleanupAnchors = false;

        if (args.length > 0) {
            configFileName = args[0];
            if (args.length > 1) {
                if (args[1].equalsIgnoreCase("-noupdate")) {
                    noUpdate = true;
                } else if (args[1].equalsIgnoreCase("-cleanupGrievingAnchors")) {
                    cleanupAnchors = true;
                }
            }
        }

        try {
            SolrIndexerDaemon.getInstance().start(configFileName, noUpdate, cleanupAnchors);
        } catch (FatalIndexerException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        }
        System.exit(0);
    }

    /**
     * <p>
     * start.
     * </p>
     * 
     * @param configFilePath a {@link java.lang.String} object.
     * @param noUpdate a boolean.
     * @param cleanupAnchors a boolean.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     */
    public void start(String configFilePath, boolean noUpdate, boolean cleanupAnchors) throws FatalIndexerException {
        if (running) {
            logger.warn("Indexer is already running");
            return;
        }
        logger.info(Version.asString());
        if (StringUtils.isNotEmpty(configFilePath)) {
            confFilename = configFilePath;
        }
        if (noUpdate) {
            MetsIndexer.noTimestampUpdate = true;
            LidoIndexer.noTimestampUpdate = true;
        }
        if (MetsIndexer.noTimestampUpdate) {
            logger.warn("WARNING: No update mode - DATEUPDATED timestamps will not be updated.");
        }

        if (!checkSolrSchemaName(
                SolrSearchIndex.getSolrSchemaDocument(Configuration.getInstance(confFilename).getConfiguration("solrUrl")))) {
            throw new FatalIndexerException("Incompatible Solr schema, exiting..");
        }

        // create hotfolder
        Hotfolder hotfolder =
                new Hotfolder(confFilename,
                        SolrSearchIndex.getNewHttpSolrClient(Configuration.getInstance(confFilename).getConfiguration("solrUrl"),
                                SolrSearchIndex.TIMEOUT_SO, SolrSearchIndex.TIMEOUT_CONNECTION, true),
                        SolrSearchIndex.getNewHttpSolrClient(Configuration.getInstance(confFilename).getConfiguration("oldSolrUrl"),
                                SolrSearchIndex.TIMEOUT_SO, SolrSearchIndex.TIMEOUT_CONNECTION, true));

        if (hotfolder.getSuccess() == null || !Files.isDirectory(hotfolder.getSuccess())) {
            throw new FatalIndexerException("Configured path for 'successFolder' does not exist, exiting...");
        }

        running = true;

        if (cleanupAnchors) {
            logger.info("GRIEVING ANCHOR CLEANUP MODE");
            logger.info("Removed {} anchor documents with no volumes.", hotfolder.getSearchIndex().removeGrievingAnchors());
            logger.info("Shutting down...");
            running = false;
            return;
        }
        // Set the hotfolder sleep interval
        try {
            sleepInterval = Integer.valueOf(Configuration.getInstance().getConfiguration("sleep"));
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

        logger.info("Using {} CPU thread(s).", Configuration.getInstance().getThreads());

        Utils.submitDataToViewer(hotfolder.countRecordFiles());

        // main loop
        logger.info("Program started, monitoring hotfolder...");
        while (running) {
            hotfolder.scan();
            try {
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * <p>
     * stop.
     * </p>
     */
    public void stop() {
        logger.info("Stopping indexer...");
        try {
            Configuration.getInstance().killReloadTimer();
        } catch (FatalIndexerException e) {
            logger.error(e.getMessage());
        }
        running = false;
    }

    private static boolean checkSolrSchemaName(Document doc) {
        if (doc != null) {
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
        } else {
            logger.error("Could not read the Solr schema name.");
        }

        return false;
    }
}
