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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.mail.MessagingException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.jdom2.Document;
import org.jdom2.output.XMLOutputter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.DenkXwebIndexer;
import io.goobi.viewer.indexer.DocUpdateIndexer;
import io.goobi.viewer.indexer.DublinCoreIndexer;
import io.goobi.viewer.indexer.Indexer;
import io.goobi.viewer.indexer.LidoIndexer;
import io.goobi.viewer.indexer.MetsIndexer;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.Version;
import io.goobi.viewer.indexer.WorldViewsIndexer;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.FatalIndexerException;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.datarepository.strategy.IDataRepositoryStrategy;
import io.goobi.viewer.indexer.model.datarepository.strategy.MaxRecordNumberStrategy;
import io.goobi.viewer.indexer.model.datarepository.strategy.RemainingSpaceStrategy;
import io.goobi.viewer.indexer.model.datarepository.strategy.SingleRepositoryStrategy;

/**
 * <p>
 * Hotfolder class.
 * </p>
 *
 */
public class Hotfolder {

    private static final Logger logger = LoggerFactory.getLogger(Hotfolder.class);

    private static final String SHUTDOWN_FILE = ".SHUTDOWN_INDEXER";
    private static final int WAIT_IF_FILE_EMPTY = 5000;

    /** Constant <code>metsEnabled=true</code> */
    public static boolean metsEnabled = true;
    /** Constant <code>lidoEnabled=true</code> */
    public static boolean lidoEnabled = true;
    /** Constant <code>denkxwebEnabled=true</code> */
    public static boolean denkxwebEnabled = true;
    /** If no indexedDC folder is configured, Dublin Core indexing will be automatically disabled via this flag. */
    public static boolean dcEnabled = true;
    /** Constant <code>worldviewsEnabled=true</code> */
    public static boolean worldviewsEnabled = true;

    private StringWriter swSecondaryLog;
    private WriterAppender secondaryAppender;

    private final SolrSearchIndex searchIndex;
    private final SolrSearchIndex oldSearchIndex;
    private final IDataRepositoryStrategy dataRepositoryStrategy;
    private final Queue<Path> reindexQueue = new LinkedList<>();

    private int minStorageSpace = 2048;
    public long metsFileSizeThreshold = 10485760;
    public long dataFolderSizeThreshold = 157286400;

    private Path hotfolderPath;
    private Path tempFolderPath;
    private Path updatedMets;
    private Path deletedMets;
    private Path errorMets;
    private Path origLido;
    private Path origDenkxWeb;
    private Path success;

    private Indexer currentIndexer;
    private boolean addVolumeCollectionsToAnchor = false;
    private boolean deleteContentFilesOnFailure = true;

    /** Constant <code>filterDataFile</code> */
    public static FilenameFilter filterDataFile = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return (name.toLowerCase().endsWith(Indexer.XML_EXTENSION) || name.toLowerCase().endsWith(".delete")
                    || name.toLowerCase().endsWith(".purge") || name.endsWith(MetsIndexer.ANCHOR_UPDATE_EXTENSION));
        }
    };

    /** Constant <code>filterMediaFolder</code> */
    public static FilenameFilter filterMediaFolder = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return (new File(dir, name).isDirectory() && (name.endsWith("_tif") || name.endsWith("_media")));
        }
    };

    /**
     * <p>
     * Constructor for Hotfolder.
     * </p>
     *
     * @param confFilename a {@link java.lang.String} object.
     * @param solrClient SolrClient object
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException if any.
     */
    public Hotfolder(String confFilename, SolrClient solrClient) throws FatalIndexerException {
        this(confFilename, solrClient, null);
    }

    /**
     * <p>
     * Constructor for Hotfolder.
     * </p>
     *
     * @param confFilename a {@link java.lang.String} object.
     * @param solrClient SolrClient object
     * @param oldSolrClient Optional old SolrClient for data migration
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException if any.
     */
    @SuppressWarnings("unchecked")
    public Hotfolder(String confFilename, SolrClient solrClient, SolrClient oldSolrClient) throws FatalIndexerException {
        logger.debug("Config file: {}", confFilename);
        Configuration config = Configuration.getInstance(confFilename);

        this.searchIndex = new SolrSearchIndex(solrClient);
        logger.info("Using Solr server at {}", config.getConfiguration("solrUrl"));
        if (oldSolrClient != null) {
            this.oldSearchIndex = new SolrSearchIndex(oldSolrClient);
            logger.info("Also using old Solr server at {}", config.getConfiguration("oldSolrUrl"));
        } else {
            this.oldSearchIndex = null;
        }

        try {
            minStorageSpace = Integer.valueOf(config.getConfiguration("minStorageSpace"));
        } catch (NumberFormatException e) {
            logger.error("<minStorageSpace> must contain a numerical value - using default ({}) instead.", minStorageSpace);
        }
        try {
            //            Path currentDir = Paths.get("");
            hotfolderPath = Paths.get(config.getConfiguration("hotFolder"));
            if (!Utils.checkAndCreateDirectory(hotfolderPath)) {
                logger.error("Could not create folder '{}', exiting...", hotfolderPath);
                throw new FatalIndexerException("Configuration error, see log for details.");
            }
        } catch (Exception e) {
            logger.error("<hotFolder> not defined.");
            throw new FatalIndexerException("Configuration error, see log for details.");
        }

        try {
            String viewerHomePath = config.getViewerHome();
            if (!Files.isDirectory(Paths.get(viewerHomePath))) {
                logger.error("Path defined in <viewerHome> does not exist, exiting...");
                throw new FatalIndexerException("Configuration error, see log for details.");
            }
        } catch (Exception e) {
            logger.error("<viewerHome> not defined, exiting...");
            throw new FatalIndexerException("Configuration error, see log for details.");
        }

        try {
            tempFolderPath = Paths.get(config.getConfiguration("tempFolder"));
            if (!Utils.checkAndCreateDirectory(tempFolderPath)) {
                logger.error("Could not create folder '{}', exiting...", tempFolderPath);
                throw new FatalIndexerException("Configuration error, see log for details.");
            }
        } catch (Exception e) {
            logger.error("<tempFolder> not defined.");
            throw new FatalIndexerException("Configuration error, see log for details.");
        }

        //        String repositoryClassName = "io.goobi.viewer.indexer.model.datarepository.strategy." + config.getDataRepositoryStrategy();
        //        logger.info("Data repositories strategy: {}", repositoryClassName);
        //        try {
        //            dataRepositoryStrategy = (IDataRepositoryStrategy) Class.forName(repositoryClassName).getConstructor(Configuration.class).newInstance(
        //                    config);
        //        } catch (InstantiationException e) {
        //            throw new FatalIndexerException(e.getClass().getName() + ": " + e.getMessage());
        //        } catch (IllegalAccessException e) {
        //            throw new FatalIndexerException(e.getClass().getName() + ": " + e.getMessage());
        //        } catch (ClassNotFoundException e) {
        //            throw new FatalIndexerException(e.getClass().getName() + ": " + e.getMessage());
        //        } catch (IllegalArgumentException e) {
        //            throw new FatalIndexerException(e.getClass().getName() + ": " + e.getMessage());
        //        } catch (InvocationTargetException e) {
        //            throw new FatalIndexerException(e.getClass().getName() + ": " + e.getMessage());
        //        } catch (NoSuchMethodException e) {
        //            throw new FatalIndexerException(e.getClass().getName() + ": " + e.getMessage());
        //        } catch (SecurityException e) {
        //            throw new FatalIndexerException(e.getClass().getName() + ": " + e.getMessage());
        //        }

        logger.info("Data repository strategy: {}", config.getDataRepositoryStrategy());
        switch (config.getDataRepositoryStrategy()) {
            case "SingleRepositoryStrategy":
                dataRepositoryStrategy = new SingleRepositoryStrategy(config);
                break;
            case "MaxRecordNumberStrategy":
                dataRepositoryStrategy = new MaxRecordNumberStrategy(config);
                break;
            case "RemainingSpaceStrategy":
                dataRepositoryStrategy = new RemainingSpaceStrategy(config);
                break;
            default:
                logger.error("Unknown data repository strategy: '{}', using SingleRepositoryStrategy instead.");
                dataRepositoryStrategy = new SingleRepositoryStrategy(config);
        }

        // METS folders
        if (config.getConfiguration(DataRepository.PARAM_INDEXED_METS) == null) {
            metsEnabled = false;
            logger.warn("<{}> not defined - METS indexing is disabled.", DataRepository.PARAM_INDEXED_METS);
        }
        try {
            updatedMets = Paths.get(config.getConfiguration("updatedMets"));
            if (!Utils.checkAndCreateDirectory(updatedMets)) {
                throw new FatalIndexerException("Could not create directory : " + updatedMets.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            throw new FatalIndexerException("<updatedMets> not defined.");
        }
        try {
            deletedMets = Paths.get(config.getConfiguration("deletedMets"));
            if (!Utils.checkAndCreateDirectory(deletedMets)) {
                throw new FatalIndexerException("Could not create directory : " + deletedMets.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            throw new FatalIndexerException("<deletedMets> not defined.");
        }
        try {
            errorMets = Paths.get(config.getConfiguration("errorMets"));
            if (!Utils.checkAndCreateDirectory(errorMets)) {
                throw new FatalIndexerException("Could not create directory : " + errorMets.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            throw new FatalIndexerException("<errorMets> not defined.");
        }

        // LIDO folders
        if (config.getConfiguration(DataRepository.PARAM_INDEXED_LIDO) == null) {
            lidoEnabled = false;
            logger.warn("<{}> not defined - LIDO indexing is disabled.", DataRepository.PARAM_INDEXED_LIDO);
        }
        if (config.getConfiguration("origLido") != null) {
            origLido = Paths.get(config.getConfiguration("origLido"));
            if (!Utils.checkAndCreateDirectory(origLido)) {
                throw new FatalIndexerException("Could not create directory : " + origLido.toAbsolutePath().toString());
            }
        } else {
            lidoEnabled = false;
            logger.warn("<origLido> not defined - LIDO indexing is disabled.");
        }

        // DenkXweb folders
        if (config.getConfiguration(DataRepository.PARAM_INDEXED_DENKXWEB) == null) {
            denkxwebEnabled = false;
            logger.warn("<{}> not defined - DenkXweb indexing is disabled.", DataRepository.PARAM_INDEXED_DENKXWEB);
        }
        if (config.getConfiguration("origDenkXweb") != null) {
            origDenkxWeb = Paths.get(config.getConfiguration("origDenkXweb"));
            if (!Utils.checkAndCreateDirectory(origDenkxWeb)) {
                throw new FatalIndexerException("Could not create directory : " + origDenkxWeb.toAbsolutePath().toString());
            }
        } else {
            denkxwebEnabled = false;
            logger.warn("<origDenkXweb> not defined - DenkXweb indexing is disabled.");
        }

        // Dublin Core folder
        if (config.getConfiguration(DataRepository.PARAM_INDEXED_DUBLINCORE) == null) {
            dcEnabled = false;
            logger.warn("<{}> not defined - Dublin Core indexing is disabled.", DataRepository.PARAM_INDEXED_DUBLINCORE);
        }

        try {
            success = Paths.get(config.getConfiguration("successFolder"));
            if (!Utils.checkAndCreateDirectory(success)) {
                throw new FatalIndexerException("Could not create directory : " + success.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            throw new FatalIndexerException("<successFolder> not defined.");
        }
        metsFileSizeThreshold = Configuration.getInstance().getInt("performance.metsFileSizeThreshold", 10485760);
        dataFolderSizeThreshold = Configuration.getInstance().getInt("performance.dataFolderSizeThreshold", 157286400);

        SolrSearchIndex.optimize = Boolean.valueOf(Configuration.getInstance().isAutoOptimize());
        logger.info("Auto-optimize: {}", SolrSearchIndex.optimize);

        try {
            addVolumeCollectionsToAnchor = Configuration.getInstance().isAddVolumeCollectionsToAnchor();
            if (addVolumeCollectionsToAnchor) {
                logger.info("Volume collections WILL BE ADDED to anchors.");
            } else {
                logger.info("Volume collections WILL NOT BE ADDED to anchors.");
            }
        } catch (Exception e) {
            logger.error("<addVolumeCollectionsToAnchor> not defined.");
        }

        String temp = Configuration.getInstance().getConfiguration("deleteContentFilesOnFailure");
        if (temp != null) {
            deleteContentFilesOnFailure = Boolean.valueOf(temp);
        }
        if (deleteContentFilesOnFailure) {
            logger.info("Content files will be REMOVED from the hotfolder in case of indexing errors.");
        } else {
            logger.info("Content files will be PRESERVED in the hotfolder in case of indexing errors.");
        }

        // Norm data fields to be added to DEFAULT
        MetadataHelper.addNormDataFieldsToDefault = config.getList("init.addNormDataFieldsToDefault.field");
        if (MetadataHelper.addNormDataFieldsToDefault != null) {
            for (String field : MetadataHelper.addNormDataFieldsToDefault) {
                logger.info("{} values will be added to DEFAULT", field);
            }
        }
    }

    /**
     * Empties and re-inits the secondary logger.
     */
    private void resetSecondaryLog() {
        if (swSecondaryLog != null) {
            try {
                swSecondaryLog.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        swSecondaryLog = new StringWriter();

        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        final org.apache.logging.log4j.core.config.Configuration config = context.getConfiguration();
        if (secondaryAppender != null) {
            secondaryAppender.stop();
            context.getRootLogger().removeAppender(secondaryAppender);
        }

        final PatternLayout layout = PatternLayout.newBuilder()
                .withPattern("%-5level %d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] (%F\\:%M\\:%L)%n        %msg%n")
                .withConfiguration(config)
                .build();
        secondaryAppender = WriterAppender.createAppender(layout, null, swSecondaryLog, "record_appender", true, true);
        secondaryAppender.start();
        config.addAppender(secondaryAppender);
        context.getRootLogger().addAppender(secondaryAppender);
        context.updateLoggers();
    }

    private static void checkAndSendErrorReport(String subject, String body) throws FatalIndexerException {
        logger.debug("body:\n{}", body);
        // Send report e-mail if the text body contains at least one ERROR level log message
        if (!body.contains("ERROR")) {
            return;
        }

        String recipients = Configuration.getInstance().getString("init.email.recipients");
        if (StringUtils.isEmpty(recipients)) {
            logger.warn("init.email.recipients not configured, cannot send e-mail report.");
            return;
        }
        String smtpServer = Configuration.getInstance().getString("init.email.smtpServer");
        if (StringUtils.isEmpty(smtpServer)) {
            logger.warn("init.email.smtpServer not configured, cannot send e-mail report.");
            return;
        }
        String smtpUser = Configuration.getInstance().getString("init.email.smtpUser");
        String smtpPassword = Configuration.getInstance().getString("init.email.smtpPassword");
        String smtpSenderAddress = Configuration.getInstance().getString("init.email.smtpSenderAddress");
        if (StringUtils.isEmpty(smtpSenderAddress)) {
            logger.debug("init.email.smtpSenderAddress not configured, cannot send e-mail report.");
            return;
        }
        String smtpSenderName = Configuration.getInstance().getString("init.email.smtpSenderName");
        if (StringUtils.isEmpty(smtpSenderName)) {
            logger.warn("init.email.smtpSenderName not configured, cannot send e-mail report.");
            return;
        }
        String smtpSecurity = Configuration.getInstance().getString("init.email.smtpSecurity");
        if (StringUtils.isEmpty(smtpSecurity)) {
            logger.warn("init.email.smtpSecurity not configured, cannot send e-mail report.");
            return;
        }
        String[] recipientsSplit = recipients.split(";");

        try {
            Utils.postMail(Arrays.asList(recipientsSplit), subject, body, smtpServer, smtpUser, smtpPassword, smtpSenderAddress, smtpSenderName,
                    smtpSecurity);
        } catch (UnsupportedEncodingException | MessagingException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Scans the hotfolder for new files and executes appropriate actions.
     *
     * @return boolean true if successful; false othewise.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     */
    public boolean scan() throws FatalIndexerException {
        boolean noerror = true;
        if (!Files.isDirectory(hotfolderPath)) {
            noerror = false;
            logger.error("Hotfolder not found!");
            return noerror;

        }
        Path fileToReindex = reindexQueue.poll();
        if (fileToReindex != null) {
            resetSecondaryLog();
            logger.info("Found file '{}' (re-index queue).", fileToReindex.getFileName());
            checkFreeSpace();
            Map<String, Boolean> reindexSettings = new HashMap<>();
            reindexSettings.put(DataRepository.PARAM_FULLTEXT, true);
            reindexSettings.put(DataRepository.PARAM_TEIWC, true);
            reindexSettings.put(DataRepository.PARAM_ALTO, true);
            reindexSettings.put(DataRepository.PARAM_MIX, true);
            reindexSettings.put(DataRepository.PARAM_UGC, true);
            noerror = handleDataFile(fileToReindex, true, reindexSettings);
            if (swSecondaryLog != null) {
                checkAndSendErrorReport(fileToReindex.getFileName() + ": Indexing failed (" + Version.asString() + ")",
                        swSecondaryLog.toString());
            }
        } else {
            // Check for the shutdown trigger file first
            Path shutdownFile = Paths.get(hotfolderPath.toAbsolutePath().toString(), SHUTDOWN_FILE);
            if (currentIndexer == null && Files.exists(shutdownFile)) {
                logger.info("Shutdown trigger file detected, shutting down...");
                try {
                    Files.delete(shutdownFile);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
                SolrIndexerDaemon.getInstance().stop();
                return true;
            }
            logger.trace("Hotfolder: Listing files...");
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, "*.{xml,delete,purge,docupdate,UPDATED}")) {
                for (Path path : stream) {
                    // Only one file at a time right now
                    if (currentIndexer != null) {
                        break;
                    }
                    Path recordFile = path;
                    if (!recordFile.getFileName().toString().endsWith(MetsIndexer.ANCHOR_UPDATE_EXTENSION)) {
                        // Check whether the data folders for this record have been copied completely, otherwise skip
                        if (!isDataFolderExportDone(recordFile)) {
                            logger.trace("Export not yet finished for '{}'", recordFile.getFileName());
                            continue;
                        }
                        resetSecondaryLog();
                        logger.info("Found file '{}' (hotfolder).", recordFile.getFileName());
                        if (MetsIndexer.noTimestampUpdate) {
                            logger.warn("WARNING: No update mode - DATEUPDATED timestamps will not be updated.");
                        }
                        checkFreeSpace();
                        Map<String, Boolean> reindexSettings = new HashMap<>();
                        reindexSettings.put(DataRepository.PARAM_FULLTEXT, false);
                        reindexSettings.put(DataRepository.PARAM_TEIWC, false);
                        reindexSettings.put(DataRepository.PARAM_ALTO, false);
                        reindexSettings.put(DataRepository.PARAM_MIX, false);
                        reindexSettings.put(DataRepository.PARAM_UGC, false);
                        noerror = handleDataFile(recordFile, false, reindexSettings);
                        // logger.error("for the lulz");
                        checkAndSendErrorReport(recordFile.getFileName() + ": Indexing failed (" + Version.asString() + ")",
                                swSecondaryLog.toString());
                    } else {
                        logger.info("Found file '{}' which is not in the re-index queue. This file will be deleted.", recordFile.getFileName());
                        Files.delete(recordFile);
                    }
                    break; // always break after attempting to index a file, so that the loop restarts
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

        return noerror;
    }

    /**
     * Checks whether there is at least as much free storage space available as configured. If not, the program will shut down.
     * 
     * @throws FatalIndexerException
     */
    private void checkFreeSpace() throws FatalIndexerException {
        // TODO alternate check if RemainingSpaceStrategy is selected
        int freeSpace = (int) (hotfolderPath.toFile().getFreeSpace() / 1048576);
        logger.debug("Available storage space: {}M", freeSpace);
        if (freeSpace < minStorageSpace) {
            logger.error("Insufficient free space: {} / {} MB available. Indexer will now shut down.", freeSpace, minStorageSpace);
            if (swSecondaryLog != null) {
                checkAndSendErrorReport("Record indexing failed due to insufficient space (" + Version.asString() + ")",
                        swSecondaryLog.toString());
            }
            throw new FatalIndexerException("Insufficient free space");
        }
    }

    /**
     * 
     * @param dataFile
     * @param fromReindexQueue
     * @param reindexSettings
     * @return Boolean
     * @throws FatalIndexerException
     */
    private boolean handleDataFile(Path dataFile, boolean fromReindexQueue, Map<String, Boolean> reindexSettings) throws FatalIndexerException {
        logger.trace("handleDataFile: {}", dataFile);
        // Always unselect repository
        String filename = dataFile.getFileName().toString();
        try {
            if (filename.endsWith(".xml")) {
                // INPUT o. UPDATE
                if (Files.size(dataFile) == 0) {
                    // Check whether the file is actually empty or just hasn't finished copying yet
                    try {
                        Thread.sleep(WAIT_IF_FILE_EMPTY);
                    } catch (InterruptedException e) {
                    }
                    if (Files.size(dataFile) == 0) {
                        logger.error("Empty data file '{}' found, deleting...", dataFile.toAbsolutePath());
                        Files.delete(dataFile);
                        return false;
                    }
                }

                // Check file format and start the appropriate indexing routine
                FileFormat fileType = JDomXP.determineFileFormat(dataFile.toFile());
                switch (fileType) {
                    case METS:
                        if (metsEnabled) {
                            addMetsToIndex(dataFile, fromReindexQueue, reindexSettings);
                        } else {
                            logger.error("METS indexing is disabled - please make sure all folders are configured.");
                            Files.delete(dataFile);
                        }
                        break;
                    case LIDO:
                        if (lidoEnabled) {
                            addLidoToIndex(dataFile, reindexSettings);
                        } else {
                            logger.error("LIDO indexing is disabled - please make sure all folders are configured.");
                            Files.delete(dataFile);
                        }
                        break;
                    case DENKXWEB:
                        if (denkxwebEnabled) {
                            addDenkXwebToIndex(dataFile, reindexSettings);
                        } else {
                            logger.error("DenkXweb indexing is disabled - please make sure all folders are configured.");
                            Files.delete(dataFile);
                        }
                        break;
                    case DUBLINCORE:
                        if (dcEnabled) {
                            addDublinCoreToIndex(dataFile, reindexSettings);
                        } else {
                            logger.error("Dublin Core indexing is disabled - please make sure all folders are configured.");
                            Files.delete(dataFile);
                        }
                        break;
                    case WORLDVIEWS:
                        if (worldviewsEnabled) {
                            addWorldViewsToIndex(dataFile, fromReindexQueue, reindexSettings);
                        } else {
                            logger.error("WorldViews indexing is disabled - please make sure all folders are configured.");
                            Files.delete(dataFile);
                        }
                        break;
                    default:
                        logger.error("Unknown file format, deleting: {}", filename);
                        Files.delete(dataFile);
                        return false;
                }
                Utils.submitVersion();

            } else if (filename.endsWith(".delete")) {
                // DELETE
                DataRepository[] repositories = dataRepositoryStrategy.selectDataRepository(null, dataFile, null, searchIndex, oldSearchIndex);
                removeFromIndex(dataFile, repositories[1] != null ? repositories[1] : repositories[0], true);
            } else if (filename.endsWith(".purge")) {
                // PURGE (delete with no "deleted" doc)
                DataRepository[] repositories = dataRepositoryStrategy.selectDataRepository(null, dataFile, null, searchIndex, oldSearchIndex);
                removeFromIndex(dataFile, repositories[1] != null ? repositories[1] : repositories[0], false);
            } else if (filename.endsWith(MetsIndexer.ANCHOR_UPDATE_EXTENSION)) {
                // SUPERUPDATE
                DataRepository[] repositories = dataRepositoryStrategy.selectDataRepository(null, dataFile, null, searchIndex, oldSearchIndex);
                MetsIndexer.superupdate(dataFile, updatedMets, repositories[1] != null ? repositories[1] : repositories[0]);
            } else if (filename.endsWith(DocUpdateIndexer.FILE_EXTENSION)) {
                // Single Solr document update
                updateSingleDocument(dataFile);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            try {
                Files.delete(dataFile);
            } catch (IOException e1) {
                logger.error(e1.getMessage(), e1);
            }
            return false;
        }

        return true;
    }

    /**
     * Removes the document and its data folders represented by the file name.
     * 
     * @param deleteFile {@link Path}
     * @param dataRepository Data repository in which the record data is stored
     * @param trace A Lucene document with DATEDELETED timestamp will be created if true.
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     */
    private void removeFromIndex(Path deleteFile, DataRepository dataRepository, boolean trace) throws IOException, FatalIndexerException {
        if (deleteFile == null) {
            throw new IllegalArgumentException("deleteFile may not be null");
        }
        if (dataRepository == null) {
            throw new IllegalArgumentException("dataRepository may not be null");
        }

        String baseFileName = FilenameUtils.getBaseName(deleteFile.getFileName().toString());
        try {
            // Check for empty file names, otherwise the entire content folders will be deleted!
            if (StringUtils.isBlank(baseFileName)) {
                logger.error("File '{}' contains no identifier, aborting...", deleteFile.getFileName());
                return;
            }

            Path actualXmlFile =
                    Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), baseFileName + ".xml");
            if (!Files.exists(actualXmlFile)) {
                actualXmlFile =
                        Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toAbsolutePath().toString(), baseFileName + ".xml");
            }
            if (!Files.exists(actualXmlFile)) {
                actualXmlFile =
                        Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_DENKXWEB).toAbsolutePath().toString(), baseFileName + ".xml");
            }
            if (!Files.exists(actualXmlFile)) {
                actualXmlFile =
                        Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE).toAbsolutePath().toString(), baseFileName + ".xml");
            }
            FileFormat format = FileFormat.UNKNOWN;
            if (!Files.exists(actualXmlFile)) {
                logger.warn("XML file '{}' not found.", actualXmlFile.getFileName().toString());
            }
            // Determine document format
            String[] fields = { SolrConstants.SOURCEDOCFORMAT, SolrConstants.DATEDELETED, SolrConstants.DOCTYPE };
            SolrDocumentList result = searchIndex.search(SolrConstants.PI + ":" + baseFileName, Arrays.asList(fields));
            if (!result.isEmpty()) {
                SolrDocument doc = result.get(0);
                format = FileFormat.getByName((String) doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
                // Attempt to determine the file format by the path if no SOURCEDOCFORMAT field exists
                if (format.equals(FileFormat.UNKNOWN)) {
                    logger.warn("SOURCEDOCFORMAT not found, attempting to determine the format via the file path...");
                    if (deleteFile.getParent().equals(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS))) {
                        format = FileFormat.METS;
                    } else if (deleteFile.getParent().equals(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO))) {
                        format = FileFormat.LIDO;
                    } else if (deleteFile.getParent().equals(dataRepository.getDir(DataRepository.PARAM_INDEXED_DENKXWEB))) {
                        format = FileFormat.DENKXWEB;
                    } else if (deleteFile.getParent().equals(dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE))) {
                        format = FileFormat.DUBLINCORE;
                    } else if (doc.containsKey(SolrConstants.DATEDELETED)) {
                        format = FileFormat.METS;
                        trace = false;
                        logger.info("Record '{}' is already a trace document and will be removed without a trace.", baseFileName);
                    } else if (DocType.GROUP.name().equals(doc.getFirstValue(SolrConstants.DOCTYPE))) {
                        format = FileFormat.METS;
                        trace = false;
                        logger.info("Record '{}' is a group document and will be removed without a trace.", baseFileName);
                    }
                }
            } else {
                logger.error("Record '{}' not found in index.", baseFileName);
                return;
            }

            boolean success = false;
            switch (format) {
                case METS:
                case LIDO:
                case DENKXWEB:
                case DUBLINCORE:
                case WORLDVIEWS:
                    if (trace) {
                        logger.info("Deleting {} file '{}'...", format.name(), actualXmlFile.getFileName());
                    } else {
                        logger.info("Deleting {} file '{}' (no trace document will be created)...", format.name(), actualXmlFile.getFileName());
                    }
                    success = Indexer.delete(baseFileName, trace, searchIndex);
                    break;
                default:
                    logger.error("Unknown format: {}", format);
                    return;
            }
            if (success) {
                dataRepository.deleteDataFoldersForRecord(baseFileName);
                if (actualXmlFile.toFile().exists()) {
                    Path deleted = Paths.get(deletedMets.toAbsolutePath().toString(), actualXmlFile.getFileName().toString());
                    Files.copy(actualXmlFile, deleted, StandardCopyOption.REPLACE_EXISTING);
                    Files.delete(actualXmlFile);
                    logger.info("'{}' has been successfully deleted.", actualXmlFile.getFileName());
                }
            } else {
                Path errorFile = Paths.get(errorMets.toAbsolutePath().toString(), baseFileName + ".delete_error");
                Files.createFile(errorFile);
                logger.error("'{}' could not be deleted.", actualXmlFile.getFileName());
            }
        } catch (SolrServerException e) {
            logger.error("'{}' could not be deleted.", baseFileName);
            logger.error(e.getMessage(), e);
        } finally {
            try {
                Files.delete(deleteFile);
            } catch (IOException e) {
                logger.warn("'{}' could not be deleted.", deleteFile.toAbsolutePath());
            }
        }
    }

    /**
     * Indexes the given METS file.
     * 
     * @param metsFile {@link File}
     * @param fromReindexQueue
     * @param reindexSettings
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     * 
     */
    private void addMetsToIndex(Path metsFile, boolean fromReindexQueue, Map<String, Boolean> reindexSettings)
            throws IOException, FatalIndexerException {
        // index file
        String[] resp = { null, null };
        Map<String, Path> dataFolders = new HashMap<>();

        String fileNameRoot = FilenameUtils.getBaseName(metsFile.getFileName().toString());

        // Check data folders in the hotfolder
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new StringBuilder(fileNameRoot).append("_*").toString())) {
            for (Path path : stream) {
                logger.info("Found data folder: {}", path.getFileName());
                String fileNameSansRoot = path.getFileName().toString().substring(fileNameRoot.length());
                switch (fileNameSansRoot) {
                    case "_tif":
                    case "_media": // GBVMETSAdapter uses _media folders
                        dataFolders.put(DataRepository.PARAM_MEDIA, path);
                        break;
                    case "_txt":
                        dataFolders.put(DataRepository.PARAM_FULLTEXT, path);
                        break;
                    case "_txtcrowd":
                        dataFolders.put(DataRepository.PARAM_FULLTEXTCROWD, path);
                        break;
                    case "_wc":
                        dataFolders.put(DataRepository.PARAM_TEIWC, path);
                        break;
                    case "_neralto":
                        dataFolders.put(DataRepository.PARAM_ALTO, path);
                        logger.info("NER ALTO folder found: {}", path.getFileName());
                        break;
                    case "_alto":
                        // Only add regular ALTO path if no NER ALTO folder is found
                        if (dataFolders.get(DataRepository.PARAM_ALTO) == null) {
                            dataFolders.put(DataRepository.PARAM_ALTO, path);
                        }
                        break;
                    case "_altocrowd":
                        dataFolders.put(DataRepository.PARAM_ALTOCROWD, path);
                        break;
                    case "_xml":
                        dataFolders.put(DataRepository.PARAM_ABBYY, path);
                        break;
                    case "_pdf":
                        dataFolders.put(DataRepository.PARAM_PAGEPDF, path);
                        break;
                    case "_mix":
                        dataFolders.put(DataRepository.PARAM_MIX, path);
                        break;
                    case "_src":
                        dataFolders.put(DataRepository.PARAM_SOURCE, path);
                        break;
                    case "_ugc":
                        dataFolders.put(DataRepository.PARAM_UGC, path);
                        break;
                    case "_cms":
                        dataFolders.put(DataRepository.PARAM_CMS, path);
                        break;
                    case "_tei":
                        dataFolders.put(DataRepository.PARAM_TEIMETADATA, path);
                        break;
                    case "_annotations":
                        dataFolders.put(DataRepository.PARAM_ANNOTATIONS, path);
                        break;
                    default:
                        // nothing
                }
            }
        }

        // Use existing folders for those missing in the hotfolder
        if (dataFolders.get(DataRepository.PARAM_MEDIA) == null) {
            reindexSettings.put(DataRepository.PARAM_MEDIA, true);
        }
        if (dataFolders.get(DataRepository.PARAM_ALTO) == null) {
            reindexSettings.put(DataRepository.PARAM_ALTO, true);
        }
        if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) == null) {
            reindexSettings.put(DataRepository.PARAM_ALTOCROWD, true);
        }
        if (dataFolders.get(DataRepository.PARAM_FULLTEXT) == null) {
            reindexSettings.put(DataRepository.PARAM_FULLTEXT, true);
        }
        if (dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) == null) {
            reindexSettings.put(DataRepository.PARAM_FULLTEXTCROWD, true);
        }
        if (dataFolders.get(DataRepository.PARAM_TEIWC) == null) {
            reindexSettings.put(DataRepository.PARAM_TEIWC, true);
        }
        if (dataFolders.get(DataRepository.PARAM_ABBYY) == null) {
            reindexSettings.put(DataRepository.PARAM_ABBYY, true);
        }
        if (dataFolders.get(DataRepository.PARAM_MIX) == null) {
            reindexSettings.put(DataRepository.PARAM_MIX, true);
        }
        if (dataFolders.get(DataRepository.PARAM_UGC) == null) {
            reindexSettings.put(DataRepository.PARAM_UGC, true);
        }
        if (dataFolders.get(DataRepository.PARAM_CMS) == null) {
            reindexSettings.put(DataRepository.PARAM_CMS, true);
        }
        if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) == null) {
            reindexSettings.put(DataRepository.PARAM_TEIMETADATA, true);
        }
        if (dataFolders.get(DataRepository.PARAM_ANNOTATIONS) == null) {
            reindexSettings.put(DataRepository.PARAM_ANNOTATIONS, true);
        }

        DataRepository dataRepository;
        DataRepository previousDataRepository;
        try {
            currentIndexer = new MetsIndexer(this);
            resp = ((MetsIndexer) currentIndexer).index(metsFile, fromReindexQueue, dataFolders, null,
                    Configuration.getInstance().getPageCountStart());
        } finally {
            dataRepository = currentIndexer.getDataRepository();
            previousDataRepository = currentIndexer.getPreviousDataRepository();
            currentIndexer = null;
        }

        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            // String newMetsFileName = URLEncoder.encode(resp[0], "utf-8");
            String newMetsFileName = resp[0];
            String pi = FilenameUtils.getBaseName(newMetsFileName);
            Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), newMetsFileName);
            if (metsFile.equals(indexed)) {
                logger.debug("'{}' is an existing indexed file - not moving it.", metsFile.getFileName());
                return;
            }
            if (Files.exists(indexed)) {
                // Add a timestamp to the old file name
                String oldMetsFilename =
                        FilenameUtils.getBaseName(newMetsFileName) + "_" + LocalDateTime.now().format(MetadataHelper.formatterBasicDateTime) + ".xml";
                Path newFile = Paths.get(updatedMets.toAbsolutePath().toString(), oldMetsFilename);
                Files.copy(indexed, newFile);
                logger.debug("Old METS file copied to '{}'.", newFile.toAbsolutePath());
            }
            Files.copy(metsFile, indexed, StandardCopyOption.REPLACE_EXISTING);

            if (previousDataRepository != null) {
                // Move non-repository data folders to the selected repository
                previousDataRepository.moveDataFoldersToRepository(dataRepository, FilenameUtils.getBaseName(newMetsFileName));
            }

            // Copy and delete media folder
            if (dataRepository.checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_MEDIA,
                    dataRepositoryStrategy.getAllDataRepositories()) > 0) {
                String msg = Utils.removeRecordImagesFromCache(FilenameUtils.getBaseName(resp[0]));
                if (msg != null) {
                    logger.info(msg);
                }
            }

            // Copy data folders
            dataRepository.copyAndDeleteAllDataFolders(pi, dataFolders, reindexSettings, dataRepositoryStrategy.getAllDataRepositories());

            // Delete unsupported data folders
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new StringBuilder(fileNameRoot).append("_*").toString())) {
                for (Path path : stream) {
                    if (path.getFileName().toString().startsWith(fileNameRoot) && Files.isDirectory(path)) {
                        if (Utils.deleteDirectory(path)) {
                            logger.info("Deleted unsupported folder '{}'.", path.getFileName());
                        } else {
                            logger.warn("'{}' could not be deleted.", path.toAbsolutePath());
                        }
                    }
                }
            }

            // success for goobi
            Path successFile = Paths.get(success.toAbsolutePath().toString(), metsFile.getFileName().toString());
            try {
                Files.createFile(successFile);
                Files.setLastModifiedTime(successFile, FileTime.fromMillis(System.currentTimeMillis()));
            } catch (FileAlreadyExistsException e) {
                Files.delete(successFile);
                Files.createFile(successFile);
                Files.setLastModifiedTime(successFile, FileTime.fromMillis(System.currentTimeMillis()));
            }

            try {
                Files.delete(metsFile);
            } catch (IOException e) {
                logger.warn("'{}' could not be deleted! Please delete it manually!", metsFile.toAbsolutePath());
            }

            // Update data repository cache map in the Goobi viewer
            if (previousDataRepository != null) {
                Utils.updateDataRepositoryCache(pi, dataRepository.getPath());
            }
        } else {
            // Error
            if (deleteContentFilesOnFailure) {
                // Delete all data folders for this record from the hotfolder
                DataRepository.deleteDataFolders(dataFolders, reindexSettings);
            }
            handleError(metsFile, resp[1]);
            try {
                Files.delete(metsFile);
            } catch (IOException e) {
                logger.error("'{}' could not be deleted! Please delete it manually!", metsFile.toAbsolutePath());
            }
        }
    }

    /**
     * Indexes the given LIDO file.
     * 
     * @param lidoFile {@link File}
     * @param reindexSettings
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     * 
     */
    @SuppressWarnings("unchecked")
    private void addLidoToIndex(Path lidoFile, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        logger.debug("Indexing LIDO file '{}'...", lidoFile.getFileName());
        String[] resp = { null, null };
        Map<String, Path> dataFolders = new HashMap<>();
        String fileNameRoot = FilenameUtils.getBaseName(lidoFile.getFileName().toString());

        // Check data folders in the hotfolder
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new StringBuilder(fileNameRoot).append("_*").toString())) {
            for (Path path : stream) {
                logger.info("Found data folder: {}", path.getFileName());
                String fileNameSansRoot = path.getFileName().toString().substring(fileNameRoot.length());
                switch (fileNameSansRoot) {
                    case "_tif":
                    case "_media":
                        dataFolders.put(DataRepository.PARAM_MEDIA, path);
                        break;
                    case "_mix":
                        dataFolders.put(DataRepository.PARAM_MIX, path);
                        break;
                    case "_downloadimages":
                        dataFolders.put(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER, path);
                        break;
                    case "_tei":
                        dataFolders.put(DataRepository.PARAM_TEIMETADATA, path);
                        break;
                    default:
                        // nothing;
                }
            }
        }

        if (dataFolders.containsKey(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER)) {
            logger.info("External images will be downloaded.");
            Path newMediaFolder = Paths.get(hotfolderPath.toString(), fileNameRoot + "_tif");
            dataFolders.put(DataRepository.PARAM_MEDIA, newMediaFolder);
            if (!Files.exists(newMediaFolder)) {
                Files.createDirectory(newMediaFolder);
                logger.info("Created media folder {}", newMediaFolder.toAbsolutePath().toString());
            }
        }

        // Use existing folders for those missing in the hotfolder
        if (dataFolders.get(DataRepository.PARAM_MEDIA) == null) {
            reindexSettings.put(DataRepository.PARAM_MEDIA, true);
        }
        if (dataFolders.get(DataRepository.PARAM_MIX) == null) {
            reindexSettings.put(DataRepository.PARAM_MIX, true);
        }
        if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) == null) {
            reindexSettings.put(DataRepository.PARAM_TEIMETADATA, true);
        }

        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile.toFile());
        logger.info("File contains {} LIDO documents.", lidoDocs.size());
        XMLOutputter outputter = new XMLOutputter();
        boolean errors = false;
        try {
            for (Document doc : lidoDocs) {
                DataRepository dataRepository;
                DataRepository previousDataRepository;
                try {
                    currentIndexer = new LidoIndexer(this);
                    resp = ((LidoIndexer) currentIndexer).index(doc, dataFolders, null, Configuration.getInstance().getPageCountStart(),
                            Configuration.getInstance().getList("init.lido.imageXPath"),
                            dataFolders.containsKey(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER));
                } finally {
                    dataRepository = currentIndexer.getDataRepository();
                    previousDataRepository = currentIndexer.getPreviousDataRepository();
                    currentIndexer = null;
                }
                if (!"ERROR".equals(resp[0])) {
                    // String newMetsFileName = URLEncoder.encode(resp[0], "utf-8");
                    String identifier = resp[0];
                    String newLidoFileName = identifier + ".xml";

                    // Write individual LIDO records as separate files
                    Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toAbsolutePath().toString(), newLidoFileName);
                    try (FileOutputStream out = new FileOutputStream(indexed.toFile())) {
                        outputter.output(doc, out);
                    }

                    // Move non-repository data directories to the selected repository
                    if (previousDataRepository != null) {
                        previousDataRepository.moveDataFoldersToRepository(dataRepository, identifier);
                    }

                    // copy media files
                    boolean mediaFilesCopied = false;
                    Path destMediaDir = Paths.get(dataRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), identifier);
                    if (!Files.exists(destMediaDir)) {
                        Files.createDirectory(destMediaDir);
                    }

                    int imageCounter = 0;
                    if (StringUtils.isNotEmpty(resp[1]) && dataFolders.get(DataRepository.PARAM_MEDIA) != null) {
                        logger.info("Copying image files...");
                        String[] imgFileNamesSplit = resp[1].split(";");
                        Set<String> imgFileNames = new HashSet<>(Arrays.asList(imgFileNamesSplit));
                        try (DirectoryStream<Path> mediaFileStream = Files.newDirectoryStream(dataFolders.get(DataRepository.PARAM_MEDIA))) {
                            for (Path mediaFile : mediaFileStream) {
                                if (Files.isRegularFile(mediaFile) && imgFileNames.contains(mediaFile.getFileName().toString())) {
                                    logger.info("Copying file {} to {}", mediaFile.toAbsolutePath(), destMediaDir.toAbsolutePath());
                                    Files.copy(mediaFile, Paths.get(destMediaDir.toAbsolutePath().toString(), mediaFile.getFileName().toString()),
                                            StandardCopyOption.REPLACE_EXISTING);
                                    imageCounter++;
                                }
                            }
                        }
                        mediaFilesCopied = true;
                    } else {
                        logger.warn("No media file names could be extracted from '{}'.", identifier);
                    }
                    if (!mediaFilesCopied) {
                        logger.warn("No media folder found for '{}'.", lidoFile);

                        // Check for a data folder in different repositories (fixing broken migration from old-style data repositories to new)
                        if (reindexSettings.get(DataRepository.PARAM_MEDIA) != null && reindexSettings.get(DataRepository.PARAM_MEDIA)) {
                            for (DataRepository repo : dataRepositoryStrategy.getAllDataRepositories()) {
                                if (!repo.equals(dataRepository) && repo.getDir(DataRepository.PARAM_MEDIA) != null) {
                                    Path misplacedDataDir =
                                            Paths.get(repo.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), identifier);
                                    if (Files.isDirectory(misplacedDataDir)) {
                                        logger.warn("Found data folder for this record in different data repository: {}",
                                                misplacedDataDir.toAbsolutePath().toString());
                                        logger.warn("Moving data folder to new repository...");
                                        repo.moveDataFolderToRepository(dataRepository, identifier, DataRepository.PARAM_MEDIA);
                                    }
                                }
                            }
                        }
                    } else {
                        logger.info("{} media file(s) copied.", imageCounter);
                    }

                    // Copy and delete MIX files
                    if (!reindexSettings.get(DataRepository.PARAM_MIX)) {
                        Path destMixDir = Paths.get(dataRepository.getDir(DataRepository.PARAM_MIX).toAbsolutePath().toString(), identifier);
                        if (!Files.exists(destMixDir)) {
                            Files.createDirectory(destMixDir);
                        }
                        int counter = 0;
                        if (StringUtils.isNotEmpty(resp[1]) && Files.isDirectory(dataFolders.get(DataRepository.PARAM_MIX))) {
                            String[] mixFileNamesSplit = resp[1].split(";");
                            List<String> mixFileNames = new ArrayList<>();
                            for (String fileName : mixFileNamesSplit) {
                                mixFileNames.add(FilenameUtils.getBaseName(fileName) + ".xml");
                            }
                            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataFolders.get(DataRepository.PARAM_MIX))) {
                                for (Path file : stream) {
                                    if (mixFileNames.contains(file.getFileName().toString())) {
                                        Files.copy(file, Paths.get(destMixDir.toAbsolutePath().toString(), file.getFileName().toString()),
                                                StandardCopyOption.REPLACE_EXISTING);
                                        counter++;
                                    }
                                }
                                logger.info("{} MIX file(s) copied.", counter);
                            }
                        }
                    } else {
                        errors = true;
                        handleError(lidoFile, resp[1]);
                    }

                    // Update data repository cache map in the Goobi viewer
                    if (previousDataRepository != null) {
                        Utils.updateDataRepositoryCache(identifier, dataRepository.getPath());
                    }
                }
            }
        } finally {
            currentIndexer = null;
        }

        // Copy original LIDO file into the orig folder
        Path orig = Paths.get(origLido.toAbsolutePath().toString(), lidoFile.getFileName().toString());
        Files.copy(lidoFile, orig, StandardCopyOption.REPLACE_EXISTING);

        // Delete files from the hotfolder
        try {
            Files.delete(lidoFile);
        } catch (IOException e) {
            logger.error("'{}' could not be deleted; please delete it manually.", lidoFile.toAbsolutePath());
        }
        if (dataFolders.get(DataRepository.PARAM_MEDIA) != null && Files.isDirectory(dataFolders.get(DataRepository.PARAM_MEDIA))
                && !Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_MEDIA))) {
            logger.warn("'{}' could not be deleted; please delete it manually.", dataFolders.get(DataRepository.PARAM_MEDIA).toAbsolutePath());
        }
        if (!reindexSettings.get(DataRepository.PARAM_MIX) && dataFolders.get(DataRepository.PARAM_MIX) != null
                && Files.isDirectory(dataFolders.get(DataRepository.PARAM_MIX))
                && !Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_MIX))) {
            logger.warn("'{}' could not be deleted; please delete it manually.", dataFolders.get(DataRepository.PARAM_MIX).toAbsolutePath());
        }
        if (dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER) != null
                && Files.isDirectory(dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER))
                && !Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER))) {
            logger.warn("'{}' could not be deleted; please delete it manually.",
                    dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER).toAbsolutePath());
        }

        //        if (deleteContentFilesOnFailure) {
        //            // Delete all folders
        //            if (dataFolders.get(DataRepository.PARAM_MEDIA) != null && Files.isDirectory(dataFolders.get(DataRepository.PARAM_MEDIA))) {
        //                Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_MEDIA));
        //            }
        //            if (!reindexSettings.get(DataRepository.PARAM_MIX) && dataFolders.get(DataRepository.PARAM_MIX) != null
        //                    && Files.isDirectory(dataFolders.get(DataRepository.PARAM_MIX))) {
        //                Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_MIX));
        //            }
        //            if (dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER) != null
        //                    && Files.isDirectory(dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER))) {
        //                Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER));
        //            }
        //        }
        if (deleteContentFilesOnFailure) {
            // Delete all data folders for this record from the hotfolder
            DataRepository.deleteDataFolders(dataFolders, reindexSettings);
        }
    }

    /**
     * Indexes the given DenkXweb file.
     * 
     * @param denkxwebFile {@link File}
     * @param reindexSettings
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     * 
     */
    private void addDenkXwebToIndex(Path denkxwebFile, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        if (denkxwebFile == null) {
            throw new IllegalArgumentException("denkxwebFile may not be null");
        }

        logger.debug("Indexing DenkXweb file '{}'...", denkxwebFile.getFileName());
        String[] resp = { null, null };
        Map<String, Path> dataFolders = new HashMap<>();
        String fileNameRoot = FilenameUtils.getBaseName(denkxwebFile.getFileName().toString());

        // Check data folders in the hotfolder
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new StringBuilder(fileNameRoot).append("_*").toString())) {
            for (Path path : stream) {
                logger.info("Found data folder: {}", path.getFileName());
                String fileNameSansRoot = path.getFileName().toString().substring(fileNameRoot.length());
                switch (fileNameSansRoot) {
                    case "_tif":
                    case "_media":
                        dataFolders.put(DataRepository.PARAM_MEDIA, path);
                        break;
                    case "_downloadimages":
                        dataFolders.put(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER, path);
                        break;
                    default:
                        // nothing;
                }
            }
        }

        if (dataFolders.containsKey(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER)) {
            logger.info("External images will be downloaded.");
            Path newMediaFolder = Paths.get(hotfolderPath.toString(), fileNameRoot + "_tif");
            dataFolders.put(DataRepository.PARAM_MEDIA, newMediaFolder);
            if (!Files.exists(newMediaFolder)) {
                Files.createDirectory(newMediaFolder);
                logger.info("Created media folder {}", newMediaFolder.toAbsolutePath().toString());
            }
        }

        // Use existing folders for those missing in the hotfolder
        if (dataFolders.get(DataRepository.PARAM_MEDIA) == null) {
            reindexSettings.put(DataRepository.PARAM_MEDIA, true);
        }

        List<Document> denkxwebDocs = JDomXP.splitDenkXwebFile(denkxwebFile.toFile());
        logger.info("File contains {} DenkXweb documents.", denkxwebDocs.size());
        XMLOutputter outputter = new XMLOutputter();
        boolean errors = false;
        try {
            for (Document doc : denkxwebDocs) {
                DataRepository dataRepository;
                DataRepository previousDataRepository;
                try {
                    currentIndexer = new DenkXwebIndexer(this);
                    resp = ((DenkXwebIndexer) currentIndexer).index(doc, dataFolders, null, Configuration.getInstance().getPageCountStart(),
                            dataFolders.containsKey(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER));
                } finally {
                    dataRepository = currentIndexer.getDataRepository();
                    previousDataRepository = currentIndexer.getPreviousDataRepository();
                    currentIndexer = null;
                }
                if (!"ERROR".equals(resp[0])) {
                    String identifier = resp[0];
                    String newDenkXwebFileName = identifier + ".xml";

                    // Write individual LIDO records as separate files
                    Path indexed =
                            Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_DENKXWEB).toAbsolutePath().toString(), newDenkXwebFileName);
                    try (FileOutputStream out = new FileOutputStream(indexed.toFile())) {
                        outputter.output(doc, out);
                    }

                    // Move non-repository data directories to the selected repository
                    if (previousDataRepository != null) {
                        previousDataRepository.moveDataFoldersToRepository(dataRepository, identifier);
                    }

                    // copy media files
                    boolean mediaFilesCopied = false;
                    Path destMediaDir = Paths.get(dataRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), identifier);
                    if (!Files.exists(destMediaDir)) {
                        Files.createDirectory(destMediaDir);
                    }

                    int imageCounter = 0;
                    if (StringUtils.isNotEmpty(resp[1]) && dataFolders.get(DataRepository.PARAM_MEDIA) != null) {
                        logger.info("Copying image files...");
                        String[] imgFileNamesSplit = resp[1].split(";");
                        Set<String> imgFileNames = new HashSet<>(Arrays.asList(imgFileNamesSplit));
                        try (DirectoryStream<Path> mediaFileStream = Files.newDirectoryStream(dataFolders.get(DataRepository.PARAM_MEDIA))) {
                            for (Path mediaFile : mediaFileStream) {
                                if (Files.isRegularFile(mediaFile) && imgFileNames.contains(mediaFile.getFileName().toString())) {
                                    logger.info("Copying file {} to {}", mediaFile.toAbsolutePath(), destMediaDir.toAbsolutePath());
                                    Files.copy(mediaFile, Paths.get(destMediaDir.toAbsolutePath().toString(), mediaFile.getFileName().toString()),
                                            StandardCopyOption.REPLACE_EXISTING);
                                    imageCounter++;
                                }
                            }
                        }
                        mediaFilesCopied = true;
                    } else {
                        //                        logger.warn("No media file names could be extracted from '{}'.", identifier);
                    }
                    if (!mediaFilesCopied) {
                        logger.debug("No media folder found for '{}'.", denkxwebFile);

                        // Check for a data folder in different repositories (fixing broken migration from old-style data repositories to new)
                        if (reindexSettings.get(DataRepository.PARAM_MEDIA) != null && reindexSettings.get(DataRepository.PARAM_MEDIA)) {
                            for (DataRepository repo : dataRepositoryStrategy.getAllDataRepositories()) {
                                if (!repo.equals(dataRepository) && repo.getDir(DataRepository.PARAM_MEDIA) != null) {
                                    Path misplacedDataDir =
                                            Paths.get(repo.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), identifier);
                                    if (Files.isDirectory(misplacedDataDir)) {
                                        logger.warn("Found data folder for this record in different data repository: {}",
                                                misplacedDataDir.toAbsolutePath().toString());
                                        logger.warn("Moving data folder to new repository...");
                                        repo.moveDataFolderToRepository(dataRepository, identifier, DataRepository.PARAM_MEDIA);
                                    }
                                }
                            }
                        }
                    } else {
                        logger.info("{} media file(s) copied.", imageCounter);
                    }

                    // Update data repository cache map in the Goobi viewer
                    if (previousDataRepository != null) {
                        Utils.updateDataRepositoryCache(identifier, dataRepository.getPath());
                    }
                }
            }
        } finally {
            currentIndexer = null;
        }

        // Copy original DenkXweb file into the orig folder
        Path orig = Paths.get(origDenkxWeb.toAbsolutePath().toString(), denkxwebFile.getFileName().toString());
        Files.copy(denkxwebFile, orig, StandardCopyOption.REPLACE_EXISTING);

        // Delete files from the hotfolder
        try {
            Files.delete(denkxwebFile);
        } catch (IOException e) {
            logger.error("'{}' could not be deleted; please delete it manually.", denkxwebFile.toAbsolutePath());
        }
        if (dataFolders.get(DataRepository.PARAM_MEDIA) != null && Files.isDirectory(dataFolders.get(DataRepository.PARAM_MEDIA))
                && !Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_MEDIA))) {
            logger.warn("'{}' could not be deleted; please delete it manually.", dataFolders.get(DataRepository.PARAM_MEDIA).toAbsolutePath());
        }
        if (dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER) != null
                && Files.isDirectory(dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER))
                && !Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER))) {
            logger.warn("'{}' could not be deleted; please delete it manually.",
                    dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER).toAbsolutePath());
        }

        //        if (deleteContentFilesOnFailure) {
        //            // Delete all folders
        //            if (dataFolders.get(DataRepository.PARAM_MEDIA) != null && Files.isDirectory(dataFolders.get(DataRepository.PARAM_MEDIA))) {
        //                Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_MEDIA));
        //            }
        //            if (dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER) != null
        //                    && Files.isDirectory(dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER))) {
        //                Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER));
        //            }
        //        }
        if (deleteContentFilesOnFailure) {
            // Delete all data folders for this record from the hotfolder
            DataRepository.deleteDataFolders(dataFolders, reindexSettings);
        }
    }

    /**
     * Indexes the given DublinCore file.
     * 
     * @param dcFile {@link File}
     * @param reindexSettings
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     * 
     */
    private void addDublinCoreToIndex(Path dcFile, Map<String, Boolean> reindexSettings)
            throws IOException, FatalIndexerException {
        // index file
        String[] resp = { null, null };
        Map<String, Path> dataFolders = new HashMap<>();

        String fileNameRoot = FilenameUtils.getBaseName(dcFile.getFileName().toString());

        // Check data folders in the hotfolder
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new StringBuilder(fileNameRoot).append("_*").toString())) {
            for (Path path : stream) {
                logger.info("Found data folder: {}", path.getFileName());
                String fileNameSansRoot = path.getFileName().toString().substring(fileNameRoot.length());
                switch (fileNameSansRoot) {
                    case "_tif":
                    case "_media": // GBVMETSAdapter uses _media folders
                        dataFolders.put(DataRepository.PARAM_MEDIA, path);
                        break;
                    case "_txt":
                        dataFolders.put(DataRepository.PARAM_FULLTEXT, path);
                        break;
                    case "_txtcrowd":
                        dataFolders.put(DataRepository.PARAM_FULLTEXTCROWD, path);
                        break;
                    case "_wc":
                        dataFolders.put(DataRepository.PARAM_TEIWC, path);
                        break;
                    case "_neralto":
                        dataFolders.put(DataRepository.PARAM_ALTO, path);
                        logger.info("NER ALTO folder found: {}", path.getFileName());
                        break;
                    case "_alto":
                        // Only add regular ALTO path if no NER ALTO folder is found
                        if (dataFolders.get(DataRepository.PARAM_ALTO) == null) {
                            dataFolders.put(DataRepository.PARAM_ALTO, path);
                        }
                        break;
                    case "_altocrowd":
                        dataFolders.put(DataRepository.PARAM_ALTOCROWD, path);
                        break;
                    case "_xml":
                        dataFolders.put(DataRepository.PARAM_ABBYY, path);
                        break;
                    case "_pdf":
                        dataFolders.put(DataRepository.PARAM_PAGEPDF, path);
                        break;
                    case "_mix":
                        dataFolders.put(DataRepository.PARAM_MIX, path);
                        break;
                    case "_src":
                        dataFolders.put(DataRepository.PARAM_SOURCE, path);
                        break;
                    case "_ugc":
                        dataFolders.put(DataRepository.PARAM_UGC, path);
                        break;
                    case "_cms":
                        dataFolders.put(DataRepository.PARAM_CMS, path);
                        break;
                    case "_tei":
                        dataFolders.put(DataRepository.PARAM_TEIMETADATA, path);
                        break;
                    case "_annotations":
                        dataFolders.put(DataRepository.PARAM_ANNOTATIONS, path);
                        break;
                    default:
                        // nothing
                }
            }
        }

        // Use existing folders for those missing in the hotfolder
        if (dataFolders.get(DataRepository.PARAM_MEDIA) == null) {
            reindexSettings.put(DataRepository.PARAM_MEDIA, true);
        }
        if (dataFolders.get(DataRepository.PARAM_ALTO) == null) {
            reindexSettings.put(DataRepository.PARAM_ALTO, true);
        }
        if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) == null) {
            reindexSettings.put(DataRepository.PARAM_ALTOCROWD, true);
        }
        if (dataFolders.get(DataRepository.PARAM_FULLTEXT) == null) {
            reindexSettings.put(DataRepository.PARAM_FULLTEXT, true);
        }
        if (dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) == null) {
            reindexSettings.put(DataRepository.PARAM_FULLTEXTCROWD, true);
        }
        if (dataFolders.get(DataRepository.PARAM_TEIWC) == null) {
            reindexSettings.put(DataRepository.PARAM_TEIWC, true);
        }
        if (dataFolders.get(DataRepository.PARAM_ABBYY) == null) {
            reindexSettings.put(DataRepository.PARAM_ABBYY, true);
        }
        if (dataFolders.get(DataRepository.PARAM_MIX) == null) {
            reindexSettings.put(DataRepository.PARAM_MIX, true);
        }
        if (dataFolders.get(DataRepository.PARAM_UGC) == null) {
            reindexSettings.put(DataRepository.PARAM_UGC, true);
        }
        if (dataFolders.get(DataRepository.PARAM_CMS) == null) {
            reindexSettings.put(DataRepository.PARAM_CMS, true);
        }
        if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) == null) {
            reindexSettings.put(DataRepository.PARAM_TEIMETADATA, true);
        }
        if (dataFolders.get(DataRepository.PARAM_ANNOTATIONS) == null) {
            reindexSettings.put(DataRepository.PARAM_ANNOTATIONS, true);
        }

        DataRepository dataRepository;
        DataRepository previousDataRepository;
        try {
            currentIndexer = new DublinCoreIndexer(this);
            resp = ((DublinCoreIndexer) currentIndexer).index(dcFile, dataFolders, null,
                    Configuration.getInstance().getPageCountStart());
        } finally {
            dataRepository = currentIndexer.getDataRepository();
            previousDataRepository = currentIndexer.getPreviousDataRepository();
            currentIndexer = null;
        }

        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            String newDcFileName = resp[0];
            String pi = FilenameUtils.getBaseName(newDcFileName);
            Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE).toAbsolutePath().toString(), newDcFileName);
            if (dcFile.equals(indexed)) {
                logger.debug("'{}' is an existing indexed file - not moving it.", dcFile.getFileName());
                return;
            }
            Files.copy(dcFile, indexed, StandardCopyOption.REPLACE_EXISTING);

            if (previousDataRepository != null) {
                // Move non-repository data folders to the selected repository
                previousDataRepository.moveDataFoldersToRepository(dataRepository, FilenameUtils.getBaseName(newDcFileName));
            }

            // Copy and delete media folder
            if (dataRepository.checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_MEDIA,
                    dataRepositoryStrategy.getAllDataRepositories()) > 0) {
                String msg = Utils.removeRecordImagesFromCache(FilenameUtils.getBaseName(resp[0]));
                if (msg != null) {
                    logger.info(msg);
                }
            }

            // Copy data folders
            dataRepository.copyAndDeleteAllDataFolders(pi, dataFolders, reindexSettings, dataRepositoryStrategy.getAllDataRepositories());

            // Delete unsupported data folders
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new StringBuilder(fileNameRoot).append("_*").toString())) {
                for (Path path : stream) {
                    if (path.getFileName().toString().startsWith(fileNameRoot) && Files.isDirectory(path)) {
                        if (Utils.deleteDirectory(path)) {
                            logger.info("Deleted unsupported folder '{}'.", path.getFileName());
                        } else {
                            logger.warn("'{}' could not be deleted.", path.toAbsolutePath());
                        }
                    }
                }
            }

            try {
                Files.delete(dcFile);
            } catch (IOException e) {
                logger.warn("'{}' could not be deleted! Please delete it manually!", dcFile.toAbsolutePath());
            }

            // Update data repository cache map in the Goobi viewer
            if (previousDataRepository != null) {
                Utils.updateDataRepositoryCache(pi, dataRepository.getPath());
            }
        } else {
            // Error
            if (deleteContentFilesOnFailure) {
                // Delete all data folders for this record from the hotfolder
                DataRepository.deleteDataFolders(dataFolders, reindexSettings);
            }
            try {
                Files.delete(dcFile);
            } catch (IOException e) {
                logger.error("'{}' could not be deleted! Please delete it manually!", dcFile.toAbsolutePath());
            }
        }
    }

    /**
     * Indexes the given WorldViews file.
     * 
     * @param mainFile {@link File}
     * @param fromReindexQueue
     * @param reindexSettings
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     * 
     */
    private void addWorldViewsToIndex(Path mainFile, boolean fromReindexQueue, Map<String, Boolean> reindexSettings)
            throws IOException, FatalIndexerException {
        logger.debug("Indexing WorldViews file '{}'...", mainFile.getFileName());
        String[] resp = { null, null };
        Map<String, Path> dataFolders = new HashMap<>();
        boolean useOldPyramidTiffs = false;

        String fileNameRoot = FilenameUtils.getBaseName(mainFile.getFileName().toString());

        // Check data folders in the hotfolder
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new StringBuilder(fileNameRoot).append("_*").toString())) {
            for (Path path : stream) {
                logger.info("Found data folder: {}", path.getFileName());
                String fileNameSansRoot = path.getFileName().toString().substring(fileNameRoot.length());
                switch (fileNameSansRoot) {
                    case "_tif":
                    case "_media":
                        dataFolders.put(DataRepository.PARAM_MEDIA, path);
                        break;
                    case "_tei":
                        dataFolders.put(DataRepository.PARAM_TEIMETADATA, path);
                        break;
                    case "_cmdi":
                        dataFolders.put(DataRepository.PARAM_CMDI, path);
                        break;
                    default:
                        // nothing;
                }
            }
        }

        // Use existing folders for those missing in the hotfolder
        if (dataFolders.get(DataRepository.PARAM_MEDIA) == null) {
            reindexSettings.put(DataRepository.PARAM_MEDIA, true);
        }
        if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) == null) {
            reindexSettings.put(DataRepository.PARAM_TEIMETADATA, true);
        }
        if (dataFolders.get(DataRepository.PARAM_CMDI) == null) {
            reindexSettings.put(DataRepository.PARAM_CMDI, true);
        }

        DataRepository dataRepository;
        DataRepository previousDataRepository;
        try {
            currentIndexer = new WorldViewsIndexer(this);
            resp = ((WorldViewsIndexer) currentIndexer).index(mainFile, fromReindexQueue, dataFolders, null,
                    Configuration.getInstance().getPageCountStart());
        } finally {
            dataRepository = currentIndexer.getDataRepository();
            previousDataRepository = currentIndexer.getPreviousDataRepository();
            currentIndexer = null;
        }

        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            // String newMetsFileName = URLEncoder.encode(resp[0], "utf-8");
            String newMetsFileName = resp[0];
            String pi = FilenameUtils.getBaseName(newMetsFileName);
            Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), newMetsFileName);
            if (mainFile.equals(indexed)) {
                logger.debug("'{}' is an existing indexed file - not moving it.", mainFile.getFileName());
                return;
            }
            if (Files.exists(indexed)) {
                // Add a timestamp to the old file name
                String oldMetsFilename =
                        FilenameUtils.getBaseName(newMetsFileName) + "_" + LocalDateTime.now().format(MetadataHelper.formatterBasicDateTime) + ".xml";
                Path newFile = Paths.get(updatedMets.toAbsolutePath().toString(), oldMetsFilename);
                Files.copy(indexed, newFile);
                logger.debug("Old METS file copied to '{}'.", newFile.toAbsolutePath());
            }
            Files.copy(mainFile, indexed, StandardCopyOption.REPLACE_EXISTING);

            if (previousDataRepository != null) {
                // Move non-repository data folders to the selected repository
                previousDataRepository.moveDataFoldersToRepository(dataRepository, FilenameUtils.getBaseName(newMetsFileName));
            }

            // Copy and delete media folder
            if (dataRepository.checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_MEDIA,
                    dataRepositoryStrategy.getAllDataRepositories()) > 0) {
                String msg = Utils.removeRecordImagesFromCache(FilenameUtils.getBaseName(resp[0]));
                if (msg != null) {
                    logger.info(msg);
                }
            }

            // Copy other data folders
            dataRepository.copyAndDeleteAllDataFolders(pi, dataFolders, reindexSettings, dataRepositoryStrategy.getAllDataRepositories());

            // Delete unsupported data folders
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new StringBuilder(fileNameRoot).append("_*").toString())) {
                for (Path path : stream) {
                    if (path.getFileName().toString().startsWith(fileNameRoot) && Files.isDirectory(path)) {
                        if (Utils.deleteDirectory(path)) {
                            logger.info("Deleted unsupported folder '{}'.", path.getFileName());
                        } else {
                            logger.warn("'{}' could not be deleted.", path.toAbsolutePath());
                        }
                    }
                }
            }

            // Create success file for Goobi workflow
            Path successFile = Paths.get(success.toAbsolutePath().toString(), mainFile.getFileName().toString());
            try {
                Files.createFile(successFile);
                Files.setLastModifiedTime(successFile, FileTime.fromMillis(System.currentTimeMillis()));
            } catch (FileAlreadyExistsException e) {
                Files.delete(successFile);
                Files.createFile(successFile);
                Files.setLastModifiedTime(successFile, FileTime.fromMillis(System.currentTimeMillis()));
            }

            try {
                Files.delete(mainFile);
            } catch (IOException e) {
                logger.error("'{}' could not be deleted! Please delete it manually!", mainFile.toAbsolutePath());
            }

            // Update data repository cache map in the Goobi viewer
            if (previousDataRepository != null) {
                Utils.updateDataRepositoryCache(pi, dataRepository.getPath());
            }
        } else {
            // Error
            if (deleteContentFilesOnFailure) {
                // Delete all data folders for this record from the hotfolder
                DataRepository.deleteDataFolders(dataFolders, reindexSettings);
            }
            handleError(mainFile, resp[1]);
            try {
                Files.delete(mainFile);
            } catch (IOException e) {
                logger.error("'{}' could not be deleted! Please delete it manually!", mainFile.toAbsolutePath());
            }
        }
    }

    /**
     * Updates the Solr document described by the given data file with content from data folders in the hotfolder.
     * 
     * @param dataFile {@link File}
     * @param reindexSettings
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     * 
     */
    private void updateSingleDocument(Path dataFile) throws IOException, FatalIndexerException {
        // index file
        String[] resp = { null, null };
        Map<String, Path> dataFolders = new HashMap<>();

        String fileNameRoot = FilenameUtils.getBaseName(dataFile.getFileName().toString());

        // Check data folders in the hotfolder
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new StringBuilder(fileNameRoot).append("_*").toString())) {
            for (Path path : stream) {
                logger.info("Found data folder: {}", path.getFileName());
                String fileNameSansRoot = path.getFileName().toString().substring(fileNameRoot.length());
                switch (fileNameSansRoot) {
                    case "_txtcrowd":
                        dataFolders.put(DataRepository.PARAM_FULLTEXTCROWD, path);
                        break;
                    case "_altocrowd":
                        dataFolders.put(DataRepository.PARAM_ALTOCROWD, path);
                        break;
                    case "_ugc":
                        dataFolders.put(DataRepository.PARAM_UGC, path);
                        break;
                    case "_cms":
                        dataFolders.put(DataRepository.PARAM_CMS, path);
                        break;
                    default:
                        // nothing
                }
            }
        }

        if (dataFolders.isEmpty()) {
            logger.info("No data folders found for '{}', file won't be processed.", dataFile.getFileName().toString());
            try {
                Files.delete(dataFile);
            } catch (IOException e) {
                logger.error("'{}' could not be deleted! Please delete it manually!", dataFile.toAbsolutePath());
            }
            return;
        }

        DataRepository dataRepository;
        try {
            currentIndexer = new DocUpdateIndexer(this);
            resp = ((DocUpdateIndexer) currentIndexer).index(dataFile, dataFolders);
        } finally {
            dataRepository = currentIndexer.getDataRepository();
            currentIndexer = null;
        }

        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            String pi = resp[0];

            dataRepository.copyAndDeleteAllDataFolders(pi, dataFolders, new HashMap<>(), dataRepositoryStrategy.getAllDataRepositories());

            // Delete unsupported data folders
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new StringBuilder(fileNameRoot).append("_*").toString())) {
                for (Path path : stream) {
                    if (path.getFileName().toString().startsWith(fileNameRoot) && Files.isDirectory(path)) {
                        if (Utils.deleteDirectory(path)) {
                            logger.info("Deleted unsupported folder '{}'.", path.getFileName());
                        } else {
                            logger.warn("'{}' could not be deleted.", path.toAbsolutePath());
                        }
                    }
                }
            }

            try {
                Files.delete(dataFile);
            } catch (IOException e) {
                logger.error("'{}' could not be deleted! Please delete it manually!", dataFile.toAbsolutePath());
            }

        } else {
            // Error
            logger.error(resp[1]);
            if (deleteContentFilesOnFailure) {
                // Delete all data folders in hotfolder
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new DirectoryStream.Filter<Path>() {

                    @Override
                    public boolean accept(Path entry) throws IOException {
                        return Files.isDirectory(entry)
                                && (entry.getFileName().toString().endsWith("_tif") || entry.getFileName().toString().endsWith("_media"));
                    }
                });) {
                    for (Path path : stream) {
                        logger.info("Found data folder: {}", path.getFileName());
                        Utils.deleteDirectory(path);
                    }
                }

                if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) != null) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_ALTOCROWD));
                }
                if (dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) != null) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD));
                }
                if (dataFolders.get(DataRepository.PARAM_UGC) != null) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_UGC));
                }
            }
            handleError(dataFile, resp[1]);
            try {
                Files.delete(dataFile);
            } catch (IOException e) {
                logger.error("'{}' could not be deleted! Please delete it manually!", dataFile.toAbsolutePath());
            }
        }
    }

    /**
     * Move data file to the error folder.
     * 
     * @param dataFile {@link File}
     * @param error
     */
    private void handleError(Path dataFile, String error) {
        // Error log file
        File logFile = new File(errorMets.toFile(), FilenameUtils.getBaseName(dataFile.getFileName().toString()) + ".log");
        try (FileWriter fw = new FileWriter(logFile); BufferedWriter out = new BufferedWriter(fw)) {
            Files.copy(dataFile, Paths.get(errorMets.toAbsolutePath().toString(), dataFile.getFileName().toString()),
                    StandardCopyOption.REPLACE_EXISTING);
            if (error != null) {
                out.write(error);
            }
        } catch (IOException e) {
            logger.error("Data file could not be moved to errorMets!", e);
        }
    }

    /**
     * Checks whether the data folders for the given record file have finished being copied.
     *
     * @param recordFile a {@link java.nio.file.Path} object.
     * @return a boolean.
     */
    protected boolean isDataFolderExportDone(Path recordFile) {
        //        if (logger.isDebugEnabled()) {
        //            logger.debug("Hotfolder: Checking for ongoing export for '" + recordFile.getName() + "'...");
        //        }
        DataFolderSizeCounter sc = new DataFolderSizeCounter(recordFile.getFileName().toString());
        hotfolderPath.toFile().listFiles(sc);
        long total1 = sc.getTotal();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            logger.error("Error checking the hotfolder size.", e);
            return false;
        }
        sc = new DataFolderSizeCounter(recordFile.getFileName().toString());
        hotfolderPath.toFile().listFiles(sc);
        long total2 = sc.getTotal();

        return total1 == total2;
    }

    /**
     * <p>
     * copyDirectory.
     * </p>
     *
     * @param sourceLocation {@link java.io.File}
     * @param targetLocation {@link java.io.File}
     * @return number of copied files.
     * @throws java.io.IOException in case of errors.
     */
    public static int copyDirectory(File sourceLocation, File targetLocation) throws IOException {
        if (sourceLocation == null) {
            throw new IllegalArgumentException("targetsourceLocationLocation may not be null");
        }
        if (targetLocation == null) {
            throw new IllegalArgumentException("targetLocation may not be null");
        }

        int count = sourceLocation.listFiles().length;
        if (count > 0) {
            File[] currentTargetFiles = targetLocation.listFiles();
            int origCount = currentTargetFiles != null ? currentTargetFiles.length : 0;
            FileUtils.copyDirectory(sourceLocation, targetLocation);
        }

        return count;
    }

    protected class DataFolderSizeCounter implements FileFilter {

        private String recordFileName;
        private long total = 0;

        /** Empty constructor. */
        public DataFolderSizeCounter(String recordFileName) {
            this.recordFileName = recordFileName;
        }

        @Override
        public boolean accept(File pathname) {
            if (pathname.getName().startsWith(FilenameUtils.getBaseName(recordFileName) + "_")) {
                if (pathname.isFile()) {
                    //                    total += pathname.length();
                    total += FileUtils.sizeOf(pathname);
                } else {
                    pathname.listFiles(this);
                    total += FileUtils.sizeOfDirectory(pathname);
                }
            }

            return false;
        }

        public long getTotal() {
            return total;
        }
    }

    /**
     * <p>
     * Getter for the field <code>reindexQueue</code>.
     * </p>
     *
     * @return a {@link java.util.Queue} object.
     */
    public Queue<Path> getReindexQueue() {
        return reindexQueue;
    }

    /**
     * <p>
     * getHotfolder.
     * </p>
     *
     * @return a {@link java.nio.file.Path} object.
     */
    public Path getHotfolder() {
        return hotfolderPath;
    }

    /**
     * <p>
     * getTempFolder.
     * </p>
     *
     * @return a {@link java.nio.file.Path} object.
     */
    public Path getTempFolder() {
        return tempFolderPath;
    }

    /**
     * <p>
     * isAddVolumeCollectionsToAnchor.
     * </p>
     *
     * @return the addVolumeCollectionsToAnchor
     */
    public boolean isAddVolumeCollectionsToAnchor() {
        return addVolumeCollectionsToAnchor;
    }

    /**
     * <p>
     * Getter for the field <code>dataRepositoryStrategy</code>.
     * </p>
     *
     * @return the dataRepositoryStrategy
     */
    public IDataRepositoryStrategy getDataRepositoryStrategy() {
        return dataRepositoryStrategy;
    }

    /**
     * <p>
     * Getter for the field <code>updatedMets</code>.
     * </p>
     *
     * @return a {@link java.nio.file.Path} object.
     */
    public Path getUpdatedMets() {
        return updatedMets;
    }

    /**
     * <p>
     * Getter for the field <code>deletedMets</code>.
     * </p>
     *
     * @return a {@link java.nio.file.Path} object.
     */
    public Path getDeletedMets() {
        return deletedMets;
    }

    /**
     * <p>
     * Getter for the field <code>errorMets</code>.
     * </p>
     *
     * @return a {@link java.nio.file.Path} object.
     */
    public Path getErrorMets() {
        return errorMets;
    }

    /**
     * <p>
     * Getter for the field <code>origLido</code>.
     * </p>
     *
     * @return the origLido
     */
    public Path getOrigLido() {
        return origLido;
    }

    /**
     * <p>
     * Getter for the field <code>success</code>.
     * </p>
     *
     * @return a {@link java.nio.file.Path} object.
     */
    public Path getSuccess() {
        return success;
    }

    /**
     * <p>
     * Getter for the field <code>searchIndex</code>.
     * </p>
     *
     * @return the searchIndex
     */
    public SolrSearchIndex getSearchIndex() {
        return searchIndex;
    }

    /**
     * @return the oldSearchIndex
     */
    public SolrSearchIndex getOldSearchIndex() {
        return oldSearchIndex;
    }

    /**
     * <p>
     * getDataFolderFilter.
     * </p>
     *
     * @param prefix a {@link java.lang.String} object.
     * @return a {@link java.io.FilenameFilter} object.
     */
    public static FilenameFilter getDataFolderFilter(final String prefix) {
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return (name.startsWith(prefix) && new File(dir, name).isDirectory());
            }
        };

        return filter;
    }

}
