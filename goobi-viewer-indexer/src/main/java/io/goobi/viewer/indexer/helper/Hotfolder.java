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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import javax.mail.MessagingException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import io.goobi.viewer.indexer.DenkXwebIndexer;
import io.goobi.viewer.indexer.DocUpdateIndexer;
import io.goobi.viewer.indexer.DublinCoreIndexer;
import io.goobi.viewer.indexer.Indexer;
import io.goobi.viewer.indexer.LidoIndexer;
import io.goobi.viewer.indexer.MetsIndexer;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.UsageStatisticsIndexer;
import io.goobi.viewer.indexer.Version;
import io.goobi.viewer.indexer.WorldViewsIndexer;
import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.logging.SecondaryAppender;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.datarepository.strategy.AbstractDataRepositoryStrategy;
import io.goobi.viewer.indexer.model.datarepository.strategy.IDataRepositoryStrategy;

/**
 * <p>
 * Hotfolder class.
 * </p>
 *
 */
public class Hotfolder {

    private static final Logger logger = LogManager.getLogger(Hotfolder.class);

    private static final String SHUTDOWN_FILE = ".SHUTDOWN_INDEXER";
    private static final int WAIT_IF_FILE_EMPTY = 5000;

    public static final String ERROR_COULD_NOT_CREATE_DIR = "Could not create directory: ";

    public static final String FILENAME_EXTENSION_DELETE = ".delete";
    public static final String FILENAME_EXTENSION_PURGE = ".purge";

    private static final String FILENAME_PREFIX_STATISTICS_USAGE = "statistics-usage-";

    /** Constant <code>metsEnabled=true</code> */
    private boolean metsEnabled = true;
    /** Constant <code>lidoEnabled=true</code> */
    private boolean lidoEnabled = true;
    /** Constant <code>denkxwebEnabled=true</code> */
    private boolean denkxwebEnabled = true;
    /** If no indexedDC folder is configured, Dublin Core indexing will be automatically disabled via this flag. */
    private boolean dcEnabled = true;
    /** Constant <code>worldviewsEnabled=true</code> */
    private boolean worldviewsEnabled = true;

    private SecondaryAppender secondaryAppender;

    private final SolrSearchIndex searchIndex;
    private final SolrSearchIndex oldSearchIndex;
    private final IDataRepositoryStrategy dataRepositoryStrategy;
    private final Queue<Path> indexQueue = new LinkedList<>();
    private final Queue<Path> reindexQueue = new LinkedList<>();

    private int minStorageSpace = 2048;
    private long metsFileSizeThreshold = 10485760;
    private long dataFolderSizeThreshold = 157286400;

    private Path hotfolderPath;
    private Path tempFolderPath;
    private Path updatedMets;
    private Path deletedMets;
    private Path errorMets;
    private Path origLido;
    private Path origDenkxWeb;
    private Path successFolder;

    private Indexer currentIndexer;
    private boolean addVolumeCollectionsToAnchor = false;
    private boolean deleteContentFilesOnFailure = true;
    private boolean emailConfigurationComplete = false;

    /**
     * <p>
     * Constructor for Hotfolder.
     * </p>
     *
     * @param confFilename a {@link java.lang.String} object.
     * @param solrClient SolrClient object
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException if any.
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
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException if any.
     */
    public Hotfolder(String confFilename, SolrClient solrClient, SolrClient oldSolrClient) throws FatalIndexerException {
        logger.debug("Config file: {}", confFilename);
        Configuration config = Configuration.getInstance(confFilename);

        this.searchIndex = new SolrSearchIndex(solrClient);
        if (logger.isInfoEnabled()) {
            logger.info("Using Solr server at {}", config.getConfiguration("solrUrl"));
        }
        if (oldSolrClient != null) {
            this.oldSearchIndex = new SolrSearchIndex(oldSolrClient);
            if (logger.isInfoEnabled()) {
                logger.info("Also using old Solr server at {}", config.getConfiguration("oldSolrUrl"));
            }
        } else {
            this.oldSearchIndex = null;
        }

        dataRepositoryStrategy = AbstractDataRepositoryStrategy.create(config);

        initFolders(config);

        metsFileSizeThreshold = Configuration.getInstance().getInt("performance.metsFileSizeThreshold", 10485760);
        dataFolderSizeThreshold = Configuration.getInstance().getInt("performance.dataFolderSizeThreshold", 157286400);

        this.searchIndex.setOptimize(Configuration.getInstance().isAutoOptimize());
        logger.info("Auto-optimize: {}", this.searchIndex.isOptimize());

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

        MetadataHelper.authorityDataEnabled = config.getBoolean("init.authorityData[@enabled]", true);
        if (MetadataHelper.authorityDataEnabled) {
            // Authority data fields to be added to DEFAULT
            MetadataHelper.addAuthorityDataFieldsToDefault = config.getStringList("init.authorityData.addFieldsToDefault.field");
            if (MetadataHelper.addAuthorityDataFieldsToDefault != null) {
                for (String field : MetadataHelper.addAuthorityDataFieldsToDefault) {
                    logger.info("{} values will be added to DEFAULT", field);
                }
            }
        } else {
            logger.info("Authority data retrieval is disabled.");
        }

        // REST API token configuration
        if (StringUtils.isEmpty(Configuration.getInstance().getViewerAuthorizationToken())) {
            logger.warn("Goobi viewer REST API token not found, communications disabled.");
        }

        // E-mail configuration
        emailConfigurationComplete = Configuration.checkEmailConfiguration();
        if (emailConfigurationComplete) {
            logger.info("E-mail configuration OK.");
        }

        // Secondary logging appender
        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        secondaryAppender = (SecondaryAppender) context.getConfiguration().getAppenders().get("record");
    }

    /**
     * 
     * @param config
     * @throws FatalIndexerException
     */
    private void initFolders(Configuration config) throws FatalIndexerException {

        try {
            minStorageSpace = Integer.valueOf(config.getConfiguration("minStorageSpace"));
        } catch (NumberFormatException e) {
            logger.error("<minStorageSpace> must contain a numerical value - using default ({}) instead.", minStorageSpace);
        }
        try {
            hotfolderPath = Paths.get(config.getConfiguration("hotFolder"));
            if (!Utils.checkAndCreateDirectory(hotfolderPath)) {
                logger.error("Could not create folder '{}', exiting...", hotfolderPath);
                throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
            }
        } catch (Exception e) {
            logger.error("<hotFolder> not defined.");
            throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
        }

        try {
            String viewerHomePath = config.getViewerHome();
            if (!Files.isDirectory(Paths.get(viewerHomePath))) {
                logger.error("Path defined in <viewerHome> does not exist, exiting...");
                throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
            }
        } catch (Exception e) {
            logger.error("<viewerHome> not defined, exiting...");
            throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
        }

        try {
            tempFolderPath = Paths.get(config.getConfiguration("tempFolder"));
            if (!Utils.checkAndCreateDirectory(tempFolderPath)) {
                logger.error("Could not create folder '{}', exiting...", tempFolderPath);
                throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
            }
        } catch (Exception e) {
            logger.error("<tempFolder> not defined.");
            throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
        }

        // METS folders
        if (config.getConfiguration(DataRepository.PARAM_INDEXED_METS) == null) {
            metsEnabled = false;
            logger.warn("<{}> not defined - METS indexing is disabled.", DataRepository.PARAM_INDEXED_METS);
        }
        try {
            updatedMets = Paths.get(config.getConfiguration("updatedMets"));
            if (!Utils.checkAndCreateDirectory(updatedMets)) {
                throw new FatalIndexerException(ERROR_COULD_NOT_CREATE_DIR + updatedMets.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            throw new FatalIndexerException("<updatedMets> not defined.");
        }
        try {
            deletedMets = Paths.get(config.getConfiguration("deletedMets"));
            if (!Utils.checkAndCreateDirectory(deletedMets)) {
                throw new FatalIndexerException(ERROR_COULD_NOT_CREATE_DIR + deletedMets.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            throw new FatalIndexerException("<deletedMets> not defined.");
        }
        try {
            errorMets = Paths.get(config.getConfiguration("errorMets"));
            if (!Utils.checkAndCreateDirectory(errorMets)) {
                throw new FatalIndexerException(ERROR_COULD_NOT_CREATE_DIR + errorMets.toAbsolutePath().toString());
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
                throw new FatalIndexerException(ERROR_COULD_NOT_CREATE_DIR + origLido.toAbsolutePath().toString());
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
                throw new FatalIndexerException(ERROR_COULD_NOT_CREATE_DIR + origDenkxWeb.toAbsolutePath().toString());
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
            successFolder = Paths.get(config.getConfiguration("successFolder"));
            if (!Utils.checkAndCreateDirectory(successFolder)) {
                throw new FatalIndexerException(ERROR_COULD_NOT_CREATE_DIR + successFolder.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            throw new FatalIndexerException("<successFolder> not defined.");
        }
    }

    /**
     * Empties and re-inits the secondary logger.
     */
    private void resetSecondaryLog() {
        if (secondaryAppender != null) {
            secondaryAppender.reset();
        }
    }

    /**
     * 
     * @param subject
     * @param body
     * @throws FatalIndexerException
     */
    private static void checkAndSendErrorReport(String subject, String body) throws FatalIndexerException {
        logger.debug("checkAndSendErrorReport");
        logger.trace("body:\n{}", body);
        if (StringUtils.isEmpty(body)) {
            logger.warn("E-Mail body is empty.");
        }
        // Send report e-mail if the text body contains at least one ERROR level log message
        if (!body.contains(Indexer.STATUS_ERROR)) {
            return;
        }

        String recipients = Configuration.getInstance().getString("init.email.recipients");
        if (StringUtils.isEmpty(recipients)) {
            return;
        }
        String smtpServer = Configuration.getInstance().getString("init.email.smtpServer");
        if (StringUtils.isEmpty(smtpServer)) {
            return;
        }
        String smtpUser = Configuration.getInstance().getString("init.email.smtpUser");
        String smtpPassword = Configuration.getInstance().getString("init.email.smtpPassword");
        String smtpSenderAddress = Configuration.getInstance().getString("init.email.smtpSenderAddress");
        if (StringUtils.isEmpty(smtpSenderAddress)) {
            return;
        }
        String smtpSenderName = Configuration.getInstance().getString("init.email.smtpSenderName");
        if (StringUtils.isEmpty(smtpSenderName)) {
            return;
        }
        String smtpSecurity = Configuration.getInstance().getString("init.email.smtpSecurity");
        if (StringUtils.isEmpty(smtpSecurity)) {
            return;
        }
        int smtpPort = Configuration.getInstance().getInt("init.email.smtpPort", -1);
        String[] recipientsSplit = recipients.split(";");

        try {
            Utils.postMail(Arrays.asList(recipientsSplit), subject, body, smtpServer, smtpUser, smtpPassword, smtpSenderAddress, smtpSenderName,
                    smtpSecurity, smtpPort);
            logger.info("Report e-mailed to configured recipients.");
        } catch (UnsupportedEncodingException | MessagingException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Scans the hotfolder for new files and executes appropriate actions.
     *
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     */
    public void scan() throws FatalIndexerException {
        if (!Files.isDirectory(hotfolderPath)) {
            logger.error("Hotfolder not found!");
            return;
        }
        Path fileToReindex = reindexQueue.poll();
        if (fileToReindex != null) {
            resetSecondaryLog();
            logger.info("Found file '{}' (re-index queue).", fileToReindex.getFileName());
            doIndex(fileToReindex);
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
                return;
            }

            if (!indexQueue.isEmpty()) {
                Path recordFile = indexQueue.poll();
                // Check whether the data folders for this record have been copied completely, otherwise skip
                Set<Path> alreadyCheckedFiles = new HashSet<>();
                while (!isDataFolderExportDone(recordFile)) {
                    logger.info("Export not yet finished for '{}'", recordFile.getFileName());
                    alreadyCheckedFiles.add(recordFile);
                    indexQueue.add(recordFile);
                    if (alreadyCheckedFiles.contains(indexQueue.peek())) {
                        logger.info("All files in queue have not yet finished export.");
                        return;
                    }
                    recordFile = indexQueue.poll();
                }
                logger.info("Processing {} from memory queue...", recordFile.getFileName());
                doIndex(recordFile);
                return; // always break after attempting to index a file, so that the loop restarts
            }

            logger.trace("Hotfolder: Listing files...");
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, "*.{xml,json,delete,purge,docupdate,UPDATED}")) {
                for (Path path : stream) {
                    // Only one file at a time right now
                    if (currentIndexer != null) {
                        break;
                    }
                    Path recordFile = path;
                    if (!recordFile.getFileName().toString().endsWith(MetsIndexer.ANCHOR_UPDATE_EXTENSION)) {
                        indexQueue.add(recordFile);
                        logger.info("Added file to index queue: {}", path.getFileName());
                    } else {
                        logger.info("Found file '{}' which is not in the re-index queue. This file will be deleted.", recordFile.getFileName());
                        Files.delete(recordFile);
                    }
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 
     * @param recordFile
     * @return
     * @throws FatalIndexerException
     */
    boolean doIndex(Path recordFile) throws FatalIndexerException {
        if (recordFile == null) {
            return false;
        }

        resetSecondaryLog();
        checkFreeSpace();
        Map<String, Boolean> reindexSettings = new HashMap<>();
        reindexSettings.put(DataRepository.PARAM_FULLTEXT, false);
        reindexSettings.put(DataRepository.PARAM_TEIWC, false);
        reindexSettings.put(DataRepository.PARAM_ALTO, false);
        reindexSettings.put(DataRepository.PARAM_MIX, false);
        reindexSettings.put(DataRepository.PARAM_UGC, false);
        boolean ret = handleSourceFile(recordFile, false, reindexSettings);
        if (secondaryAppender != null && emailConfigurationComplete) {
            checkAndSendErrorReport(recordFile.getFileName() + ": Indexing failed (" + Version.asString() + ")",
                    secondaryAppender.getLog());
        }

        return ret;
    }

    /**
     * Returns the number of record and command (delete, update) files in the hotfolder.
     * 
     * @return Number of files
     * @throws FatalIndexerException
     * @should count files correctly
     */
    public long countRecordFiles() throws FatalIndexerException {
        if (!Configuration.getInstance().isCountHotfolderFiles()) {
            return 0;
        }

        try (Stream<Path> files = Files.list(hotfolderPath)) {
            long ret = files.filter(p -> !Files.isDirectory(p))
                    .map(Path::toString)
                    .filter(f -> (f.toLowerCase().endsWith(".xml") || f.endsWith(FILENAME_EXTENSION_DELETE) || f.endsWith(FILENAME_EXTENSION_PURGE)
                            || f.endsWith(".docupdate")
                            || f.endsWith(".UPDATED")))
                    .count();
            logger.trace("{} files in hotfolder", ret);
            return ret;
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return 0;
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
            if (secondaryAppender != null && emailConfigurationComplete) {
                checkAndSendErrorReport("Record indexing failed due to insufficient space (" + Version.asString() + ")",
                        secondaryAppender.getLog());
            }
            throw new FatalIndexerException("Insufficient free space");
        }
    }

    /**
     * 
     * @param sourceFile File containing the record(s)
     * @param fromReindexQueue true if file is coming from the re-index queue; false if from the hotfolder
     * @param reindexSettings
     * @return true if successful; false otherwise
     * @throws FatalIndexerException
     */
    private boolean handleSourceFile(Path sourceFile, boolean fromReindexQueue, Map<String, Boolean> reindexSettings) throws FatalIndexerException {
        logger.trace("handleSourceFile: {}", sourceFile);
        // Always unselect repository
        String filename = sourceFile.getFileName().toString();
        try {
            if (filename.endsWith(".xml")) {
                // INPUT o. UPDATE
                if (Files.size(sourceFile) == 0) {
                    // Check whether the file is actually empty or just hasn't finished copying yet
                    try {
                        Thread.sleep(WAIT_IF_FILE_EMPTY);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage());
                        Thread.currentThread().interrupt();
                    }
                    if (Files.size(sourceFile) == 0) {
                        logger.error("Empty data file '{}' found, deleting...", sourceFile.toAbsolutePath());
                        Files.delete(sourceFile);
                        return false;
                    }
                }

                // Check file format and start the appropriate indexing routine
                FileFormat fileType = JDomXP.determineFileFormat(sourceFile.toFile());
                switch (fileType) {
                    case METS:
                        if (metsEnabled) {
                            try {
                                currentIndexer = new MetsIndexer(this);
                                currentIndexer.addToIndex(sourceFile, fromReindexQueue, reindexSettings);
                            } finally {
                                currentIndexer = null;
                            }
                        } else {
                            logger.error("METS indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case LIDO:
                        if (lidoEnabled) {
                            try {
                                currentIndexer = new LidoIndexer(this);
                                currentIndexer.addToIndex(sourceFile, false, reindexSettings);
                            } finally {
                                currentIndexer = null;
                            }
                        } else {
                            logger.error("LIDO indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case DENKXWEB:
                        if (denkxwebEnabled) {
                            try {
                                currentIndexer = new DenkXwebIndexer(this);
                                currentIndexer.addToIndex(sourceFile, false, reindexSettings);
                            } finally {
                                currentIndexer = null;
                            }
                        } else {
                            logger.error("DenkXweb indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case DUBLINCORE:
                        if (dcEnabled) {
                            try {
                                currentIndexer = new DublinCoreIndexer(this);
                                currentIndexer.addToIndex(sourceFile, fromReindexQueue, reindexSettings);
                            } finally {
                                currentIndexer = null;
                            }
                        } else {
                            logger.error("Dublin Core indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case WORLDVIEWS:
                        if (worldviewsEnabled) {
                            try {
                                currentIndexer = new WorldViewsIndexer(this);
                                currentIndexer.addToIndex(sourceFile, fromReindexQueue, reindexSettings);
                            } finally {
                                currentIndexer = null;
                            }
                        } else {
                            logger.error("WorldViews indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    default:
                        logger.error("Unknown file format, deleting: {}", filename);
                        Files.delete(sourceFile);
                        return false;
                }
                Utils.submitDataToViewer(countRecordFiles());
            } else if (filename.endsWith(".json")) {
                if (filename.startsWith(FILENAME_PREFIX_STATISTICS_USAGE)) {
                    try {
                        this.currentIndexer = new UsageStatisticsIndexer(this);
                        currentIndexer.addToIndex(sourceFile, false, null);
                    } finally {
                        this.currentIndexer = null;
                    }
                    Files.delete(sourceFile);
                }
            } else if (filename.endsWith(FILENAME_EXTENSION_DELETE)) {
                if (filename.startsWith(FILENAME_PREFIX_STATISTICS_USAGE)) {
                    removeUsageStatisticsFromIndex(sourceFile);
                    Files.delete(sourceFile);
                } else {
                    // DELETE
                    DataRepository[] repositories = dataRepositoryStrategy.selectDataRepository(null, sourceFile, null, searchIndex, oldSearchIndex);
                    removeFromIndex(sourceFile, repositories[1] != null ? repositories[1] : repositories[0], true);
                    Utils.submitDataToViewer(countRecordFiles());
                }
            } else if (filename.endsWith(FILENAME_EXTENSION_PURGE)) {
                if (filename.startsWith(FILENAME_PREFIX_STATISTICS_USAGE)) {
                    removeUsageStatisticsFromIndex(sourceFile);
                    Files.delete(sourceFile);
                } else {
                    // PURGE (delete with no "deleted" doc)
                    DataRepository[] repositories = dataRepositoryStrategy.selectDataRepository(null, sourceFile, null, searchIndex, oldSearchIndex);
                    removeFromIndex(sourceFile, repositories[1] != null ? repositories[1] : repositories[0], false);
                    Utils.submitDataToViewer(countRecordFiles());
                }
            } else if (filename.endsWith(MetsIndexer.ANCHOR_UPDATE_EXTENSION)) {
                // SUPERUPDATE
                DataRepository[] repositories = dataRepositoryStrategy.selectDataRepository(null, sourceFile, null, searchIndex, oldSearchIndex);
                MetsIndexer.anchorSuperupdate(sourceFile, updatedMets, repositories[1] != null ? repositories[1] : repositories[0]);
                Utils.submitDataToViewer(countRecordFiles());
            } else if (filename.endsWith(DocUpdateIndexer.FILE_EXTENSION)) {
                // Single Solr document update
                try {
                    currentIndexer = new DocUpdateIndexer(this);
                    currentIndexer.addToIndex(sourceFile, false, null);
                } finally {
                    currentIndexer = null;
                }
                Utils.submitDataToViewer(countRecordFiles());
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            try {
                Files.delete(sourceFile);
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

            Path actualXmlFile = dataRepository.getDir(DataRepository.PARAM_INDEXED_METS) != null
                    ? Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), baseFileName + ".xml")
                    : Paths.get("" + System.currentTimeMillis() + ".foo");
            if (!Files.exists(actualXmlFile) && dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO) != null) {
                actualXmlFile =
                        Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toAbsolutePath().toString(), baseFileName + ".xml");
            }
            if (!Files.exists(actualXmlFile) && dataRepository.getDir(DataRepository.PARAM_INDEXED_DENKXWEB) != null) {
                actualXmlFile =
                        Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_DENKXWEB).toAbsolutePath().toString(), baseFileName + ".xml");
            }
            if (!Files.exists(actualXmlFile) && dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE) != null) {
                actualXmlFile =
                        Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE).toAbsolutePath().toString(), baseFileName + ".xml");
            }
            FileFormat format = FileFormat.UNKNOWN;
            if (!Files.exists(actualXmlFile)) {
                logger.warn("XML file '{}' not found.", actualXmlFile.getFileName());
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
                logger.error(StringConstants.LOG_COULD_NOT_BE_DELETED, actualXmlFile.getFileName());
            }
        } catch (SolrServerException e) {
            logger.error(StringConstants.LOG_COULD_NOT_BE_DELETED, baseFileName);
            logger.error(e.getMessage(), e);
        } finally {
            try {
                Files.delete(deleteFile);
            } catch (IOException e) {
                logger.warn(StringConstants.LOG_COULD_NOT_BE_DELETED, deleteFile.toAbsolutePath());
            }
        }
    }

    /**
     * @param sourceFile
     * @throws FatalIndexerException
     */
    private boolean removeUsageStatisticsFromIndex(Path sourceFile) throws FatalIndexerException {
        if (sourceFile == null) {
            throw new IllegalArgumentException("usage statistics file may not be null");
        } else if (!Files.isRegularFile(sourceFile)) {
            throw new IllegalArgumentException("usage statistics file {} does not exist".replace("{}", sourceFile.toString()));
        }
        return new UsageStatisticsIndexer(this).removeFromIndex(sourceFile);
    }

    /**
     * Checks whether the data folders for the given record file have finished being copied.
     *
     * @param recordFile a {@link java.nio.file.Path} object.
     * @return a boolean.
     */
    protected boolean isDataFolderExportDone(Path recordFile) {
        DataFolderSizeCounter sc = new DataFolderSizeCounter(recordFile.getFileName().toString());
        hotfolderPath.toFile().listFiles(sc);
        long total1 = sc.getTotal();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            logger.error("Error checking the hotfolder size.", e);
            Thread.currentThread().interrupt();
            return false;
        }
        sc = new DataFolderSizeCounter(recordFile.getFileName().toString());
        hotfolderPath.toFile().listFiles(sc);
        long total2 = sc.getTotal();

        return total1 == total2;
    }

    protected class DataFolderSizeCounter implements FileFilter {

        private String recordFileName;
        private long total = 0;

        /** Empty constructor. */
        public DataFolderSizeCounter(String recordFileName) {
            this.recordFileName = recordFileName;
        }

        @Override
        public boolean accept(File pathName) {
            if (pathName != null && pathName.getName().startsWith(FilenameUtils.getBaseName(recordFileName) + "_")) {
                try {
                    if (pathName.isFile()) {
                        total += FileUtils.sizeOf(pathName);
                    } else if (pathName.isDirectory()) {
                        pathName.listFiles(this);
                        total += FileUtils.sizeOfDirectory(pathName);
                    }
                } catch (IllegalArgumentException e) {
                    logger.error(e.getMessage());
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
     * getHotfolderPath.
     * </p>
     *
     * @return a {@link java.nio.file.Path} object.
     */
    public Path getHotfolderPath() {
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
     * @return the deleteContentFilesOnFailure
     */
    public boolean isDeleteContentFilesOnFailure() {
        return deleteContentFilesOnFailure;
    }

    /**
     * @param deleteContentFilesOnFailure the deleteContentFilesOnFailure to set
     */
    public void setDeleteContentFilesOnFailure(boolean deleteContentFilesOnFailure) {
        this.deleteContentFilesOnFailure = deleteContentFilesOnFailure;
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
     * @return the origDenkxWeb
     */
    public Path getOrigDenkxWeb() {
        return origDenkxWeb;
    }

    /**
     * <p>
     * Getter for the field <code>successFolder</code>.
     * </p>
     *
     * @return a {@link java.nio.file.Path} object.
     */
    public Path getSuccessFolder() {
        return successFolder;
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
     * @return the metsFileSizeThreshold
     */
    public long getMetsFileSizeThreshold() {
        return metsFileSizeThreshold;
    }

    /**
     * @return the dataFolderSizeThreshold
     */
    public long getDataFolderSizeThreshold() {
        return dataFolderSizeThreshold;
    }
}
