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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import io.goobi.viewer.indexer.CmsPageIndexer;
import io.goobi.viewer.indexer.DenkXwebIndexer;
import io.goobi.viewer.indexer.DocUpdateIndexer;
import io.goobi.viewer.indexer.DublinCoreIndexer;
import io.goobi.viewer.indexer.Ead3Indexer;
import io.goobi.viewer.indexer.EadIndexer;
import io.goobi.viewer.indexer.Indexer;
import io.goobi.viewer.indexer.LidoIndexer;
import io.goobi.viewer.indexer.MetsIndexer;
import io.goobi.viewer.indexer.MetsMarcIndexer;
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
import jakarta.mail.MessagingException;

/**
 * <p>
 * Hotfolder class.
 * </p>
 */
public class Hotfolder {

    private static final Logger logger = LogManager.getLogger(Hotfolder.class);

    private static final String SHUTDOWN_FILE = ".SHUTDOWN_INDEXER";
    private static final int WAIT_IF_FILE_EMPTY = 5000;

    /** Constant <code>ERROR_COULD_NOT_CREATE_DIR="Could not create directory: "</code> */
    public static final String ERROR_COULD_NOT_CREATE_DIR = "Could not create directory: ";

    /** Constant <code>FILENAME_EXTENSION_DELETE=".delete"</code> */
    public static final String FILENAME_EXTENSION_DELETE = ".delete";
    /** Constant <code>FILENAME_EXTENSION_PURGE=".purge"</code> */
    public static final String FILENAME_EXTENSION_PURGE = ".purge";

    private static final String FILENAME_PREFIX_STATISTICS_USAGE = "statistics-usage-";

    /** Constant <code>metsEnabled=true</code> */
    private boolean metsEnabled = true;
    /** Constant <code>lidoEnabled=true</code> */
    private boolean lidoEnabled = true;
    /** Constant <code>eadEnabled=true</code> */
    private boolean eadEnabled = true;
    /** Constant <code>denkxwebEnabled=true</code> */
    private boolean denkxwebEnabled = true;
    /** If no indexedDC folder is configured, Dublin Core indexing will be automatically disabled via this flag. */
    private boolean dcEnabled = true;
    /** Constant <code>worldviewsEnabled=true</code> */
    private boolean worldviewsEnabled = true;
    /** Constant <code>cmsEnabled=true</code> */
    private boolean cmsEnabled = true;
    /** Constant <code>usageStatisticsEnabled=true</code> */
    private boolean usageStatisticsEnabled = true;

    private int queueCapacity = 500;
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

    private SecondaryAppender secondaryAppender;

    private final IDataRepositoryStrategy dataRepositoryStrategy;
    /** Regular index queue for files found in the regular hotfolder. */
    private final Queue<Path> indexQueue = new LinkedBlockingQueue<>(queueCapacity);
    /** High priority index queue for volume re-indexing, etc. */
    private final Queue<Path> highPriorityIndexQueue = new LinkedList<>();

    /**
     * Zero-arg constructor for tests.
     * 
     * @throws FatalIndexerException
     */
    Hotfolder() throws FatalIndexerException {
        logger.trace("Hotfolder()");
        this.dataRepositoryStrategy = AbstractDataRepositoryStrategy.create(SolrIndexerDaemon.getInstance().getConfiguration());
    }

    /**
     * <p>
     * Constructor for Hotfolder.
     * </p>
     *
     * @param hotfolderPath a {@link java.lang.String} object
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException if any.
     */
    public Hotfolder(String hotfolderPath) throws FatalIndexerException {
        logger.trace("Hotfolder({})", hotfolderPath);
        dataRepositoryStrategy = AbstractDataRepositoryStrategy.create(SolrIndexerDaemon.getInstance().getConfiguration());

        initFolders(hotfolderPath, SolrIndexerDaemon.getInstance().getConfiguration());

        metsFileSizeThreshold = SolrIndexerDaemon.getInstance().getConfiguration().getInt("performance.metsFileSizeThreshold", 10485760);
        dataFolderSizeThreshold = SolrIndexerDaemon.getInstance().getConfiguration().getInt("performance.dataFolderSizeThreshold", 157286400);

        addVolumeCollectionsToAnchor = SolrIndexerDaemon.getInstance().getConfiguration().isAddVolumeCollectionsToAnchor();
        if (addVolumeCollectionsToAnchor) {
            logger.info("Volume collections WILL BE ADDED to anchors.");
        } else {
            logger.info("Volume collections WILL NOT BE ADDED to anchors.");
        }

        String temp = SolrIndexerDaemon.getInstance().getConfiguration().getConfiguration("deleteContentFilesOnFailure");
        if (temp != null) {
            deleteContentFilesOnFailure = Boolean.valueOf(temp);
        }
        if (deleteContentFilesOnFailure) {
            logger.info("Content files will be REMOVED from the hotfolder in case of indexing errors.");
        } else {
            logger.info("Content files will be PRESERVED in the hotfolder in case of indexing errors.");
        }

        MetadataHelper.setAuthorityDataEnabled(SolrIndexerDaemon.getInstance().getConfiguration().getBoolean("init.authorityData[@enabled]", true));
        if (MetadataHelper.isAuthorityDataEnabled()) {
            // Authority data fields to be added to DEFAULT
            MetadataHelper.setAddAuthorityDataFieldsToDefault(
                    SolrIndexerDaemon.getInstance().getConfiguration().getStringList("init.authorityData.addFieldsToDefault.field"));
            if (MetadataHelper.getAddAuthorityDataFieldsToDefault() != null) {
                for (String field : MetadataHelper.getAddAuthorityDataFieldsToDefault()) {
                    logger.info("{} values will be added to DEFAULT", field);
                }
            }
        } else {
            logger.info("Authority data retrieval is disabled.");
        }

        // REST API token configuration
        if (StringUtils.isEmpty(SolrIndexerDaemon.getInstance().getConfiguration().getViewerAuthorizationToken())) {
            logger.warn("Goobi viewer REST API token not found, communications disabled.");
        }

        // E-mail configuration
        emailConfigurationComplete = SolrIndexerDaemon.getInstance().getConfiguration().checkEmailConfiguration();
        if (emailConfigurationComplete) {
            logger.info("E-mail configuration OK.");
        }

        // Secondary logging appender
        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        secondaryAppender = (SecondaryAppender) context.getConfiguration().getAppenders().get("record");
    }

    /**
     * 
     * @param hotfolderPathString
     * @param config
     * @throws FatalIndexerException
     * @should throw FatalIndexerException if hotfolderPathString null
     * @should throw FatalIndexerException if viewerHome not defined
     * @should throw FatalIndexerException if tempFolder not defined
     * @should throw FatalIndexerException if successFolder not defined
     */
    void initFolders(String hotfolderPathString, Configuration config) throws FatalIndexerException {
        try {
            minStorageSpace = Integer.valueOf(config.getConfiguration("minStorageSpace"));
        } catch (NumberFormatException e) {
            logger.error("<minStorageSpace> must contain a numerical value - using default ({}) instead.", minStorageSpace);
        }
        if (StringUtils.isEmpty(hotfolderPathString)) {
            logger.error("Given <hotFolder> not defined, exiting...");
            throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
        }
        hotfolderPath = Paths.get(hotfolderPathString);
        if (!Utils.checkAndCreateDirectory(hotfolderPath)) {
            logger.error("Could not create folder '{}', exiting...", hotfolderPath);
            throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
        }

        String viewerHomePath = config.getViewerHome();
        if (StringUtils.isEmpty(viewerHomePath)) {
            logger.error("<viewerHome> not defined, exiting...");
            throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
        }
        if (!Files.isDirectory(Paths.get(viewerHomePath))) {
            logger.error("Path defined in <viewerHome> does not exist, exiting...");
            throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
        }

        if (StringUtils.isEmpty(config.getConfiguration("tempFolder"))) {
            logger.error("<tempFolder> not defined, exiting...");
            throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
        }
        tempFolderPath = Paths.get(config.getConfiguration("tempFolder"));
        if (!Utils.checkAndCreateDirectory(tempFolderPath)) {
            logger.error("Could not create folder '{}', exiting...", tempFolderPath);
            throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
        }

        // METS folders
        if (StringUtils.isEmpty(config.getConfiguration(DataRepository.PARAM_INDEXED_METS))) {
            metsEnabled = false;
            logger.warn("<{}> not defined - METS indexing is disabled.", DataRepository.PARAM_INDEXED_METS);
        }

        updatedMets = Paths.get(config.getConfiguration("updatedMets"));
        if (!Utils.checkAndCreateDirectory(updatedMets)) {
            throw new FatalIndexerException(ERROR_COULD_NOT_CREATE_DIR + updatedMets.toAbsolutePath().toString());
        }

        deletedMets = Paths.get(config.getConfiguration("deletedMets"));
        if (!Utils.checkAndCreateDirectory(deletedMets)) {
            throw new FatalIndexerException(ERROR_COULD_NOT_CREATE_DIR + deletedMets.toAbsolutePath().toString());
        }

        errorMets = Paths.get(config.getConfiguration("errorMets"));
        if (!Utils.checkAndCreateDirectory(errorMets)) {
            throw new FatalIndexerException(ERROR_COULD_NOT_CREATE_DIR + errorMets.toAbsolutePath().toString());
        }

        // LIDO folders
        if (StringUtils.isEmpty(config.getConfiguration(DataRepository.PARAM_INDEXED_LIDO))) {
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

        // EAD folder
        if (StringUtils.isEmpty(config.getConfiguration(DataRepository.PARAM_INDEXED_EAD))) {
            eadEnabled = false;
            logger.warn("<{}> not defined - EAD indexing is disabled.", DataRepository.PARAM_INDEXED_EAD);
        }

        // DenkXweb folders
        if (StringUtils.isEmpty(config.getConfiguration(DataRepository.PARAM_INDEXED_DENKXWEB))) {
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
        if (StringUtils.isEmpty(config.getConfiguration(DataRepository.PARAM_INDEXED_DUBLINCORE))) {
            dcEnabled = false;
            logger.warn("<{}> not defined - Dublin Core indexing is disabled.", DataRepository.PARAM_INDEXED_DUBLINCORE);
        }

        // CMS page folder
        if (StringUtils.isEmpty(config.getConfiguration(DataRepository.PARAM_INDEXED_CMS))) {
            cmsEnabled = false;
            logger.warn("<{}> not defined - CMS page indexing is disabled.", DataRepository.PARAM_INDEXED_CMS);
        }

        // Usage statistics folder
        if (StringUtils.isEmpty(config.getConfiguration(DataRepository.PARAM_INDEXED_STATISTICS))) {
            usageStatisticsEnabled = false;
            logger.warn("<{}> not defined - usage statistics indexing is disabled.", DataRepository.PARAM_INDEXED_STATISTICS);
        }

        if (StringUtils.isEmpty(config.getConfiguration("successFolder"))) {
            logger.error("<successFolder> not defined, exiting...");
            throw new FatalIndexerException(StringConstants.ERROR_CONFIG);
        }
        successFolder = Paths.get(config.getConfiguration("successFolder"));
        if (!Utils.checkAndCreateDirectory(successFolder)) {
            throw new FatalIndexerException(ERROR_COULD_NOT_CREATE_DIR + successFolder.toAbsolutePath().toString());
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
     * @return true if successful; false otherwise
     * @should return false if body contains no error
     * @should return false if recipients not configured
     * @should return false if smtpServer not configured
     * @should return false if smtpSenderAddress not configured
     * @should return false if smtpSenderName not configured
     * @should return false if smtpSecurity not configured
     * @should return false if sending mail fails
     */
    static boolean checkAndSendErrorReport(String subject, String body) {
        logger.debug("checkAndSendErrorReport");
        logger.trace("body:\n{}", body);
        if (StringUtils.isEmpty(body)) {
            logger.warn("E-Mail body is empty.");
        }
        // Send report e-mail if the text body contains at least one ERROR level log message
        if (!body.contains(Indexer.STATUS_ERROR)) {
            return false;
        }

        String recipients = SolrIndexerDaemon.getInstance().getConfiguration().getString("init.email.recipients");
        if (StringUtils.isEmpty(recipients)) {
            return false;
        }
        String smtpServer = SolrIndexerDaemon.getInstance().getConfiguration().getString("init.email.smtpServer");
        if (StringUtils.isEmpty(smtpServer)) {
            return false;
        }
        String smtpUser = SolrIndexerDaemon.getInstance().getConfiguration().getString("init.email.smtpUser");
        String smtpPassword = SolrIndexerDaemon.getInstance().getConfiguration().getString("init.email.smtpPassword");
        String smtpSenderAddress = SolrIndexerDaemon.getInstance().getConfiguration().getString("init.email.smtpSenderAddress");
        if (StringUtils.isEmpty(smtpSenderAddress)) {
            return false;
        }
        String smtpSenderName = SolrIndexerDaemon.getInstance().getConfiguration().getString("init.email.smtpSenderName");
        if (StringUtils.isEmpty(smtpSenderName)) {
            return false;
        }
        String smtpSecurity = SolrIndexerDaemon.getInstance().getConfiguration().getString("init.email.smtpSecurity");
        if (StringUtils.isEmpty(smtpSecurity)) {
            return false;
        }
        int smtpPort = SolrIndexerDaemon.getInstance().getConfiguration().getInt("init.email.smtpPort", -1);
        String[] recipientsSplit = recipients.split(";");

        try {
            Utils.postMail(Arrays.asList(recipientsSplit), subject, body, smtpServer, smtpUser, smtpPassword, smtpSenderAddress, smtpSenderName,
                    smtpSecurity, smtpPort);
            logger.info("Report e-mailed to configured recipients.");
            return true;
        } catch (UnsupportedEncodingException | MessagingException e) {
            logger.error(e.getMessage(), e);
        }

        return false;
    }

    /**
     * Scans the hotfolder for new files and executes appropriate actions.
     *
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @return a boolean
     */
    public boolean scan() throws FatalIndexerException {
        logger.debug("scan ({})", getHotfolderPath().getFileName());
        if (!Files.isDirectory(getHotfolderPath())) {
            logger.error("Hotfolder not found in file system: {}", hotfolderPath);
            return false;
        }
        Path fileToReindex = highPriorityIndexQueue.poll();
        if (fileToReindex != null) {
            resetSecondaryLog();
            logger.info("Found file '{}' (priority queue).", fileToReindex.getFileName());
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
                return false;
            }

            if (!indexQueue.isEmpty()) {
                Path recordFile = indexQueue.poll();
                // Check whether the data folders for this record have been copied completely, otherwise skip
                Set<Path> alreadyCheckedFiles = new HashSet<>();
                while (!isDataFolderExportDone(recordFile)) {
                    logger.info("Export not yet finished for '{}'", recordFile.getFileName());
                    alreadyCheckedFiles.add(recordFile);
                    indexQueue.add(recordFile); // re-add at the end
                    if (alreadyCheckedFiles.contains(indexQueue.peek())) {
                        logger.info("All files in queue have not yet finished export.");
                        return true;
                    }
                    recordFile = indexQueue.poll();
                }
                logger.info("Processing {} from memory queue ({})...", recordFile.getFileName(), getHotfolderPath().getFileName());
                doIndex(recordFile);
                return true; // always break after attempting to index a file, so that the loop restarts
            }

            logger.debug("Hotfolder ({}): Listing files...", getHotfolderPath().getFileName());
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, "*.{xml,json,delete,purge,docupdate,UPDATED}")) {
                for (Path path : stream) {
                    // Only one file at a time right now
                    if (currentIndexer != null) {
                        break;
                    }
                    Path recordFile = path;
                    if (!recordFile.getFileName().toString().endsWith(MetsIndexer.ANCHOR_UPDATE_EXTENSION) && !indexQueue.contains(recordFile)) {
                        if (indexQueue.offer(recordFile)) {
                            logger.info("Added file from '{}' to index queue: {}", getHotfolderPath().getFileName(), path.getFileName());
                        } else {
                            logger.debug("Queue full ({})", getHotfolderPath().getFileName());
                        }
                    }
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

        return !highPriorityIndexQueue.isEmpty() || !indexQueue.isEmpty();
    }

    /**
     * 
     * @param recordFile
     * @return true if successful; false otherwise
     * @throws FatalIndexerException
     * @should return false if recordFile null
     * @should return true if successful
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
        boolean ret = handleSourceFile(recordFile, reindexSettings);
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
     * @should count files correctly
     */
    public long countRecordFiles() {
        if (!SolrIndexerDaemon.getInstance().getConfiguration().isCountHotfolderFiles()) {
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
        long freeSpace = hotfolderPath.toFile().getFreeSpace() / 1048576;
        logger.debug("Available storage space in hotfolder: {}M", freeSpace);
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
     * @param reindexSettings
     * @return true if successful; false otherwise
     * @throws FatalIndexerException
     */
    private boolean handleSourceFile(Path sourceFile, Map<String, Boolean> reindexSettings) throws FatalIndexerException {
        logger.info("handleSourceFile: {}", sourceFile);
        // Always unselect repository
        String filename = sourceFile.getFileName().toString();
        try {
            if (StringUtils.endsWithIgnoreCase(filename, FileTools.XML_EXTENSION)) {
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
                List<String> identifiers = Collections.emptyList();
                FileFormat fileType = JDomXP.determineFileFormat(sourceFile.toFile());
                switch (fileType) {
                    case METS:
                        if (metsEnabled) {
                            try {
                                currentIndexer = new MetsIndexer(this);
                                identifiers = currentIndexer.addToIndex(sourceFile, reindexSettings);
                            } finally {
                                currentIndexer = null;
                            }
                        } else {
                            logger.error("METS indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case METS_MARC:
                        if (metsEnabled) {
                            try {
                                currentIndexer = new MetsMarcIndexer(this);
                                identifiers = currentIndexer.addToIndex(sourceFile, reindexSettings);
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
                                identifiers = currentIndexer.addToIndex(sourceFile, reindexSettings);
                            } finally {
                                currentIndexer = null;
                            }
                        } else {
                            logger.error("LIDO indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case EAD:
                        if (eadEnabled) {
                            try {
                                currentIndexer = new EadIndexer(this);
                                identifiers = currentIndexer.addToIndex(sourceFile, reindexSettings);
                            } finally {
                                currentIndexer = null;
                            }
                        } else {
                            logger.error("EAD indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case EAD3:
                        if (eadEnabled) {
                            try {
                                currentIndexer = new Ead3Indexer(this);
                                identifiers = currentIndexer.addToIndex(sourceFile, reindexSettings);
                            } finally {
                                currentIndexer = null;
                            }
                        } else {
                            logger.error("EAD indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case DENKXWEB:
                        if (denkxwebEnabled) {
                            try {
                                currentIndexer = new DenkXwebIndexer(this);
                                identifiers = currentIndexer.addToIndex(sourceFile, reindexSettings);
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
                                identifiers = currentIndexer.addToIndex(sourceFile, reindexSettings);
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
                                identifiers = currentIndexer.addToIndex(sourceFile, reindexSettings);
                            } finally {
                                currentIndexer = null;
                            }
                        } else {
                            logger.error("WorldViews indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case CMS:
                        if (cmsEnabled) {
                            try {
                                currentIndexer = new CmsPageIndexer(this);
                                identifiers = currentIndexer.addToIndex(sourceFile, reindexSettings);
                            } finally {
                                currentIndexer = null;
                            }
                        } else {
                            logger.error("CMS page indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    default:
                        logger.error("Unknown file format, deleting: {}", filename);
                        Files.delete(sourceFile);
                        return false;
                }
                Utils.submitDataToViewer(identifiers, countRecordFiles());
            } else if (filename.endsWith(".json")) {
                if (filename.startsWith(FILENAME_PREFIX_STATISTICS_USAGE)) {
                    if (usageStatisticsEnabled) {
                        try {
                            this.currentIndexer = new UsageStatisticsIndexer(this);
                            currentIndexer.addToIndex(sourceFile, null);
                        } finally {
                            this.currentIndexer = null;
                        }
                    } else {
                        logger.error("Usage statistics indexing is disabled - please make sure all folders are configured.");
                    }
                    Files.delete(sourceFile);
                }
            } else if (filename.endsWith(FILENAME_EXTENSION_PURGE) || filename.endsWith(FILENAME_EXTENSION_DELETE)) {
                if (filename.startsWith(FILENAME_PREFIX_STATISTICS_USAGE)) {
                    removeUsageStatisticsFromIndex(sourceFile);
                    Files.delete(sourceFile);
                } else {
                    // PURGE / DELETE
                    DataRepository[] repositories = dataRepositoryStrategy.selectDataRepository(null, sourceFile, null,
                            SolrIndexerDaemon.getInstance().getSearchIndex(), SolrIndexerDaemon.getInstance().getOldSearchIndex());
                    String pi = removeFromIndex(sourceFile, repositories[1] != null ? repositories[1] : repositories[0],
                            filename.endsWith(FILENAME_EXTENSION_DELETE));
                    Utils.submitDataToViewer(Collections.singletonList(pi), countRecordFiles());
                }
            } else if (filename.endsWith(MetsIndexer.ANCHOR_UPDATE_EXTENSION)) {
                // SUPERUPDATE
                DataRepository[] repositories = dataRepositoryStrategy.selectDataRepository(null, sourceFile, null,
                        SolrIndexerDaemon.getInstance().getSearchIndex(), SolrIndexerDaemon.getInstance().getOldSearchIndex());
                MetsIndexer.anchorSuperupdate(sourceFile, updatedMets, repositories[1] != null ? repositories[1] : repositories[0]);
                Utils.submitDataToViewer(Collections.emptyList(), countRecordFiles()); // TODO submit any record identifiers here?
            } else if (filename.endsWith(DocUpdateIndexer.FILE_EXTENSION)) {
                // Single Solr document update
                List<String> identifiers = Collections.emptyList();
                try {
                    currentIndexer = new DocUpdateIndexer(this);
                    identifiers = currentIndexer.addToIndex(sourceFile, null);
                } finally {
                    currentIndexer = null;
                }
                Utils.submitDataToViewer(identifiers, countRecordFiles());
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
     * @param createTraceDoc A Solr document with DATEDELETED timestamp will be created if true.
     * @return Identifier of the deleted record, if successful; otherwise null
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     */
    private String removeFromIndex(Path deleteFile, DataRepository dataRepository, final boolean createTraceDoc)
            throws IOException, FatalIndexerException {
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
                return null;
            }

            Path actualXmlFile = dataRepository.getDir(DataRepository.PARAM_INDEXED_METS) != null
                    ? Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), baseFileName + ".xml")
                    : Paths.get("" + System.currentTimeMillis() + ".foo");
            if (!Files.exists(actualXmlFile) && dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO) != null) {
                actualXmlFile =
                        Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toAbsolutePath().toString(), baseFileName + ".xml");
            }
            if (!Files.exists(actualXmlFile) && dataRepository.getDir(DataRepository.PARAM_INDEXED_EAD) != null) {
                actualXmlFile =
                        Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_EAD).toAbsolutePath().toString(), baseFileName + ".xml");
            }
            if (!Files.exists(actualXmlFile) && dataRepository.getDir(DataRepository.PARAM_INDEXED_DENKXWEB) != null) {
                actualXmlFile =
                        Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_DENKXWEB).toAbsolutePath().toString(), baseFileName + ".xml");
            }
            if (!Files.exists(actualXmlFile) && dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE) != null) {
                actualXmlFile =
                        Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE).toAbsolutePath().toString(), baseFileName + ".xml");
            }
            if (!Files.exists(actualXmlFile) && dataRepository.getDir(DataRepository.PARAM_INDEXED_CMS) != null) {
                actualXmlFile =
                        Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_CMS).toAbsolutePath().toString(), baseFileName + ".xml");
            }
            FileFormat format = FileFormat.UNKNOWN;
            if (!Files.exists(actualXmlFile)) {
                logger.warn("XML file '{}' not found.", actualXmlFile.getFileName());
            }
            // Determine document format
            String[] fields = { SolrConstants.SOURCEDOCFORMAT, SolrConstants.DATEDELETED, SolrConstants.DOCTYPE };
            SolrDocumentList result =
                    SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + baseFileName, Arrays.asList(fields));
            boolean trace = createTraceDoc;
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
                    } else if (deleteFile.getParent().equals(dataRepository.getDir(DataRepository.PARAM_INDEXED_EAD))) {
                        format = FileFormat.EAD;
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
                if (format.equals(FileFormat.CMS)) {
                    logger.warn("CMS record '{}' not found in index.", baseFileName);
                } else {
                    logger.error("Record '{}' not found in index.", baseFileName);
                }
                return null;
            }

            boolean success = false;
            switch (format) {
                case CMS:
                case DENKXWEB:
                case DUBLINCORE:
                case EAD:
                case LIDO:
                case METS:
                case METS_MARC:
                case WORLDVIEWS:
                    if (trace) {
                        logger.info("Deleting {} file '{}'...", format.name(), actualXmlFile.getFileName());
                    } else {
                        logger.info("Deleting {} file '{}' (no trace document will be created)...", format.name(), actualXmlFile.getFileName());
                    }
                    success = Indexer.delete(baseFileName, trace, SolrIndexerDaemon.getInstance().getSearchIndex());
                    break;
                default:
                    logger.error("Unknown format: {}", format);
                    return null;
            }
            if (success) {
                dataRepository.deleteDataFoldersForRecord(baseFileName);
                if (actualXmlFile.toFile().exists()) {
                    Path deleted = Paths.get(deletedMets.toAbsolutePath().toString(), actualXmlFile.getFileName().toString());
                    Files.copy(actualXmlFile, deleted, StandardCopyOption.REPLACE_EXISTING);
                    Files.delete(actualXmlFile);
                    logger.info("'{}' has been successfully deleted.", actualXmlFile.getFileName());
                    return baseFileName;
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

        return null;
    }

    /**
     * @param sourceFile
     * @return true if successful; false otherwise
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
     * <p>
     * removeSourceFileFromQueue.
     * </p>
     *
     * @param pi a {@link java.lang.String} object
     * @throws java.io.IOException
     */
    public void removeSourceFileFromQueue(String pi) throws IOException {
        if (StringUtils.isEmpty(pi)) {
            return;
        }
        logger.info("removeSourceFileFromQueue: {}/{}.xml", getHotfolderPath().getFileName(), pi);

        Path matchingFile = null;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, "*.{xml,json,delete,purge,docupdate,UPDATED}")) {
            for (Path path : stream) {
                if (FilenameUtils.getBaseName(path.getFileName().toString()).equals(pi)) {
                    matchingFile = path;
                    break;
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        if (matchingFile != null) {
            if (indexQueue.contains(matchingFile)) {
                indexQueue.remove(matchingFile);
                logger.info("Removed '{}' from hotfolder '{}' index queue.", matchingFile.getFileName(), getHotfolderPath().getFileName());
            }
            Files.delete(matchingFile);
            logger.info("Deleted '{}' from hotfolder '{}'.", matchingFile.getFileName(), getHotfolderPath().getFileName());
        }
    }

    /**
     * Checks whether the data folders for the given record file have finished being copied.
     *
     * @param recordFile a {@link java.nio.file.Path} object.
     * @return a boolean.
     * @should return true if hotfolder content not changing
     */
    boolean isDataFolderExportDone(Path recordFile) {
        logger.debug("isDataFolderExportDone: {}", recordFile.getFileName());
        DataFolderSizeCounter counter = new DataFolderSizeCounter(recordFile.getFileName().toString());

        long total1 = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(getHotfolderPath(), counter)) {
            total1 = counter.getTotal();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            logger.error("Error checking the hotfolder size.", e);
            Thread.currentThread().interrupt();
            return false;
        }

        counter.resetTotal();
        long total2 = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(getHotfolderPath(), counter)) {
            total2 = counter.getTotal();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        logger.trace("Data export done: {}", total1 == total2);
        return total1 == total2;
    }

    protected class DataFolderSizeCounter implements Filter<Path> {

        private String recordFileName;
        private long total = 0;

        /**
         * @param recordFileName
         */
        public DataFolderSizeCounter(String recordFileName) {
            this.recordFileName = recordFileName;
        }

        /* (non-Javadoc)
         * @see java.nio.file.DirectoryStream.Filter#accept(java.lang.Object)
         */
        @Override
        public boolean accept(Path entry) throws IOException {
            if (entry != null && entry.getFileName().startsWith(FilenameUtils.getBaseName(recordFileName) + "_")) {
                try {
                    if (Files.isRegularFile(entry)) {
                        total += FileUtils.sizeOf(entry.toFile());
                    } else if (Files.isDirectory(entry)) {
                        total += FileUtils.sizeOfDirectory(entry.toFile());
                    }
                } catch (IllegalArgumentException e) {
                    logger.error(e.getMessage());
                }
            }

            return false; // reject everything, only the count matters
        }

        public long getTotal() {
            return total;
        }

        public void resetTotal() {
            total = 0;
        }
    }

    /**
     * <p>
     * Getter for the field <code>highPriorityIndexQueue</code>.
     * </p>
     *
     * @return a {@link java.util.Queue} object.
     */
    public Queue<Path> getHighPriorityQueue() {
        return highPriorityIndexQueue;
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
     * <p>
     * isDeleteContentFilesOnFailure.
     * </p>
     *
     * @return the deleteContentFilesOnFailure
     */
    public boolean isDeleteContentFilesOnFailure() {
        return deleteContentFilesOnFailure;
    }

    /**
     * <p>
     * Setter for the field <code>deleteContentFilesOnFailure</code>.
     * </p>
     *
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
     * <p>
     * Getter for the field <code>origDenkxWeb</code>.
     * </p>
     *
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
     * Getter for the field <code>metsFileSizeThreshold</code>.
     * </p>
     *
     * @return the metsFileSizeThreshold
     */
    public long getMetsFileSizeThreshold() {
        return metsFileSizeThreshold;
    }

    /**
     * <p>
     * Getter for the field <code>dataFolderSizeThreshold</code>.
     * </p>
     *
     * @return the dataFolderSizeThreshold
     */
    public long getDataFolderSizeThreshold() {
        return dataFolderSizeThreshold;
    }
}
