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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.mail.MessagingException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.jdom2.Document;
import org.jdom2.output.XMLOutputter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.AbstractIndexer;
import de.intranda.digiverso.presentation.solr.DocUpdateIndexer;
import de.intranda.digiverso.presentation.solr.LidoIndexer;
import de.intranda.digiverso.presentation.solr.MetsIndexer;
import de.intranda.digiverso.presentation.solr.SolrIndexerDaemon;
import de.intranda.digiverso.presentation.solr.helper.JDomXP.FileFormat;
import de.intranda.digiverso.presentation.solr.model.DataRepository;
import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;
import de.intranda.digiverso.presentation.solr.model.SolrConstants;

public class Hotfolder {

    private static final Logger logger = LoggerFactory.getLogger(Hotfolder.class);

    private static final String SHUTDOWN_FILE = ".SHUTDOWN_INDEXER";
    private static final int WAIT_IF_FILE_EMPTY = 5000;

    private StringWriter swSecondaryLog;
    private WriterAppender secondaryAppender;

    private final SolrHelper solrHelper;
    private final List<DataRepository> dataRepositories = new ArrayList<>();
    private final Queue<Path> reindexQueue = new LinkedList<>();

    private int minStorageSpace = 2048;
    public long metsFileSizeThreshold = 10485760;
    public long dataFolderSizeThreshold = 157286400;

    private Path hotfolderPath;
    private Path tempFolderPath;
    private Path viewerHomePath;
    private Path dataRepositoriesHomePath;
    private Path updatedMets;
    private Path deletedMets;
    private Path errorMets;
    private Path origLido;
    private Path success;

    private AbstractIndexer currentIndexer;
    private DataRepository selectedDataRepository;
    private DataRepository dummyRepository;
    private boolean dataRepositoriesEnabled = false;
    private boolean addVolumeCollectionsToAnchor = false;
    private boolean deleteContentFilesOnFailure = true;

    public static FilenameFilter filterDataFile = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return (name.toLowerCase().endsWith(AbstractIndexer.XML_EXTENSION) || name.toLowerCase().endsWith(".delete") || name.toLowerCase()
                    .endsWith(".purge") || name.endsWith(MetsIndexer.ANCHOR_UPDATE_EXTENSION));
        }
    };

    public static FilenameFilter filterMediaFolder = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return (new File(dir, name).isDirectory() && (name.endsWith("_tif") || name.endsWith("_media")));
        }
    };

    @SuppressWarnings("unchecked")
    public Hotfolder(String confFilename, SolrServer solrServer) throws FatalIndexerException {
        this.solrHelper = new SolrHelper(solrServer);
        logger.debug("Config file: {}", confFilename);
        Configuration config = Configuration.getInstance(confFilename);
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
            tempFolderPath = Paths.get(config.getConfiguration("tempFolder"));
            if (!Utils.checkAndCreateDirectory(tempFolderPath)) {
                logger.error("Could not create folder '{}', exiting...", tempFolderPath);
                throw new FatalIndexerException("Configuration error, see log for details.");
            }
        } catch (Exception e) {
            logger.error("<tempFolder> not defined.");
            throw new FatalIndexerException("Configuration error, see log for details.");
        }

        try {
            viewerHomePath = Paths.get(config.getConfiguration("viewerHome"));
            if (!Files.isDirectory(viewerHomePath)) {
                logger.error("Path defined in <viewerHome> does not exist, exiting...");
                throw new FatalIndexerException("Configuration error, see log for details.");
            }
        } catch (Exception e) {
            logger.error("<viewerHome> not defined, exiting...");
            throw new FatalIndexerException("Configuration error, see log for details.");
        }

        dataRepositoriesEnabled = Configuration.getInstance().isDataRepositoriesEnabled();
        logger.info("Data repositories are {}", (dataRepositoriesEnabled ? "enabled." : "disabled."));
        try {
            dataRepositoriesHomePath = Paths.get(config.getString("init.dataRepositories.dataRepositoriesHome"));
            if (!Utils.checkAndCreateDirectory(dataRepositoriesHomePath)) {
                throw new FatalIndexerException("Could not create directory : " + dataRepositoriesHomePath.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            logger.error("<dataRepositoriesHome> not defined.");
            throw new FatalIndexerException("Configuration error, see log for details.");
        }
        // Load data repositories
        List<String> dataRepositoryNames = config.getList("init.dataRepositories.dataRepository");
        if (dataRepositoriesEnabled && dataRepositoryNames != null) {
            for (String name : dataRepositoryNames) {
                DataRepository dataRepository = new DataRepository(dataRepositoriesHomePath, name);
                if (dataRepository.isValid()) {
                    dataRepositories.add(dataRepository);
                    if (dataRepositoriesEnabled) {
                        logger.info("Found configured data repository: {}", name);
                    }
                }
            }
        }
        try {
            DataRepository.dataRepositoriesMaxRecords = config.getInt("init.dataRepositories.maxRecords", 10000);
        } catch (Exception e) {
            logger.error("<dataRepositories.maxRecords> not defined.");
        }

        try {
            updatedMets = Paths.get(config.getConfiguration("updatedMets"));
            if (!Utils.checkAndCreateDirectory(updatedMets)) {
                throw new FatalIndexerException("Could not create directory : " + updatedMets.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            logger.error("<updatedMets> not defined.");
        }
        try {
            deletedMets = Paths.get(config.getConfiguration("deletedMets"));
            if (!Utils.checkAndCreateDirectory(deletedMets)) {
                throw new FatalIndexerException("Could not create directory : " + deletedMets.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            logger.error("<deletedMets> not defined.");
        }
        try {
            errorMets = Paths.get(config.getConfiguration("errorMets"));
            if (!Utils.checkAndCreateDirectory(errorMets)) {
                throw new FatalIndexerException("Could not create directory : " + errorMets.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            logger.error("<errorMets> not defined.");
        }
        try {
            origLido = Paths.get(config.getConfiguration("origLido"));
            if (!Utils.checkAndCreateDirectory(origLido)) {
                throw new FatalIndexerException("Could not create directory : " + origLido.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            logger.error("<origLido> not defined.");
        }
        try {
            success = Paths.get(config.getConfiguration("successFolder"));
            if (!Utils.checkAndCreateDirectory(success)) {
                throw new FatalIndexerException("Could not create directory : " + success.toAbsolutePath().toString());
            }
        } catch (Exception e) {
            logger.error("<successFolder> not defined.");
        }
        try {
            metsFileSizeThreshold = Configuration.getInstance().getInt("performance.metsFileSizeThreshold", 10485760);
        } catch (Exception e) {
            logger.error("<metsFileSizeThreshold> not defined.");
        }
        try {
            dataFolderSizeThreshold = Configuration.getInstance().getInt("performance.dataFolderSizeThreshold", 157286400);
        } catch (Exception e) {
            logger.error("<dataFolderSizeThreshold> not defined.");
        }

        SolrHelper.optimize = Boolean.valueOf(Configuration.getInstance().isAutoOptimize());
        logger.info("Auto-optimize: {}", SolrHelper.optimize);

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

        final PatternLayout layout = PatternLayout.newBuilder().withPattern(
                "%-5level %d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] (%F\\:%M\\:%L)%n        %msg%n").withConfiguration(config).build();
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
            logger.debug("init.email.recipients not configured, cannot send e-mail report.");
            return;
        }
        String smtpServer = Configuration.getInstance().getString("init.email.smtpServer");
        if (StringUtils.isEmpty(smtpServer)) {
            logger.debug("init.email.smtpServer not configured, cannot send e-mail report.");
            return;
        }
        String smtpUser = Configuration.getInstance().getString("init.email.smtpUser");
        if (StringUtils.isEmpty(smtpUser)) {
            logger.debug("init.email.smtpUser not configured, cannot send e-mail report.");
            return;
        }
        String smtpPassword = Configuration.getInstance().getString("init.email.smtpPassword");
        if (StringUtils.isEmpty(smtpPassword)) {
            logger.debug("init.email.smtpPassword not configured, cannot send e-mail report.");
            return;
        }
        String smtpSenderAddress = Configuration.getInstance().getString("init.email.smtpSenderAddress");
        if (StringUtils.isEmpty(smtpSenderAddress)) {
            logger.debug("init.email.smtpSenderAddress not configured, cannot send e-mail report.");
            return;
        }
        String smtpSenderName = Configuration.getInstance().getString("init.email.smtpSenderName");
        if (StringUtils.isEmpty(smtpSenderName)) {
            logger.debug("init.email.smtpSenderName not configured, cannot send e-mail report.");
            return;
        }
        String smtpSecurity = Configuration.getInstance().getString("init.email.smtpSecurity");
        if (StringUtils.isEmpty(smtpSecurity)) {
            logger.debug("init.email.smtpSecurity not configured, cannot send e-mail report.");
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
     * @throws FatalIndexerException
     */
    public boolean scan() throws FatalIndexerException {
        boolean noerror = true;
        if (Files.isDirectory(hotfolderPath)) {
            Path fileToReindex = reindexQueue.poll();
            if (fileToReindex != null) {
                resetSecondaryLog();
                logger.info("Found file '{}' (re-index queue).", fileToReindex.getFileName());
                checkFreeSpace();
                Map<String, Boolean> reindexSettings = new HashMap<>();
                reindexSettings.put("reindexText", true);
                reindexSettings.put("reindexWordCoords", true);
                reindexSettings.put("reindexAlto", true);
                reindexSettings.put("reindexMix", true);
                reindexSettings.put("reindexUGC", true);
                noerror = handleDataFile(fileToReindex, true, reindexSettings);
                if (swSecondaryLog != null) {
                    checkAndSendErrorReport(fileToReindex.getFileName() + ": Indexing failed (v" + SolrIndexerDaemon.VERSION + ")", swSecondaryLog
                            .toString());
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
                            reindexSettings.put("reindexText", false);
                            reindexSettings.put("reindexWordCoords", false);
                            reindexSettings.put("reindexAlto", false);
                            reindexSettings.put("reindexMix", false);
                            reindexSettings.put("reindexUGC", false);
                            noerror = handleDataFile(recordFile, false, reindexSettings);
                            // logger.error("for the lulz");
                            checkAndSendErrorReport(recordFile.getFileName() + ": Indexing failed (v" + SolrIndexerDaemon.VERSION + ")",
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

        } else {
            noerror = false;
            logger.error("Hotfolder not found!");
        }
        return noerror;
    }

    /**
     * Checks whether there is at least as much free storage space available as configured. If not, the program will shut down.
     * 
     * @throws FatalIndexerException
     */
    private void checkFreeSpace() throws FatalIndexerException {
        // TODO
        int freeSpace = (int) (hotfolderPath.toFile().getFreeSpace() / 1048576);
        logger.debug("Available storage space: {}M", freeSpace);
        if (freeSpace < minStorageSpace) {
            logger.error("Insufficient free space: {} / {} MB available. Indexer will now shut down.", freeSpace, minStorageSpace);
            if (swSecondaryLog != null) {
                checkAndSendErrorReport("Record indexing failed due to insufficient space (v" + SolrIndexerDaemon.VERSION + ")", swSecondaryLog
                        .toString());
            }
            throw new FatalIndexerException("Insufficient free space");
        }
    }

    /**
     * Selects available data repository for the given record. If no repository could be selected, the indexer MUST be halted.
     * 
     * @param file
     * @param inPi
     * @throws FatalIndexerException
     */
    public void selectDataRepository(Path file, String inPi) throws FatalIndexerException {
        String pi = inPi;
        if (StringUtils.isEmpty(pi) && file != null) {
            String fileExtension = FilenameUtils.getExtension(file.getFileName().toString());
            if (MetsIndexer.ANCHOR_UPDATE_EXTENSION.equals("." + fileExtension) || "delete".equals(fileExtension) || "purge".equals(fileExtension)) {
                pi = Utils.extractPiFromFileName(file);
            }
        }
        if (StringUtils.isNotBlank(pi)) {
            String previousRepository = null;
            try {
                // Look up previous repository in the index
                previousRepository = solrHelper.findCurrentDataRepository(pi);
            } catch (SolrServerException e) {
                logger.error(e.getMessage(), e);
            }
            if (previousRepository != null) {
                if ("?".equals(previousRepository)) {
                    if (dataRepositoriesEnabled) {
                        // Record is already indexed, but not in a data repository
                        dummyRepository = new DataRepository(Paths.get(Configuration.getInstance().getString("init.viewerHome")), "");
                        logger.info(
                                "This record is already indexed, but its data files are not in a repository. The data files will be moved to the selected repository.");
                    }
                } else {
                    // Find previous repository
                    for (DataRepository repository : dataRepositories) {
                        if (previousRepository.equals(repository.getName())) {
                            if (dataRepositoriesEnabled) {
                                logger.info("Using previous data repository for '{}': {}", pi, previousRepository);
                                selectedDataRepository = repository;
                            } else {
                                logger.info(
                                        "'{}' is currently indexed in data repository '{}'. Since data repositories are disabled, it will be moved to out of the repository.",
                                        pi, previousRepository);
                                dummyRepository = repository;
                                selectedDataRepository = new DataRepository(Paths.get(Configuration.getInstance().getString("init.viewerHome")), "");
                            }
                            return;
                        }
                    }
                    logger.warn("Previous data repository for '{}' does not exist: {}", pi, previousRepository);
                }
            }

            if (!dataRepositoriesEnabled) {
                selectedDataRepository = new DataRepository(viewerHomePath, "");
                return;
            }

            // Find available repository
            try {
                for (DataRepository repository : dataRepositories) {
                    int records = repository.getNumRecords();
                    if (records < DataRepository.dataRepositoriesMaxRecords) {
                        selectedDataRepository = repository;
                        logger.info("Repository selected for '{}': {} (currently contains {} records)", pi, selectedDataRepository.getName(),
                                records);
                        return;
                    } else if (records > DataRepository.dataRepositoriesMaxRecords) {
                        logger.warn("Repository '{}' contains {} records, the limit is {}, though.", repository.getName(), records,
                                DataRepository.dataRepositoriesMaxRecords);
                    }
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
            logger.error("No data repository available for indexing. Please configure additional repositories. Exiting...");
            throw new FatalIndexerException("No data repository available for indexing. Please configure additional repositories. Exiting...");
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
        selectedDataRepository = null;
        dummyRepository = null;
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
                        addMetsToIndex(dataFile, fromReindexQueue, reindexSettings);
                        break;
                    case LIDO:
                        addLidoToIndex(dataFile, reindexSettings);
                        break;
                    default:
                        logger.error("Unknown file format, deleting: {}", filename);
                        Files.delete(dataFile);
                        return false;
                }

            } else if (filename.endsWith(".delete")) {
                // DELETE
                selectDataRepository(dataFile, null);
                removeFromIndex(dataFile, true);
            } else if (filename.endsWith(".purge")) {
                // PURGE (delete with no "deleted" doc)
                selectDataRepository(dataFile, null);
                removeFromIndex(dataFile, false);
            } else if (filename.endsWith(MetsIndexer.ANCHOR_UPDATE_EXTENSION)) {
                // SUPERUPDATE
                selectDataRepository(dataFile, null);
                MetsIndexer.superupdate(dataFile, updatedMets, selectedDataRepository);
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
     * @param trace A Lucene document with DATEDELETED timestamp will be created if true.
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     */
    private void removeFromIndex(Path deleteFile, boolean trace) throws IOException, FatalIndexerException {
        String baseFileName = FilenameUtils.getBaseName(deleteFile.getFileName().toString());
        try {
            // Check for empty file names, otherwise the entire content folders will be deleted!
            if (StringUtils.isBlank(baseFileName)) {
                logger.error("File '{}' contains no identifier, aborting...", deleteFile.getFileName());
                return;
            }

            DataRepository useRepository = selectedDataRepository;
            if (dummyRepository != null) {
                useRepository = dummyRepository;
            }

            Path actualXmlFile = Paths.get(useRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), baseFileName
                    + ".xml");
            if (!Files.exists(actualXmlFile)) {
                actualXmlFile = Paths.get(useRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toAbsolutePath().toString(), baseFileName + ".xml");
            }
            FileFormat format = FileFormat.UNKNOWN;
            if (!Files.exists(actualXmlFile)) {
                logger.warn("XML file '{}' not found.", actualXmlFile.getFileName().toString());
            }
            // Determine document format
            String[] fields = { SolrConstants.SOURCEDOCFORMAT, SolrConstants.DATEDELETED };
            SolrDocumentList result = solrHelper.search(SolrConstants.PI + ":" + baseFileName, Arrays.asList(fields));
            if (!result.isEmpty()) {
                SolrDocument doc = result.get(0);
                format = FileFormat.getByName((String) doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
                // Attempt to determine the file format by the path if no SOURCEDOCFORMAT field exists
                if (format.equals(FileFormat.UNKNOWN)) {
                    logger.warn("SOURCEDOCFORMAT not found, attempting to determine the format via the file path...");
                    if (deleteFile.getParent().equals(useRepository.getDir(DataRepository.PARAM_INDEXED_METS))) {
                        format = FileFormat.METS;
                    } else if (deleteFile.getParent().equals(useRepository.getDir(DataRepository.PARAM_INDEXED_LIDO))) {
                        format = FileFormat.LIDO;
                    } else if (doc.containsKey(SolrConstants.DATEDELETED)) {
                        format = FileFormat.METS;
                        trace = false;
                        logger.info("Record '{}' is already a trace document and will be removed without a trace.", baseFileName);
                    }
                }
            } else {
                logger.error("Record '{}' not found in index.", baseFileName);
                return;
            }

            boolean success = false;
            switch (format) {
                case METS:
                    if (trace) {
                        logger.info("Deleting {} file '{}'...", format.name(), actualXmlFile.getFileName());
                    } else {
                        logger.info("Deleting {} file '{}' (no trace document will be created)...", format.name(), actualXmlFile.getFileName());
                    }
                    success = AbstractIndexer.delete(baseFileName, trace, solrHelper);
                    break;
                case LIDO:
                    if (trace) {
                        logger.info("Deleting {} file '{}'...", format.name(), actualXmlFile.getFileName());
                    } else {
                        logger.info("Deleting {} file '{}' (no trace document will be created)...", format.name(), actualXmlFile.getFileName());
                    }
                    success = AbstractIndexer.delete(baseFileName, trace, solrHelper);
                    break;
                default:
                    logger.error("Unknown format: {}", format);
                    return;
            }
            if (success) {
                useRepository.deleteDataFoldersForRecord(baseFileName);
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
     * Indexes the fiven METS file.
     * 
     * @param metsFile {@link File}
     * @param fromReindexQueue
     * @param reindexSettings
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     * 
     */
    private void addMetsToIndex(Path metsFile, boolean fromReindexQueue, Map<String, Boolean> reindexSettings) throws IOException,
            FatalIndexerException {
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
                        dataFolders.put(DataRepository.PARAM_TEI, path);
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
                    case "_overview":
                        dataFolders.put(DataRepository.PARAM_OVERVIEW, path);
                        break;
                    default:
                        // nothing
                }
            }
        }

        // Use existing folders for those missing in the hotfolder
        if (dataFolders.get(DataRepository.PARAM_MEDIA) == null) {
            reindexSettings.put("reindexMedia", true);
        }
        if (dataFolders.get(DataRepository.PARAM_ALTO) == null) {
            reindexSettings.put("reindexAlto", true);
        }
        if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) == null) {
            reindexSettings.put("reindexCrowdAlto", true);
        }
        if (dataFolders.get(DataRepository.PARAM_FULLTEXT) == null) {
            reindexSettings.put("reindexText", true);
        }
        if (dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) == null) {
            reindexSettings.put("reindexCrowdText", true);
        }
        if (dataFolders.get(DataRepository.PARAM_TEI) == null) {
            reindexSettings.put("reindexWordCoords", true);
        }
        if (dataFolders.get(DataRepository.PARAM_ABBYY) == null) {
            reindexSettings.put("reindexAbbyy", true);
        }
        if (dataFolders.get(DataRepository.PARAM_MIX) == null) {
            reindexSettings.put("reindexMix", true);
        }
        if (dataFolders.get(DataRepository.PARAM_UGC) == null) {
            reindexSettings.put("reindexUGC", true);
        }
        if (dataFolders.get(DataRepository.PARAM_OVERVIEW) == null) {
            reindexSettings.put("reindexOverview", true);
        }

        try {
            currentIndexer = new MetsIndexer(this);
            resp = ((MetsIndexer) currentIndexer).index(metsFile, fromReindexQueue, dataFolders, null, Configuration.getInstance()
                    .getPageCountStart());
        } finally {
            currentIndexer = null;
        }

        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            // String newMetsFileName = URLEncoder.encode(resp[0], "utf-8");
            String newMetsFileName = resp[0];
            String pi = FilenameUtils.getBaseName(newMetsFileName);
            // kopieren
            Path indexed = Paths.get(selectedDataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), newMetsFileName);
            if (metsFile.equals(indexed)) {
                logger.debug("'{}' is an existing indexed file - not moving it.", metsFile.getFileName());
                return;
            }
            if (Files.exists(indexed)) {
                // Add a timestamp to the old file name
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss", Locale.getDefault());
                String oldMetsFilename = FilenameUtils.getBaseName(newMetsFileName) + "_" + sdf.format(new Date()) + ".xml";
                Path newFile = Paths.get(updatedMets.toAbsolutePath().toString(), oldMetsFilename);
                Files.copy(indexed, newFile);
                logger.debug("Old METS file copied to '{}'.", newFile.toAbsolutePath());
            }
            Files.copy(metsFile, indexed, StandardCopyOption.REPLACE_EXISTING);

            if (dummyRepository != null) {
                // Move non-repository data folders to the selected repository
                dummyRepository.moveDataFoldersToRepository(selectedDataRepository, FilenameUtils.getBaseName(newMetsFileName));
            }

            // Copy and delete media folder
            if (reindexSettings.get("reindexMedia") == null || !reindexSettings.get("reindexMedia")) {
                if (selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_MEDIA), DataRepository.PARAM_MEDIA, pi) > 0) {
                    String msg = Utils.removeRecordImagesFromCache(FilenameUtils.getBaseName(resp[0]));
                    if (msg != null) {
                        logger.info(msg);
                    }
                }
            }
            // Copy and delete ALTO folder
            if (reindexSettings.get("reindexAlto") == null || !reindexSettings.get("reindexAlto")) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_ALTO), DataRepository.PARAM_ALTO, pi);
            }
            // Copy and delete crowdsourcing ALTO folder
            if ((reindexSettings.get("reindexCrowdAlto") == null || !reindexSettings.get("reindexCrowdAlto")) && selectedDataRepository.getDir(
                    DataRepository.PARAM_ALTOCROWD) != null) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_ALTOCROWD), DataRepository.PARAM_ALTOCROWD, pi);
            }
            // Copy and delete fulltext folder
            if (reindexSettings.get("reindexText") == null || !reindexSettings.get("reindexText")) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_FULLTEXT), DataRepository.PARAM_FULLTEXT, pi);
            }
            // Copy and delete crowdsourcing fulltext folder
            if ((reindexSettings.get("reindexCrowdText") == null || !reindexSettings.get("reindexCrowdText")) && selectedDataRepository.getDir(
                    DataRepository.PARAM_FULLTEXTCROWD) != null) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD),
                        DataRepository.PARAM_FULLTEXTCROWD, pi);
            }
            // Copy and delete TEI word coordinates folder
            if (reindexSettings.get("reindexWordCoords") == null || !reindexSettings.get("reindexWordCoords")) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_TEI), DataRepository.PARAM_TEI, pi);
            }

            // Copy and delete ABBYY word coordinates folder
            if (reindexSettings.get("reindexAbbyy") == null || !reindexSettings.get("reindexAbbyy")) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_ABBYY), DataRepository.PARAM_ABBYY, pi);
            }

            // Copy and delete MIX files
            if (reindexSettings.get("reindexMix") == null || !reindexSettings.get("reindexMix")) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_MIX), DataRepository.PARAM_MIX, pi);
            }

            // Copy and delete page PDF files
            if (dataFolders.get(DataRepository.PARAM_PAGEPDF) != null) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_PAGEPDF), DataRepository.PARAM_PAGEPDF, pi);
            }

            // Copy and delete original content files
            if (dataFolders.get(DataRepository.PARAM_SOURCE) != null) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_SOURCE), DataRepository.PARAM_SOURCE, pi);
            }

            // Copy and delete user generated content files
            if (reindexSettings.get("reindexUGC") == null || !reindexSettings.get("reindexUGC")) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_UGC), DataRepository.PARAM_UGC, pi);
            }

            // Copy and delete overview page text files
            if (reindexSettings.get("reindexOverview") == null || !reindexSettings.get("reindexOverview")) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_OVERVIEW), DataRepository.PARAM_OVERVIEW, pi);
            }

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
                logger.error("'{}' could not be deleted! Please delete it manually!", metsFile.toAbsolutePath());
            }

        } else {
            // Error
            if (deleteContentFilesOnFailure) {
                // Delete all data folders in hotfolder
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new DirectoryStream.Filter<Path>() {

                    @Override
                    public boolean accept(Path entry) throws IOException {
                        return Files.isDirectory(entry) && (entry.getFileName().toString().endsWith("_tif") || entry.getFileName().toString()
                                .endsWith("_media"));
                    }
                });) {
                    for (Path path : stream) {
                        logger.info("Found data folder: {}", path.getFileName());
                        Utils.deleteDirectory(path);
                    }
                }

                if (reindexSettings.get("reindexAlto") == null || !reindexSettings.get("reindexAlto")) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_ALTO));
                }
                if (reindexSettings.get("reindexCrowdAlto") == null || !reindexSettings.get("reindexCrowdAlto")) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_ALTOCROWD));
                }
                if (reindexSettings.get("reindexText") == null || !reindexSettings.get("reindexText")) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_FULLTEXT));
                }
                if (reindexSettings.get("reindexCrowdText") == null || !reindexSettings.get("reindexCrowdText")) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD));
                }
                if (reindexSettings.get("reindexWordCoords") == null || !reindexSettings.get("reindexWordCoords")) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_TEI));
                }
                if (reindexSettings.get("reindexAbbyy") == null || !reindexSettings.get("reindexAbbyy")) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_ABBYY));
                }
                if (reindexSettings.get("reindexMix") == null || !reindexSettings.get("reindexMix")) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_MIX));
                }
                if (dataFolders.get(DataRepository.PARAM_PAGEPDF) != null) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_PAGEPDF));
                }
                if (dataFolders.get(DataRepository.PARAM_SOURCE) != null) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_SOURCE));
                }
                if (dataFolders.get(DataRepository.PARAM_UGC) != null) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_UGC));
                }
                if ((reindexSettings.get("reindexOverview") == null || !reindexSettings.get("reindexOverview")) && dataFolders.get(
                        DataRepository.PARAM_OVERVIEW) != null) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_OVERVIEW));
                }
                // Delete unsupported data folders
                //                List<File> unknownDirs = Arrays.asList(hotfolderPath.listFiles(getDataFolderFilter(fileNameRoot + "_")));
                //                if (unknownDirs != null) {
                //                    for (File f : unknownDirs) {
                //                        if (!deleteDirectory(f)) {
                //                            logger.warn("'" + f.getAbsolutePath() + "' could not be deleted.");
                //                        }
                //                    }
                //                }
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
            reindexSettings.put("reindexMedia", true);
        }
        if (dataFolders.get(DataRepository.PARAM_MIX) == null) {
            reindexSettings.put("reindexMix", true);
        }

        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile.toFile());
        logger.info("File contains {} LIDO documents.", lidoDocs.size());
        XMLOutputter outputter = new XMLOutputter();
        boolean errors = false;
        try {
            for (Document doc : lidoDocs) {
                currentIndexer = new LidoIndexer(this);
                resp = ((LidoIndexer) currentIndexer).index(doc, dataFolders, null, Configuration.getInstance().getPageCountStart(), Configuration
                        .getInstance().getList("init.lido.imageXPath"), dataFolders.containsKey(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER));
                if (!"ERROR".equals(resp[0])) {
                    // String newMetsFileName = URLEncoder.encode(resp[0], "utf-8");
                    String identifier = resp[0];
                    String newLidoFileName = identifier + ".xml";

                    // Write invidivual LIDO records as separate files
                    Path indexed = Paths.get(selectedDataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toAbsolutePath().toString(),
                            newLidoFileName);
                    try (FileOutputStream out = new FileOutputStream(indexed.toFile())) {
                        outputter.output(doc, out);
                    }

                    // Move non-repository data directories to the selected repository
                    if (dummyRepository != null) {
                        dummyRepository.moveDataFoldersToRepository(selectedDataRepository, identifier);
                    }

                    // copy media files
                    boolean mediaFilesCopied = false;
                    Path destMediaDir = Paths.get(selectedDataRepository.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), identifier);
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
                    } else {
                        logger.info("{} media file(s) copied.", imageCounter);
                    }

                    // Copy and delete MIX files
                    if (!reindexSettings.get("reindexMix")) {
                        Path destMixDir = Paths.get(selectedDataRepository.getDir(DataRepository.PARAM_MIX).toAbsolutePath().toString(), identifier);
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
        if (dataFolders.get(DataRepository.PARAM_MEDIA) != null && Files.isDirectory(dataFolders.get(DataRepository.PARAM_MEDIA)) && !Utils
                .deleteDirectory(dataFolders.get(DataRepository.PARAM_MEDIA))) {
            logger.warn("'{}' could not be deleted; please delete it manually.", dataFolders.get(DataRepository.PARAM_MEDIA).toAbsolutePath());
        }
        if (!reindexSettings.get("reindexMix") && dataFolders.get(DataRepository.PARAM_MIX) != null && Files.isDirectory(dataFolders.get(
                DataRepository.PARAM_MIX)) && !Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_MIX))) {
            logger.warn("'{}' could not be deleted; please delete it manually.", dataFolders.get(DataRepository.PARAM_MIX).toAbsolutePath());
        }
        if (dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER) != null && Files.isDirectory(dataFolders.get(
                DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER)) && !Utils.deleteDirectory(dataFolders.get(
                        DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER))) {
            logger.warn("'{}' could not be deleted; please delete it manually.", dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER)
                    .toAbsolutePath());
        }

        if (deleteContentFilesOnFailure) {
            // Delete all folders
            if (dataFolders.get(DataRepository.PARAM_MEDIA) != null && Files.isDirectory(dataFolders.get(DataRepository.PARAM_MEDIA))) {
                Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_MEDIA));
            }
            if (!reindexSettings.get("reindexMix") && dataFolders.get(DataRepository.PARAM_MIX) != null && Files.isDirectory(dataFolders.get(
                    DataRepository.PARAM_MIX))) {
                Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_MIX));
            }
            if (dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER) != null && Files.isDirectory(dataFolders.get(
                    DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER))) {
                Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER));
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

        try {
            currentIndexer = new DocUpdateIndexer(this);
            resp = ((DocUpdateIndexer) currentIndexer).index(dataFile, dataFolders);
        } finally {
            currentIndexer = null;
        }

        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            String pi = resp[0];

            // Copy and delete crowdsourcing ALTO folder
            if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) != null && selectedDataRepository.getDir(DataRepository.PARAM_ALTOCROWD) != null) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_ALTOCROWD), DataRepository.PARAM_ALTOCROWD, pi);
            }
            // Copy and delete crowdsourcing fulltext folder
            if (dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) != null && selectedDataRepository.getDir(
                    DataRepository.PARAM_FULLTEXTCROWD) != null) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD),
                        DataRepository.PARAM_FULLTEXTCROWD, pi);
            }
            // Copy and delete user generated content files
            if (dataFolders.get(DataRepository.PARAM_UGC) != null && selectedDataRepository.getDir(DataRepository.PARAM_UGC) != null) {
                selectedDataRepository.copyAndDeleteDataFolder(dataFolders.get(DataRepository.PARAM_UGC), DataRepository.PARAM_UGC, pi);
            }

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
                        return Files.isDirectory(entry) && (entry.getFileName().toString().endsWith("_tif") || entry.getFileName().toString()
                                .endsWith("_media"));
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
     * @param recordFile
     * @return
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
     * 
     * 
     * @param sourceLocation {@link File}
     * @param targetLocation {@link File}
     * @return number of copied files.
     * @throws IOException in case of errors.
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

    public Queue<Path> getReindexQueue() {
        return reindexQueue;
    }

    public Path getHotfolder() {
        return hotfolderPath;
    }

    public Path getTempFolder() {
        return tempFolderPath;
    }

    /**
     * @return the dataRepositoriesEnabled
     */
    public boolean isDataRepositoriesEnabled() {
        return dataRepositoriesEnabled;
    }

    /**
     * @return the addVolumeCollectionsToAnchor
     */
    public boolean isAddVolumeCollectionsToAnchor() {
        return addVolumeCollectionsToAnchor;
    }

    /**
     * @return the repositoriesHomePath
     */
    public Path getDataRepositoriesHomePath() {
        return dataRepositoriesHomePath;
    }

    /**
     * @return the dataRepositories
     */
    public List<DataRepository> getDataRepositories() {
        return dataRepositories;
    }

    public Path getUpdatedMets() {
        return updatedMets;
    }

    public Path getDeletedMets() {
        return deletedMets;
    }

    public Path getErrorMets() {
        return errorMets;
    }

    /**
     * @return the origLido
     */
    public Path getOrigLido() {
        return origLido;
    }

    public Path getSuccess() {
        return success;
    }

    public static FilenameFilter getDataFolderFilter(final String prefix) {
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return (name.startsWith(prefix) && new File(dir, name).isDirectory());
            }
        };

        return filter;
    }

    /**
     * Returns the currently selected data repository. If the current record is already indexed, the dummy repository (= old style fodler structure)
     * is returned.
     * 
     * @return
     */
    public DataRepository getSelectedRepository() {
        return selectedDataRepository;
    }

    public DataRepository getDataRepository() {
        if (dummyRepository != null) {
            return dummyRepository;
        }
        return selectedDataRepository;
    }

    public void setDummyRepository(DataRepository dummyRepository) {
        this.dummyRepository = dummyRepository;
    }

    /**
     * @return the solrHelper
     */
    public SolrHelper getSolrHelper() {
        return solrHelper;
    }
}
