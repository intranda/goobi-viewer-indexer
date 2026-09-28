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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import io.goobi.viewer.indexer.CmsPageIndexer;
import io.goobi.viewer.indexer.Indexer;
import io.goobi.viewer.indexer.DenkXwebIndexer;
import io.goobi.viewer.indexer.DocUpdateIndexer;
import io.goobi.viewer.indexer.DublinCoreIndexer;
import io.goobi.viewer.indexer.Ead3Indexer;
import io.goobi.viewer.indexer.EadIndexer;
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

    private List<String> addVolumeCollectionsToAnchorFields = Collections.emptyList();
    private boolean deleteContentFilesOnFailure = true;
    private boolean prioritizeLargeImageFolders = false;
    private boolean emailConfigurationComplete = false;

    private SecondaryAppender secondaryAppender;

    private final IDataRepositoryStrategy dataRepositoryStrategy;
    /** Regular index queue for files found in the regular hotfolder. */
    private final Queue<Path> indexQueue = new LinkedBlockingQueue<>(queueCapacity);
    /**
     * High priority index queue for anchor re-indexing, anchor update (.UPDATED) files, etc. Concurrent because record
     * workers add .UPDATED files to it (via {@link #getHighPriorityQueue()}) while the scan thread polls it.
     */
    private final Queue<Path> highPriorityIndexQueue = new ConcurrentLinkedQueue<>();
    /**
     * PIs of anchor records whose volume count changed and that must be reconciled (NUMVOLUMES + METS) once queues drain.
     * <p>
     * This set is in-memory only and is not persisted across restarts. If the indexer is stopped between a volume import and the deferred
     * anchor reconciliation, the affected anchor's NUMVOLUMES may remain stale until the next volume of that anchor is imported (which
     * re-triggers {@link #flagAnchorForReconciliation(String)}). Since {@link #reconcileAnchor(String)} always validates the stored count
     * against Solr, this causes only a delayed count refresh, never data corruption.
     * <p>
     * A concurrent set because, under record-level parallelism ({@code performance.threads} &gt; 1), several volume
     * workers may flag their anchor at the same time.
     */
    private final Set<String> anchorPisPendingReconciliation = ConcurrentHashMap.newKeySet();

    /**
     * Per-anchor-PI locks serializing writes to the same anchor (the {@code anchorMerge}/anchor-doc write in
     * {@link io.goobi.viewer.indexer.MetsIndexer} and {@code anchorSuperupdate}) when records index in parallel. Different
     * anchors proceed concurrently; volumes take no lock (they only flag their anchor). Entries are created on demand and
     * intentionally not evicted — the number of distinct anchor PIs per run is small.
     */
    private final ConcurrentMap<String, ReentrantLock> anchorWriteLocks = new ConcurrentHashMap<>();

    /** Executor for record-level parallelism; null when serial ({@code performance.threads}=1 and shared pool off). */
    private ExecutorService recordExecutor;
    /** True if this hotfolder created {@link #recordExecutor} itself (and must shut it down); false when it is the shared daemon pool. */
    private boolean ownsRecordExecutor;
    /** Records submitted to {@link #recordExecutor} and not yet awaited. Accessed only from the single scan thread. */
    private final List<Future<Boolean>> inFlightRecords = new ArrayList<>();
    /** Upper bound on submitted-but-unfinished records; beyond it the scan thread blocks on the oldest for backpressure. */
    private int maxInFlightRecords = 1;

    /**
     * Zero-arg constructor for tests.
     * 
     * @throws FatalIndexerException
     */
    Hotfolder() throws FatalIndexerException {
        logger.trace("Hotfolder()");
        this.dataRepositoryStrategy = SolrIndexerDaemon.getInstance().getDataRepositoryStrategy();
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
        dataRepositoryStrategy = SolrIndexerDaemon.getInstance().getDataRepositoryStrategy();

        initFolders(hotfolderPath, SolrIndexerDaemon.getInstance().getConfiguration());

        metsFileSizeThreshold = SolrIndexerDaemon.getInstance().getConfiguration().getInt("performance.metsFileSizeThreshold", 10485760);
        dataFolderSizeThreshold = SolrIndexerDaemon.getInstance().getConfiguration().getInt("performance.dataFolderSizeThreshold", 157286400);

        // The settings below come from the global configuration, so they are logged once at startup by logGlobalSettings()
        // rather than once per hotfolder here.
        addVolumeCollectionsToAnchorFields = SolrIndexerDaemon.getInstance().getConfiguration().getAddVolumeCollectionsToAnchorFields();
        prioritizeLargeImageFolders = SolrIndexerDaemon.getInstance().getConfiguration().isPrioritizeLargeImageFolders();
        deleteContentFilesOnFailure = readDeleteContentFilesOnFailure(SolrIndexerDaemon.getInstance().getConfiguration());

        MetadataHelper.setAuthorityDataEnabled(SolrIndexerDaemon.getInstance().getConfiguration().getBoolean("init.authorityData[@enabled]", true));
        if (MetadataHelper.isAuthorityDataEnabled()) {
            MetadataHelper.setAuthorityGndEnabled(
                    SolrIndexerDaemon.getInstance().getConfiguration().getBoolean("init.authorityData.gnd[@enabled]", true));
            MetadataHelper.setAuthorityViafEnabled(
                    SolrIndexerDaemon.getInstance().getConfiguration().getBoolean("init.authorityData.viaf[@enabled]", true));

            // Authority data fields to be added to DEFAULT
            MetadataHelper.setAddAuthorityDataFieldsToDefault(
                    SolrIndexerDaemon.getInstance().getConfiguration().getStringList("init.authorityData.addFieldsToDefault.field"));
        }

        // E-mail configuration (checked silently; problems are logged once by logGlobalSettings())
        emailConfigurationComplete = SolrIndexerDaemon.getInstance().getConfiguration().checkEmailConfiguration(false);

        // Secondary logging appender
        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        secondaryAppender = (SecondaryAppender) context.getConfiguration().getAppenders().get("record");

        // Record-level parallelism (the thread configuration is logged once by SolrIndexerDaemon.start())
        if (SolrIndexerDaemon.getInstance().getConfiguration().isSharedWorkerPool()) {
            // Shared work-stealing pool: records and their nested page/child tasks draw from one auto-balanced budget.
            recordExecutor = SolrIndexerDaemon.getInstance().getWorkerPool();
            ownsRecordExecutor = false;
            maxInFlightRecords = SolrIndexerDaemon.getInstance().getConfiguration().getThreads() * 2;
        } else {
            int recordThreads = SolrIndexerDaemon.getInstance().getConfiguration().getThreads();
            if (recordThreads > 1) {
                recordExecutor = Executors.newFixedThreadPool(recordThreads);
                ownsRecordExecutor = true;
                maxInFlightRecords = recordThreads * 2;
            }
        }
    }

    /**
     * Logs the hotfolder settings that come from the global indexer configuration and are therefore identical for every
     * hotfolder. Called once at startup instead of from each constructor, so the lines are not repeated per hotfolder.
     *
     * @param config indexer configuration
     */
    public static void logGlobalSettings(Configuration config) {
        List<String> anchorFields = config.getAddVolumeCollectionsToAnchorFields();
        if (!anchorFields.isEmpty()) {
            logger.info("Volume collections WILL BE ADDED to anchors for fields: {}", anchorFields);
        } else {
            logger.info("Volume collections WILL NOT BE ADDED to anchors.");
        }

        if (config.isPrioritizeLargeImageFolders()) {
            logger.info("Large image folder prioritization is ENABLED.");
        }

        if (readDeleteContentFilesOnFailure(config)) {
            logger.info("Content files will be REMOVED from the hotfolder in case of indexing errors.");
        } else {
            logger.info("Content files will be PRESERVED in the hotfolder in case of indexing errors.");
        }

        if (config.getBoolean("init.authorityData[@enabled]", true)) {
            List<String> defaultFields = config.getStringList("init.authorityData.addFieldsToDefault.field");
            if (defaultFields != null) {
                for (String field : defaultFields) {
                    logger.info("{} values will be added to DEFAULT", field);
                }
            }
        } else {
            logger.info("Authority data retrieval is disabled.");
        }

        if (StringUtils.isEmpty(config.getViewerAuthorizationToken())) {
            logger.warn("Goobi viewer REST API token not found, communications disabled.");
        }

        if (config.checkEmailConfiguration()) {
            logger.info("E-mail configuration OK.");
        }
    }

    /**
     * @param config indexer configuration
     * @return value of deleteContentFilesOnFailure; true if not configured
     */
    private static boolean readDeleteContentFilesOnFailure(Configuration config) {
        String value = config.getConfiguration("deleteContentFilesOnFailure");
        return value == null || Boolean.parseBoolean(value);
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
     * Discards the captured secondary log for the given record.
     *
     * @param recordKey record identifier (the MDC {@value SecondaryAppender#MDC_KEY} value)
     */
    private void resetSecondaryLog(String recordKey) {
        if (secondaryAppender != null) {
            secondaryAppender.reset(recordKey);
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
        if (!body.contains("ERROR")) {
            return false;
        }

        List<String> recipients = SolrIndexerDaemon.getInstance().getConfiguration().getEmailRecipients();
        if (recipients.isEmpty()) {
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

        try {
            Utils.postMail(recipients, subject, body, smtpServer, smtpUser, smtpPassword, smtpSenderAddress, smtpSenderName,
                    smtpSecurity, smtpPort);
            logger.info("Report e-mailed to configured recipients.");
            return true;
        } catch (UnsupportedEncodingException | MessagingException e) {
            logger.error(e.getMessage(), e);
        }

        return false;
    }

    /**
     * Enqueues any pre-existing {@code *.UPDATED} (anchor-update) files found in the hotfolder into the high priority queue. Meant to be called
     * once at startup to recover files that were still queued in memory when the indexer was last stopped: the regular scan (see {@link #scan()})
     * deliberately ignores {@code .UPDATED} files, and the high priority queue is not persisted, so such files would otherwise never be processed
     * and their anchor records would remain stale forever.
     *
     * @return number of files enqueued
     * @should enqueue orphaned UPDATED files
     */
    public int enqueueOrphanedAnchorUpdateFiles() {
        if (!Files.isDirectory(hotfolderPath)) {
            logger.error("Hotfolder not found in file system: {}", hotfolderPath);
            return 0;
        }
        int count = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, "*" + MetsIndexer.ANCHOR_UPDATE_EXTENSION)) {
            for (Path path : stream) {
                if (Files.isDirectory(path) || highPriorityIndexQueue.contains(path)) {
                    continue;
                }
                highPriorityIndexQueue.add(path);
                count++;
                logger.info("Re-queued orphaned anchor update file after restart: {}", path.getFileName());
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return count;
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
            logger.info("Found file '{}' (priority queue).", fileToReindex.getFileName());
            if (recordExecutor != null && !ownsRecordExecutor) {
                pruneCompletedRecords();
            }
            if (recordExecutor != null && !ownsRecordExecutor && inFlightRecords.isEmpty()) {
                // Shared pool, idle (e.g. reconciliation after a queue drain): run on a pool worker so the record's page/child
                // work nests into the shared pool, but wait for it here so priority items stay serial as before.
                joinRecord(recordExecutor.submit(() -> doIndex(fileToReindex)));
            } else {
                // Serial/two-pool mode, or shared pool still busy with regular records (anchor files queued by record workers
                // mid-batch): run inline right away instead of waiting behind the records already in the pool.
                doIndex(fileToReindex);
            }
        } else {
            // Check for the shutdown trigger file first
            Path shutdownFile = Paths.get(hotfolderPath.toAbsolutePath().toString(), SHUTDOWN_FILE);
            if (Files.exists(shutdownFile)) {
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
                if (recordExecutor == null) {
                    // Serial: index the next export-ready record inline (unchanged behavior).
                    Path recordFile = pollExportReadyRecord();
                    if (recordFile != null) {
                        logger.info("Processing {} from memory queue ({})...", recordFile.getFileName(), getHotfolderPath().getFileName());
                        doIndex(recordFile);
                    }
                } else {
                    // Parallel: fill the pool with as many export-ready records as capacity allows this pass, so records
                    // index concurrently instead of one per scan cycle.
                    pruneCompletedRecords();
                    while (inFlightRecords.size() < maxInFlightRecords && !indexQueue.isEmpty()) {
                        Path recordFile = pollExportReadyRecord();
                        if (recordFile == null) {
                            break; // remaining queued files are still exporting
                        }
                        submitRecord(recordFile);
                    }
                }
                return true; // always break so the loop restarts (refill/rescan)
            }

            // Both queues have drained. Wait for any records still indexing in parallel to finish, then reconcile any
            // anchors whose volume count changed during this batch. Coalescing here means a bulk import re-indexes each
            // anchor at most once per batch (per queue drain) instead of once per volume; awaiting first keeps the
            // reconcile from racing an in-flight volume of the same anchor.
            awaitInFlightRecords();
            reconcilePendingAnchors();

            // Flush any records still pending under the batched-commit policy now that the queue has drained, so a
            // sub-batch remainder is committed promptly rather than waiting for the next batch.
            SolrIndexerDaemon.getInstance().getSearchIndex().commitPending();

            logger.debug("Hotfolder ({}): Listing files...", getHotfolderPath().getFileName());
            List<Path> newFiles = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, "*.{xml,json,delete,purge,docupdate,UPDATED}")) {
                for (Path path : stream) {
                    if (!path.getFileName().toString().endsWith(MetsIndexer.ANCHOR_UPDATE_EXTENSION) && !indexQueue.contains(path)) {
                        newFiles.add(path);
                    }
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
            if (prioritizeLargeImageFolders && !newFiles.isEmpty()) {
                newFiles.sort((a, b) -> Long.compare(getDataFolderSize(b), getDataFolderSize(a)));
            }
            for (Path recordFile : newFiles) {
                if (indexQueue.offer(recordFile)) {
                    logger.info("Added file from '{}' to index queue: {}", getHotfolderPath().getFileName(), recordFile.getFileName());
                } else {
                    logger.debug("Queue full ({})", getHotfolderPath().getFileName());
                    break;
                }
            }
        }

        return !highPriorityIndexQueue.isEmpty() || !indexQueue.isEmpty();
    }

    /**
     * Flags the anchor record with the given PI for deferred reconciliation of its volume count (NUMVOLUMES) and METS
     * logical structure. The actual re-indexing is coalesced and carried out by {@link #reconcilePendingAnchors()} once
     * the regular index queue has drained, so importing many volumes of the same anchor results in a single anchor
     * re-index per batch instead of one re-index per volume.
     *
     * @param anchorPi PI of the anchor to reconcile; null or empty values are ignored
     */
    public void flagAnchorForReconciliation(String anchorPi) {
        if (StringUtils.isNotEmpty(anchorPi)) {
            anchorPisPendingReconciliation.add(anchorPi);
        }
    }

    /**
     * Acquires the write lock for the given anchor PI (created on demand), serializing concurrent writes to the same
     * anchor's Solr doc and METS file under record-level parallelism. Callers MUST release it in a finally via
     * {@link #releaseAnchorWriteLock(String)}. Uncontended (effectively a no-op) when records are indexed serially.
     *
     * @param anchorPi anchor PI to lock; ignored if null or empty
     */
    public void acquireAnchorWriteLock(String anchorPi) {
        if (StringUtils.isNotEmpty(anchorPi)) {
            anchorWriteLocks.computeIfAbsent(anchorPi, k -> new ReentrantLock()).lock();
        }
    }

    /**
     * Releases the anchor write lock previously acquired via {@link #acquireAnchorWriteLock(String)}. Safe to call even
     * if the current thread does not hold it (then it is a no-op), so it can sit unconditionally in a finally block.
     *
     * @param anchorPi anchor PI to unlock; ignored if null or empty
     */
    public void releaseAnchorWriteLock(String anchorPi) {
        if (StringUtils.isNotEmpty(anchorPi)) {
            ReentrantLock lock = anchorWriteLocks.get(anchorPi);
            if (lock != null && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * Polls the regular index queue for the next record whose data folders have finished exporting, re-queuing any that
     * are still exporting. Returns null if the queue is empty or every remaining file is still exporting (the caller
     * should then stop for this pass). Note: {@link #isDataFolderExportDone(Path)} sleeps ~1s per checked file on the
     * calling (scan) thread, so this throttles how fast records can be handed to the pool.
     *
     * @return an export-ready record file, or null if none is ready right now
     */
    private Path pollExportReadyRecord() {
        Path recordFile = indexQueue.poll();
        if (recordFile == null) {
            return null;
        }
        // Check whether the data folders for this record have been copied completely, otherwise skip and re-queue.
        Set<Path> alreadyCheckedFiles = new HashSet<>();
        while (!isDataFolderExportDone(recordFile)) {
            logger.info("Export not yet finished for '{}'", recordFile.getFileName());
            alreadyCheckedFiles.add(recordFile);
            indexQueue.add(recordFile); // re-add at the end
            if (alreadyCheckedFiles.contains(indexQueue.peek())) {
                logger.info("All files in queue have not yet finished export.");
                return null;
            }
            recordFile = indexQueue.poll();
            if (recordFile == null) {
                return null;
            }
        }
        return recordFile;
    }

    /**
     * Submits a record for parallel indexing on {@link #recordExecutor}, applying backpressure so at most
     * {@link #maxInFlightRecords} records are in flight (the scan thread blocks on the oldest once the bound is reached).
     *
     * @param recordFile record file to index
     * @throws FatalIndexerException if an already-finished record failed fatally
     */
    private void submitRecord(Path recordFile) throws FatalIndexerException {
        pruneCompletedRecords();
        while (inFlightRecords.size() >= maxInFlightRecords) {
            awaitOldestRecord();
            pruneCompletedRecords();
        }
        logger.info("Submitting {} for parallel indexing ({})...", recordFile.getFileName(), getHotfolderPath().getFileName());
        inFlightRecords.add(recordExecutor.submit(() -> doIndex(recordFile)));
    }

    /**
     * Removes and joins all in-flight records that have already completed, surfacing any fatal error.
     *
     * @throws FatalIndexerException if a completed record failed fatally
     */
    private void pruneCompletedRecords() throws FatalIndexerException {
        for (Iterator<Future<Boolean>> it = inFlightRecords.iterator(); it.hasNext();) {
            Future<Boolean> future = it.next();
            if (future.isDone()) {
                it.remove();
                joinRecord(future);
            }
        }
    }

    /**
     * Blocks until the oldest in-flight record finishes, then removes and joins it.
     *
     * @throws FatalIndexerException if that record failed fatally
     */
    private void awaitOldestRecord() throws FatalIndexerException {
        if (!inFlightRecords.isEmpty()) {
            joinRecord(inFlightRecords.remove(0));
        }
    }

    /**
     * Blocks until every in-flight record has finished and clears the list. Used before anchor reconciliation and on
     * shutdown so that no record is still writing when those run.
     *
     * @throws FatalIndexerException if any record failed fatally
     */
    private void awaitInFlightRecords() throws FatalIndexerException {
        for (Future<Boolean> future : inFlightRecords) {
            joinRecord(future);
        }
        inFlightRecords.clear();
    }

    /**
     * Waits for a single submitted record and propagates a fatal failure; a non-fatal failure is logged (the record's own
     * error handling and e-mail already ran on the worker thread).
     *
     * @param future the record's future
     * @throws FatalIndexerException if the record failed with a {@link FatalIndexerException}
     */
    private static void joinRecord(Future<Boolean> future) throws FatalIndexerException {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(e.getMessage(), e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof FatalIndexerException fatal) {
                throw fatal;
            }
            logger.error("Parallel record indexing failed: {}", cause != null ? cause.getMessage() : e.getMessage(), cause);
        }
    }

    /**
     * Awaits any outstanding parallel records and shuts down the record executor (if record-level parallelism is on).
     * Called during daemon shutdown.
     */
    public void shutdownRecordExecutor() {
        if (recordExecutor != null) {
            try {
                awaitInFlightRecords();
                // Commit any records still pending under the batched-commit policy before shutting down.
                SolrIndexerDaemon.getInstance().getSearchIndex().commitPending();
            } catch (FatalIndexerException e) {
                logger.error(e.getMessage(), e);
            }
            // Only shut down a pool this hotfolder owns; the shared work-stealing pool is shut down by the daemon.
            if (ownsRecordExecutor) {
                recordExecutor.shutdown();
            }
        }
    }

    /**
     * Re-indexes every flagged anchor whose stored NUMVOLUMES no longer matches the actual number of indexed volumes.
     * Called when both queues have drained, so volume membership changes from a batch are coalesced into at most one
     * anchor re-index per anchor.
     *
     * @throws FatalIndexerException
     */
    public void reconcilePendingAnchors() throws FatalIndexerException {
        if (anchorPisPendingReconciliation.isEmpty()) {
            return;
        }
        List<String> anchorPis = new ArrayList<>(anchorPisPendingReconciliation);
        anchorPisPendingReconciliation.clear();
        for (String anchorPi : anchorPis) {
            reconcileAnchor(anchorPi);
        }
    }

    /**
     * Schedules a re-index of the given anchor's indexed METS file, but only if its stored NUMVOLUMES differs from the
     * actual number of currently indexed volumes. Anchors that are already up to date are skipped, so this stays cheap
     * when nothing changed. The anchor file is resolved from the anchor's own data repository.
     *
     * @param anchorPi PI of the anchor to reconcile
     * @throws FatalIndexerException
     */
    private void reconcileAnchor(String anchorPi) throws FatalIndexerException {
        SolrSearchIndex searchIndex = SolrIndexerDaemon.getInstance().getSearchIndex();
        try {
            SolrDocumentList anchorHits =
                    searchIndex.search(SolrConstants.PI + ":\"" + anchorPi + '"', Collections.singletonList(SolrConstants.NUMVOLUMES));
            if (anchorHits.isEmpty()) {
                // Anchor not (yet) indexed; its own indexing will compute the correct count.
                logger.debug("Anchor '{}' is not in the index (yet), skipping reconciliation.", anchorPi);
                return;
            }
            long storedNumVolumes = -1;
            Object numVolumesValue = anchorHits.get(0).getFieldValue(SolrConstants.NUMVOLUMES);
            if (numVolumesValue != null) {
                storedNumVolumes = Long.parseLong(numVolumesValue.toString());
            }
            long actualNumVolumes = searchIndex.getNumHits(new StringBuilder(SolrConstants.PI_PARENT).append(":\"")
                    .append(anchorPi)
                    .append('"')
                    .append(SolrConstants.SOLR_QUERY_AND)
                    .append(SolrConstants.ISWORK)
                    .append(SolrConstants.SOLR_QUERY_TRUE)
                    .toString());
            // When volume collections are copied up into anchors, a volume metadata change may alter the anchor's collections without
            // changing the volume count, so the anchor must still be re-indexed even if NUMVOLUMES is unchanged.
            if (storedNumVolumes == actualNumVolumes && addVolumeCollectionsToAnchorFields.isEmpty()) {
                logger.debug("Anchor '{}' is up to date (NUMVOLUMES={}), no re-index needed.", anchorPi, actualNumVolumes);
                return;
            }
            DataRepository[] repositories = dataRepositoryStrategy.selectDataRepository(anchorPi, null, null, searchIndex,
                    SolrIndexerDaemon.getInstance().getOldSearchIndex());
            DataRepository anchorRepository = repositories[1] != null ? repositories[1] : repositories[0];
            if (anchorRepository == null || anchorRepository.getDir(DataRepository.PARAM_INDEXED_METS) == null) {
                logger.warn("Could not resolve a data repository for anchor '{}'; skipping reconciliation.", anchorPi);
                return;
            }
            Path indexedAnchor = Paths.get(anchorRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(),
                    anchorPi + ".xml");
            if (!Files.exists(indexedAnchor)) {
                logger.warn("Indexed anchor file not found for '{}' at '{}'; cannot reconcile NUMVOLUMES.", anchorPi, indexedAnchor);
                return;
            }
            if (!highPriorityIndexQueue.contains(indexedAnchor)) {
                highPriorityIndexQueue.add(indexedAnchor);
            }
            logger.info("Anchor '{}' volume count changed ({} -> {}); scheduled for re-index.", anchorPi, storedNumVolumes,
                    actualNumVolumes);
        } catch (SolrServerException | IOException e) {
            logger.error("Could not reconcile anchor '{}': {}", anchorPi, e.getMessage());
            // Retry on a later queue drain
            anchorPisPendingReconciliation.add(anchorPi);
        } catch (NumberFormatException e) {
            logger.error("Malformed NUMVOLUMES value for anchor '{}': {}", anchorPi, e.getMessage());
        }
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

        // Tag this record on the MDC so that every log line (including those from the intra-record worker pools, which
        // re-apply this context) is attributable to it, and so the secondary appender collects this record's log in its
        // own buffer even when several records are indexed in parallel. The displayed tag ('rec') starts as the file name
        // and is upgraded to the PI once the record is parsed (see Indexer.selectDataRepository); the buffer key ('recId')
        // stays the file name for the whole record so all its lines share one buffer.
        String recordKey = recordFile.getFileName().toString();
        ThreadContext.put(SecondaryAppender.MDC_KEY, recordKey);
        ThreadContext.put(SecondaryAppender.MDC_KEY_ID, recordKey);
        try {
            resetSecondaryLog(recordKey);
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
                        secondaryAppender.getLog(recordKey));
            }

            return ret;
        } finally {
            // Release this record's data-repository reservation (no-op for strategies that do not reserve).
            dataRepositoryStrategy.releaseReservation();
            resetSecondaryLog(recordKey);
            ThreadContext.remove(SecondaryAppender.MDC_KEY);
            ThreadContext.remove(SecondaryAppender.MDC_KEY_ID);
        }
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
                        secondaryAppender.getLog(ThreadContext.get(SecondaryAppender.MDC_KEY)));
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
            if (Strings.CI.endsWith(filename, FileTools.XML_EXTENSION)) {
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
                logger.info("Document format detected: {}", fileType.name());
                switch (fileType) {
                    case METS:
                        if (metsEnabled) {
                            identifiers = new MetsIndexer(this).addToIndex(sourceFile, reindexSettings);
                        } else {
                            logger.error("METS indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case METS_MARC:
                        if (metsEnabled) {
                            identifiers = new MetsMarcIndexer(this).addToIndex(sourceFile, reindexSettings);
                        } else {
                            logger.error("METS indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case LIDO:
                        if (lidoEnabled) {
                            identifiers = new LidoIndexer(this).addToIndex(sourceFile, reindexSettings);
                        } else {
                            logger.error("LIDO indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case EAD:
                        if (eadEnabled) {
                            identifiers = new EadIndexer(this).addToIndex(sourceFile, reindexSettings);
                        } else {
                            logger.error("EAD indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case EAD3:
                        if (eadEnabled) {
                            identifiers = new Ead3Indexer(this).addToIndex(sourceFile, reindexSettings);
                        } else {
                            logger.error("EAD indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case DENKXWEB:
                        if (denkxwebEnabled) {
                            identifiers = new DenkXwebIndexer(this).addToIndex(sourceFile, reindexSettings);
                        } else {
                            logger.error("DenkXweb indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case DUBLINCORE:
                        if (dcEnabled) {
                            identifiers = new DublinCoreIndexer(this).addToIndex(sourceFile, reindexSettings);
                        } else {
                            logger.error("Dublin Core indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case WORLDVIEWS:
                        if (worldviewsEnabled) {
                            identifiers = new WorldViewsIndexer(this).addToIndex(sourceFile, reindexSettings);
                        } else {
                            logger.error("WorldViews indexing is disabled - please make sure all folders are configured.");
                            Files.delete(sourceFile);
                        }
                        break;
                    case CMS:
                        if (cmsEnabled) {
                            identifiers = new CmsPageIndexer(this).addToIndex(sourceFile, reindexSettings);
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
                        new UsageStatisticsIndexer(this).addToIndex(sourceFile, null);
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
                // Derive the clean anchor PI (strip the '#' anti-collision part) so this file move serializes against a
                // concurrent re-index of the same anchor via the same per-PI lock.
                String anchorPi = FilenameUtils.getBaseName(filename);
                if (anchorPi.contains("#")) {
                    anchorPi = anchorPi.substring(0, anchorPi.indexOf('#'));
                }
                acquireAnchorWriteLock(anchorPi);
                try {
                    MetsIndexer.anchorSuperupdate(sourceFile, updatedMets, repositories[1] != null ? repositories[1] : repositories[0]);
                } finally {
                    releaseAnchorWriteLock(anchorPi);
                }
                Utils.submitDataToViewer(Collections.emptyList(), countRecordFiles()); // TODO submit any record identifiers here?
            } else if (filename.endsWith(DocUpdateIndexer.FILE_EXTENSION)) {
                // Single Solr document update
                List<String> identifiers = new DocUpdateIndexer(this).addToIndex(sourceFile, null);
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
            String[] fields = { SolrConstants.SOURCEDOCFORMAT, SolrConstants.DATEDELETED, SolrConstants.DOCTYPE, SolrConstants.PI_PARENT };
            SolrDocumentList result =
                    SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + baseFileName, Arrays.asList(fields));
            boolean trace = createTraceDoc;
            // PI of the parent anchor if the record being deleted is a volume; used to reconcile the anchor afterwards
            String deletedRecordAnchorPi = null;
            if (!result.isEmpty()) {
                SolrDocument doc = result.get(0);
                deletedRecordAnchorPi = (String) doc.getFieldValue(SolrConstants.PI_PARENT);
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
                // A volume was removed: flag its anchor so NUMVOLUMES + METS get reconciled (deferred/coalesced).
                // No-op for non-volume records (deletedRecordAnchorPi is null).
                flagAnchorForReconciliation(deletedRecordAnchorPi);
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
     * Returns the total size of data folders associated with the given record file.
     *
     * @param recordFile
     * @return total size in bytes
     */
    long getDataFolderSize(Path recordFile) {
        String baseName = FilenameUtils.getBaseName(recordFile.getFileName().toString());
        String mediaFolderSuffix = SolrIndexerDaemon.getInstance().getConfiguration().getConfiguration(DataRepository.PARAM_MEDIA);
        if (StringUtils.isEmpty(mediaFolderSuffix)) {
            mediaFolderSuffix = "media";
        }
        Path mediaFolder = getHotfolderPath().resolve(baseName + "_" + mediaFolderSuffix);
        if (Files.isDirectory(mediaFolder)) {
            try {
                return FileUtils.sizeOfDirectory(mediaFolder.toFile());
            } catch (IllegalArgumentException e) {
                logger.error(e.getMessage());
            }
        }
        return 0;
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
        return !addVolumeCollectionsToAnchorFields.isEmpty();
    }

    /**
     * <p>
     * Returns the list of Solr field names whose values are copied from volumes up into their anchor record.
     * </p>
     *
     * @return the configured field names; an empty list if the feature is disabled
     */
    public List<String> getAddVolumeCollectionsToAnchorFields() {
        return addVolumeCollectionsToAnchorFields;
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
