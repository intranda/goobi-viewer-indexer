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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Attribute;
import org.jdom2.Element;
import org.jdom2.Namespace;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.DateTools;
import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.HttpConnector;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.GroupedMetadata;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.PhysicalElement;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.config.FieldConfig;
import io.goobi.viewer.indexer.model.config.XPathConfig;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * Indexer implementation for METS/MODS documents.
 */
public class MetsIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(MetsIndexer.class);

    /** Constant <code>PRESENTATION_FILEGROUP="PRESENTATION"</code> */
    public static final String PRESENTATION_FILEGROUP = "PRESENTATION";
    /** Constant <code>DEFAULT_FILEGROUP="DEFAULT"</code> */
    public static final String DEFAULT_FILEGROUP = "DEFAULT";
    /** Constant <code>OBJECT_FILEGROUP="OBJECT"</code> */
    public static final String OBJECT_FILEGROUP = "OBJECT";
    /** Constant <code>ALTO_FILEGROUP="ALTO"</code> */
    public static final String ALTO_FILEGROUP = "ALTO";
    /** Constant <code>FULLTEXT_FILEGROUP="FULLTEXT"</code> */
    public static final String FULLTEXT_FILEGROUP = "FULLTEXT";
    /** Constant <code>DOWNLOAD_FILEGROUP="DOWNLOAD"</code> */
    public static final String DOWNLOAD_FILEGROUP = "DOWNLOAD";
    /** Constant <code>ANCHOR_UPDATE_EXTENSION=".UPDATED"</code> */
    public static final String ANCHOR_UPDATE_EXTENSION = ".UPDATED";

    protected static final String ATTRIBUTE_CONTENTIDS = "CONTENTIDS";

    protected static final String XPATH_DMDSEC = "/mets:mets/mets:dmdSec[@ID='"; //NOSONAR XPath, not URI
    protected static final String XPATH_FILE = "mets:file";
    protected static final String XPATH_FILEGRP = "/mets:mets/mets:fileSec/mets:fileGrp[@USE=\""; //NOSONAR XPath, not URI
    private static final String XPATH_ANCHOR_PI_PART =
            "/mets:mdWrap[@MDTYPE='MODS']/mets:xmlData/mods:mods/mods:relatedItem[@type='host']/mods:recordInfo/mods:recordIdentifier";

    /** */
    protected static List<Path> reindexedChildrenFileList = new ArrayList<>();

    private String selectedPreferredImageFileGroup = null;
    private List<String> availablePreferredImageFileGroups = SolrIndexerDaemon.getInstance().getConfiguration().getMetsPreferredImageFileGroups();
    volatile String useFileGroupGlobal = null;

    /**
     * Constructor.
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public MetsIndexer(Hotfolder hotfolder) {
        super();
        this.hotfolder = hotfolder;
    }

    /**
     * 
     * @param hotfolder
     * @param httpConnector
     */
    public MetsIndexer(Hotfolder hotfolder, HttpConnector httpConnector) {
        super(httpConnector);
        this.hotfolder = hotfolder;
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
    @Override
    public void addToIndex(Path metsFile, boolean fromReindexQueue, Map<String, Boolean> reindexSettings)
            throws IOException, FatalIndexerException {
        String fileNameRoot = FilenameUtils.getBaseName(metsFile.getFileName().toString());

        // Check data folders in the hotfolder
        Map<String, Path> dataFolders = checkDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

        // Use existing folders for those missing in the hotfolder
        checkReindexSettings(dataFolders, reindexSettings);

        String[] resp = index(metsFile, fromReindexQueue, dataFolders, null,
                SolrIndexerDaemon.getInstance().getConfiguration().getPageCountStart(),
                dataFolders.containsKey(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER));

        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            String newMetsFileName = resp[0];
            String pi = FilenameUtils.getBaseName(newMetsFileName);
            Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), newMetsFileName);
            if (metsFile.equals(indexed)) {
                return;
            }

            if (Files.exists(indexed)) {
                // Add a timestamp to the old file name
                String oldMetsFilename =
                        FilenameUtils.getBaseName(newMetsFileName) + "_" + LocalDateTime.now().format(DateTools.formatterBasicDateTime) + ".xml";
                Path newFile = Paths.get(hotfolder.getUpdatedMets().toAbsolutePath().toString(), oldMetsFilename);
                Files.copy(indexed, newFile);
                logger.debug("Old METS file copied to '{}'.", newFile.toAbsolutePath());
            }
            Files.copy(metsFile, indexed, StandardCopyOption.REPLACE_EXISTING);
            dataRepository.checkOtherRepositoriesForRecordFileDuplicates(newMetsFileName, DataRepository.PARAM_INDEXED_METS,
                    hotfolder.getDataRepositoryStrategy().getAllDataRepositories());

            if (previousDataRepository != null) {
                // Move non-repository data folders to the selected repository
                previousDataRepository.moveDataFoldersToRepository(dataRepository, FilenameUtils.getBaseName(newMetsFileName));
            }

            // Copy and delete media folder
            if (dataRepository.checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_MEDIA,
                    hotfolder.getDataRepositoryStrategy().getAllDataRepositories()) > 0) {
                String msg = Utils.removeRecordImagesFromCache(FilenameUtils.getBaseName(resp[0]));
                if (msg != null) {
                    logger.info(msg);
                }
            }

            // Copy data folders
            dataRepository.copyAndDeleteAllDataFolders(pi, dataFolders, reindexSettings,
                    hotfolder.getDataRepositoryStrategy().getAllDataRepositories());

            // Delete unsupported data folders
            FileTools.deleteUnsupportedDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

            // success for goobi
            Path successFile = Paths.get(hotfolder.getSuccessFolder().toAbsolutePath().toString(), metsFile.getFileName().toString());
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
                logger.warn(LOG_COULD_NOT_BE_DELETED, metsFile.toAbsolutePath());
            }

            // Update data repository cache map in the Goobi viewer
            if (previousDataRepository != null) {
                try {
                    Utils.updateDataRepositoryCache(pi, dataRepository.getPath());
                } catch (HTTPException e) {
                    logger.error(e.getMessage(), e);
                }
            }
            prerenderPagePdfsIfRequired(pi, dataFolders.get(DataRepository.PARAM_MEDIA) != null);
            logger.info("Successfully finished indexing '{}'.", metsFile.getFileName());

            // Remove this file from lower priority hotfolders to avoid overriding changes with older version
            SolrIndexerDaemon.getInstance().removeRecordFileFromLowerPriorityHotfolders(pi, hotfolder);
        } else {
            // Error
            if (hotfolder.isDeleteContentFilesOnFailure()) {
                // Delete all data folders for this record from the hotfolder
                DataRepository.deleteDataFoldersFromHotfolder(dataFolders, reindexSettings);
            }
            handleError(metsFile, resp[1], getSourceDocFormat());
            try {
                Files.delete(metsFile);
            } catch (IOException e) {
                logger.error(LOG_COULD_NOT_BE_DELETED, metsFile.toAbsolutePath());
            }
        }
    }

    /**
     * Indexes the given METS file.
     *
     * @param metsFile {@link java.nio.file.Path}
     * @param fromReindexQueue a boolean.
     * @param dataFolders a {@link java.util.Map} object.
     * @param inWriteStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @param pageCountStart Order number for the first page.
     * @param downloadExternalImages
     * @return an array of {@link java.lang.String} objects.
     * @should index record correctly
     * @should index metadata groups correctly
     * @should index multi volume records correctly
     * @should update record correctly
     * @should set access conditions correctly
     * @should write cms page texts into index
     * @should write shape metadata correctly
     * @should keep volume count up to date in anchor
     * @should read datecreated from mets with correct time zone
     * @should not add dateupdated if value already exists
     * 
     */
    public String[] index(Path metsFile, boolean fromReindexQueue, Map<String, Path> dataFolders, final ISolrWriteStrategy inWriteStrategy,
            int pageCountStart, boolean downloadExternalImages) {
        String[] ret = { null, null };

        if (metsFile == null || !Files.exists(metsFile)) {
            throw new IllegalArgumentException("metsFile must point to an existing METS file.");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null.");
        }

        logger.debug("Indexing METS file '{}'...", metsFile.getFileName());
        ISolrWriteStrategy writeStrategy = inWriteStrategy;
        try {
            initJDomXP(metsFile);
            IndexObject indexObj = new IndexObject(getNextIddoc(SolrIndexerDaemon.getInstance().getSearchIndex()));
            logger.debug("IDDOC: {}", indexObj.getIddoc());
            indexObj.setVolume(isVolume());
            logger.debug("Document is volume: {}", indexObj.isVolume());
            indexObj.setAnchor(isAnchor());
            Element structNode = findStructNode(indexObj);
            if (structNode == null) {
                throw new IndexerException("STRUCT NODE not found.");
            }

            indexObj.setRootStructNode(structNode);

            // set some simple data in den indexObject
            setSimpleData(indexObj);
            setUrn(indexObj);

            // Set PI
            String[] foundPi = MetadataHelper.getPIFromXML(getPiRootPath(indexObj.getDmdid()), xp);
            if (foundPi.length == 0 || StringUtils.isBlank(foundPi[0])) {
                ret[1] = "PI not found.";
                throw new IndexerException(ret[1]);
            }

            String pi = MetadataHelper.applyIdentifierModifications(foundPi[0]);
            logger.info("Record PI: {}", pi);

            // Do not allow identifiers with characters that cannot be used in file names
            if (!Utils.validatePi(pi)) {
                ret[1] = new StringBuilder("PI contains illegal characters: ").append(pi).toString();
                throw new IndexerException(ret[1]);
            }
            indexObj.setPi(pi);
            indexObj.setTopstructPI(pi);

            // Add PI to default
            if (foundPi.length > 1 && "addToDefault".equals(foundPi[1])) {
                indexObj.setDefaultValue(indexObj.getDefaultValue() + " " + pi);
            }

            // Determine the data repository to use
            DataRepository[] repositories =
                    hotfolder.getDataRepositoryStrategy()
                            .selectDataRepository(pi, metsFile, dataFolders, SolrIndexerDaemon.getInstance().getSearchIndex(),
                                    SolrIndexerDaemon.getInstance().getOldSearchIndex());
            dataRepository = repositories[0];
            previousDataRepository = repositories[1];
            if (StringUtils.isNotEmpty(dataRepository.getPath())) {
                indexObj.setDataRepository(dataRepository.getPath());
            }

            ret[0] = new StringBuilder(indexObj.getPi()).append(FileTools.XML_EXTENSION).toString();

            // Check and use old data folders, if no new ones found
            checkOldDataFolder(dataFolders, DataRepository.PARAM_MEDIA, pi);
            checkOldDataFolder(dataFolders, DataRepository.PARAM_FULLTEXT, pi);
            checkOldDataFolder(dataFolders, DataRepository.PARAM_FULLTEXTCROWD, pi);
            checkOldDataFolder(dataFolders, DataRepository.PARAM_ABBYY, pi);
            checkOldDataFolder(dataFolders, DataRepository.PARAM_TEIWC, pi);
            checkOldDataFolder(dataFolders, DataRepository.PARAM_ALTO, pi);
            checkOldDataFolder(dataFolders, DataRepository.PARAM_ALTOCROWD, pi);
            checkOldDataFolder(dataFolders, DataRepository.PARAM_MIX, pi);
            checkOldDataFolder(dataFolders, DataRepository.PARAM_UGC, pi);
            checkOldDataFolder(dataFolders, DataRepository.PARAM_CMS, pi);
            checkOldDataFolder(dataFolders, DataRepository.PARAM_TEIMETADATA, pi);
            checkOldDataFolder(dataFolders, DataRepository.PARAM_ANNOTATIONS, pi);

            if (writeStrategy == null) {
                // Request appropriate write strategy
                writeStrategy = AbstractWriteStrategy.create(metsFile, dataFolders, hotfolder);
            } else {
                logger.info("Solr write strategy injected by caller: {}", writeStrategy.getClass().getName());
            }

            prepareUpdate(indexObj);

            int hierarchyLevel = 0; // depth of the docstrct that has ISWORK (volume or monograph)
            if (indexObj.isVolume()) {
                // Find anchor document for this volume
                hierarchyLevel = 1;
                String anchorPi = getAnchorPi();
                if (StringUtils.isNotEmpty(anchorPi)) {
                    anchorPi = MetadataHelper.applyIdentifierModifications(anchorPi);
                    indexObj.setParentPI(anchorPi);
                    indexObj.setAnchorPI(anchorPi);
                    String[] fields = { SolrConstants.IDDOC, SolrConstants.DOCSTRCT };
                    String parentIddoc = null;
                    String parentDocstrct = null;
                    SolrDocumentList hits = SolrIndexerDaemon.getInstance()
                            .getSearchIndex()
                            .search(new StringBuilder().append(SolrConstants.PI).append(":\"").append(anchorPi).append('"').toString(),
                                    Arrays.asList(fields));
                    if (hits != null && hits.getNumFound() > 0) {
                        parentIddoc = (String) hits.get(0).getFieldValue(SolrConstants.IDDOC);
                        parentDocstrct = (String) hits.get(0).getFieldValue(SolrConstants.DOCSTRCT);
                    }
                    // Create parent IndexObject
                    if (anchorPi != null && parentIddoc != null) {
                        logger.debug("Creating anchor for '{}' (PI:{}, IDDOC:{})", indexObj.getIddoc(), anchorPi, parentIddoc);
                        IndexObject anchor = new IndexObject(Long.valueOf(parentIddoc), anchorPi);
                        if (anchor.getIddoc() == indexObj.getIddoc()) {
                            throw new IndexerException("Anchor and volume have the same IDDOC: " + indexObj.getIddoc());
                        }
                        // Set anchor properties manually because this IndexObject does not undergo the normal procedure
                        anchor.setAnchor(true);
                        anchor.setVolume(false);
                        if (parentDocstrct == null) {
                            logger.warn("Anchor docstruct not found in the index document, determining by volume type...");
                            parentDocstrct = "generic_anchor";
                        }
                        anchor.setType(parentDocstrct);
                        indexObj.setParent(anchor);
                    }
                }
            }

            // write opac url
            String opacXpath =
                    "/mets:mets/mets:amdSec/mets:digiprovMD[@ID='DIGIPROV']/mets:mdWrap[@OTHERMDTYPE='DVLINKS']"
                            + "/mets:xmlData/dv:links/dv:reference/text()"; //NOSONAR XPath, not URI
            String opacUrl = xp.evaluateToString(opacXpath, null);
            if (StringUtils.isEmpty(opacUrl)) {
                opacUrl = xp.evaluateToCdata(opacXpath, null);
            }
            logger.debug("OPACURL: {}", opacUrl);
            if (StringUtils.isNotEmpty(opacUrl)) {
                indexObj.addToLucene(SolrConstants.OPACURL, opacUrl.trim());
            }

            // Process TEI files
            if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) != null) {
                MetadataHelper.processTEIMetadataFiles(indexObj, dataFolders.get(DataRepository.PARAM_TEIMETADATA));
            }

            // put some simple data in lucene array
            indexObj.pushSimpleDataToLuceneArray();

            // Write metadata relative to the mdWrap
            logger.debug("'Collecting DMDSEC metadata");
            MetadataHelper.writeMetadataToObject(indexObj, xp.getMdWrap(indexObj.getDmdid()), "", xp);

            // Write root metadata (outside of MODS sections)
            logger.debug("Collecting root metadata");
            MetadataHelper.writeMetadataToObject(indexObj, xp.getRootElement(), "", xp);

            // If this is a volume (= has an anchor) that has already been indexed, copy access conditions from the anchor element
            if (indexObj.isVolume() && indexObj.getAccessConditions().isEmpty() && StringUtils.isNotEmpty(indexObj.getAnchorPI())) {
                indexObj.setAnchorPI(indexObj.getAnchorPI());
                SolrDocumentList hits = SolrIndexerDaemon.getInstance()
                        .getSearchIndex()
                        .search(SolrConstants.PI + ":" + indexObj.getAnchorPI(), Collections.singletonList(SolrConstants.ACCESSCONDITION));
                if (hits != null && !hits.isEmpty()) {
                    Collection<Object> fields = hits.get(0).getFieldValues(SolrConstants.ACCESSCONDITION);
                    if (fields != null) {
                        for (Object o : fields) {
                            indexObj.getAccessConditions().add(o.toString());
                        }
                    } else {
                        logger.error(
                                "Anchor document '{}' has no ACCESSCONDITION values. Please check whether it is a proper anchor and not a group!",
                                indexObj.getAnchorPI());
                    }
                }
            }

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // Read DATECREATED/DATEUPDATED from METS
            indexObj.populateDateCreatedUpdated(getMetsCreateDate());

            // Write created/updated timestamps
            indexObj.writeDateModified(false);

            if (indexObj.isAnchor()) {
                // Anchors: add NUMVOLUMES
                indexObj.addToLucene(SolrConstants.ISANCHOR, "true");
                long numVolumes = SolrIndexerDaemon.getInstance()
                        .getSearchIndex()
                        .getNumHits(new StringBuilder(SolrConstants.PI_PARENT).append(":")
                                .append(indexObj.getPi())
                                .append(SolrConstants.SOLR_QUERY_AND)
                                .append(SolrConstants.ISWORK)
                                .append(SolrConstants.SOLR_QUERY_TRUE)
                                .toString());
                indexObj.addToLucene(SolrConstants.NUMVOLUMES, String.valueOf(numVolumes));
                logger.info("Added number of volumes: {}", numVolumes);
            } else {
                // Look for a download URL for the entire record
                collectDownloadUrl(indexObj);

                // Generate docs for all pages and add to the write strategy
                generatePageDocuments(writeStrategy, dataFolders, dataRepository, indexObj.getPi(), pageCountStart, downloadExternalImages);

                // If images have been found for any page, set a boolean in the root doc indicating that the record does have images
                indexObj.addToLucene(FIELD_IMAGEAVAILABLE, String.valueOf(recordHasImages));

                // If full-text has been indexed for any page, set a boolean in the root doc indicating that the record does have full-text
                indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, String.valueOf(recordHasFulltext));

                // write all page URNs sequentially into one field
                generatePageUrns(indexObj);

                // Add THUMBNAIL,THUMBPAGENO,THUMBPAGENOLABEL
                // (must be done AFTER writeDateMondified(), writeAccessConditions() and generatePageDocuments()!)
                List<LuceneField> thumbnailFields = mapPagesToDocstruct(indexObj, null, true, writeStrategy, hierarchyLevel);
                if (thumbnailFields != null) {
                    indexObj.getLuceneFields().addAll(thumbnailFields);
                }

                // ISWORK only for non-anchors
                indexObj.addToLucene(SolrConstants.ISWORK, "true");
                logger.trace("ISWORK: {}", indexObj.getLuceneFieldWithName(SolrConstants.ISWORK).getValue());
            }

            // Add DEFAULT field
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(indexObj.getDefaultValue()));
                indexObj.setDefaultValue("");
            }

            // CMS texts
            if (dataFolders.get(DataRepository.PARAM_CMS) != null) {
                Path staticPageFolder = dataFolders.get(DataRepository.PARAM_CMS);
                if (Files.isDirectory(staticPageFolder)) {
                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(staticPageFolder, "*.{xml,htm,html,xhtml}")) {
                        for (Path file : stream) {
                            // Add a new CMS_TEXT_* field for each file
                            String field = FilenameUtils.getBaseName(file.getFileName().toString()).toUpperCase();
                            String content = FileTools.readFileToString(file.toFile(), null);
                            String value = TextHelper.cleanUpHtmlTags(content);
                            indexObj.addToLucene(SolrConstants.PREFIX_CMS_TEXT + field, value);
                            indexObj.addToLucene(SolrConstants.CMS_TEXT_ALL, value);
                        }
                    }
                }
            }

            // Create group documents if this record is part of a group and no doc exists for that group yet
            for (String groupIdField : indexObj.getGroupIds().keySet()) {
                String groupSuffix = groupIdField.replace(SolrConstants.PREFIX_GROUPID, "");
                Map<String, String> moreMetadata = new HashMap<>();
                String titleField = "MD_TITLE_" + groupSuffix;
                String sortTitleField = "SORT_TITLE_" + groupSuffix;
                for (LuceneField field : indexObj.getLuceneFields()) {
                    if (titleField.equals(field.getField())) {
                        // Add title/label
                        moreMetadata.put(SolrConstants.LABEL, field.getValue());
                        moreMetadata.put("MD_TITLE", field.getValue());
                    } else if (sortTitleField.equals(field.getField())) {
                        // Add title/label
                        moreMetadata.put("SORT_TITLE", field.getValue());
                    } else if (field.getField().endsWith(groupSuffix)
                            && (field.getField().startsWith("MD_")
                                    || field.getField().startsWith("MD2_")
                                    || field.getField().startsWith("MDNUM_"))) {
                        // Add any MD_*_GROUPSUFFIX field to the group doc
                        moreMetadata.put(field.getField().replace("_" + groupSuffix, ""), field.getValue());
                    }
                }
                SolrInputDocument doc = SolrIndexerDaemon.getInstance()
                        .getSearchIndex()
                        .checkAndCreateGroupDoc(groupIdField, indexObj.getGroupIds().get(groupIdField), moreMetadata,
                                getNextIddoc(SolrIndexerDaemon.getInstance().getSearchIndex()));
                if (doc != null) {
                    writeStrategy.addDoc(doc);
                    logger.debug("Created group document for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                } else {
                    logger.debug("Group document already exists for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                }
            }

            // If this is a new volume, force anchor update to keep its volume count consistent
            if (indexObj.isVolume() && !indexObj.isUpdate() && indexObj.getParent() != null) {
                logger.info("This is a new volume - anchor updated needed.");
                copyAndReIndexAnchor(indexObj, hotfolder, dataRepository);
            }

            boolean indexedChildrenFileList = false;
            if (indexObj.isAnchor()) {
                // Create and index new anchor file that includes all currently indexed children (priority queue)
                logger.debug("'{}' is an anchor file.", metsFile.getFileName());
                anchorMerge(indexObj);
                // Then re-index child volumes that need an IDDOC_PARENT update (also priority queue)
                updateAnchorChildrenParentIddoc(indexObj);
            } else {
                // Index all child elements recursively
                List<IndexObject> childObjectList = indexAllChildren(indexObj, hierarchyLevel + 1, writeStrategy);
                indexObj.addChildMetadata(childObjectList);

                // Remove this record from re-index list
                logger.debug("reindexedChildrenFileList.size(): {}", MetsIndexer.reindexedChildrenFileList.size());
                if (MetsIndexer.reindexedChildrenFileList.contains(metsFile)) {
                    logger.debug("{} in reindexedChildrenFileList, removing...", metsFile.toAbsolutePath());
                    MetsIndexer.reindexedChildrenFileList.remove(metsFile);
                    indexedChildrenFileList = true;
                }

                if (indexObj.getNumPages() > 0) {
                    // Write number of pages
                    indexObj.addToLucene(SolrConstants.NUMPAGES, String.valueOf(indexObj.getNumPages()));
                    if (indexObj.getFirstPageLabel() != null) {
                        indexObj.addToLucene(SolrConstants.ORDERLABELFIRST, indexObj.getFirstPageLabel());
                    }
                    if (indexObj.getLastPageLabel() != null) {
                        indexObj.addToLucene(SolrConstants.ORDERLABELLAST, indexObj.getLastPageLabel());
                    }
                    if (indexObj.getFirstPageLabel() != null && indexObj.getLastPageLabel() != null) {
                        indexObj.addToLucene("MD_ORDERLABELRANGE",
                                new StringBuilder(indexObj.getFirstPageLabel()).append(" - ").append(indexObj.getLastPageLabel()).toString());
                    }

                    // Add used-generated content docs
                    writeUserGeneratedContents(writeStrategy, dataFolders, indexObj);
                }
            }

            // Add grouped metadata as separate documents
            addGroupedMetadataDocs(writeStrategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

            // Apply field modifications that should happen at the very end
            indexObj.applyFinalModifications();

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)

            logger.debug("Writing document to index...");
            SolrInputDocument rootDoc = SolrSearchIndex.createDocument(indexObj.getLuceneFields());
            writeStrategy.setRootDoc(rootDoc);

            writeStrategy.writeDocs(SolrIndexerDaemon.getInstance().getConfiguration().isAggregateRecords());
            if (indexObj.isVolume() && (!indexObj.isUpdate() || indexedChildrenFileList)) {
                logger.info("Re-indexing anchor...");
                copyAndReIndexAnchor(indexObj, hotfolder, dataRepository);
            }
            logger.info("Finished writing data for '{}' to Solr.", pi);
        } catch (InterruptedException e) {
            logger.error("Indexing of '{}' could not be finished due to an error.", metsFile.getFileName());
            logger.error(e.getMessage(), e);
            ret[1] = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            SolrIndexerDaemon.getInstance().getSearchIndex().rollback();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Indexing of '{}' could not be finished due to an error.", metsFile.getFileName());
            logger.error(e.getMessage(), e);
            ret[1] = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            SolrIndexerDaemon.getInstance().getSearchIndex().rollback();
        } finally {
            if (writeStrategy != null) {
                writeStrategy.cleanup();
            }
        }

        return ret;
    }

    /**
     * 
     * @param xp
     * @param filegroup
     * @return {@link String}
     * @return File name or path where USE="banner"; empty string if none found
     */
    private static String getFilePathBannerFromFileSec(JDomXP xp, String filegroup) {
        String filePathBanner = "";
        String xpath = XPATH_FILEGRP + filegroup + "\"]/mets:file[@USE=\"banner\"]/mets:FLocat/@xlink:href";
        filePathBanner = xp.evaluateToAttributeStringValue(xpath, null);
        if (StringUtils.isNotEmpty(filePathBanner)) {
            // Only extract file name if not URL
            if (!filePathBanner.startsWith("https://") && !filePathBanner.startsWith("http://")) {
                filePathBanner = FilenameUtils.getName(filePathBanner);
            }
            return filePathBanner;
        }
        return "";
    }

    /**
     * 
     * @param xp
     * @param filegroup
     * @return
     */
    private static String getFilePathBannerFromPhysicalStructMap(JDomXP xp, String filegroup) {
        String filePathBanner = "";
        String xpathFilePtr =
                "/mets:mets/mets:structMap[@TYPE='PHYSICAL']/mets:div[@TYPE='physSequence']"
                        + "/mets:div[@xlink:label=\"START_PAGE\"]/mets:fptr/@FILEID"; //NOSONAR XPath, not URI
        List<String> fileIds = xp.evaluateToAttributes(xpathFilePtr, null).stream().map(Attribute::getValue).toList();
        for (String fileId : fileIds) {
            String xpath = XPATH_FILEGRP + filegroup + "\"]/mets:file[@ID='" + fileId + "']/mets:FLocat/@xlink:href";
            filePathBanner = xp.evaluateToAttributeStringValue(xpath, null);
            if (StringUtils.isNotBlank(filePathBanner)) {
                return filePathBanner;
            }
        }
        return filePathBanner;
    }

    /**
     * Generates thumbnail info fields for the given docstruct. Also generates page docs mapped to this docstruct. <code>IndexObj.topstructPi</code>
     * must be set before calling this method.
     * 
     * @param indexObj {@link IndexObject}
     * @param parentIndexObject
     * @param isWork
     * @param writeStrategy
     * @param depth Depth of the current docstruct in the docstruct hierarchy.
     * @return {@link LuceneField}
     * @throws IndexerException -
     * @throws FatalIndexerException
     */
    private List<LuceneField> mapPagesToDocstruct(IndexObject indexObj, final IndexObject parentIndexObject, boolean isWork,
            ISolrWriteStrategy writeStrategy, int depth) throws IndexerException, FatalIndexerException {
        if (StringUtils.isEmpty(indexObj.getLogId())) {
            throw new IndexerException("Object has no LOG_ID.");
        }

        // Determine all PHYSID mapped to the current LOGID
        String xpath = "/mets:mets/mets:structLink/mets:smLink[@xlink:from=\"" + indexObj.getLogId() + "\"]/@xlink:to";
        List<String> physIdList = xp.evaluateToStringList(xpath, null);
        if (physIdList == null || physIdList.isEmpty()) {
            logger.warn("No pages mapped to '{}'.", indexObj.getLogId());
            return Collections.emptyList();
        }

        List<PhysicalElement> pages = writeStrategy.getPagesForPhysIdList(physIdList);
        if (pages.isEmpty()) {
            logger.warn("No pages found for {}", indexObj.getLogId());
        }
        // Collections.sort(pageDocs, new SolrInputDocPageOrderComparator());
        Collections.sort(pages); // Mapping order may be shuffled, so restore page order

        // If this is a top struct element, look for a representative image
        String filePathBanner = null;
        if (isWork) {
            filePathBanner = getFilePathBannerFromFileSec(xp, useFileGroupGlobal);
            if (StringUtils.isNotBlank(filePathBanner)) {
                logger.info("Found representation thumbnail for {} in METS filesec: {}", indexObj.getLogId(), filePathBanner);
            } else {
                filePathBanner = getFilePathBannerFromPhysicalStructMap(xp, useFileGroupGlobal);
                if (StringUtils.isNotBlank(filePathBanner)) {
                    logger.info("Found representation thumbnail for {} in METS physical structMap: {}", indexObj.getLogId(), filePathBanner);
                } else if (SolrIndexerDaemon.getInstance().getConfiguration().isUseFirstPageAsDefaultRepresentative()
                        && StringUtils.isNotEmpty(indexObj.getThumbnailRepresent())) {
                    filePathBanner = indexObj.getThumbnailRepresent();
                    logger.debug("No representation thumbnail for {} found in METS, using previous file: {}", indexObj.getLogId(), filePathBanner);
                }
            }
        }
        boolean thumbnailSet = false;
        List<LuceneField> ret = new ArrayList<>();
        PhysicalElement firstPage = !pages.isEmpty() ? pages.get(0) : null;
        if (StringUtils.isEmpty(filePathBanner) && SolrIndexerDaemon.getInstance().getConfiguration().isUseFirstPageAsDefaultRepresentative()
                && firstPage != null) {
            // Add thumbnail information from the first page
            logger.debug("THUMBNAIL from first page");
            String thumbnailFileName = checkThumbnailFileName((String) firstPage.getDoc().getFieldValue(SolrConstants.FILENAME), firstPage.getDoc());
            ret.add(new LuceneField(SolrConstants.THUMBNAIL, thumbnailFileName));
            if (DocType.SHAPE.name().equals(firstPage.getDoc().getFieldValue(SolrConstants.DOCTYPE))) {
                ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPage.getDoc().getFieldValue("ORDER_PARENT"))));
            } else {
                ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPage.getDoc().getFieldValue(SolrConstants.ORDER))));
            }
            ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) firstPage.getDoc().getFieldValue(SolrConstants.ORDERLABEL)));
            ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) firstPage.getDoc().getFieldValue(SolrConstants.MIMETYPE)));
            thumbnailSet = true;
        }
        for (PhysicalElement page : pages) {
            String pageFileName = checkThumbnailFileName((String) page.getDoc().getFieldValue(SolrConstants.FILENAME), page.getDoc());
            String pageFileBaseName = FilenameUtils.getBaseName(pageFileName);
            // Add thumbnail information from the representative page
            if (!thumbnailSet && StringUtils.isNotEmpty(filePathBanner) && filePathBanner.equals(pageFileName)) {
                ret.add(new LuceneField(SolrConstants.THUMBNAIL, pageFileName));
                // THUMBNAILREPRESENT is just used to identify the presence of a custom
                // representation thumbnail to the indexer, it is not used in the viewer
                ret.add(new LuceneField(SolrConstants.THUMBNAILREPRESENT, pageFileName));
                ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(page.getDoc().getFieldValue(SolrConstants.ORDER))));
                ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) page.getDoc().getFieldValue(SolrConstants.ORDERLABEL)));
                ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) page.getDoc().getFieldValue(SolrConstants.MIMETYPE)));
                thumbnailSet = true;
            }

            int currentDepth = -1;
            if (page.getDoc().getField(FIELD_OWNERDEPTH) != null) {
                currentDepth = (int) page.getDoc().getField(FIELD_OWNERDEPTH).getValue();
            }

            // Make sure IDDOC_OWNER of a page contains the IDDOC of the lowest possible mapped docstruct
            if (depth > currentDepth) {
                page.getDoc().setField(SolrConstants.IDDOC_OWNER, String.valueOf(indexObj.getIddoc()));
                page.getDoc().setField(FIELD_OWNERDEPTH, depth);

                // Add the parent document's LOGID value to the page
                page.getDoc().setField(SolrConstants.LOGID, indexObj.getLogId());

                // Add the parent document's structure element to the page
                page.getDoc().setField(SolrConstants.DOCSTRCT, indexObj.getType());

                // Add topstruct type to the page
                if (!page.getDoc().containsKey(SolrConstants.DOCSTRCT_TOP) && indexObj.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP) != null) {
                    page.getDoc().setField(SolrConstants.DOCSTRCT_TOP, indexObj.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP).getValue());
                }

                // Remove SORT_ fields from a previous, higher up docstruct
                Set<String> fieldsToRemove = new HashSet<>();
                for (String fieldName : page.getDoc().getFieldNames()) {
                    if (fieldName.startsWith(SolrConstants.PREFIX_SORT)) {
                        fieldsToRemove.add(fieldName);
                    }
                }
                for (String fieldName : fieldsToRemove) {
                    page.getDoc().removeField(fieldName);
                }
                //  Add this docstruct's SORT_* fields to page
                if (indexObj.getIddoc() == Long.valueOf((String) page.getDoc().getFieldValue(SolrConstants.IDDOC_OWNER))) {
                    for (LuceneField field : indexObj.getLuceneFields()) {
                        if (field.getField().startsWith(SolrConstants.PREFIX_SORT)) {
                            page.getDoc().addField(field.getField(), field.getValue());
                        }
                    }
                }
            }

            // Add PI_TOPSTRUCT
            if (page.getDoc().getField(SolrConstants.PI_TOPSTRUCT) == null) {
                page.getDoc().addField(SolrConstants.PI_TOPSTRUCT, indexObj.getTopstructPI());
            }
            // Add PI_ANCHOR
            if (StringUtils.isNotEmpty(indexObj.getAnchorPI()) && page.getDoc().getField(SolrConstants.PI_ANCHOR) == null) {
                page.getDoc().addField(SolrConstants.PI_ANCHOR, indexObj.getAnchorPI());
            }
            // Add GROUPID_*
            if (!indexObj.getGroupIds().isEmpty()) {
                for (String groupId : indexObj.getGroupIds().keySet()) {
                    if (!page.getDoc().containsKey(groupId)) {
                        page.getDoc().addField(groupId, indexObj.getLuceneFieldWithName(groupId).getValue());
                    }
                }
            }
            // Add DATAREPOSITORY
            if (page.getDoc().getField(SolrConstants.DATAREPOSITORY) == null && indexObj.getDataRepository() != null) {
                page.getDoc().addField(SolrConstants.DATAREPOSITORY, indexObj.getDataRepository());
            }
            if (page.getDoc().getField(SolrConstants.DATEUPDATED) == null && !indexObj.getDateUpdated().isEmpty()) {
                for (Long date : indexObj.getDateUpdated()) {
                    page.getDoc().addField(SolrConstants.DATEUPDATED, date);
                }
            }
            if (page.getDoc().getField(SolrConstants.DATEINDEXED) == null && !indexObj.getDateIndexed().isEmpty()) {
                for (Long date : indexObj.getDateIndexed()) {
                    page.getDoc().addField(SolrConstants.DATEINDEXED, date);
                }
            }

            // Add of each docstruct access conditions (no duplicates)
            Set<String> existingAccessConditions = new HashSet<>();
            if (page.getDoc().getFieldValues(SolrConstants.ACCESSCONDITION) != null) {
                for (Object obj : page.getDoc().getFieldValues(SolrConstants.ACCESSCONDITION)) {
                    existingAccessConditions.add((String) obj);
                }
            }
            for (String s : indexObj.getAccessConditions()) {
                if (!existingAccessConditions.contains(s) && !SolrConstants.OPEN_ACCESS_VALUE.equals(s)) {
                    // Override OPENACCESS if a different access condition comes from a lower docstruct
                    if (depth > currentDepth && existingAccessConditions.contains(SolrConstants.OPEN_ACCESS_VALUE)) {
                        // Remove all instances of ACCESSCONDITION, then re-add existing values (minus OPENACCSS)
                        page.getDoc().removeField(SolrConstants.ACCESSCONDITION);
                        for (String existingS : existingAccessConditions) {
                            if (!SolrConstants.OPEN_ACCESS_VALUE.equals(existingS)) {
                                page.getDoc().addField(SolrConstants.ACCESSCONDITION, existingS);
                            }
                        }
                    }
                    // Add new non-OPENACCESS condition
                    page.getDoc().addField(SolrConstants.ACCESSCONDITION, s);
                } else if (SolrConstants.OPEN_ACCESS_VALUE.equals(s) && depth > currentDepth) {
                    // If OPENACCESS is on a lower docstruct, however, remove all previous access conditions and override with OPENACCESS
                    page.getDoc().removeField(SolrConstants.ACCESSCONDITION);
                    page.getDoc().addField(SolrConstants.ACCESSCONDITION, s);
                }
            }
            if (indexObj.getAccessConditions().isEmpty()) {
                logger.warn("{}: {} has no access conditions.", pageFileBaseName, indexObj.getIddoc());
            }

            // Add owner docstruct's metadata (tokenized only!) and SORT_* fields to the page
            Set<String> existingMetadataFieldNames = new HashSet<>();
            Set<String> existingSortFieldNames = new HashSet<>();
            for (String fieldName : page.getDoc().getFieldNames()) {
                if (SolrIndexerDaemon.getInstance()
                        .getConfiguration()
                        .getMetadataConfigurationManager()
                        .getFieldsToAddToPages()
                        .contains(fieldName)) {
                    for (Object value : page.getDoc().getFieldValues(fieldName)) {
                        existingMetadataFieldNames.add(new StringBuilder(fieldName).append(String.valueOf(value)).toString());
                    }
                } else if (fieldName.startsWith(SolrConstants.PREFIX_SORT)) {
                    existingSortFieldNames.add(fieldName);
                }
            }
            for (LuceneField field : indexObj.getLuceneFields()) {
                if (SolrIndexerDaemon.getInstance()
                        .getConfiguration()
                        .getMetadataConfigurationManager()
                        .getFieldsToAddToPages()
                        .contains(field.getField())
                        && !existingMetadataFieldNames.contains(new StringBuilder(field.getField()).append(field.getValue()).toString())) {
                    // Avoid duplicates (same field name + value)
                    page.getDoc().addField(field.getField(), field.getValue());
                    logger.debug("Added {}:{} to page {}", field.getField(), field.getValue(), page.getDoc().getFieldValue(SolrConstants.ORDER));
                } else if (field.getField().startsWith(SolrConstants.PREFIX_SORT) && !existingSortFieldNames.contains(field.getField())) {
                    // Only one instance of each SORT_ field may exist
                    page.getDoc().addField(field.getField(), field.getValue());
                }
            }

            // For shape page docs, create grouped metadata docs for their mapped docstruct
            if (DocType.SHAPE.name().equals(page.getDoc().getFieldValue(SolrConstants.DOCTYPE))) {
                GroupedMetadata shapeGmd = new GroupedMetadata();
                shapeGmd.getFields().add(new LuceneField(SolrConstants.METADATATYPE, DocType.SHAPE.name()));
                shapeGmd.getFields().add(new LuceneField(SolrConstants.GROUPFIELD, String.valueOf(page.getDoc().getFieldValue(SolrConstants.IDDOC))));
                shapeGmd.getFields().add(new LuceneField(SolrConstants.LABEL, (String) page.getDoc().getFieldValue(FIELD_COORDS)));
                shapeGmd.getFields().add(new LuceneField(SolrConstants.LOGID, indexObj.getLogId()));
                shapeGmd.getFields().add(new LuceneField(FIELD_COORDS, (String) page.getDoc().getFieldValue(FIELD_COORDS)));
                shapeGmd.getFields().add(new LuceneField(FIELD_SHAPE, (String) page.getDoc().getFieldValue(FIELD_SHAPE)));
                shapeGmd.getFields().add(new LuceneField(SolrConstants.MD_VALUE, (String) page.getDoc().getFieldValue(FIELD_COORDS)));
                shapeGmd.getFields().add(new LuceneField(SolrConstants.ORDER, String.valueOf(page.getDoc().getFieldValue("ORDER_PARENT"))));
                // Add main value, otherwise the document will be skipped
                shapeGmd.setMainValue((String) page.getDoc().getFieldValue(FIELD_COORDS));
                indexObj.getGroupedMetadataFields().add(shapeGmd);
                // Make sure the shape metadata is on the lowest docstruct
                IndexObject useParentIndexObject = parentIndexObject;
                while (useParentIndexObject != null) {
                    if (useParentIndexObject.getGroupedMetadataFields().contains(shapeGmd)) {
                        useParentIndexObject.getGroupedMetadataFields().remove(shapeGmd);
                        logger.debug("removed shape metadata {} from {}", shapeGmd.getMainValue(), useParentIndexObject.getLogId());
                    }
                    useParentIndexObject = useParentIndexObject.getParent();
                }
                logger.debug("Mapped SHAPE document {} to {}", page.getDoc().getFieldValue(SolrConstants.ORDER), indexObj.getLogId());
            }

            // Update the doc in the write strategy (otherwise some implementations might ignore the changes).
            writeStrategy.updatePage(page);
        }

        // If a representative image is set but not mapped to any docstructs, do not use it
        if (!thumbnailSet && StringUtils.isNotEmpty(filePathBanner) && firstPage.getDoc() != null) {
            logger.warn("Selected representative image '{}' is not mapped to any structure element - using first mapped image instead.",
                    filePathBanner);
            String pageFileName = checkThumbnailFileName((String) firstPage.getDoc().getFieldValue(SolrConstants.FILENAME), firstPage.getDoc());
            ret.add(new LuceneField(SolrConstants.THUMBNAIL, pageFileName));
            // THUMBNAILREPRESENT is just used to identify the presence of a custom representation thumbnail to the indexer,
            // it is not used in the viewer
            ret.add(new LuceneField(SolrConstants.THUMBNAILREPRESENT, pageFileName));
            ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPage.getDoc().getFieldValue(SolrConstants.ORDER))));
            ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) firstPage.getDoc().getFieldValue(SolrConstants.ORDERLABEL)));
            ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) firstPage.getDoc().getFieldValue(SolrConstants.MIMETYPE)));
        }

        // Add the number of assigned pages and the labels of the first and last page to this structure element
        indexObj.setNumPages(pages.size());
        if (firstPage != null) {
            PhysicalElement lastPage = pages.get(pages.size() - 1);
            String firstPageLabel = (String) firstPage.getDoc().getFieldValue(SolrConstants.ORDERLABEL);
            String lastPageLabel = (String) lastPage.getDoc().getFieldValue(SolrConstants.ORDERLABEL);
            if (firstPageLabel != null && !"-".equals(firstPageLabel.trim())) {
                indexObj.setFirstPageLabel(firstPageLabel);
            }
            if (lastPageLabel != null && !"-".equals(lastPageLabel.trim())) {
                indexObj.setLastPageLabel(lastPageLabel);
            }
        }

        return ret;
    }

    /**
     * 
     * @param indexObj
     */
    public void collectDownloadUrl(final IndexObject indexObj) {
        String xpath = "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div[@TYPE=\"physSequence\"]"; //NOSONAR XPath expression, not URI...
        List<Element> eleStructMapPhysSequenceList = xp.evaluateToElements(xpath, null);
        if (!eleStructMapPhysSequenceList.isEmpty()) {
            List<Element> eleFptrList =
                    eleStructMapPhysSequenceList.get(0)
                            .getChildren("fptr", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mets"));
            if (eleFptrList != null && !eleFptrList.isEmpty()) {
                for (Element eleFptr : eleFptrList) {
                    String fileID = eleFptr.getAttributeValue("FILEID");
                    String url = xp.evaluateToAttributeStringValue(
                            XPATH_FILEGRP + DOWNLOAD_FILEGROUP + "\"]/mets:file[@ID=\""
                                    + fileID + "\" and @MIMETYPE=\"text/html\"]/mets:FLocat[@LOCTYPE=\"URL\"]/@xlink:href",
                            null);
                    if (StringUtils.isNotEmpty(url)) {
                        indexObj.addToLucene(SolrConstants.DOWNLOAD_URL_EXTERNAL, url);
                        logger.info("Found external download URL: {}", url);
                    }
                }
            }
        }
    }

    /**
     * Generates a SolrInputDocument for each page that is mapped to a docstruct. Adds all page metadata except those that come from the owning
     * docstruct (such as docstruct iddoc, type, collection, etc.).
     *
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @param dataFolders a {@link java.util.Map} object.
     * @param dataRepository a {@link io.goobi.viewer.indexer.model.datarepository.DataRepository} object.
     * @param pi a {@link java.lang.String} object.
     * @param pageCountStart a int.
     * @param downloadExternalImages
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should create documents for all mapped pages
     * @should set correct ORDER values
     * @should skip unmapped pages
     * @should switch to DEFAULT file group correctly
     * @should maintain page order after parallel processing
     */
    public void generatePageDocuments(final ISolrWriteStrategy writeStrategy, final Map<String, Path> dataFolders,
            final DataRepository dataRepository, final String pi, int pageCountStart, boolean downloadExternalImages)
            throws InterruptedException, FatalIndexerException {
        // Get all physical elements
        String xpath = buildPagesXpathExpresson();
        List<Element> eleStructMapPhysicalList = xp.evaluateToElements(xpath, null);
        if (eleStructMapPhysicalList.isEmpty()) {
            logger.info("No pages found.");
            return;
        }

        useFileGroupGlobal = selectImageFileGroup(downloadExternalImages);
        logger.info("Image file group selected: {}", useFileGroupGlobal);
        logger.info("Generating {} page documents (count starts at {})...", eleStructMapPhysicalList.size(), pageCountStart);

        if (SolrIndexerDaemon.getInstance().getConfiguration().getThreads() > 1) {
            // Generate each page document in its own thread
            ForkJoinPool pool = new ForkJoinPool(SolrIndexerDaemon.getInstance().getConfiguration().getThreads());
            ConcurrentHashMap<Long, Boolean> usedIddocsMap = new ConcurrentHashMap<>();
            try {
                pool.submit(() -> eleStructMapPhysicalList.parallelStream().forEach(eleStructMapPhysical -> {
                    try {
                        long iddoc = getNextIddoc(SolrIndexerDaemon.getInstance().getSearchIndex());
                        if (usedIddocsMap.containsKey(iddoc)) {
                            logger.error("Duplicate IDDOC: {}", iddoc);
                        }
                        PhysicalElement page =
                                generatePageDocument(eleStructMapPhysical, String.valueOf(iddoc), pi, null, dataFolders, dataRepository,
                                        downloadExternalImages);
                        if (page != null) {
                            writeStrategy.addPage(page);
                            // Shapes must be added as regular pages to the WriteStrategy to ensure correct docstrcut mapping
                            for (PhysicalElement shape : page.getShapes()) {
                                writeStrategy.addPage(shape);
                            }
                            page.getShapes().clear();
                        }
                        usedIddocsMap.put(iddoc, true);
                    } catch (FatalIndexerException e) {
                        logger.error("Should be exiting here now...");
                    }
                })).get(GENERATE_PAGE_DOCUMENT_TIMEOUT_HOURS, TimeUnit.HOURS);
            } catch (ExecutionException e) {
                logger.error(e.getMessage(), e);
                SolrIndexerDaemon.getInstance().stop();
            } catch (TimeoutException e) {
                throw new InterruptedException("Generating page documents timed out for object " + pi);
            } finally {
                pool.shutdown();
            }
        } else {
            // Generate pages sequentially
            int order = pageCountStart;
            for (final Element eleStructMapPhysical : eleStructMapPhysicalList) {
                PhysicalElement page =
                        generatePageDocument(eleStructMapPhysical, String.valueOf(getNextIddoc(SolrIndexerDaemon.getInstance().getSearchIndex())),
                                pi, order, dataFolders, dataRepository, downloadExternalImages);
                if (page != null) {
                    writeStrategy.addPage(page);
                    // Shapes must be added as regular pages to the WriteStrategy to ensure correct docstrcut mapping
                    for (PhysicalElement shape : page.getShapes()) {
                        writeStrategy.addPage(shape);
                    }
                    page.getShapes().clear();
                    order++;
                }
            }
        }
        logger.info("Generated {} pages.", writeStrategy.getPageDocsSize());
    }

    /**
     * Builds XPath expression for physical elements.
     * 
     * @return Constructed expression
     * @should build expression correctly
     */
    static String buildPagesXpathExpresson() {
        StringBuilder sb = new StringBuilder("/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div[@TYPE=\"page\"");
        List<String> allowedTypes = SolrIndexerDaemon.getInstance().getConfiguration().getMetsAllowedPhysicalTypes();
        for (String type : allowedTypes) {
            sb.append(" or @TYPE=\"").append(type).append('"');
        }
        sb.append(']');

        return sb.toString();
    }

    /**
     * 
     * @param downloadExternalImages
     * @return Selected file group name
     */
    String selectImageFileGroup(boolean downloadExternalImages) {
        String xpath = "/mets:mets/mets:fileSec/mets:fileGrp"; //NOSONAR XPath, not URI
        List<Element> eleFileGrpList = xp.evaluateToElements(xpath, null);
        if (eleFileGrpList.isEmpty()) {
            logger.info("No file groups found.");
            return "";
        }

        String ret = "";
        for (Element eleFileGrp : eleFileGrpList) {
            String use = eleFileGrp.getAttributeValue("USE");
            switch (use) {
                case DEFAULT_FILEGROUP:
                    if (!PRESENTATION_FILEGROUP.equals(ret) || downloadExternalImages) {
                        ret = use;
                    }
                    break;
                case PRESENTATION_FILEGROUP:
                    if (!downloadExternalImages) {
                        ret = use;
                    }
                    break;
                case OBJECT_FILEGROUP:
                    if (!PRESENTATION_FILEGROUP.equals(ret) || !DEFAULT_FILEGROUP.equals(ret)) {
                        ret = use;
                    }
                    break;
                default:
                    for (String g : availablePreferredImageFileGroups) {
                        if (g.equals(use)) {
                            return g;
                        }
                    }
                    break;
            }
        }

        return ret;
    }

    /**
     * 
     * @param eleStructMapPhysical
     * @param iddoc
     * @param pi
     * @param inOrder
     * @param dataFolders
     * @param dataRepository
     * @param downloadExternalImages
     * @return Generated {@link PhysicalElement}
     * @throws FatalIndexerException
     * @should add all basic fields
     * @should add crowdsourcing ALTO field correctly
     * @should add crowdsourcing fulltext field correctly
     * @should add fulltext field correctly
     * @should create ALTO file from ABBYY correctly
     * @should create ALTO file from TEI correctly
     * @should create ALTO file from fileId if none provided in data folders
     * @should add FILENAME_HTML-SANDBOXED field for url paths
     * @should add width and height from techMD correctly
     * @should add width and height from ABBYY correctly
     * @should add width and height from MIX correctly
     * @should add page metadata correctly
     * @should add shape metadata as page documents
     */
    PhysicalElement generatePageDocument(Element eleStructMapPhysical, String iddoc, String pi, final Integer inOrder,
            final Map<String, Path> dataFolders, final DataRepository dataRepository, boolean downloadExternalImages) throws FatalIndexerException {
        if (dataFolders != null && dataRepository == null) {
            throw new IllegalArgumentException("dataRepository may not be null if dataFolders is not null");
        }
        if (useFileGroupGlobal == null) {
            throw new IllegalStateException("useFileGroupGlobal not set");
        }

        String id = eleStructMapPhysical.getAttributeValue("ID");
        int order = inOrder;
        if (inOrder == null) {
            String orderValue = eleStructMapPhysical.getAttributeValue("ORDER");
            if (StringUtils.isNotEmpty(orderValue)) {
                order = Integer.parseInt(orderValue);
            } else {
                logger.warn("ORDER attribute no found, skipping...");
                return null;
            }
        }
        logger.trace("generatePageDocument: {} (IDDOC {}) processed by thread {}", id, iddoc, Thread.currentThread().getId());
        // Check whether this physical element is mapped to any logical element, skip if not
        StringBuilder sbXPath = new StringBuilder(70);
        sbXPath.append("/mets:mets/mets:structLink/mets:smLink[@xlink:to=\"").append(id).append("\"]");
        List<Element> eleStructLinkList = xp.evaluateToElements(sbXPath.toString(), null);
        if (eleStructLinkList.isEmpty()) {
            logger.warn("Page {} is not mapped to a structure element, skipping...", order);
            return null;
        }

        List<Element> eleFptrList =
                eleStructMapPhysical.getChildren("fptr", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mets"));

        // Create object for this page
        PhysicalElement ret = new PhysicalElement(order);
        ret.getDoc().addField(SolrConstants.IDDOC, iddoc);
        ret.getDoc().addField(SolrConstants.GROUPFIELD, iddoc);
        ret.getDoc().addField(SolrConstants.DOCTYPE, DocType.PAGE.name());
        ret.getDoc().addField(SolrConstants.PHYSID, id);
        ret.getDoc().addField(SolrConstants.ORDER, order);

        // Determine the FILEID root (part of the FILEID that doesn't change for different mets:fileGroups)
        String fileIdRoot = null;
        String useFileID = null;
        for (Element eleFptr : eleFptrList) {
            String fileID = eleFptr.getAttributeValue("FILEID");
            logger.trace("fileID: {}", fileID);
            if (fileID.contains(useFileGroupGlobal)) {
                useFileID = fileID;
            }

            // Piggyback shape metadata on fake page documents to ensure their mapping to corresponding shape docstructs
            if (eleFptr.getChild("seq", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mets")) != null) {
                List<Element> eleListArea = eleFptr.getChild("seq", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mets"))
                        .getChildren("area", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mets"));
                if (eleListArea != null && !eleListArea.isEmpty()) {
                    int count = 1;
                    for (Element eleArea : eleListArea) {
                        String coords = eleArea.getAttributeValue("COORDS");
                        String physId = eleArea.getAttributeValue("ID");
                        String shape = eleArea.getAttributeValue(DocType.SHAPE.name());

                        PhysicalElement shapePage = new PhysicalElement(Utils.generateLongOrderNumber(order, count));
                        shapePage.getDoc().addField(SolrConstants.IDDOC, getNextIddoc(SolrIndexerDaemon.getInstance().getSearchIndex()));
                        shapePage.getDoc().setField(SolrConstants.DOCTYPE, DocType.SHAPE.name());
                        shapePage.getDoc().addField(SolrConstants.ORDER, shapePage.getOrder());
                        shapePage.getDoc().addField(SolrConstants.PHYSID, physId);
                        shapePage.getDoc().addField(FIELD_COORDS, coords);
                        shapePage.getDoc().addField(FIELD_SHAPE, shape);
                        shapePage.getDoc().addField("ORDER_PARENT", order);
                        ret.getShapes().add(shapePage);
                        count++;
                        logger.debug("Added SHAPE page document: {}", shapePage.getOrder());
                    }
                }
            }
        }
        if (useFileGroupGlobal != null && StringUtils.isEmpty(useFileID)) {
            logger.warn("FILEID not found for file group {}", useFileGroupGlobal);
            useFileID = "";
        }

        boolean fileGroupPrefix = false; // FILEID starts with the file group name
        boolean fileGroupSuffix = false; // FILEID ends with the file group name
        char fileIdSeparator = '_'; // Separator character between the group name and the rest and the file ID

        // Remove the file group part from the file ID
        if (useFileID != null && useFileGroupGlobal != null && useFileID.contains(useFileGroupGlobal)) {
            if (useFileID.startsWith(useFileGroupGlobal + '_')) {
                fileGroupPrefix = true;
            } else if (useFileID.startsWith(useFileGroupGlobal + '.')) {
                fileGroupPrefix = true;
                fileIdSeparator = '.';
            } else if (useFileID.endsWith('_' + useFileGroupGlobal)) {
                fileGroupSuffix = true;
            } else if (useFileID.endsWith('.' + useFileGroupGlobal)) {
                fileGroupSuffix = true;
                fileIdSeparator = '.';
            }
            if (fileGroupPrefix) {
                fileIdRoot = useFileID.replace(useFileGroupGlobal + fileIdSeparator, "");
            } else if (fileGroupSuffix) {
                fileIdRoot = useFileID.replace(fileIdSeparator + useFileGroupGlobal, "");
            }
            ret.getDoc().addField(SolrConstants.FILEIDROOT, fileIdRoot);
        }

        // Double page view
        boolean doubleImage =
                "double page".equals(eleStructMapPhysical.getAttributeValue("label",
                        SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("xlink")));
        if (doubleImage) {
            ret.getDoc().addField(SolrConstants.BOOL_DOUBLE_IMAGE, doubleImage);
        }

        // ORDERLABEL / LABEL
        String orderLabel = eleStructMapPhysical.getAttributeValue("ORDERLABEL");
        if (StringUtils.isNotEmpty(orderLabel)) {
            ret.getDoc().addField(SolrConstants.ORDERLABEL, orderLabel);
        } else {
            if (StringUtils.isNotEmpty(orderLabel)) {
                ret.getDoc().addField(SolrConstants.ORDERLABEL, orderLabel);
            } else {
                ret.getDoc().addField(SolrConstants.ORDERLABEL, SolrIndexerDaemon.getInstance().getConfiguration().getEmptyOrderLabelReplacement());
            }
        }

        String contentIDs = eleStructMapPhysical.getAttributeValue(ATTRIBUTE_CONTENTIDS);
        if (Utils.isUrn(contentIDs)) {
            ret.getDoc().addField(SolrConstants.IMAGEURN, contentIDs);
        }
        String dmdId = eleStructMapPhysical.getAttributeValue(SolrConstants.DMDID);
        if (StringUtils.isNotEmpty(dmdId)) {
            IndexObject pageObj = new IndexObject(0);
            MetadataHelper.writeMetadataToObject(pageObj, xp.getMdWrap(dmdId), "", xp);
            for (LuceneField field : pageObj.getLuceneFields()) {
                ret.getDoc().addField(field.getField(), field.getValue());
            }
        }

        String altoURL = null;
        // For each mets:fileGroup in the mets:fileSec
        String xpath = "/mets:mets/mets:fileSec/mets:fileGrp";
        List<Element> eleFileGrpList = xp.evaluateToElements(xpath, null);
        for (Element eleFileGrp : eleFileGrpList) {
            String fileGrpUse = eleFileGrp.getAttributeValue("USE");
            String fileGrpId = eleFileGrp.getAttributeValue("ID");
            logger.debug("Found file group: {}", fileGrpUse);
            String fileId = null;
            if (fileGroupPrefix) {
                fileId = fileGrpUse + fileIdSeparator + fileIdRoot;
            } else if (fileGroupSuffix) {
                fileId = fileIdRoot + fileIdSeparator + fileGrpUse;
            } else {
                fileId = fileIdRoot;
            }
            // File ID containing the file group's ID value instead of USE
            String fileIdAlt = null;
            if (fileGrpId != null) {
                if (fileGroupPrefix) {
                    fileIdAlt = fileGrpId + fileIdSeparator + fileIdRoot;
                } else if (fileGroupSuffix) {
                    fileIdAlt = fileIdRoot + fileIdSeparator + fileGrpId;
                }
            }
            logger.debug("fileId: {}", fileId);

            // If fileId is not null, use an XPath expression for the appropriate file element;
            // otherwise get all file elements and get the one with the index of the page order
            String fileIdXPathCondition = "";
            if (fileId != null) {
                if (fileIdAlt != null) {
                    // File ID may contain the value of ID instead of USE
                    fileIdXPathCondition = "[@ID=\"" + fileId + "\" or @ID=\"" + fileIdAlt + "\"]";
                } else {
                    fileIdXPathCondition = "[@ID=\"" + fileId + "\"]";
                }
            }
            int attrListIndex = fileId != null ? 0 : order - 1;

            // Check whether the fileId_fileGroup pattern applies for this file group, otherwise just use the fileId
            xpath = XPATH_FILE + fileIdXPathCondition + "/mets:FLocat/@xlink:href";
            logger.debug(xpath);
            List<Attribute> filepathAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
            if (filepathAttrList == null || filepathAttrList.size() <= attrListIndex) {
                if (useFileGroupGlobal.equals(fileGrpUse)) {
                    logger.warn("Skipping selected file group {} - nothing found at: {}", fileGrpUse, xpath);
                } else {
                    logger.debug("Skipping file group {}", fileGrpUse);
                }
                continue;
            }

            String filePath = filepathAttrList.get(attrListIndex).getValue();
            logger.trace("filePath: {}", filePath);
            if (StringUtils.isEmpty(filePath)) {
                logger.warn("{}: file path not found in file group '{}'.", fileId, fileGrpUse);
            }

            String fileName;
            if (Utils.isFileNameMatchesRegex(filePath, IIIF_IMAGE_FILE_NAMES)) {
                // Extract correct original file name from IIIF
                fileName = Utils.getFileNameFromIiifUrl(filePath);
            } else {
                fileName = FilenameUtils.getName(filePath);
            }

            // Mime type
            xpath = XPATH_FILE + fileIdXPathCondition + "/@MIMETYPE";
            List<Attribute> mimetypeAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
            if (mimetypeAttrList == null || mimetypeAttrList.isEmpty()) {
                logger.error("{}: mime type not found in file group '{}'.", fileId, fileGrpUse);
                break;
            }

            String mimetype = mimetypeAttrList.get(attrListIndex).getValue();
            if (StringUtils.isEmpty(mimetype)) {
                logger.error("{}: mime type is blank in file group '{}'.", fileId, fileGrpUse);
                break;
            }

            String[] mimetypeSplit = mimetype.split("/");
            if (mimetypeSplit.length != 2) {
                logger.error("Illegal mime type '{}' in file group '{}'.", mimetype, fileGrpUse);
                break;
            }

            if (fileGrpUse.equals(useFileGroupGlobal)) {
                // The file name from the main file group (usually PRESENTATION or DEFAULT) will be used for thumbnail purposes etc.
                if (filePath.startsWith("http")) {
                    // Should write the full URL into FILENAME because otherwise a PI_TOPSTRUCT+FILENAME combination may no longer be unique
                    if (ret.getDoc().containsKey(SolrConstants.FILENAME)) {
                        if (StringUtils.isNotEmpty(selectedPreferredImageFileGroup) && selectedPreferredImageFileGroup.equals(fileGrpUse)) {
                            // Preferred file group overrides any already added values
                            ret.getDoc().remove(SolrConstants.FILENAME);
                        } else {
                            logger.error("Page {} already contains FILENAME={}, but attempting to add another value from filegroup {}", iddoc,
                                    filePath,
                                    fileGrpUse);
                        }
                    }

                    String viewerUrl = SolrIndexerDaemon.getInstance().getConfiguration().getViewerUrl();
                    if (downloadExternalImages && dataFolders.get(DataRepository.PARAM_MEDIA) != null && viewerUrl != null
                            && !filePath.startsWith(viewerUrl)) {
                        logger.info("Downloading file: {}", filePath);
                        try {
                            filePath = Path.of(downloadExternalImage(filePath, dataFolders.get(DataRepository.PARAM_MEDIA), fileName))
                                    .getFileName()
                                    .toString();
                        } catch (IOException | URISyntaxException e) {
                            logger.warn("Could not download file: {}", filePath);
                        }
                    }
                    ret.getDoc().addField(SolrConstants.FILENAME, filePath);
                    if (!ret.getShapes().isEmpty()) {
                        for (PhysicalElement shape : ret.getShapes()) {
                            shape.getDoc().addField(SolrConstants.FILENAME, filePath);
                        }
                    }
                    // RosDok IIIF
                    //Don't use if images are downloaded. Then we haven them locally
                    if (!downloadExternalImages && DEFAULT_FILEGROUP.equals(useFileGroupGlobal)
                            && !ret.getDoc().containsKey(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED)) {
                        ret.getDoc().addField(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED, filePath);
                    }
                } else {
                    if (ret.getDoc().containsKey(SolrConstants.FILENAME)) {
                        logger.error("Page {} already contains FILENAME={}, but attempting to add another value from filegroup {}", iddoc, fileName,
                                fileGrpUse);
                    }
                    ret.getDoc().addField(SolrConstants.FILENAME, fileName);
                    if (!ret.getShapes().isEmpty()) {
                        for (PhysicalElement shape : ret.getShapes()) {
                            shape.getDoc().addField(SolrConstants.FILENAME, fileName);
                        }
                    }
                }

                // Add mime type
                if (ret.getDoc().containsKey(SolrConstants.MIMETYPE) && StringUtils.isNotEmpty(selectedPreferredImageFileGroup)
                        && selectedPreferredImageFileGroup.equals(fileGrpUse)) {
                    // Preferred file group overrides any already added values
                    ret.getDoc().removeField(SolrConstants.MIMETYPE);
                }
                ret.getDoc().addField(SolrConstants.MIMETYPE, mimetype);
                if (!ret.getShapes().isEmpty()) {
                    for (PhysicalElement shape : ret.getShapes()) {
                        shape.getDoc().addField(SolrConstants.MIMETYPE, mimetype);
                    }
                }
                // Add file size
                if (dataFolders != null) {
                    try {
                        Path dataFolder = dataFolders.get(DataRepository.PARAM_MEDIA);
                        if (dataFolder != null) {
                            Path path = Paths.get(dataFolder.toAbsolutePath().toString(), fileName);
                            ret.getDoc().addField(FIELD_FILESIZE, Files.size(path));
                        } else {
                            ret.getDoc().addField(FIELD_FILESIZE, -1);
                        }
                    } catch (FileNotFoundException | NoSuchFileException e) {
                        logger.warn("File not found: {}", e.getMessage());
                        ret.getDoc().addField(FIELD_FILESIZE, -1);
                    } catch (IOException | IllegalArgumentException e) {
                        logger.error(e.getMessage(), e);
                        ret.getDoc().addField(FIELD_FILESIZE, -1);
                    }
                }

            } else if (fileGrpUse.equals(ALTO_FILEGROUP) || fileGrpUse.equals(FULLTEXT_FILEGROUP)) {
                altoURL = filePath;
            } else {
                String fieldName = SolrConstants.FILENAME + "_" + mimetypeSplit[1].toUpperCase();
                if (ret.getDoc().getField(fieldName) == null) {
                    switch (mimetypeSplit[1]) {
                        case "html-sandboxed":
                            // Add full URL
                            ret.getDoc().addField(SolrConstants.FILENAME + "_" + mimetypeSplit[1].toUpperCase(), filePath);
                            break;
                        case "object":
                            ret.getDoc().addField(SolrConstants.FILENAME, fileName);
                            ret.getDoc().addField(SolrConstants.MIMETYPE, mimetypeSplit[1]);
                            break;
                        default:
                            ret.getDoc().addField(SolrConstants.FILENAME + "_" + mimetypeSplit[1].toUpperCase(), fileName);
                    }
                }
            }

            // Width + height (from IIIF)
            if (SolrIndexerDaemon.getInstance().getConfiguration().isReadImageDimensionsFromIIIF()
                    && ret.getDoc().getField(SolrConstants.WIDTH) == null
                    && ret.getDoc().getField(SolrConstants.HEIGHT) == null && !downloadExternalImages && filePath != null
                    && filePath.endsWith("info.json")) {
                int[] dim = getImageDimensionsFromIIIF(filePath);
                if (dim.length == 2) {
                    ret.getDoc().addField(SolrConstants.WIDTH, dim[0]);
                    ret.getDoc().addField(SolrConstants.HEIGHT, dim[1]);
                    logger.debug("Added WIDTH from IIIF: {}", ret.getDoc().getFieldValue(SolrConstants.WIDTH));
                    logger.debug("Added HEIGHT from IIIF: {}", ret.getDoc().getFieldValue(SolrConstants.HEIGHT));
                }
            }

            // Width + height (from techMD)
            if (ret.getDoc().getField(SolrConstants.WIDTH) == null && ret.getDoc().getField(SolrConstants.HEIGHT) == null) {
                // Width + height (from techMD)
                xpath = XPATH_FILE + fileIdXPathCondition + "/@ADMID";
                List<Attribute> amdIdAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
                if (amdIdAttrList != null && !amdIdAttrList.isEmpty() && StringUtils.isNotBlank(amdIdAttrList.get(0).getValue())) {
                    String amdId = amdIdAttrList.get(0).getValue();
                    xpath = "/mets:mets/mets:amdSec/mets:techMD[@ID='" + amdId
                            + "']/mets:mdWrap[@MDTYPE='OTHER']/mets:xmlData/pbcoreInstantiation/formatFrameSize/text()";
                    String frameSize = xp.evaluateToString(xpath, null);
                    if (StringUtils.isNotEmpty(frameSize)) {
                        String[] frameSizeSplit = frameSize.split("x");
                        if (frameSizeSplit.length == 2) {
                            ret.getDoc().addField(SolrConstants.WIDTH, frameSizeSplit[0].trim());
                            ret.getDoc().addField(SolrConstants.HEIGHT, frameSizeSplit[1].trim());
                            logger.info("WIDTH: {}", ret.getDoc().getFieldValue(SolrConstants.WIDTH));
                            logger.info("HEIGHT: {}", ret.getDoc().getFieldValue(SolrConstants.HEIGHT));
                        } else {
                            logger.warn("Invalid formatFrameSize value in mets:techMD[@ID='{}']: '{}'", amdId, frameSize);
                        }
                    }
                }
            }

            // Width + height (invalid)
            if (ret.getDoc().getField(SolrConstants.WIDTH) == null && ret.getDoc().getField(SolrConstants.HEIGHT) == null) {
                xpath = XPATH_FILE + fileIdXPathCondition + "/@WIDTH";
                List<Attribute> widthAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
                Integer width = null;
                Integer height = null;
                if (widthAttrList != null && !widthAttrList.isEmpty() && StringUtils.isNotBlank(widthAttrList.get(0).getValue())) {
                    width = Integer.valueOf(widthAttrList.get(0).getValue());
                    logger.warn("mets:file[@ID='{}'] contains illegal WIDTH attribute. It will still be used, though.", fileId);
                }
                xpath = XPATH_FILE + fileIdXPathCondition + "/@HEIGHT";
                List<Attribute> heightAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
                if (heightAttrList != null && !heightAttrList.isEmpty() && StringUtils.isNotBlank(heightAttrList.get(0).getValue())) {
                    height = Integer.valueOf(heightAttrList.get(0).getValue());
                    logger.warn("mets:file[@ID='{}'] contains illegal HEIGHT attribute. It will still be used, though.", fileId);
                }
                if (width != null && height != null) {
                    ret.getDoc().addField(SolrConstants.WIDTH, width);
                    ret.getDoc().addField(SolrConstants.HEIGHT, height);
                }

            }
        }

        // FIELD_IMAGEAVAILABLE indicates whether this page has an image
        if (ret.getDoc().containsKey(SolrConstants.FILENAME) && ret.getDoc().containsKey(SolrConstants.MIMETYPE)
                && ((String) ret.getDoc().getFieldValue(SolrConstants.MIMETYPE)).startsWith("image")) {
            ret.getDoc().addField(FIELD_IMAGEAVAILABLE, true);
            recordHasImages = true;
        } else {
            ret.getDoc().addField(FIELD_IMAGEAVAILABLE, false);
        }

        if (dataFolders != null || altoURL != null) {
            addFullTextToPageDoc(ret.getDoc(), dataFolders, dataRepository, pi, order, altoURL);
        }

        // Page metadata
        String admId = eleStructMapPhysical.getAttributeValue("ADMID");
        if (StringUtils.isNotEmpty(admId)) {
            String techXpath = "/mets:mets/mets:amdSec/mets:techMD[@ID='" + admId
                    + "']/mets:mdWrap/mets:mdWrap[@MDTYPE='OTHER']/mets:xmlData/mets:mdWrap[@MDTYPE='OTHER']"; // TODO check whether METS correct
            List<Element> eletechMdList = xp.evaluateToElements(techXpath, null);
            if (!eletechMdList.isEmpty()) {
                IndexObject indexObj = new IndexObject(1L, pi);
                indexObj.setSourceDocFormat(FileFormat.MIX);
                List<LuceneField> fields = MetadataHelper.retrieveElementMetadata(eletechMdList.get(0), "", indexObj, xp);
                for (LuceneField field : fields) {
                    if (!MetadataHelper.FIELD_HAS_WKT_COORDS.equals(field.getField())) {
                        ret.getDoc().addField(field.getField(), field.getValue());
                        logger.debug("Added techMD field: {}", field);
                    }
                }
                ret.getGroupedMetadata().addAll(indexObj.getGroupedMetadataFields());
            }
        }

        return ret;
    }

    /**
     * Updates the anchor METS file by looking up all indexed children and updating the links. The updated anchor file is placed into the high
     * priority re-indexing queue.
     * 
     * @param indexObj {@link IndexObject}
     * @throws IndexerException in case of errors.
     * @throws IOException in case of errors.
     * @throws SolrServerException
     */
    private void anchorMerge(IndexObject indexObj) throws IndexerException, IOException, SolrServerException {
        logger.debug("anchorMerge: {}", indexObj.getPi());
        SolrDocumentList hits =
                SolrIndexerDaemon.getInstance()
                        .getSearchIndex()
                        .search(SolrConstants.PI_PARENT + ":" + indexObj.getPi() + SolrConstants.SOLR_QUERY_AND + SolrConstants.ISWORK
                                + SolrConstants.SOLR_QUERY_TRUE, null);
        if (hits.isEmpty()) {
            logger.warn("Anchor '{}' has no volumes, no merge needed.", indexObj.getPi());
            return;
        }

        Map<Long, String> childrenInfo = new HashMap<>();
        Map<String, String> labelInfo = new HashMap<>();
        Map<String, Long> orderInfo = new HashMap<>();
        Map<String, String> urnInfo = new HashMap<>();
        Map<String, String> typeInfo = new HashMap<>();
        List<String> childrenInfoUnsorted = new ArrayList<>();
        List<String> collections = new ArrayList<>();
        boolean labelSort = false;
        // Collect volume info
        for (SolrDocument doc : hits) {
            if (collectVolumeInfo(doc, orderInfo, urnInfo, typeInfo, labelInfo, childrenInfo, collections, childrenInfoUnsorted,
                    hotfolder.isAddVolumeCollectionsToAnchor())) {
                labelSort = true;
            }
        }

        SortedMap<Long, String> sortedChildrenMap = null;
        List<Element> childrenE = new ArrayList<>();

        if (!labelSort) {
            sortedChildrenMap = new TreeMap<>(childrenInfo);
        } else {
            sortedChildrenMap = new TreeMap<>();
            for (String childrenPi : childrenInfo.values()) {
                childrenInfoUnsorted.add(childrenPi);
            }
            Collections.sort(childrenInfoUnsorted);
            for (int k = 0; k < childrenInfoUnsorted.size(); k++) {
                sortedChildrenMap.put(Long.valueOf(k), childrenInfoUnsorted.get(k));
            }
        }

        if (indexObj.getRootStructNode().getChildren().isEmpty()) {
            logger.error("Anchor file contains no child elements!");
            return;
        }

        // Merge anchor and volume collections and add them all to the anchor
        boolean newAnchorCollections = false;
        if (hotfolder.isAddVolumeCollectionsToAnchor()) {
            newAnchorCollections = addVolumeCollectionsToAnchor(indexObj, collections);
        }

        // Generate volume elements
        Element firstChild = indexObj.getRootStructNode().getChildren().get(0);
        for (int j = 0; j < sortedChildrenMap.size(); j++) {
            long currentNo = (Long) sortedChildrenMap.keySet().toArray()[j];
            String pi = sortedChildrenMap.get(currentNo);
            childrenE.add(generateVolumeElement(firstChild, j, pi, orderInfo.get(pi), urnInfo.get(pi), labelInfo.get(pi), typeInfo.get(pi)));
        }

        // Remove old children
        indexObj.getRootStructNode().removeContent();
        for (Element element : childrenE) {
            indexObj.getRootStructNode().addContent(element);
        }

        // Write XML file
        writeAnchorXmlFile(indexObj.getPi(), newAnchorCollections);
    }

    /**
     * 
     * @param doc
     * @param orderInfo
     * @param urnInfo
     * @param typeInfo
     * @param labelInfo
     * @param childrenInfo
     * @param collections
     * @param childrenInfoUnsorted
     * @param addVolumeCollectionsToAnchor
     * @return true if volumes are sorted by label; false otherwise
     * @throws IndexerException
     */
    private static boolean collectVolumeInfo(SolrDocument doc, Map<String, Long> orderInfo, Map<String, String> urnInfo, Map<String, String> typeInfo,
            Map<String, String> labelInfo, Map<Long, String> childrenInfo, List<String> collections, List<String> childrenInfoUnsorted,
            boolean addVolumeCollectionsToAnchor) throws IndexerException {
        boolean ret = false;

        String pi = null;
        long num = 0;
        if (doc.getFieldValue(SolrConstants.PI) != null) {
            pi = (String) doc.getFieldValue(SolrConstants.PI);
            if (doc.getFieldValue(SolrConstants.CURRENTNOSORT) != null) {
                try {
                    if (doc.getFieldValue(SolrConstants.CURRENTNOSORT) instanceof Integer) {
                        // Compatibility mode with old indexes
                        num = (int) doc.getFieldValue(SolrConstants.CURRENTNOSORT);
                    } else {
                        num = (long) doc.getFieldValue(SolrConstants.CURRENTNOSORT);
                    }
                    orderInfo.put(pi, num);
                } catch (ClassCastException e) {
                    logger.error("'{}' is not a numerical value.", doc.getFieldValue(SolrConstants.CURRENTNOSORT));
                }
            } else {
                childrenInfoUnsorted.add(pi);
            }
        } else {
            throw new IndexerException("Volume PI could not be found!");
        }

        String label = "";
        if (doc.getFieldValue(SolrConstants.LABEL) != null) {
            label = doc.getFieldValue(SolrConstants.LABEL).toString();
        } else {
            label = "-";
            logger.warn("Volume label for '{}' could not be found.", pi);
        }

        // Read URN
        if (doc.getFieldValue(SolrConstants.URN) != null) {
            urnInfo.put(pi, (String) doc.getFieldValue(SolrConstants.URN));
        }
        // Read TYPE
        if (doc.getFieldValue(SolrConstants.DOCSTRCT) != null) {
            typeInfo.put(pi, (String) doc.getFieldValue(SolrConstants.DOCSTRCT));
        }

        labelInfo.put(pi, label);
        if (childrenInfoUnsorted.isEmpty()) {
            childrenInfo.put(num, pi);
        } else {
            // sort by label
            ret = true;
        }

        // Collect all volume collections
        if (addVolumeCollectionsToAnchor && doc.getFieldValues(SolrConstants.DC) != null) {
            for (Object obj : doc.getFieldValues(SolrConstants.DC)) {
                String dc = (String) obj;
                dc = dc.replace(".", "#");
                if (!collections.contains(dc)) {
                    logger.debug("Found volume colletion: {}", dc);
                    collections.add(dc);
                }
            }
        }

        return ret;
    }

    /**
     * 
     * @param firstChild
     * @param index
     * @param pi
     * @param order
     * @param urn
     * @param label
     * @param type
     * @return Volume element
     */
    private static Element generateVolumeElement(Element firstChild, int index, String pi, Long order, String urn, String label, String type) {
        Element child = firstChild.clone();

        // Set URN
        if (urn != null) {
            child.setAttribute(ATTRIBUTE_CONTENTIDS, urn);
        } else {
            child.removeAttribute(ATTRIBUTE_CONTENTIDS);
        }

        // Normalize LOGID
        String strIdTail = String.valueOf(index + 1);
        String strId = strIdTail;
        if (index < 10) {
            strId = "000" + strIdTail;
        } else if (index < 100) {
            strId = "00" + strIdTail;
        } else if (index < 1000) {
            strId = "0" + strIdTail;
        }

        // Set ORDER
        if (order != null) {
            child.setAttribute("ORDER", String.valueOf(order));
        }

        // Set LOGID
        child.setAttribute("ID", "LOG_" + strId);
        // Set LABEL
        child.setAttribute(SolrConstants.LABEL, label);

        // Set TYPE
        if (type != null) {
            child.setAttribute("TYPE", type);
        }

        // URL
        Namespace nsMets = SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mets");
        Namespace nsXlink = SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("xlink");
        Element mptr = child.getChild("mptr", nsMets);
        String href = mptr.getAttribute("href", nsXlink).getValue();
        if (href.contains("=")) {
            // Resolver URL has a paramater name
            int i = href.indexOf('=');
            href = href.substring(0, i + 1);
        } else {
            // Resolver URL has no parameter name (/ppnresolver/?)
            int i = href.indexOf('?');
            href = href.substring(0, i + 1);
        }
        Attribute attr = new Attribute("href", href + pi, nsXlink);
        mptr.setAttribute(attr);

        return child;
    }

    private void writeAnchorXmlFile(String pi, boolean newAnchorCollections) {
        String extension;
        if (newAnchorCollections) {
            extension = FileTools.XML_EXTENSION;
            logger.info("Anchor document '{}' has received new collection entries and will be reindexed immediately.", pi);
        } else {
            extension = ANCHOR_UPDATE_EXTENSION;
        }

        Path updatedAnchorFile =
                Utils.getCollisionFreeDataFilePath(hotfolder.getHotfolderPath().toAbsolutePath().toString(), pi, "#", extension);

        xp.writeDocumentToFile(updatedAnchorFile.toAbsolutePath().toString());
        if (Files.exists(updatedAnchorFile)) {
            hotfolder.getHighPriorityQueue().add(updatedAnchorFile);
        }
    }

    /**
     * 
     * @param indexObj
     * @param collections
     * @return true if new collections were added; false otherwise
     */
    protected boolean addVolumeCollectionsToAnchor(IndexObject indexObj, List<String> collections) {
        boolean ret = false;
        List<Element> eleDmdSecList =
                xp.evaluateToElements(XPATH_DMDSEC + indexObj.getDmdid() + "']/mets:mdWrap[@MDTYPE='MODS' or @MDTYPE='MARC']", null);
        if (eleDmdSecList != null && !eleDmdSecList.isEmpty()) {
            Element eleDmdSec = eleDmdSecList.get(0);
            List<Element> eleModsList = xp.evaluateToElements("mets:xmlData/mods:mods", eleDmdSec); // TODO MARC
            if (eleModsList != null && !eleModsList.isEmpty()) {
                Element eleMods = eleModsList.get(0);
                List<FieldConfig> collectionConfigFields =
                        SolrIndexerDaemon.getInstance()
                                .getConfiguration()
                                .getMetadataConfigurationManager()
                                .getConfigurationListForField(SolrConstants.DC);
                if (collectionConfigFields != null) {
                    logger.debug("Found {} config items for DC", collectionConfigFields.size());
                    for (FieldConfig item : collectionConfigFields) {
                        for (XPathConfig xPathConfig : item.getxPathConfigurations()) {
                            List<Element> eleCollectionList = xp.evaluateToElements(xPathConfig.getxPath(), eleDmdSec);
                            if (eleCollectionList != null && !eleCollectionList.isEmpty()) {
                                logger.debug("XPath used for collections in this document: {}", xPathConfig.getxPath());
                                for (Element eleCollection : eleCollectionList) {
                                    String oldCollection = eleCollection.getTextTrim();
                                    oldCollection = oldCollection.toLowerCase();
                                    if (StringUtils.isNotEmpty(xPathConfig.getPrefix())) {
                                        oldCollection = xPathConfig.getPrefix() + oldCollection;
                                    }
                                    if (StringUtils.isNotEmpty(xPathConfig.getSuffix())) {
                                        oldCollection = oldCollection + xPathConfig.getSuffix();
                                    }
                                    if (!collections.contains(oldCollection)) {
                                        collections.add(oldCollection);
                                        logger.debug("Found anchor collection: {}", oldCollection);
                                    }
                                }
                                Collections.sort(collections);
                                if (collections.size() > eleCollectionList.size()) {
                                    ret = true;
                                }
                                Element eleCollectionTemplate = eleCollectionList.get(0);
                                // Remove old collection elements
                                for (Element eleOldCollection : eleCollectionList) {
                                    eleMods.removeContent(eleOldCollection);
                                    logger.debug("Removing collection from the anchor: {}", eleOldCollection.getText());
                                }
                                // Add new collection elements
                                for (String collection : collections) {
                                    Element eleNewCollection = eleCollectionTemplate.clone();
                                    eleNewCollection.setText(collection);
                                    eleMods.addContent(eleNewCollection);
                                    logger.debug("Adding collection to the anchor: {}", collection);
                                }
                                break;
                            }
                        }
                    }
                }
            } else {
                logger.error("Could not find the MODS section for '{}'", indexObj.getDmdid());
            }
        } else {
            logger.error("Could not find the MODS section for '{}'", indexObj.getDmdid());
        }

        return ret;
    }

    /**
     * Adds the anchor for the given volume object to the re-index queue.
     * 
     * @param indexObj {@link IndexObject}
     * @param hotfolder
     * @param dataRepository
     */
    void copyAndReIndexAnchor(IndexObject indexObj, Hotfolder hotfolder, DataRepository dataRepository) {
        logger.debug("copyAndReIndexAnchor: {}", indexObj.getPi());
        if (indexObj.getParent() == null) {
            logger.warn("No anchor file has been indexed for this {} yet.", indexObj.getPi());
            return;
        }

        String piParent = indexObj.getParent().getPi();
        String indexedAnchorFilePath =
                new StringBuilder(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString()).append("/")
                        .append(piParent)
                        .append(FileTools.XML_EXTENSION)
                        .toString();
        Path indexedAnchor = Paths.get(indexedAnchorFilePath);
        if (Files.exists(indexedAnchor)) {
            hotfolder.getHighPriorityQueue().add(indexedAnchor);
        }
    }

    /***
     * Re-indexes all child records of the given anchor document, in case the anchor's IDDOC has changed after re-indexing and those child records
     * still point to the old IDDOC. The records are added to a high priority re-indexing queue.
     * 
     * @param indexObj {@link IndexObject}
     * @throws IOException -
     * @throws SolrServerException
     */
    protected void updateAnchorChildrenParentIddoc(IndexObject indexObj) throws IOException, SolrServerException {
        logger.debug("Scheduling all METS files that belong to this anchor for re-indexing...");
        SolrDocumentList hits = SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search(new StringBuilder(SolrConstants.PI_PARENT).append(":")
                        .append(indexObj.getPi())
                        .append(SolrConstants.SOLR_QUERY_AND)
                        .append(SolrConstants.ISWORK)
                        .append(SolrConstants.SOLR_QUERY_TRUE)
                        .toString(), null);
        if (hits.isEmpty()) {
            logger.debug("No volume METS files found for this anchor.");
            return;
        }
        for (SolrDocument doc : hits) {
            // Do not use PI here, as older documents might not have that field, use PPN instead
            String pi = doc.getFieldValue(SolrConstants.PI).toString();
            if (doc.getFieldValue(SolrConstants.IDDOC_PARENT) != null
                    && doc.getFieldValue(SolrConstants.IDDOC_PARENT).toString().equals(String.valueOf(indexObj.getIddoc()))) {
                logger.debug("{} already has the correct parent, skipping.", pi);
                continue;
            }
            String indexedMetsFilePath = dataRepository.getDir(DataRepository.PARAM_INDEXED_METS) + File.separator + pi + FileTools.XML_EXTENSION;
            Path indexedMets = Paths.get(indexedMetsFilePath);
            if (Files.exists(indexedMets)) {
                hotfolder.getHighPriorityQueue().add(indexedMets);
                MetsIndexer.reindexedChildrenFileList.add(indexedMets);
                logger.debug("Added '{}' to reindexedChildrenPiList.", pi);
            }
        }
    }

    /**
     * Recursively re-indexes the logical docstruct subtree of the node represented by the given IndexObject.
     * 
     * @param parentIndexObject {@link IndexObject}
     * @param depth OBSOLETE
     * @param writeStrategy
     * @return List of <code>LuceneField</code>s to inherit up the hierarchy.
     * @throws IOException
     * @throws FatalIndexerException
     */
    protected List<IndexObject> indexAllChildren(IndexObject parentIndexObject, int depth, ISolrWriteStrategy writeStrategy)
            throws IOException, FatalIndexerException {
        logger.trace("indexAllChildren");
        List<IndexObject> ret = new ArrayList<>();

        List<Element> childrenNodeList = xp.evaluateToElements("mets:div", parentIndexObject.getRootStructNode());
        for (int i = 0; i < childrenNodeList.size(); i++) {
            Element node = childrenNodeList.get(i);
            IndexObject indexObj = new IndexObject(getNextIddoc(SolrIndexerDaemon.getInstance().getSearchIndex()));
            indexObj.setRootStructNode(node);
            indexObj.setParent(parentIndexObject);
            indexObj.setTopstructPI(parentIndexObject.getTopstructPI());
            indexObj.getParentLabels().add(parentIndexObject.getLabel());
            indexObj.getParentLabels().addAll(parentIndexObject.getParentLabels());
            if (StringUtils.isNotEmpty(parentIndexObject.getDataRepository())) {
                indexObj.setDataRepository(parentIndexObject.getDataRepository());
            }
            setSimpleData(indexObj);
            indexObj.pushSimpleDataToLuceneArray();
            setUrn(indexObj);

            // Set parent's DATEUPDATED value (needed for OAI)
            for (Long dateUpdated : parentIndexObject.getDateUpdated()) {
                if (!indexObj.getDateUpdated().contains(dateUpdated)) {
                    indexObj.getDateUpdated().add(dateUpdated);
                    indexObj.addToLucene(SolrConstants.DATEUPDATED, String.valueOf(dateUpdated));
                }
            }

            // write metadata
            if (StringUtils.isNotEmpty(indexObj.getDmdid())) {
                MetadataHelper.writeMetadataToObject(indexObj, xp.getMdWrap(indexObj.getDmdid()), "", xp);
            }

            // Inherit PI_ANCHOR value
            if (parentIndexObject.getLuceneFieldWithName(SolrConstants.PI_ANCHOR) != null) {
                indexObj.addToLucene(parentIndexObject.getLuceneFieldWithName(SolrConstants.PI_ANCHOR), false);
            }
            // Inherit GROUPID_* fields
            if (!parentIndexObject.getGroupIds().isEmpty()) {
                for (String groupId : parentIndexObject.getGroupIds().keySet()) {
                    indexObj.addToLucene(parentIndexObject.getLuceneFieldWithName(groupId), false);
                }
            }

            // Add parent's metadata and SORT_* fields to this docstruct
            for (LuceneField field : parentIndexObject.getLuceneFields()) {
                if (SolrIndexerDaemon.getInstance()
                        .getConfiguration()
                        .getMetadataConfigurationManager()
                        .getFieldsToAddToChildren()
                        .contains(field.getField())) {
                    // Avoid duplicates (same field name + value)
                    indexObj.addToLucene(new LuceneField(field.getField(), field.getValue()), true);

                    logger.debug("Added {}:{} to child element {}", field.getField(), field.getValue(), indexObj.getLogId());
                } else if (field.getField().startsWith(SolrConstants.PREFIX_SORT)) {
                    // Only one instance of each SORT_ field may exist
                    indexObj.addToLucene(new LuceneField(field.getField(), field.getValue()), true);
                }
            }

            indexObj.writeAccessConditions(parentIndexObject);

            // Generate thumbnail info and page docs for this docstruct. PI_TOPSTRUCT must be set at this point!
            if (StringUtils.isNotEmpty(indexObj.getLogId())) {
                try {
                    List<LuceneField> thumbnailFields = mapPagesToDocstruct(indexObj, parentIndexObject, false, writeStrategy, depth);
                    if (thumbnailFields != null) {
                        indexObj.getLuceneFields().addAll(thumbnailFields);
                    }
                    // Write number of pages and first/last page labels for this docstruct
                    if (indexObj.getNumPages() > 0) {
                        indexObj.addToLucene(SolrConstants.NUMPAGES, String.valueOf(indexObj.getNumPages()));
                        if (indexObj.getFirstPageLabel() != null) {
                            indexObj.addToLucene(SolrConstants.ORDERLABELFIRST, indexObj.getFirstPageLabel());
                        }
                        if (indexObj.getLastPageLabel() != null) {
                            indexObj.addToLucene(SolrConstants.ORDERLABELLAST, indexObj.getLastPageLabel());
                        }
                        if (indexObj.getFirstPageLabel() != null && indexObj.getLastPageLabel() != null) {
                            indexObj.addToLucene("MD_ORDERLABELRANGE",
                                    new StringBuilder(indexObj.getFirstPageLabel()).append(" - ").append(indexObj.getLastPageLabel()).toString());
                        }
                    }

                } catch (IndexerException e) {
                    logger.warn(e.getMessage());
                }
            }

            // Add own and all ancestor LABEL values to the DEFAULT field
            StringBuilder sbDefaultValue = new StringBuilder();
            sbDefaultValue.append(indexObj.getDefaultValue());
            String labelWithSpaces = new StringBuilder(" ").append(indexObj.getLabel()).append(' ').toString();
            if (StringUtils.isNotEmpty(indexObj.getLabel()) && !sbDefaultValue.toString().contains(labelWithSpaces)) {
                sbDefaultValue.append(labelWithSpaces);
            }
            if (SolrIndexerDaemon.getInstance().getConfiguration().isAddLabelToChildren()) {
                for (String label : indexObj.getParentLabels()) {
                    String parentLabelWithSpaces = new StringBuilder(" ").append(label).append(' ').toString();
                    if (StringUtils.isNotEmpty(label) && !sbDefaultValue.toString().contains(parentLabelWithSpaces)) {
                        sbDefaultValue.append(parentLabelWithSpaces);
                    }
                }
            }

            indexObj.setDefaultValue(sbDefaultValue.toString());

            // Add DEFAULT field
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(indexObj.getDefaultValue()));
                // Add default value to parent doc
                indexObj.setDefaultValue("");
            }

            // Recursively index child elements 
            List<IndexObject> childObjectList = indexAllChildren(indexObj, depth + 1, writeStrategy);

            // METADATA UPWARD INHERITANCE

            // Add recursively collected child metadata fields that are configured to be inherited up
            indexObj.addChildMetadata(childObjectList);

            // Add fields configured to be inherited up to the return list (after adding child metadata first!)
            for (LuceneField field : indexObj.getLuceneFields()) {
                if (SolrIndexerDaemon.getInstance()
                        .getConfiguration()
                        .getMetadataConfigurationManager()
                        .getFieldsToAddToParents()
                        .contains(field.getField())) {
                    // Add only to topstruct
                    indexObj.getFieldsToInheritToParents().add(field.getField());
                    field.setSkip(true);
                } else if (SolrIndexerDaemon.getInstance()
                        .getConfiguration()
                        .getMetadataConfigurationManager()
                        .getFieldsToAddToParents()
                        .contains("!" + field.getField())) {
                    // Add to entire hierarchy
                    indexObj.getFieldsToInheritToParents().add(field.getField());
                }
            }
            // Add grouped fields configured to be inherited up to the return list (after adding child metadata first!)
            for (GroupedMetadata field : indexObj.getGroupedMetadataFields()) {
                if (SolrIndexerDaemon.getInstance()
                        .getConfiguration()
                        .getMetadataConfigurationManager()
                        .getFieldsToAddToParents()
                        .contains(field.getLabel())) {
                    // Add only to topstruct
                    indexObj.getFieldsToInheritToParents().add(field.getLabel());
                    field.setSkip(true);
                } else if (SolrIndexerDaemon.getInstance()
                        .getConfiguration()
                        .getMetadataConfigurationManager()
                        .getFieldsToAddToParents()
                        .contains("!" + field.getLabel())) {
                    // Add to entire hierarchy
                    indexObj.getFieldsToInheritToParents().add(field.getLabel());
                }
            }

            // If there are fields to inherit up the hierarchy, add this index object to the return list
            if (!indexObj.getFieldsToInheritToParents().isEmpty()) {
                ret.add(indexObj);
            }

            // The following steps must be performed after adding child metadata and marking own metadata for skipping

            // Add grouped metadata as separate documents (must be done after mapping page docs to this docstrct
            // and after adding grouped metadata from child elements)
            addGroupedMetadataDocs(writeStrategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

            // Apply field modifications that should happen at the very end
            indexObj.applyFinalModifications();

            // Write to Solr
            logger.debug("Writing child document '{}'...", indexObj.getIddoc());
            writeStrategy.addDoc(SolrSearchIndex.createDocument(indexObj.getLuceneFields()));
        }

        return ret;
    }

    /**
     * Sets DMDID, ID, TYPE and LABEL from the METS document.
     * 
     * @param indexObj {@link IndexObject}
     */
    private void setSimpleData(IndexObject indexObj) {
        logger.trace("setSimpleData(IndexObject) - start");
        indexObj.setSourceDocFormat(getSourceDocFormat());
        Element structNode = indexObj.getRootStructNode();

        // DMDID
        indexObj.setDmdid(TextHelper.normalizeSequence(structNode.getAttributeValue(SolrConstants.DMDID)));
        logger.trace("DMDID: {}", indexObj.getDmdid());

        // LOGID
        String value = TextHelper.normalizeSequence(structNode.getAttributeValue("ID"));
        if (value != null) {
            indexObj.setLogId(value);
        }
        logger.trace("LOGID: {}", indexObj.getLogId());

        // TYPE
        value = TextHelper.normalizeSequence(structNode.getAttributeValue("TYPE"));
        if (value != null) {
            indexObj.setType(value);
        }
        logger.trace("TYPE: {}", indexObj.getType());

        // LABEL
        value = TextHelper.normalizeSequence(structNode.getAttributeValue("LABEL"));
        if (value != null) {
            // Remove non-sort characters from LABEL, if configured to do so
            if (SolrIndexerDaemon.getInstance().getConfiguration().isLabelCleanup()) {
                value = value.replace("<ns>", "");
                value = value.replace("</ns>", "");
                value = value.replace("<<", "");
                value = value.replace(">>", "");
                value = value.replace("¬", "");
            }
            indexObj.setLabel(value);
        }
        logger.trace("LABEL: {}", indexObj.getLabel());
    }

    /**
     * Finds all physical page URNs for the given IndexObject and adds them to its metadata sequentially as one string. Should only be used with the
     * top docstruct (ISWORK). TODO get from generated pages instead of METS.
     * 
     * @param indexObj The IndexObject to find URNs for.
     */
    private void generatePageUrns(IndexObject indexObj) {
        String query1 = "/mets:mets/mets:structMap[@TYPE='PHYSICAL']/mets:div[@TYPE='physSequence']/mets:div/@CONTENTIDS";
        List<String> physUrnList = xp.evaluateToStringList(query1, null);
        if (physUrnList != null) {
            StringBuilder sbPageUrns = new StringBuilder();
            List<String> imageUrns = new ArrayList<>(physUrnList.size());
            for (String pageUrn : physUrnList) {
                String urn = null;
                if (Utils.isUrn(pageUrn)) {
                    urn = pageUrn.replace("\\\\", "");
                }
                if (StringUtils.isEmpty(urn)) {
                    urn = "NOURN";
                }
                sbPageUrns.append(urn).append(' ');
                imageUrns.add(urn);
            }
            indexObj.setImageUrns(imageUrns);
        }
    }

    /**
     * Retrieves and sets the URN for mets:structMap[@TYPE='LOGICAL'] elements.
     * 
     * @param indexObj
     * @return The URN
     */
    private String setUrn(IndexObject indexObj) {
        String query = "/mets:mets/mets:structMap[@TYPE='LOGICAL']//mets:div[@ID='" + indexObj.getLogId() + "']/@CONTENTIDS";
        String urn = xp.evaluateToAttributeStringValue(query, null);
        if (Utils.isUrn(urn)) {
            indexObj.setUrn(urn);
            indexObj.addToLucene(SolrConstants.URN, urn);
        }

        return urn;
    }

    /**
     * Returns the logical root node.
     * 
     * @param indexObj
     * @return {@link Element} or null
     * 
     */
    private Element findStructNode(IndexObject indexObj) {
        String query = "";
        if (!indexObj.isVolume()) {
            query = "//mets:mets/mets:structMap[@TYPE='LOGICAL']/mets:div[@DMDID and @ID]";
        } else {
            query = "//mets:mets/mets:structMap[@TYPE='LOGICAL']/*//mets:div[@DMDID and @ID]";
        }
        List<Element> elements = xp.evaluateToElements(query, null);
        if (!elements.isEmpty()) {
            return elements.get(0);
        }

        return null;
    }

    /**
     * Checks whether the METS document represents an anchor.
     * 
     * 
     * @return boolean
     * @throws FatalIndexerException
     */
    @Override
    boolean isAnchor() throws FatalIndexerException {
        String anchorQuery = "/mets:mets/mets:structMap[@TYPE='PHYSICAL']";
        List<Element> anchorList = xp.evaluateToElements(anchorQuery, null);

        return anchorList == null || anchorList.isEmpty();
    }

    /**
     * Checks whether this is a volume of a multivolume work (should be false for monographs and anchors).
     * 
     * @return boolean
     * @should return true if record is volume
     * @should return false if relatedItem not anchor
     */
    protected boolean isVolume() {
        String query = SolrIndexerDaemon.getInstance().getConfiguration().getMetsVolumeCheckXPath();
        List<Element> relatedItemList = xp.evaluateToElements(query, null);

        return relatedItemList != null && !relatedItemList.isEmpty();
    }

    /**
     * <p>
     * getMetsCreateDate.
     * </p>
     *
     * @return a {@link java.time.LocalDateTime} object.
     * @should return CREATEDATE value
     * @should return null if date does not exist in METS
     */
    protected ZonedDateTime getMetsCreateDate() {
        String dateString = xp.evaluateToAttributeStringValue("/mets:mets/mets:metsHdr/@CREATEDATE", null);
        return parseCreateDate(dateString);
    }

    /**
     * 
     * @param dateString Date string to parse
     * @return {@link ZonedDateTime} parsed from the given string
     * @should parse iso instant correctly
     * @should parse iso local dateTime correctly
     * @should parse iso offset dateTime correctly
     */
    static ZonedDateTime parseCreateDate(String dateString) {
        if (StringUtils.isEmpty(dateString)) {
            return null;
        }

        try {
            return ZonedDateTime.parse(dateString, DateTools.formatterISO8601DateTimeInstant);
        } catch (DateTimeParseException e) {
            try {
                return LocalDateTime.parse(dateString, DateTools.formatterISO8601LocalDateTime).atZone(ZoneId.systemDefault());
            } catch (DateTimeParseException e1) {
                try {
                    return ZonedDateTime.parse(dateString, DateTools.formatterISO8601DateTimeWithOffset);
                } catch (DateTimeParseException e2) {
                    logger.error(e2.getMessage());
                    return null;
                }
            }
        }
    }

    /**
     * Moves an updated anchor METS file (with an .UPDATED extension) to the indexed METS folder and the previous version to the updated_mets folder
     * without doing any index operations.
     *
     * @param metsFile {@link java.nio.file.Path} z.B.: PPN1234567890.UPDATED
     * @param updatedMetsFolder Updated METS folder for old METS files.
     * @param dataRepository Data repository to which to copy the new file.
     * @throws java.io.IOException in case of errors.
     * @should copy new METS file correctly
     * @should copy old METS file to updated mets folder if file already exists
     * @should remove anti-collision name parts
     */
    public static void anchorSuperupdate(Path metsFile, Path updatedMetsFolder, DataRepository dataRepository) throws IOException {
        logger.debug("Renaming and moving updated anchor...");
        if (metsFile == null) {
            throw new IllegalArgumentException("metsFile may not be null");
        }
        if (updatedMetsFolder == null) {
            throw new IllegalArgumentException("updatedMetsFolder may not be null");
        }
        if (dataRepository == null) {
            throw new IllegalArgumentException("dataRepository may not be null");
        }

        String baseFileName = FilenameUtils.getBaseName(metsFile.getFileName().toString());
        // Remove anti-collision name part
        if (baseFileName.contains("#")) {
            baseFileName = baseFileName.substring(0, baseFileName.indexOf('#'));
        }
        StringBuilder sbNewFilename = new StringBuilder(baseFileName).append(".xml");
        if (sbNewFilename.length() > 0) {
            Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), sbNewFilename.toString());
            try {
                // Java NIO is non-blocking, so copying a file in one call and then deleting it in a second might run into problems.
                // Instead, move the file.
                Files.move(Paths.get(metsFile.toAbsolutePath().toString()), indexed);
            } catch (FileAlreadyExistsException e) {
                // Add a timestamp to the old file nameformatterBasicDateTime
                String oldMetsFilename = new StringBuilder(FilenameUtils.getBaseName(sbNewFilename.toString())).append("_")
                        .append(LocalDateTime.now().format(DateTools.formatterBasicDateTime))
                        .append(".xml")
                        .toString();
                Path destMetsFilePath = Paths.get(updatedMetsFolder.toAbsolutePath().toString(), oldMetsFilename);
                Files.move(indexed, destMetsFilePath, StandardCopyOption.REPLACE_EXISTING);
                logger.debug("Old anchor file copied to '{}{}{}'.", updatedMetsFolder.toAbsolutePath(), File.separator, oldMetsFilename);
                // Then copy the new file again, overwriting the old
                Files.move(Paths.get(metsFile.toAbsolutePath().toString()), indexed, StandardCopyOption.REPLACE_EXISTING);
            }
            logger.info("New anchor file copied to '{}'.", indexed.toAbsolutePath());
        }
    }

    /**
     * 
     * @return {@link FileFormat}
     */
    protected FileFormat getSourceDocFormat() {
        return FileFormat.METS;
    }

    /**
     * 
     * @param dmdId
     * @return Root path for querying for PI
     */
    protected String getPiRootPath(String dmdId) {
        String ret = XPATH_DMDSEC + dmdId + "']/mets:mdWrap[@MDTYPE='MODS']/";
        logger.debug("preQuery: {}", ret);
        return ret;
    }

    /**
     * <p>
     * getAnchorPi.
     * </p>
     *
     * @return a {@link java.lang.String} object.
     */
    public String getAnchorPi() {
        String query =
                "/mets:mets/mets:dmdSec" + XPATH_ANCHOR_PI_PART;
        List<Element> relatedItemList = xp.evaluateToElements(query, null);
        if (relatedItemList != null && !relatedItemList.isEmpty()) {
            return relatedItemList.get(0).getText();
        }

        return null;
    }
}
