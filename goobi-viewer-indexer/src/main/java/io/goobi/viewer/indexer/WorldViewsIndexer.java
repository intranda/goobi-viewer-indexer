/**
 * This file is part of the Goobi Solr Indexer - a content indexing tool for the Goobi Viewer and OAI-PMH/SRU interfaces.
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
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Element;
import org.jdom2.JDOMException;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.DateTools;
import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.FulltextAugmentor;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.PhysicalElement;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * Indexer implementation for WorldViews documents.
 */
public class WorldViewsIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(WorldViewsIndexer.class);

    /** Constant <code>DEFAULT_FILEGROUP_1="PRESENTATION"</code> */
    public static final String DEFAULT_FILEGROUP_1 = "PRESENTATION";
    /** Constant <code>DEFAULT_FILEGROUP_2="DEFAULT"</code> */
    public static final String DEFAULT_FILEGROUP_2 = "DEFAULT";
    /** Constant <code>ALTO_FILEGROUP="FULLTEXT"</code> */
    public static final String ALTO_FILEGROUP = "FULLTEXT";
    /** Constant <code>ANCHOR_UPDATE_EXTENSION=".UPDATED"</code> */
    public static final String ANCHOR_UPDATE_EXTENSION = ".UPDATED";
    /** Constant <code>DEFAULT_FULLTEXT_CHARSET="Cp1250"</code> */
    public static final String DEFAULT_FULLTEXT_CHARSET = "Cp1250";

    private static List<Path> reindexedChildrenFileList = new ArrayList<>();

    /**
     * Constructor.
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public WorldViewsIndexer(Hotfolder hotfolder) {
        this.hotfolder = hotfolder;
    }

    /** {@inheritDoc} */
    @Override
    public List<String> addToIndex(Path mainFile, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        logger.debug("Indexing WorldViews file '{}'...", mainFile.getFileName());
        String fileNameRoot = FilenameUtils.getBaseName(mainFile.getFileName().toString());

        // Check data folders in the hotfolder
        Map<String, Path> dataFolders = checkDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

        // Use existing folders for those missing in the hotfolder
        checkReindexSettings(dataFolders, reindexSettings);

        String[] resp = index(mainFile, dataFolders, null, SolrIndexerDaemon.getInstance().getConfiguration().getPageCountStart());

        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            String newMetsFileName = resp[0];
            String pi = FilenameUtils.getBaseName(newMetsFileName);
            Path indexed = Paths.get(getDataRepository().getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), newMetsFileName);
            if (mainFile.equals(indexed)) {
                return Collections.singletonList(pi);
            }
            if (Files.exists(indexed)) {
                // Add a timestamp to the old file name
                String oldMetsFilename =
                        FilenameUtils.getBaseName(newMetsFileName) + "_" + LocalDateTime.now().format(DateTools.FORMATTER_BASIC_DATETIME) + ".xml";
                Path newFile = Paths.get(hotfolder.getUpdatedMets().toAbsolutePath().toString(), oldMetsFilename);
                Files.copy(indexed, newFile);
                logger.debug("Old METS file copied to '{}'.", newFile.toAbsolutePath());
            }
            Files.copy(mainFile, indexed, StandardCopyOption.REPLACE_EXISTING);
            getDataRepository().checkOtherRepositoriesForRecordFileDuplicates(newMetsFileName, DataRepository.PARAM_INDEXED_METS,
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

            // Copy other data folders
            dataRepository.copyAndDeleteAllDataFolders(pi, dataFolders, reindexSettings,
                    hotfolder.getDataRepositoryStrategy().getAllDataRepositories());

            // Delete unsupported data folders
            FileTools.deleteUnsupportedDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

            // Create success file for Goobi workflow
            Path successFile = Paths.get(hotfolder.getSuccessFolder().toAbsolutePath().toString(), mainFile.getFileName().toString());
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
                logger.error(LOG_COULD_NOT_BE_DELETED, mainFile.toAbsolutePath());
            }

            // Update data repository cache map in the Goobi viewer
            if (previousDataRepository != null) {
                try {
                    Utils.updateDataRepositoryCache(pi, dataRepository.getPath());
                } catch (HTTPException e) {
                    logger.error(e.getMessage(), e);
                }
            }
            prerenderPagePdfsIfRequired(pi);
            logger.info("Successfully finished indexing '{}'.", mainFile.getFileName());

            // Remove this file from lower priority hotfolders to avoid overriding changes with older version
            SolrIndexerDaemon.getInstance().removeRecordFileFromLowerPriorityHotfolders(pi, hotfolder);

            return Collections.singletonList(pi);
        }

        // Error
        if (hotfolder.isDeleteContentFilesOnFailure()) {
            // Delete all data folders for this record from the hotfolder
            DataRepository.deleteDataFoldersFromHotfolder(dataFolders, reindexSettings);
        }
        handleError(mainFile, resp[1], FileFormat.WORLDVIEWS);
        try {
            Files.delete(mainFile);
        } catch (IOException e) {
            logger.error(LOG_COULD_NOT_BE_DELETED, mainFile.toAbsolutePath());
        }

        return Collections.emptyList();
    }

    /**
     * Indexes the given WorldViews file.
     *
     * @param mainFile {@link java.nio.file.Path}
     * @param dataFolders a {@link java.util.Map} object.
     * @param pageCountStart Order number for the first page.
     * @should index record correctly
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @return an array of {@link java.lang.String} objects.
     */
    public String[] index(Path mainFile, Map<String, Path> dataFolders, final ISolrWriteStrategy writeStrategy,
            int pageCountStart) {
        String[] ret = { null, null };

        if (mainFile == null || !Files.exists(mainFile)) {
            throw new IllegalArgumentException("mainFile must point to an existing XML file.");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null.");
        }

        ISolrWriteStrategy useWriteStrategy = writeStrategy;
        try {
            if (useWriteStrategy == null) {
                // Request appropriate write strategy
                useWriteStrategy = AbstractWriteStrategy.create(mainFile, dataFolders, hotfolder);
            } else {
                logger.info("Solr write strategy injected by caller: {}", useWriteStrategy.getClass().getName());
            }
            initJDomXP(mainFile);
            IndexObject indexObj = new IndexObject(getNextIddoc());
            logger.debug("IDDOC: {}", indexObj.getIddoc());

            // Set PI
            String pi = validateAndApplyPI(findPI("worldviews//identifier/text()"), indexObj, false);

            indexObj.setSourceDocFormat(FileFormat.WORLDVIEWS);

            // Determine the data repository to use
            selectDataRepository(indexObj, pi, mainFile, dataFolders);

            ret[0] = new StringBuilder(indexObj.getPi()).append(FileTools.XML_EXTENSION).toString();

            // Check and use old data folders, if no new ones found
            checkOldDataFolders(dataFolders, MetsIndexer.DATA_FOLDER_PARAMS, pi);

            prepareUpdate(indexObj);

            // Docstruct
            String docstructType = xp.evaluateToString("worldviews//docType/text()", null);
            if (docstructType != null) {
                indexObj.setType(docstructType);
            } else {
                indexObj.setType("Monograph");
                logger.warn("<docType> not found, setting docstruct type to 'Monograph'.");
            }

            // LOGID
            indexObj.setLogId("LOG_0000");
            // Collections / access conditions
            List<String> collections = xp.evaluateToStringList("worldviews//collection", null);
            if (collections != null && !collections.isEmpty()) {
                for (String collection : collections) {
                    indexObj.addToLucene(SolrConstants.DC, collection);
                    indexObj.addToLucene(SolrConstants.ACCESSCONDITION, collection);
                }
            }

            // MD_WV_*SOURCE
            String primarySourcePi = xp.evaluateToString("worldviews//relatedItem[@type='primarySource']/identifier/text()", null);
            if (primarySourcePi != null) {
                indexObj.addToLucene("MD_WV_PRIMARYSOURCE", primarySourcePi);
            } else {
                // For sources use own PI
                indexObj.addToLucene("MD_WV_PRIMARYSOURCE", indexObj.getPi());
            }

            String seconsarySourcePi = xp.evaluateToString("worldviews//relatedItem[@type='secondarySource']/identifier/text()", null);
            if (seconsarySourcePi != null) {
                indexObj.addToLucene("MD_WV_SECONDARYSOURCE", seconsarySourcePi);
            }

            int workDepth = 0; // depth of the docstrct that has ISWORK (volume or monograph)

            // Process TEI files
            if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) != null) {
                MetadataHelper.processTEIMetadataFiles(indexObj, dataFolders.get(DataRepository.PARAM_TEIMETADATA));
            }

            // Add IndexObject member values as Solr fields (after processing the TEI files!)
            indexObj.pushSimpleDataToLuceneArray();

            // Write mapped metadata
            MetadataHelper.writeMetadataToObject(indexObj, xp.getRootElement(), "", xp);

            // Only keep MD_WV_SEGMENT fields on Sources
            if (!"Source".equals(indexObj.getType())) {
                List<LuceneField> removeList = indexObj.getLuceneFieldsWithName("MD_WV_SEGMENT");
                indexObj.getLuceneFields().removeAll(removeList);
                removeList = indexObj.getLuceneFieldsWithName("MD_WV_SEGMENT_UNTOKENIZED");
                indexObj.getLuceneFields().removeAll(removeList);
                logger.info("Record not a Source, removed MD_WV_SEGMENT.");
            }

            LuceneField label = indexObj.getLuceneFieldWithName("MD_TITLE");
            if (label != null) {
                indexObj.setLabel(label.getValue());
                indexObj.addToLucene(SolrConstants.LABEL, MetadataHelper.applyValueDefaultModifications(label.getValue()));
            }

            // Add language codes as metadata fields
            indexObj.writeLanguages();

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // Write created/updated timestamps
            indexObj.writeDateModified(false);

            // Generate docs for all pages and add to the write strategy
            generatePageDocuments(useWriteStrategy, dataFolders, pi, pageCountStart);
            indexObj.setNumPages(useWriteStrategy.getPageDocsSize());
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

            // If images have been found for any page, set a boolean in the root doc indicating that the record does have images
            indexObj.addToLucene(FIELD_IMAGEAVAILABLE, String.valueOf(recordHasImages));

            // If full-text has been indexed for any page, set a boolean in the root doc indicating that the record does have full-text
            indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, String.valueOf(recordHasFulltext));

            // Add THUMBNAIL,THUMBPAGENO,THUMBPAGENOLABEL (must be done AFTER writeDateMondified(),
            // writeAccessConditions() and generatePageDocuments()!
            generateChildDocstructDocuments(indexObj, useWriteStrategy, dataFolders, workDepth);

            // ISWORK only for non-anchors
            indexObj.addToLucene(SolrConstants.ISWORK, "true");

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
            addGroupDocs(indexObj, writeStrategy);

            // Add grouped metadata as separate Solr docs (remove duplicates first)
            indexObj.removeDuplicateGroupedMetadata();
            addGroupedMetadataDocs(useWriteStrategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

            boolean indexedChildrenFileList = false;

            logger.debug("reindexedChildrenFileList.size(): {}", WorldViewsIndexer.reindexedChildrenFileList.size());
            if (WorldViewsIndexer.reindexedChildrenFileList.contains(mainFile)) {
                logger.debug("{} in reindexedChildrenFileList, removing...", mainFile.toAbsolutePath());
                WorldViewsIndexer.reindexedChildrenFileList.remove(mainFile);
                indexedChildrenFileList = true;
            }

            SolrInputDocument rootDoc = SolrSearchIndex.createDocument(indexObj.getLuceneFields());
            useWriteStrategy.setRootDoc(rootDoc);

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)
            if (!iddocsToDelete.isEmpty()) {
                logger.info("Removing {} docs of the previous instance of this volume from the index...", iddocsToDelete.size());
                SolrIndexerDaemon.getInstance().getSearchIndex().deleteDocuments(new ArrayList<>(iddocsToDelete));
            }
            
            logger.debug("Writing document to index...");
            useWriteStrategy.writeDocs(SolrIndexerDaemon.getInstance().getConfiguration().isAggregateRecords());
            if (indexObj.isVolume() && (!indexObj.isUpdate() || indexedChildrenFileList)) {
                logger.info("Re-indexing anchor...");
                copyAndReIndexAnchor(indexObj, hotfolder, dataRepository);
            }
            logger.info("Finished writing data for '{}' to Solr.", pi);
        } catch (InterruptedException e) {
            logger.error("Indexing of '{}' could not be finished due to an error.", mainFile.getFileName());
            logger.error(e.getMessage(), e);
            ret[1] = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            SolrIndexerDaemon.getInstance().getSearchIndex().rollback();
            Thread.currentThread().interrupt();
        } catch (FatalIndexerException | IndexerException | IOException | JDOMException | SolrServerException e) {
            logger.error("Indexing of '{}' could not be finished due to an error.", mainFile.getFileName());
            logger.error(e.getMessage(), e);
            ret[1] = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            SolrIndexerDaemon.getInstance().getSearchIndex().rollback();
        } finally {
            if (useWriteStrategy != null) {
                useWriteStrategy.cleanup();
            }
        }

        return ret;
    }

    /**
     * Generates thumbnail info fields for the given docstruct. Also generates page docs mapped to this docstruct. <code>IndexObj.topstructPi</code>
     * must be set before calling this method.
     * 
     * @param rootIndexObj {@link IndexObject}
     * @param writeStrategy
     * @param dataFolders
     * @param depth Depth of the current docstruct in the docstruct hierarchy.
     * @throws FatalIndexerException
     */
    private void generateChildDocstructDocuments(IndexObject rootIndexObj, ISolrWriteStrategy writeStrategy,
            Map<String, Path> dataFolders, int depth) throws FatalIndexerException {
        IndexObject currentIndexObj = null;
        String currentDocstructLabel = null;

        String thumbnailFile = null;
        int thumbnailOrder = 1;
        int docstructCount = 0;
        for (int i = 1; i <= writeStrategy.getPageDocsSize(); ++i) {
            SolrInputDocument pageDoc = writeStrategy.getPageDocForOrder(i);

            if (thumbnailFile == null || pageDoc.containsKey(SolrConstants.THUMBNAILREPRESENT)
                    || (currentIndexObj != null && currentIndexObj.getThumbnailRepresent() != null
                            && currentIndexObj.getThumbnailRepresent().equals(pageDoc.getFieldValue(SolrConstants.FILENAME)))) {
                thumbnailFile = (String) pageDoc.getFieldValue(SolrConstants.FILENAME);
                thumbnailOrder = (int) pageDoc.getFieldValue(SolrConstants.ORDER);
                if (pageDoc.containsKey(SolrConstants.THUMBNAILREPRESENT)) {
                    pageDoc.removeField(SolrConstants.THUMBNAILREPRESENT);
                }
            }

            String pageDocstructLabel = (String) pageDoc.getFieldValue(SolrConstants.LABEL);
            String orderLabel = (String) pageDoc.getFieldValue(SolrConstants.ORDERLABEL);

            // New docstruct
            if (pageDocstructLabel != null && !pageDocstructLabel.equals(currentDocstructLabel)) {
                currentDocstructLabel = pageDocstructLabel;
                docstructCount++;

                // Finalize previous docstruct
                if (currentIndexObj != null) {
                    finalizeChildDocstruct(currentIndexObj, dataFolders, writeStrategy);
                }

                // Create new docstruct object
                currentIndexObj = new IndexObject(getNextIddoc());
                currentIndexObj.setParent(rootIndexObj);
                currentIndexObj.setTopstructPI(rootIndexObj.getTopstructPI());
                currentIndexObj.setType(pageDocstructLabel);
                currentIndexObj.setLabel((String) pageDoc.getFieldValue(SolrConstants.LABEL));
                currentIndexObj.getParentLabels().add(rootIndexObj.getLabel());
                currentIndexObj.getParentLabels().addAll(rootIndexObj.getParentLabels());
                if (StringUtils.isNotEmpty(rootIndexObj.getDataRepository())) {
                    currentIndexObj.setDataRepository(rootIndexObj.getDataRepository());
                }
                currentIndexObj.setLogId("LOG_" + MetadataHelper.FORMAT_FOUR_DIGITS.get().format(docstructCount));
                currentIndexObj.pushSimpleDataToLuceneArray();

                // This is a new docstruct, so the current page is its first
                currentIndexObj.setFirstPageLabel(orderLabel);

                // Set parent's DATEUPDATED value (needed for OAI)
                for (Long dateUpdated : rootIndexObj.getDateUpdated()) {
                    if (!currentIndexObj.getDateUpdated().contains(dateUpdated)) {
                        currentIndexObj.getDateUpdated().add(dateUpdated);
                        currentIndexObj.addToLucene(SolrConstants.DATEUPDATED, String.valueOf(dateUpdated));
                    }
                }

                // Set thumbnail info
                currentIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(pageDoc.getFieldValue(SolrConstants.ORDER))),
                        false);
                currentIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBPAGENOLABEL, orderLabel), false);
                currentIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBNAIL, (String) pageDoc.getFieldValue(SolrConstants.FILENAME)), false);

                // Add parent's metadata and SORT_* fields to this docstruct
                Set<String> existingMetadataFields = new HashSet<>();
                Set<String> existingSortFieldNames = new HashSet<>();
                for (LuceneField field : currentIndexObj.getLuceneFields()) {
                    if (SolrIndexerDaemon.getInstance()
                            .getConfiguration()
                            .getMetadataConfigurationManager()
                            .getFieldsToAddToChildren()
                            .contains(field.getField())) {
                        existingMetadataFields.add(new StringBuilder(field.getField()).append(field.getValue()).toString());
                    } else if (field.getField().startsWith(SolrConstants.PREFIX_SORT)) {
                        existingSortFieldNames.add(field.getField());
                    }
                }
                for (LuceneField field : rootIndexObj.getLuceneFields()) {
                    if (SolrIndexerDaemon.getInstance()
                            .getConfiguration()
                            .getMetadataConfigurationManager()
                            .getFieldsToAddToChildren()
                            .contains(field.getField())
                            && !existingMetadataFields.contains(new StringBuilder(field.getField()).append(field.getValue()).toString())) {
                        // Avoid duplicates (same field name + value)
                        currentIndexObj.addToLucene(field.getField(), field.getValue());
                        logger.debug("Added {}:{} to child element {}", field.getField(), field.getValue(), currentIndexObj.getLogId());
                    } else if (field.getField().startsWith(SolrConstants.PREFIX_SORT) && !existingSortFieldNames.contains(field.getField())) {
                        // Only one instance of each SORT_ field may exist
                        currentIndexObj.addToLucene(field.getField(), field.getValue());
                    }
                }

                currentIndexObj.writeAccessConditions(rootIndexObj);

                // Add grouped metadata as separate documents
                addGroupedMetadataDocs(writeStrategy, currentIndexObj, currentIndexObj.getGroupedMetadataFields(), currentIndexObj.getIddoc());

                // Add own and all ancestor LABEL values to the DEFAULT field
                StringBuilder sbDefaultValue = new StringBuilder();
                sbDefaultValue.append(currentIndexObj.getDefaultValue());
                String labelWithSpaces = new StringBuilder(" ").append(currentIndexObj.getLabel()).append(' ').toString();
                if (StringUtils.isNotEmpty(currentIndexObj.getLabel()) && !sbDefaultValue.toString().contains(labelWithSpaces)) {
                    // logger.info("Adding own LABEL to DEFAULT: {}", indexObj.getLabel()); //NOSONAR Debug
                    sbDefaultValue.append(labelWithSpaces);
                }
                if (SolrIndexerDaemon.getInstance().getConfiguration().isAddLabelToChildren()) {
                    for (String label : currentIndexObj.getParentLabels()) {
                        String parentLabelWithSpaces = new StringBuilder(" ").append(label).append(' ').toString();
                        if (StringUtils.isNotEmpty(label) && !sbDefaultValue.toString().contains(parentLabelWithSpaces)) {
                            // logger.info("Adding ancestor LABEL to DEFAULT: {}", label); //NOSONAR Debug
                            sbDefaultValue.append(parentLabelWithSpaces);
                        }
                    }
                }

                currentIndexObj.setDefaultValue(sbDefaultValue.toString());

                // Add DEFAULT field
                if (StringUtils.isNotEmpty(currentIndexObj.getDefaultValue())) {
                    currentIndexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(currentIndexObj.getDefaultValue()));
                    // Add default value to parent doc
                    currentIndexObj.setDefaultValue("");
                }
            }

            // Add more fields to the page
            currentIndexObj.setNumPages(currentIndexObj.getNumPages() + 1);
            pageDoc.addField(SolrConstants.IDDOC_OWNER, currentIndexObj.getIddoc());
            if (pageDoc.getField(SolrConstants.PI_TOPSTRUCT) == null) {
                pageDoc.addField(SolrConstants.PI_TOPSTRUCT, rootIndexObj.getTopstructPI());
            }
            if (pageDoc.getField(SolrConstants.DATAREPOSITORY) == null && currentIndexObj.getDataRepository() != null) {
                pageDoc.addField(SolrConstants.DATAREPOSITORY, rootIndexObj.getDataRepository());
            }
            if (pageDoc.getField(SolrConstants.DATEUPDATED) == null && !rootIndexObj.getDateUpdated().isEmpty()) {
                for (Long date : rootIndexObj.getDateUpdated()) {
                    pageDoc.addField(SolrConstants.DATEUPDATED, date);
                }
            }
            if (pageDoc.getField(SolrConstants.DATEINDEXED) == null && !rootIndexObj.getDateIndexed().isEmpty()) {
                for (Long date : rootIndexObj.getDateIndexed()) {
                    pageDoc.addField(SolrConstants.DATEINDEXED, date);
                }
            }

            // Add owner docstruct's metadata (tokenized only!) and SORT_* fields to the page
            Set<String> existingMetadataFieldNames = new HashSet<>();
            Set<String> existingSortFieldNames = new HashSet<>();
            for (String fieldName : pageDoc.getFieldNames()) {
                if (SolrIndexerDaemon.getInstance()
                        .getConfiguration()
                        .getMetadataConfigurationManager()
                        .getFieldsToAddToPages()
                        .contains(fieldName)) {
                    for (Object value : pageDoc.getFieldValues(fieldName)) {
                        existingMetadataFieldNames.add(new StringBuilder(fieldName).append(String.valueOf(value)).toString());
                    }
                } else if (fieldName.startsWith(SolrConstants.PREFIX_SORT)) {
                    existingSortFieldNames.add(fieldName);
                }
            }
            for (LuceneField field : currentIndexObj.getLuceneFields()) {
                if (SolrIndexerDaemon.getInstance()
                        .getConfiguration()
                        .getMetadataConfigurationManager()
                        .getFieldsToAddToPages()
                        .contains(field.getField())
                        && !existingMetadataFieldNames.contains(new StringBuilder(field.getField()).append(field.getValue()).toString())) {
                    // Avoid duplicates (same field name + value)
                    pageDoc.addField(field.getField(), field.getValue());
                    logger.debug("Added {}:{} to page {}", field.getField(), field.getValue(), pageDoc.getFieldValue(SolrConstants.ORDER));
                } else if (field.getField().startsWith(SolrConstants.PREFIX_SORT) && !existingSortFieldNames.contains(field.getField())) {
                    // Only one instance of each SORT_ field may exist
                    pageDoc.addField(field.getField(), field.getValue());
                }
            }

            // Make sure IDDOC_OWNER of a page contains the iddoc of the lowest possible mapped docstruct
            if (pageDoc.getField(FIELD_OWNERDEPTH) == null || depth > (Integer) pageDoc.getFieldValue(FIELD_OWNERDEPTH)) {
                pageDoc.setField(SolrConstants.IDDOC_OWNER, String.valueOf(currentIndexObj.getIddoc()));
                pageDoc.setField(FIELD_OWNERDEPTH, depth);

                // Remove SORT_ fields from a previous, higher up docstruct
                Set<String> fieldsToRemove = new HashSet<>();
                for (String fieldName : pageDoc.getFieldNames()) {
                    if (fieldName.startsWith(SolrConstants.PREFIX_SORT)) {
                        fieldsToRemove.add(fieldName);
                    }
                }
                for (String fieldName : fieldsToRemove) {
                    pageDoc.removeField(fieldName);
                }
                //  Add this docstruct's SORT_* fields to page
                if (currentIndexObj.getIddoc() != null && currentIndexObj.getIddoc().equals(pageDoc.getFieldValue(SolrConstants.IDDOC_OWNER))) {
                    for (LuceneField field : currentIndexObj.getLuceneFields()) {
                        if (field.getField().startsWith(SolrConstants.PREFIX_SORT)) {
                            pageDoc.addField(field.getField(), field.getValue());
                        }
                    }
                }
            }

            // Update the doc in the write strategy (otherwise some implementations might ignore the changes).
            // writeStrategy.updatePage(page); TODO use PhysicalElement, if ever needed again
        }

        // Finalize last docstruct
        if (currentIndexObj != null) {
            finalizeChildDocstruct(currentIndexObj, dataFolders, writeStrategy);
        }

        // Set root doc thumbnail
        rootIndexObj.setThumbnailRepresent(thumbnailFile);
        rootIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(thumbnailOrder)), false);
        rootIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBPAGENOLABEL, " - "), false);
        rootIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBNAIL, thumbnailFile), false);
        rootIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBNAILREPRESENT, thumbnailFile), false);

    }

    /**
     * 
     * @param indexObj
     * @param dataFolders
     * @param writeStrategy
     * @throws FatalIndexerException
     */
    private void finalizeChildDocstruct(IndexObject indexObj, Map<String, Path> dataFolders, ISolrWriteStrategy writeStrategy)
            throws FatalIndexerException {
        if (indexObj.getNumPages() > 0) {
            // Write number of pages and first/last page labels
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

        // Write docstruct doc to Solr
        logger.debug("Writing child document '{}'...", indexObj.getIddoc());
        writeStrategy.addDoc(SolrSearchIndex.createDocument(indexObj.getLuceneFields()));
    }

    /**
     * Generates a SolrInputDocument for each page that is mapped to a docstruct. Adds all page metadata except those that come from the owning
     * docstruct (such as docstruct iddoc, type, collection, etc.).
     *
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @param dataFolders a {@link java.util.Map} object.
     * @param pi
     * @param pageCountStart a int.
     * @throws InterruptedException
     * @should create documents for all mapped pages
     * @should set correct ORDER values
     * @should skip unmapped pages
     * @should switch to DEFAULT file group correctly
     * @should maintain page order after parallel processing
     */
    public void generatePageDocuments(final ISolrWriteStrategy writeStrategy, final Map<String, Path> dataFolders, final String pi,
            int pageCountStart) throws InterruptedException {
        // Get all physical elements
        List<Element> eleListImages = xp.evaluateToElements("worldviews/resource/images/image", null);
        logger.info("Generating {} page documents (count starts at {})...", eleListImages.size(), pageCountStart);

        if (SolrIndexerDaemon.getInstance().getConfiguration().getThreads() > 1) {
            // Generate each page document in its own thread

            ConcurrentHashMap<String, Boolean> map = new ConcurrentHashMap<>();
            try (ForkJoinPool pool = new ForkJoinPool(SolrIndexerDaemon.getInstance().getConfiguration().getThreads())) {
                pool.submit(() -> eleListImages.parallelStream().forEach(eleImage -> {
                    String iddoc = getNextIddoc();
                    if (map.containsKey(iddoc)) {
                        logger.error("Duplicate IDDOC: {}", iddoc);
                    }
                    PhysicalElement page = generatePageDocument(eleImage, String.valueOf(iddoc), pi, null, dataFolders);
                    if (page != null) {
                        writeStrategy.addPage(page);
                        map.put(iddoc, true);
                    }
                })).get(GENERATE_PAGE_DOCUMENT_TIMEOUT_HOURS, TimeUnit.HOURS);
            } catch (ExecutionException e) {
                logger.error(e.getMessage(), e);
                SolrIndexerDaemon.getInstance().stop();
            } catch (TimeoutException e) {
                throw new InterruptedException("Generating page documents timed out for object " + pi);
            }
        } else {
            int order = pageCountStart;
            for (final Element eleImage : eleListImages) {
                PhysicalElement page = generatePageDocument(eleImage, String.valueOf(getNextIddoc()), pi, order, dataFolders);
                if (page != null) {
                    writeStrategy.addPage(page);
                    order++;
                }
            }
        }
        logger.info("Generated {} page documents.", writeStrategy.getPageDocsSize());
    }

    /**
     * 
     * @param eleImage
     * @param iddoc
     * @param pi
     * @param order
     * @param dataFolders
     * @return {@link PhysicalElement}
     * @should add all basic fields
     * @should add page metadata correctly
     */
    PhysicalElement generatePageDocument(Element eleImage, String iddoc, String pi, final Integer order, Map<String, Path> dataFolders) {
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null");
        }

        int useOrder = order != null ? order : Integer.parseInt(eleImage.getChildText("sequence"));
        logger.trace("generatePageDocument: {} (IDDOC {}) processed by thread {}", useOrder, iddoc, Thread.currentThread().threadId());

        // Create object for this page
        PhysicalElement ret = createPhysicalElement(order, iddoc, "PHYS_" + MetadataHelper.FORMAT_FOUR_DIGITS.get().format(useOrder));
        ret.getDoc().addField(SolrConstants.ORDERLABEL, SolrIndexerDaemon.getInstance().getConfiguration().getEmptyOrderLabelReplacement());

        boolean displayImage = Boolean.parseBoolean(eleImage.getChildText("displayImage"));
        if (displayImage) {
            // Add file name
            String fileName = eleImage.getChildText("fileName");
            ret.getDoc().addField(SolrConstants.FILENAME, fileName);

            // Add file size
            addFileSizeToDoc(ret.getDoc(), dataFolders.get(DataRepository.PARAM_MEDIA), fileName);

            // Representative image
            boolean representative = Boolean.parseBoolean(eleImage.getChildText("representative"));
            if (representative) {
                ret.getDoc().addField(SolrConstants.THUMBNAILREPRESENT, fileName);
            }

            parseMimeType(ret.getDoc(), fileName);
        }

        // FIELD_IMAGEAVAILABLE indicates whether this page has an image
        if (ret.getDoc().containsKey(SolrConstants.FILENAME) && ret.getDoc().containsKey(SolrConstants.MIMETYPE)
                && ((String) ret.getDoc().getFieldValue(SolrConstants.MIMETYPE)).startsWith("image")) {
            ret.getDoc().addField(FIELD_IMAGEAVAILABLE, true);
            recordHasImages = true;
        } else {
            ret.getDoc().addField(FIELD_IMAGEAVAILABLE, false);
        }

        // Copyright
        String copyright = eleImage.getChildText("copyright");
        if (StringUtils.isNotEmpty(copyright)) {
            ret.getDoc().addField("MD_COPYRIGHT", copyright);
        }
        // access condition
        String license = eleImage.getChildText("licence");
        if (StringUtils.isNotEmpty(license)) {
            ret.getDoc().addField(SolrConstants.ACCESSCONDITION, license);
        }

        // Metadata payload for later evaluation
        String structType = eleImage.getChildText("structType");
        if (structType != null) {
            ret.getDoc().addField(SolrConstants.LABEL, structType);
            ret.getDoc().addField(SolrConstants.DOCSTRCT, "OtherDocStrct"); // TODO
        }

        if (new FulltextAugmentor(dataRepository).addFullTextToPageDoc(ret.getDoc(), dataFolders, pi, useOrder, null)) {
            this.recordHasFulltext = true;
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
    static void copyAndReIndexAnchor(IndexObject indexObj, Hotfolder hotfolder, DataRepository dataRepository) {
        logger.debug("copyAndReIndexAnchor: {}", indexObj.getPi());
        if (indexObj.getParent() != null) {
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
        } else {
            logger.warn("No anchor file has been indexed for this work yet.");
        }
    }

    @Override
    protected FileFormat getSourceDocFormat() {
        return FileFormat.WORLDVIEWS;
    }
}
