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
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * Indexer implementation for Goobi viewer-generated Dublin Core documents.
 */
public class DublinCoreIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(DublinCoreIndexer.class);

    private static final String[] DATA_FOLDER_PARAMS =
            { DataRepository.PARAM_MEDIA, DataRepository.PARAM_FULLTEXT, DataRepository.PARAM_FULLTEXTCROWD, DataRepository.PARAM_ABBYY,
                    DataRepository.PARAM_TEIWC, DataRepository.PARAM_ALTO, DataRepository.PARAM_ALTOCROWD, DataRepository.PARAM_MIX,
                    DataRepository.PARAM_UGC, DataRepository.PARAM_CMS, DataRepository.PARAM_TEIMETADATA, DataRepository.PARAM_ANNOTATIONS };

    /**
     * Constructor.
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public DublinCoreIndexer(Hotfolder hotfolder) {
        super();
        this.hotfolder = hotfolder;
    }

    /**
     * Indexes the given DublinCore file.
     * 
     * @see io.goobi.viewer.indexer.Indexer#addToIndex(java.nio.file.Path, java.util.Map)
     */
    @Override
    public void addToIndex(Path dcFile, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        String fileNameRoot = FilenameUtils.getBaseName(dcFile.getFileName().toString());

        // Check data folders in the hotfolder
        Map<String, Path> dataFolders = checkDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

        // Use existing folders for those missing in the hotfolder
        checkReindexSettings(dataFolders, reindexSettings);

        String[] resp = index(dcFile, dataFolders, null, SolrIndexerDaemon.getInstance().getConfiguration().getPageCountStart());
        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            String newDcFileName = resp[0];
            String pi = FilenameUtils.getBaseName(newDcFileName);
            Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_DUBLINCORE).toAbsolutePath().toString(), newDcFileName);
            if (dcFile.equals(indexed)) {
                return;
            }
            Files.copy(dcFile, indexed, StandardCopyOption.REPLACE_EXISTING);
            dataRepository.checkOtherRepositoriesForRecordFileDuplicates(newDcFileName, DataRepository.PARAM_INDEXED_DUBLINCORE,
                    hotfolder.getDataRepositoryStrategy().getAllDataRepositories());

            if (previousDataRepository != null) {
                // Move non-repository data folders to the selected repository
                previousDataRepository.moveDataFoldersToRepository(dataRepository, FilenameUtils.getBaseName(newDcFileName));
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

            try {
                Files.delete(dcFile);
            } catch (IOException e) {
                logger.warn(LOG_COULD_NOT_BE_DELETED, dcFile.toAbsolutePath());
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
            logger.info("Successfully finished indexing '{}'.", dcFile.getFileName());

            // Remove this file from lower priority hotfolders to avoid overriding changes with older version
            SolrIndexerDaemon.getInstance().removeRecordFileFromLowerPriorityHotfolders(pi, hotfolder);
        } else {
            // Error
            if (hotfolder.isDeleteContentFilesOnFailure()) {
                // Delete all data folders for this record from the hotfolder
                DataRepository.deleteDataFoldersFromHotfolder(dataFolders, reindexSettings);
            }
            handleError(dcFile, resp[1], FileFormat.DUBLINCORE);
            try {
                Files.delete(dcFile);
            } catch (IOException e) {
                logger.error(LOG_COULD_NOT_BE_DELETED, dcFile.toAbsolutePath());
            }
        }
    }

    /**
     * Indexes the given Dublin Core file.
     *
     * @param dcFile {@link java.nio.file.Path}
     * @param dataFolders a {@link java.util.Map} object.
     * @param pageCountStart Order number for the first page.
     * @should index record correctly
     * @should index metadata groups correctly
     * @should index multi volume records correctly
     * @should update record correctly
     * @should set access conditions correctly
     * @should write cms page texts into index
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @return an array of {@link java.lang.String} objects.
     */
    public String[] index(Path dcFile, Map<String, Path> dataFolders, final ISolrWriteStrategy writeStrategy, int pageCountStart) {
        String[] ret = { null, null };

        if (dcFile == null || !Files.exists(dcFile)) {
            throw new IllegalArgumentException("dcfile must point to an existing Dublin Core file.");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null.");
        }

        logger.debug("Indexing Dublin Core file '{}'...", dcFile.getFileName());
        ISolrWriteStrategy useWriteStrategy = writeStrategy;
        try {
            if (useWriteStrategy == null) {
                // Request appropriate write strategy
                useWriteStrategy = AbstractWriteStrategy.create(dcFile, dataFolders, hotfolder);
            } else {
                logger.info("Solr write strategy injected by caller: {}", useWriteStrategy.getClass().getName());
            }
            initJDomXP(dcFile);
            IndexObject indexObj = new IndexObject(getNextIddoc());
            logger.debug("IDDOC: {}", indexObj.getIddoc());
            indexObj.setRootStructNode(xp.getRootElement());

            // set some simple data in den indexObject
            setSimpleData(indexObj);
            setUrn(indexObj);

            // Set PI
            String pi = validateAndApplyPI(findPI("/record/"), indexObj, false);

            // Determine the data repository to use
            selectDataRepository(indexObj, pi, dcFile, dataFolders);

            ret[0] = new StringBuilder(indexObj.getPi()).append(FileTools.XML_EXTENSION).toString();

            // Check and use old data folders, if no new ones found
            checkOldDataFolders(dataFolders, DATA_FOLDER_PARAMS, pi);

            prepareUpdate(indexObj);

            // Process TEI files
            if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) != null) {
                MetadataHelper.processTEIMetadataFiles(indexObj, dataFolders.get(DataRepository.PARAM_TEIMETADATA));
            }

            // put some simple data to Lucene array
            indexObj.pushSimpleDataToLuceneArray();

            // Write metadata relative to the mdWrap
            MetadataHelper.writeMetadataToObject(indexObj, xp.getMdWrap(indexObj.getDmdid()), "", xp);

            // Write root metadata (outside of MODS sections)
            MetadataHelper.writeMetadataToObject(indexObj, xp.getRootElement(), "", xp);

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // Write created/updated timestamps
            indexObj.writeDateModified(true);

            // Generate docs for all pages and add to the write strategy
            generatePageDocuments(useWriteStrategy, dataFolders, dataRepository, indexObj.getPi(), pageCountStart);

            // If images have been found for any page, set a boolean in the root doc indicating that the record does have images
            indexObj.addToLucene(FIELD_IMAGEAVAILABLE, String.valueOf(recordHasImages));

            // If full-text has been indexed for any page, set a boolean in the root doc indicating that the record does have full-text
            indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, String.valueOf(recordHasFulltext));

            // Add THUMBNAIL,THUMBPAGENO,THUMBPAGENOLABEL (must be done AFTER writeDateMondified(),
            // writeAccessConditions() and generatePageDocuments()!)
            List<LuceneField> thumbnailFields = mapPagesToDocstruct(indexObj, useWriteStrategy);
            if (thumbnailFields != null) {
                indexObj.getLuceneFields().addAll(thumbnailFields);
            }

            indexObj.addToLucene(SolrConstants.ISWORK, "true");
            logger.trace("ISWORK: {}", indexObj.getLuceneFieldWithName(SolrConstants.ISWORK).getValue());

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
                            && (field.getField().startsWith("MD_") || field.getField().startsWith("MD2_") || field.getField().startsWith("MDNUM_"))) {
                        // Add any MD_*_GROUPSUFFIX field to the group doc
                        moreMetadata.put(field.getField().replace("_" + groupSuffix, ""), field.getValue());
                    }
                }
                SolrInputDocument doc = SolrIndexerDaemon.getInstance()
                        .getSearchIndex()
                        .checkAndCreateGroupDoc(groupIdField, indexObj.getGroupIds().get(groupIdField), moreMetadata, getNextIddoc());
                if (doc != null) {
                    useWriteStrategy.addDoc(doc);
                    logger.debug("Created group document for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                } else {
                    logger.debug("Group document already exists for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                }
            }

            // Add grouped metadata as separate documents
            addGroupedMetadataDocs(useWriteStrategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

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
                writeUserGeneratedContents(useWriteStrategy, dataFolders, indexObj);
            }

            SolrInputDocument rootDoc = SolrSearchIndex.createDocument(indexObj.getLuceneFields());
            useWriteStrategy.setRootDoc(rootDoc);

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)
            logger.debug("Writing document to index...");
            useWriteStrategy.writeDocs(SolrIndexerDaemon.getInstance().getConfiguration().isAggregateRecords());
            logger.info("Finished writing data for '{}' to Solr.", pi);
        } catch (IOException | IndexerException | FatalIndexerException | JDOMException | SolrServerException e) {
            logger.error("Indexing of '{}' could not be finished due to an error.", dcFile.getFileName());
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
     * 
     * @param indexObj
     * @param writeStrategy
     * @return List<LuceneField>
     * @throws FatalIndexerException
     */
    private static List<LuceneField> mapPagesToDocstruct(IndexObject indexObj, ISolrWriteStrategy writeStrategy) throws FatalIndexerException {
        List<String> physIds = new ArrayList<>(writeStrategy.getPageDocsSize());
        for (int i = 1; i <= writeStrategy.getPageDocsSize(); ++i) {
            physIds.add(String.valueOf(i));
        }
        List<PhysicalElement> pages = writeStrategy.getPagesForPhysIdList(physIds);
        if (pages.isEmpty()) {
            logger.warn("No pages found for {}", indexObj.getLogId());
            return Collections.emptyList();
        }

        List<LuceneField> ret = new ArrayList<>(5);

        PhysicalElement firstPage = pages.get(0);
        if (firstPage != null) {
            // Add thumbnail information from the first page
            String thumbnailFileName = (String) firstPage.getDoc().getFieldValue(SolrConstants.FILENAME);
            ret.add(new LuceneField(SolrConstants.THUMBNAIL, thumbnailFileName));
            if (DocType.SHAPE.name().equals(firstPage.getDoc().getFieldValue(SolrConstants.DOCTYPE))) {
                ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPage.getDoc().getFieldValue("ORDER_PARENT"))));
            } else {
                ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPage.getDoc().getFieldValue(SolrConstants.ORDER))));
            }
            ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) firstPage.getDoc().getFieldValue(SolrConstants.ORDERLABEL)));
            ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) firstPage.getDoc().getFieldValue(SolrConstants.MIMETYPE)));
        }

        // If this is a top struct element, look for a representative image
        for (PhysicalElement page : pages) {
            String pageFileName = page.getDoc().getField(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED) != null
                    ? (String) page.getDoc().getFieldValue(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED)
                    : (String) page.getDoc().getFieldValue(SolrConstants.FILENAME);
            String pageFileBaseName = FilenameUtils.getBaseName(pageFileName);

            // Make sure IDDOC_OWNER of a page contains the iddoc of the lowest possible mapped docstruct
            if (page.getDoc().getField(FIELD_OWNERDEPTH) == null || 0 > (Integer) page.getDoc().getFieldValue(FIELD_OWNERDEPTH)) {
                page.getDoc().setField(SolrConstants.IDDOC_OWNER, String.valueOf(indexObj.getIddoc()));
                page.getDoc().setField(FIELD_OWNERDEPTH, 0);

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
                if (indexObj.getIddoc() != null && indexObj.getIddoc().equals(page.getDoc().getFieldValue(SolrConstants.IDDOC_OWNER))) {
                    for (LuceneField field : indexObj.getLuceneFields()) {
                        if (field.getField().startsWith(SolrConstants.PREFIX_SORT)) {
                            page.getDoc().addField(field.getField(), field.getValue());
                        }
                    }
                }
            }

            if (page.getDoc().getField(SolrConstants.PI_TOPSTRUCT) == null) {
                page.getDoc().addField(SolrConstants.PI_TOPSTRUCT, indexObj.getTopstructPI());
            }
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
                if (!existingAccessConditions.contains(s)) {
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

            // Update the doc in the write strategy (otherwise some implementations might ignore the changes).
            writeStrategy.updatePage(page);
        }

        // Add the number of assigned pages and the labels of the first and last page to this structure element
        indexObj.setNumPages(pages.size());
        if (!pages.isEmpty()) {
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
     * Generates a SolrInputDocument for each page that is mapped to a docstruct. Adds all page metadata except those that come from the owning
     * docstruct (such as docstruct iddoc, type, collection, etc.).
     *
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @param dataFolders a {@link java.util.Map} object.
     * @param dataRepository a {@link io.goobi.viewer.indexer.model.datarepository.DataRepository} object.
     * @param pi a {@link java.lang.String} object.
     * @param pageCountStart a int.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should create documents for all mapped pages
     * @should set correct ORDER values
     * @should skip unmapped pages
     * @should switch to DEFAULT file group correctly
     * @should maintain page order after parallel processing
     */
    public void generatePageDocuments(final ISolrWriteStrategy writeStrategy, final Map<String, Path> dataFolders,
            final DataRepository dataRepository, final String pi, int pageCountStart) throws FatalIndexerException {
        // Get all physical elements
        String xpath = "/record/dc:relation";
        List<Element> eleImageList = xp.evaluateToElements(xpath, null);
        logger.info("Generating {} page documents (count starts at {})...", eleImageList.size(), pageCountStart);

        // Generate pages sequentially
        int order = pageCountStart;
        for (final Element eleImage : eleImageList) {
            PhysicalElement page = generatePageDocument(eleImage, String.valueOf(getNextIddoc()), pi, order, dataFolders);
            if (page != null) {
                writeStrategy.addPage(page);
                order++;
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
     * @throws FatalIndexerException
     */
    PhysicalElement generatePageDocument(Element eleImage, String iddoc, String pi, Integer order, Map<String, Path> dataFolders) {
        if (eleImage == null) {
            throw new IllegalArgumentException("eleImage may not be null");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null");
        }
        if (order == null) {
            // TODO page order within the metadata
            order = 1;
        }

        // Create object for this page
        PhysicalElement ret = createPhysicalElement(order, iddoc, String.valueOf(order));
        ret.getDoc().addField(SolrConstants.ORDERLABEL, String.valueOf(order));

        // URL
        String fileName = eleImage.getText();

        // Mime type
        parseMimeType(ret.getDoc(), fileName);

        // Add file size
        addFileSizeToDoc(ret.getDoc(), dataFolders.get(DataRepository.PARAM_MEDIA), fileName);

        // Add image dimension values from EXIF
        if (!ret.getDoc().containsKey(SolrConstants.WIDTH) || !ret.getDoc().containsKey(SolrConstants.HEIGHT)) {
            getSize(dataFolders.get(DataRepository.PARAM_MEDIA), (String) ret.getDoc().getFieldValue(SolrConstants.FILENAME)).ifPresent(dimension -> {
                ret.getDoc().addField(SolrConstants.WIDTH, dimension.width);
                ret.getDoc().addField(SolrConstants.HEIGHT, dimension.height);
            });
        }

        // FIELD_IMAGEAVAILABLE indicates whether this page has an image
        if (ret.getDoc().containsKey(SolrConstants.FILENAME) && ret.getDoc().containsKey(SolrConstants.MIMETYPE)
                && ((String) ret.getDoc().getFieldValue(SolrConstants.MIMETYPE)).startsWith("image")) {
            ret.getDoc().addField(FIELD_IMAGEAVAILABLE, true);
            recordHasImages = true;
        } else {
            ret.getDoc().addField(FIELD_IMAGEAVAILABLE, false);
        }

        addFullTextToPageDoc(ret.getDoc(), dataFolders, dataRepository, pi, order, null);

        return ret;
    }

    /**
     * Sets DMDID, ID, TYPE and LABEL from the METS document.
     * 
     * @param indexObj {@link IndexObject}
     */
    private static void setSimpleData(IndexObject indexObj) {
        logger.trace("setSimpleData(IndexObject) - start");

        indexObj.setSourceDocFormat(FileFormat.DUBLINCORE);

        // LOGID
        indexObj.setLogId("LOD_0000");
        logger.trace("LOGID: {}", indexObj.getLogId());

        // TYPE
        indexObj.setType("record");
        logger.trace("TYPE: {}", indexObj.getType());

        // LABEL
        String value = TextHelper
                .normalizeSequence(indexObj.getRootStructNode()
                        .getChildText("title", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("dc")));
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
     * Retrieves and sets the URN for mets:structMap[@TYPE='LOGICAL'] elements.
     * 
     * @param indexObj
     * @return The set URN value
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
     * Moves an updated anchor METS file to the indexed METS folder and the previous version to the updated_mets folder without doing any index
     * operations.
     *
     * @param metsFile {@link java.nio.file.Path} z.B.: PPN1234567890.UPDATED
     * @param updatedMetsFolder Updated METS folder for old METS files.
     * @param dataRepository Data repository to which to copy the new file.
     * @throws java.io.IOException in case of errors.
     * @should copy new METS file correctly
     * @should copy old METS file to updated mets folder if file already exists
     */
    public static void superupdate(Path metsFile, Path updatedMetsFolder, DataRepository dataRepository) throws IOException {
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
                        .append(LocalDate.now().format(DateTools.formatterBasicDateTime))
                        .append(".xml")
                        .toString();
                Files.move(indexed, Paths.get(updatedMetsFolder.toAbsolutePath().toString(), oldMetsFilename));
                logger.debug("Old anchor file copied to '{}{}{}'.", updatedMetsFolder.toAbsolutePath(), File.separator, oldMetsFilename);
                // Then copy the new file again, overwriting the old
                Files.move(Paths.get(metsFile.toAbsolutePath().toString()), indexed, StandardCopyOption.REPLACE_EXISTING);
            }
            logger.info("New anchor file copied to '{}'.", indexed.toAbsolutePath());
        }
    }

    @Override
    protected FileFormat getSourceDocFormat() {
        return FileFormat.DUBLINCORE;
    }
}
