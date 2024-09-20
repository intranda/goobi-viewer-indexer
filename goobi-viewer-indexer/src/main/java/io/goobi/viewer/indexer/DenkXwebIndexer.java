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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.XMLOutputter;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.PhysicalElement;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.config.MetadataConfigurationManager;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * <p>
 * DenkXwebIndexer class.
 * </p>
 *
 */
public class DenkXwebIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(DenkXwebIndexer.class);

    private static final String[] DATA_FOLDER_PARAMS =
            { DataRepository.PARAM_MIX, DataRepository.PARAM_UGC, DataRepository.PARAM_CMS, DataRepository.PARAM_TEIMETADATA,
                    DataRepository.PARAM_ANNOTATIONS };

    /**
     * Whitelist of file names belonging for this particular record (in case the media folder contains files for multiple records). StringBuffer is
     * thread-safe.
     */
    private StringBuilder sbImgFileNames = new StringBuilder();

    /**
     * Constructor.
     * 
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public DenkXwebIndexer(Hotfolder hotfolder) {
        super();
        this.hotfolder = hotfolder;
    }

    /**
     * Indexes the given DenkXweb file.
     * 
     * @see io.goobi.viewer.indexer.Indexer#addToIndex(java.nio.file.Path, boolean, java.util.Map)
     * @should throw IllegalArgumentException if denkxwebFile null
     */
    @Override
    public void addToIndex(Path denkxwebFile, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        if (denkxwebFile == null) {
            throw new IllegalArgumentException("denkxwebFile may not be null");
        }

        logger.debug("Indexing DenkXweb file '{}'...", denkxwebFile.getFileName());
        String[] resp = { null, null };
        String fileNameRoot = FilenameUtils.getBaseName(denkxwebFile.getFileName().toString());

        // Check data folders in the hotfolder
        Map<String, Path> dataFolders = checkDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

        if (dataFolders.containsKey(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER)) {
            logger.info("External images will be downloaded.");
            Path newMediaFolder = Paths.get(hotfolder.getHotfolderPath().toString(), fileNameRoot + "_tif");
            dataFolders.put(DataRepository.PARAM_MEDIA, newMediaFolder);
            if (!Files.exists(newMediaFolder)) {
                Files.createDirectory(newMediaFolder);
                logger.info("Created media folder {}", newMediaFolder.toAbsolutePath());
            }
        }

        // Use existing folders for those missing in the hotfolder
        checkReindexSettings(dataFolders, reindexSettings);

        List<Document> denkxwebDocs = JDomXP.splitDenkXwebFile(denkxwebFile.toFile());
        logger.info("File contains {} DenkXweb documents.", denkxwebDocs.size());
        XMLOutputter outputter = new XMLOutputter();
        for (Document doc : denkxwebDocs) {
            resp = index(doc, dataFolders, null, SolrIndexerDaemon.getInstance().getConfiguration().getPageCountStart(),
                    dataFolders.containsKey(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER));
            if (!Indexer.STATUS_ERROR.equals(resp[0])) {
                String identifier = resp[0];
                String newDenkXwebFileName = identifier + ".xml";

                // Write individual LIDO records as separate files
                Path indexed =
                        Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_DENKXWEB).toAbsolutePath().toString(), newDenkXwebFileName);
                try (FileOutputStream out = new FileOutputStream(indexed.toFile())) {
                    outputter.output(doc, out);
                }
                dataRepository.checkOtherRepositoriesForRecordFileDuplicates(newDenkXwebFileName, DataRepository.PARAM_INDEXED_DENKXWEB,
                        hotfolder.getDataRepositoryStrategy().getAllDataRepositories());

                // Move non-repository data directories to the selected repository
                if (previousDataRepository != null) {
                    previousDataRepository.moveDataFoldersToRepository(dataRepository, identifier);
                }

                // Copy media files
                int imageCounter = dataRepository.copyImagesFromMultiRecordMediaFolder(dataFolders.get(DataRepository.PARAM_MEDIA), identifier,
                        denkxwebFile.getFileName().toString(), hotfolder.getDataRepositoryStrategy(), resp[1],
                        reindexSettings.get(DataRepository.PARAM_MEDIA) != null && reindexSettings.get(DataRepository.PARAM_MEDIA));
                if (imageCounter > 0) {
                    String msg = Utils.removeRecordImagesFromCache(identifier);
                    if (msg != null) {
                        logger.info(msg);
                    }
                }

                // Update data repository cache map in the Goobi viewer
                if (previousDataRepository != null) {
                    try {
                        Utils.updateDataRepositoryCache(identifier, dataRepository.getPath());
                    } catch (HTTPException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
                prerenderPagePdfsIfRequired(identifier, dataFolders.get(DataRepository.PARAM_MEDIA) != null);
                logger.info("Successfully finished indexing '{}'.", identifier);

                // Remove this file from lower priority hotfolders to avoid overriding changes with older version
                SolrIndexerDaemon.getInstance().removeRecordFileFromLowerPriorityHotfolders(identifier, hotfolder);
            } else {
                handleError(denkxwebFile, resp[1], FileFormat.DENKXWEB);
            }
        }

        // Copy original DenkXweb file into the orig folder
        Path orig = Paths.get(hotfolder.getOrigDenkxWeb().toAbsolutePath().toString(), denkxwebFile.getFileName().toString());
        Files.copy(denkxwebFile, orig, StandardCopyOption.REPLACE_EXISTING);

        // Delete files from the hotfolder
        try {
            Files.delete(denkxwebFile);
        } catch (IOException e) {
            logger.error("'{}' could not be deleted; please delete it manually.", denkxwebFile.toAbsolutePath());
        }
        // Delete all data folders for this record from the hotfolder
        DataRepository.deleteDataFoldersFromHotfolder(dataFolders, reindexSettings);
        logger.info("Finished indexing DenkXweb file '{}'.", denkxwebFile.getFileName());
    }

    /**
     * Indexes a DenkXweb file.
     * 
     * @param doc a {@link org.jdom2.Document} object.
     * @param dataFolders a {@link java.util.Map} object.
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @param pageCountStart a int.
     * @param downloadExternalImages
     * @return an array of {@link java.lang.String} objects.
     * @should index record correctly
     * @should update record correctly
     */
    public String[] index(Document doc, Map<String, Path> dataFolders, ISolrWriteStrategy writeStrategy, int pageCountStart,
            boolean downloadExternalImages) {
        String[] ret = { STATUS_ERROR, null };
        String pi = null;
        try {
            this.xp = new JDomXP(doc);
            if (this.xp == null) {
                throw new IndexerException("Could not create XML parser.");
            }

            IndexObject indexObj = new IndexObject(getNextIddoc(SolrIndexerDaemon.getInstance().getSearchIndex()));
            logger.debug("IDDOC: {}", indexObj.getIddoc());
            Element structNode = doc.getRootElement();
            indexObj.setRootStructNode(structNode);

            // set some simple data in den indexObject
            setSimpleData(indexObj);

            // Set PI
            pi = validateAndApplyPI(findPI(""), indexObj, true);

            // Determine the data repository to use
            selectDataRepository(indexObj, pi, null, dataFolders);

            ret[0] = indexObj.getPi();

            // Check and use old data folders, if no new ones found
            checkOldDataFolders(dataFolders, DATA_FOLDER_PARAMS, pi);

            if (writeStrategy == null) {
                // Request appropriate write strategy
                writeStrategy = AbstractWriteStrategy.create(null, dataFolders, hotfolder);
            } else {
                logger.info("Solr write strategy injected by caller: {}", writeStrategy.getClass().getName());
            }

            prepareUpdate(indexObj);

            // Process TEI files
            if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) != null) {
                MetadataHelper.processTEIMetadataFiles(indexObj, dataFolders.get(DataRepository.PARAM_TEIMETADATA));
            }

            indexObj.pushSimpleDataToLuceneArray();
            MetadataHelper.writeMetadataToObject(indexObj, indexObj.getRootStructNode(), "", xp);

            // Add LABEL value
            if (StringUtils.isEmpty(indexObj.getLabel())) {
                LuceneField field = indexObj.getLuceneFieldWithName("MD_TITLE");
                if (field != null) {
                    indexObj.addToLucene(SolrConstants.LABEL, MetadataHelper.applyValueDefaultModifications(field.getValue()));
                }
            }

            // Generate pages
            generatePageDocuments(writeStrategy, dataFolders, pageCountStart, downloadExternalImages);

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // Add THUMBNAIL,THUMBPAGENO,THUMBPAGENOLABEL (must be done AFTER writeDateMondified(), writeAccessConditions() and generatePageDocuments()!)
            List<LuceneField> thumbnailFields = mapPagesToDocstruct(indexObj, writeStrategy);
            if (thumbnailFields != null) {
                indexObj.getLuceneFields().addAll(thumbnailFields);
            }

            // ISWORK only for non-anchors
            indexObj.addToLucene(SolrConstants.ISWORK, "true");
            logger.trace("ISWORK: {}", indexObj.getLuceneFieldWithName(SolrConstants.ISWORK).getValue());

            if (indexObj.getNumPages() > 0) {
                // Write number of pages
                indexObj.addToLucene(SolrConstants.NUMPAGES, String.valueOf(writeStrategy.getPageDocsSize()));

                // Add used-generated content docs
                writeUserGeneratedContents(writeStrategy, dataFolders, indexObj);
            }

            // Write created/updated timestamps
            indexObj.writeDateModified(true);

            // If images have been found for any page, set a boolean in the root doc indicating that the record does have images
            indexObj.addToLucene(FIELD_IMAGEAVAILABLE, String.valueOf(recordHasImages));

            // If full-text has been indexed for any page, set a boolean in the root doc indicating that the record does have full-text
            indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, String.valueOf(recordHasFulltext));

            // Add DEFAULT field
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(indexObj.getDefaultValue()));
                indexObj.setDefaultValue("");
            }

            // Add grouped metadata as separate documents
            addGroupedMetadataDocs(writeStrategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

            // Add root doc
            SolrInputDocument rootDoc = SolrSearchIndex.createDocument(indexObj.getLuceneFields());
            writeStrategy.setRootDoc(rootDoc);

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)
            logger.debug("Writing document to index...");
            writeStrategy.writeDocs(SolrIndexerDaemon.getInstance().getConfiguration().isAggregateRecords());

            // Return image file names
            if (sbImgFileNames.length() > 0 && sbImgFileNames.charAt(0) == ';') {
                sbImgFileNames.deleteCharAt(0);
            }
            ret[1] = sbImgFileNames.toString();
            logger.info("Finished writing data for '{}' to Solr.", pi);
        } catch (Exception e) {
            if ("No image resource sets found.".equals(e.getMessage())) {
                logger.error("Indexing of '{}' could not be finished due to an error: {}", pi, e.getMessage());
            } else {
                logger.error("Indexing of '{}' could not be finished due to an error.", pi);
                logger.error(e.getMessage(), e);
            }
            ret[0] = STATUS_ERROR;
            ret[1] = e.getMessage();
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

        // If this is a top struct element, look for a representative image
        String filePathBanner = null;
        boolean thumbnailSet = false;
        for (PhysicalElement page : pages) {
            String pageFileName = page.getDoc().getField(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED) != null
                    ? (String) page.getDoc().getFieldValue(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED)
                    : (String) page.getDoc().getFieldValue(SolrConstants.FILENAME);
            String pageFileBaseName = FilenameUtils.getBaseName(pageFileName);

            if (page.getDoc().containsKey(SolrConstants.THUMBNAILREPRESENT)) {
                filePathBanner = (String) page.getDoc().getFieldValue(SolrConstants.THUMBNAILREPRESENT);
            }

            // Add thumbnail information from the representative page
            if (!thumbnailSet && StringUtils.isNotEmpty(filePathBanner) && pageFileName.equals(filePathBanner)) {
                ret.add(new LuceneField(SolrConstants.THUMBNAIL, pageFileName));
                // THUMBNAILREPRESENT is just used to identify the presence of a custom representation thumbnail to the indexer, it is not used in the viewer
                ret.add(new LuceneField(SolrConstants.THUMBNAILREPRESENT, pageFileName));
                ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(page.getDoc().getFieldValue(SolrConstants.ORDER))));
                ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) page.getDoc().getFieldValue(SolrConstants.ORDERLABEL)));
                ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) page.getDoc().getFieldValue(SolrConstants.MIMETYPE)));
                thumbnailSet = true;
            }

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
                if (indexObj.getIddoc() == Long.valueOf((String) page.getDoc().getFieldValue(SolrConstants.IDDOC_OWNER))) {
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

        PhysicalElement firstPage = pages.get(0);

        // If a representative image is set but not mapped to any docstructs, do not use it
        if (!thumbnailSet && StringUtils.isNotEmpty(filePathBanner) && !pages.isEmpty()) {
            logger.warn("Selected representative image '{}' is not mapped to any structure element - using first mapped image instead.",
                    filePathBanner);
            String pageFileName = firstPage.getDoc().getField(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED) != null
                    ? (String) firstPage.getDoc().getFieldValue(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED)
                    : (String) firstPage.getDoc().getFieldValue(SolrConstants.FILENAME);
            ret.add(new LuceneField(SolrConstants.THUMBNAIL, pageFileName));
            // THUMBNAILREPRESENT is just used to identify the presence of a custom representation thumbnail to the indexer, it is not used in the viewer
            ret.add(new LuceneField(SolrConstants.THUMBNAILREPRESENT, pageFileName));
            ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPage.getDoc().getFieldValue(SolrConstants.ORDER))));
            ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) firstPage.getDoc().getFieldValue(SolrConstants.ORDERLABEL)));
            ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) firstPage.getDoc().getFieldValue(SolrConstants.MIMETYPE)));
        }

        // Add thumbnail information from the first page
        if (StringUtils.isEmpty(filePathBanner) && SolrIndexerDaemon.getInstance().getConfiguration().isUseFirstPageAsDefaultRepresentative()) {
            String thumbnailFileName = firstPage.getDoc().getField(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED) != null
                    ? (String) firstPage.getDoc().getFieldValue(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED)
                    : (String) firstPage.getDoc().getFieldValue(SolrConstants.FILENAME);
            ret.add(new LuceneField(SolrConstants.THUMBNAIL, thumbnailFileName));
            ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPage.getDoc().getFieldValue(SolrConstants.ORDER))));
            ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) firstPage.getDoc().getFieldValue(SolrConstants.ORDERLABEL)));
            ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) firstPage.getDoc().getFieldValue(SolrConstants.MIMETYPE)));
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
     * Sets TYPE and LABEL from the LIDO document.
     * 
     * @param indexObj {@link IndexObject}
     */
    private static void setSimpleData(IndexObject indexObj) {
        indexObj.setSourceDocFormat(FileFormat.DENKXWEB);

        Element structNode = indexObj.getRootStructNode();

        // Set type
        {
            String value = structNode.getAttributeValue("type");
            if (StringUtils.isNotEmpty(value)) {
                indexObj.setType(MetadataConfigurationManager.mapDocStrct(value).trim());
            } else {
                indexObj.setType("monument");
            }
            logger.trace("TYPE: {}", indexObj.getType());
        }

        // Set label
        {
            String value = structNode.getAttributeValue("LABEL");
            if (value != null) {
                indexObj.setLabel(value);
            }
        }
        logger.trace("LABEL: {}", indexObj.getLabel());
    }

    /**
     * 
     * @param writeStrategy
     * @param dataFolders
     * @param pageCountStart
     * @param downloadExternalImages
     * @throws FatalIndexerException
     * @should generate pages correctly
     */
    public void generatePageDocuments(ISolrWriteStrategy writeStrategy, Map<String, Path> dataFolders, int pageCountStart,
            boolean downloadExternalImages) throws FatalIndexerException {
        String xpath = "//denkxweb:images/denkxweb:image";
        List<Element> eleImageList = xp.evaluateToElements(xpath, null);
        if (eleImageList == null || eleImageList.isEmpty()) {
            // No pages
            return;
        }

        logger.info("Generating {} page documents (count starts at {})...", eleImageList.size(), pageCountStart);

        // TODO lambda instead of loop (find a way to preserve order first)
        //        eleImageList.parallelStream().forEach(
        //                eleImage -> generatePageDocument(eleImage, String.valueOf(getNextIddoc(hotfolder.getSolrHelper())), null,
        //                        writeStrategy, dataFolders, downloadExternalImages));
        int order = pageCountStart;
        for (Element eleImage : eleImageList) {
            PhysicalElement page =
                    generatePageDocument(eleImage, String.valueOf(getNextIddoc(SolrIndexerDaemon.getInstance().getSearchIndex())), order,
                            dataFolders, downloadExternalImages);
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
     * @param order
     * @param dataFolders
     * @param downloadExternalImages
     * @return PhysicalElement
     * @throws FatalIndexerException
     */
    PhysicalElement generatePageDocument(Element eleImage, String iddoc, Integer order, Map<String, Path> dataFolders, boolean downloadExternalImages)
            throws FatalIndexerException {
        if (eleImage == null) {
            throw new IllegalArgumentException("eleImage may not be null");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null");
        }
        if (order == null) {
            // TODO page order within the metadata
        }

        // Create object for this page
        PhysicalElement ret = new PhysicalElement(order);
        ret.getDoc().addField(SolrConstants.IDDOC, iddoc);
        ret.getDoc().addField(SolrConstants.GROUPFIELD, iddoc);
        ret.getDoc().addField(SolrConstants.DOCTYPE, DocType.PAGE.name());
        ret.getDoc().addField(SolrConstants.ORDER, order);
        ret.getDoc().addField(SolrConstants.PHYSID, String.valueOf(order));

        Element eleStandard = eleImage.getChild("standard", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("denkxweb"));
        if (eleStandard == null) {
            logger.warn("No element <standard> found for image {}", order);
            return null;
        }

        String orderLabel = eleStandard.getAttributeValue("ORDERLABEL");
        if (StringUtils.isNotEmpty(orderLabel)) {
            ret.getDoc().addField(SolrConstants.ORDERLABEL, orderLabel);
        } else {
            ret.getDoc().addField(SolrConstants.ORDERLABEL, SolrIndexerDaemon.getInstance().getConfiguration().getEmptyOrderLabelReplacement());
        }

        // Description
        String desc = eleImage.getChildText("description", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("denkxweb"));
        if (StringUtils.isNotEmpty(desc)) {
            ret.getDoc().addField("MD_DESCRIPTION", desc);
        }

        // Copyright
        String copyright = eleStandard.getAttributeValue("right");
        if (StringUtils.isNotEmpty(copyright)) {
            ret.getDoc().addField("MD_COPYRIGHT", copyright);
        }

        // URL
        String url = eleStandard.getAttributeValue("url");
        String fileName;
        if (StringUtils.isNotEmpty(url) && url.contains("/")) {
            if (Utils.isFileNameMatchesRegex(url, IIIF_IMAGE_FILE_NAMES)) {
                // Extract correct original file name from IIIF
                fileName = Utils.getFileNameFromIiifUrl(url);
            } else {
                fileName = url.substring(url.lastIndexOf("/") + 1);
            }
        } else {
            fileName = url;
        }

        // Handle external/internal file URL
        if (StringUtils.isNotEmpty(url)) {
            handleImageUrl(url, ret.getDoc(), fileName, dataFolders.get(DataRepository.PARAM_MEDIA), sbImgFileNames, downloadExternalImages, false,
                    "true".equals(eleImage.getAttributeValue("preferred")));
        }

        // Add file size
        try {
            Path dataFolder = dataFolders.get(DataRepository.PARAM_MEDIA);
            // TODO other mime types/folders
            if (dataFolder != null) {
                Path path = Paths.get(dataFolder.toAbsolutePath().toString(), fileName);
                if (Files.isRegularFile(path)) {
                    ret.getDoc().addField(FIELD_FILESIZE, Files.size(path));
                }
            }
        } catch (IllegalArgumentException | IOException e) {
            logger.warn(e.getMessage());
        }
        if (!ret.getDoc().containsKey(FIELD_FILESIZE)) {
            ret.getDoc().addField(FIELD_FILESIZE, -1);
        }

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

        // FULLTEXTAVAILABLE indicates whether this page has full-text
        if (ret.getDoc().getField(SolrConstants.FULLTEXT) != null) {
            ret.getDoc().addField(SolrConstants.FULLTEXTAVAILABLE, true);
            recordHasFulltext = true;
        } else {
            ret.getDoc().addField(SolrConstants.FULLTEXTAVAILABLE, false);
        }

        return ret;
    }

    @Override
    protected FileFormat getSourceDocFormat() {
        return FileFormat.DENKXWEB;
    }
}
