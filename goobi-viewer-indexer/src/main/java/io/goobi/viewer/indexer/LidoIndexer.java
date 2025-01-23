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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.output.XMLOutputter;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.Hotfolder;
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
import io.goobi.viewer.indexer.model.config.MetadataConfigurationManager;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * <p>
 * LidoIndexer class.
 * </p>
 *
 */
public class LidoIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(LidoIndexer.class);

    private static final String[] DATA_FOLDER_PARAMS =
            { DataRepository.PARAM_MEDIA, DataRepository.PARAM_MIX, DataRepository.PARAM_UGC, DataRepository.PARAM_CMS,
                    DataRepository.PARAM_TEIMETADATA, DataRepository.PARAM_ANNOTATIONS };

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
    public LidoIndexer(Hotfolder hotfolder) {
        super();
        this.hotfolder = hotfolder;
    }

    /** {@inheritDoc} */
    @Override
    public List<String> addToIndex(Path lidoFile, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        logger.debug("Indexing LIDO file '{}'...", lidoFile.getFileName());
        String fileNameRoot = FilenameUtils.getBaseName(lidoFile.getFileName().toString());

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

        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile.toFile());
        logger.info("File contains {} LIDO documents.", lidoDocs.size());
        XMLOutputter outputter = new XMLOutputter();

        List<String> ret = new ArrayList<>(lidoDocs.size());
        for (Document doc : lidoDocs) {
            String[] resp = index(doc, dataFolders, null, SolrIndexerDaemon.getInstance().getConfiguration().getPageCountStart(),
                    SolrIndexerDaemon.getInstance().getConfiguration().getStringList("init.lido.imageXPath"),
                    dataFolders.containsKey(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER),
                    reindexSettings.containsKey(DataRepository.PARAM_MEDIA));

            if (!Indexer.STATUS_ERROR.equals(resp[0])) {
                String identifier = resp[0];
                String newLidoFileName = identifier + ".xml";

                // Write individual LIDO records as separate files
                Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_LIDO).toAbsolutePath().toString(), newLidoFileName);
                try (FileOutputStream out = new FileOutputStream(indexed.toFile())) {
                    outputter.output(doc, out);
                }
                dataRepository.checkOtherRepositoriesForRecordFileDuplicates(newLidoFileName, DataRepository.PARAM_INDEXED_LIDO,
                        hotfolder.getDataRepositoryStrategy().getAllDataRepositories());

                // Move non-repository data directories to the selected repository
                if (previousDataRepository != null) {
                    previousDataRepository.moveDataFoldersToRepository(dataRepository, identifier);
                }

                // Copy media files
                int imageCounter = dataRepository.copyImagesFromMultiRecordMediaFolder(dataFolders.get(DataRepository.PARAM_MEDIA), identifier,
                        lidoFile.getFileName().toString(), hotfolder.getDataRepositoryStrategy(), resp[1],
                        reindexSettings.get(DataRepository.PARAM_MEDIA) != null && reindexSettings.get(DataRepository.PARAM_MEDIA));
                if (imageCounter > 0) {
                    String msg = Utils.removeRecordImagesFromCache(identifier);
                    if (msg != null) {
                        logger.info(msg);
                    }
                }

                // Copy MIX files
                if (reindexSettings.get(DataRepository.PARAM_MIX) == null || !reindexSettings.get(DataRepository.PARAM_MIX)) {
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
                }

                // Update data repository cache map in the Goobi viewer
                if (previousDataRepository != null) {
                    try {
                        Utils.updateDataRepositoryCache(identifier, dataRepository.getPath());
                    } catch (HTTPException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
                prerenderPagePdfsIfRequired(identifier);
                logger.info("Successfully finished indexing '{}'.", identifier);

                // Remove this file from lower priority hotfolders to avoid overriding changes with older version
                SolrIndexerDaemon.getInstance().removeRecordFileFromLowerPriorityHotfolders(identifier, hotfolder);

                // Add identifier to return list
                ret.add(identifier);
            } else {
                handleError(lidoFile, resp[1], FileFormat.LIDO);
            }
        }

        // Copy original LIDO file into the orig folder
        Path orig = Paths.get(hotfolder.getOrigLido().toAbsolutePath().toString(), lidoFile.getFileName().toString());
        Files.copy(lidoFile, orig, StandardCopyOption.REPLACE_EXISTING);

        // Delete files from the hotfolder
        try {
            Files.delete(lidoFile);
        } catch (IOException e) {
            logger.error("'{}' could not be deleted; please delete it manually.", lidoFile.toAbsolutePath());
        }
        // Delete all data folders for this record from the hotfolder
        DataRepository.deleteDataFoldersFromHotfolder(dataFolders, reindexSettings);
        logger.info("Finished indexing LIDO file '{}'.", lidoFile.getFileName());

        return ret;
    }

    /**
     * Indexes a LIDO file.
     *
     * @param doc a {@link org.jdom2.Document} object.
     * @param dataFolders a {@link java.util.Map} object.
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @param pageCountStart a int.
     * @param imageXPaths a {@link java.util.List} object.
     * @param downloadExternalImages a boolean.
     * @param useOldImageFolderIfAvailable
     * @should index record correctly
     * @should update record correctly
     * @return an array of {@link java.lang.String} objects.
     */
    public String[] index(Document doc, Map<String, Path> dataFolders, ISolrWriteStrategy writeStrategy, int pageCountStart,
            List<String> imageXPaths, boolean downloadExternalImages, boolean useOldImageFolderIfAvailable) {
        String[] ret = { STATUS_ERROR, null };
        String pi = null;
        try {
            this.xp = new JDomXP(doc);
            if (this.xp == null) {
                throw new IndexerException("Could not create XML parser.");
            }

            IndexObject indexObj = new IndexObject(getNextIddoc());
            logger.debug("IDDOC: {}", indexObj.getIddoc());
            Element structNode = doc.getRootElement();
            indexObj.setRootStructNode(structNode);

            // set some simple data in den indexObject
            setSimpleData(indexObj);

            // Set PI
            pi = validateAndApplyPI(findPI("/lido:lido/"), indexObj, false);

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

            generatePageDocuments(writeStrategy, dataFolders, pageCountStart, imageXPaths, downloadExternalImages, useOldImageFolderIfAvailable);

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // Add THUMBNAIL,THUMBPAGENO,THUMBPAGENOLABEL (must be done AFTER writeDateMondified(), writeAccessConditions() and generatePageDocuments()!)
            List<LuceneField> thumbnailFields = mapPagesToDocstruct(indexObj, writeStrategy, 0);
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

            // Generate event documents (must happen before writing the DEFAULT field!)
            List<SolrInputDocument> events = generateEvents(indexObj);
            logger.debug("Generated {} event docs.", events.size());

            // Add DEFAULT field
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(indexObj.getDefaultValue()));
                indexObj.setDefaultValue("");
            }

            // Add event docs to the main list
            writeStrategy.addDocs(events);

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
     * @param depth
     * @return
     * @throws FatalIndexerException
     */
    protected static List<LuceneField> mapPagesToDocstruct(IndexObject indexObj, ISolrWriteStrategy writeStrategy, int depth)
            throws FatalIndexerException {
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
            if (page.getDoc().getField(FIELD_OWNERDEPTH) == null || depth > (Integer) page.getDoc().getFieldValue(FIELD_OWNERDEPTH)) {
                page.getDoc().setField(SolrConstants.IDDOC_OWNER, String.valueOf(indexObj.getIddoc()));
                page.getDoc().setField(FIELD_OWNERDEPTH, depth);

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
     * <p>
     * generatePageDocuments.
     * </p>
     *
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @param dataFolders a {@link java.util.Map} object.
     * @param pageCountStart a int.
     * @param imageXPaths a {@link java.util.List} object.
     * @param downloadExternalImages a boolean.
     * @param useOldImageFolderIfAvailable
     */
    public void generatePageDocuments(ISolrWriteStrategy writeStrategy, Map<String, Path> dataFolders, int pageCountStart,
            List<String> imageXPaths, boolean downloadExternalImages, boolean useOldImageFolderIfAvailable) {
        String xpath = "/lido:lido/lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet";
        List<Element> resourceSetList = xp.evaluateToElements(xpath, null);
        if (resourceSetList == null || resourceSetList.isEmpty()) {
            // No pages
            return;
        }

        logger.info("Generating {} page documents (count starts at {})...", resourceSetList.size(), pageCountStart);

        if (imageXPaths == null || imageXPaths.isEmpty()) {
            logger.error("No init.lido.imageXPath configuration elements found, cannot add images!");
        }

        // TODO lambda instead of loop (find a way to preserve order first)
        //        resourceSetList.parallelStream().forEach(
        //                eleResourceSet -> generatePageDocument(eleResourceSet, String.valueOf(getNextIddoc(hotfolder.getSolrHelper())), null,
        //                        writeStrategy, dataFolders));
        int order = pageCountStart;
        for (Element eleResourceSet : resourceSetList) {
            String orderAttribute =
                    eleResourceSet.getAttributeValue("sortorder", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("lido"));
            // Extract page order info , if available
            if (orderAttribute != null) {
                order = Integer.valueOf(orderAttribute);
            }
            PhysicalElement page = generatePageDocument(eleResourceSet, String.valueOf(getNextIddoc()), order,
                    dataFolders, imageXPaths, downloadExternalImages, useOldImageFolderIfAvailable);
            if (page != null) {
                writeStrategy.addPage(page);
                order++;
            }
        }

        logger.info("Generated {} page documents.", writeStrategy.getPageDocsSize());
    }

    /**
     * 
     * @param eleResourceSet
     * @param iddoc
     * @param order
     * @param dataFolders
     * @param imageXPaths
     * @param downloadExternalImages
     * @param useOldImageFolderIfAvailable
     * @return {@link PhysicalElement}
     * @throws FatalIndexerException
     */
    PhysicalElement generatePageDocument(Element eleResourceSet, String iddoc, Integer order, Map<String, Path> dataFolders, List<String> imageXPaths,
            boolean downloadExternalImages, boolean useOldImageFolderIfAvailable) {
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null");
        }
        if (order == null) {
            // TODO parallel processing of pages will required Goobi to put values starting with 1 into the ORDER attribute
        }

        // Create object for this page
        PhysicalElement ret = createPhysicalElement(order, iddoc, String.valueOf(order));

        String orderLabel = eleResourceSet.getAttributeValue("ORDERLABEL");
        if (StringUtils.isNotEmpty(orderLabel)) {
            ret.getDoc().addField(SolrConstants.ORDERLABEL, orderLabel);
        } else {
            ret.getDoc().addField(SolrConstants.ORDERLABEL, SolrIndexerDaemon.getInstance().getConfiguration().getEmptyOrderLabelReplacement());
        }

        if (imageXPaths == null || imageXPaths.isEmpty()) {
            return ret;
        }

        String fileUri = null;
        for (String xpath : imageXPaths) {
            fileUri = xp.evaluateToString(xpath, eleResourceSet);
            if (StringUtils.isNotEmpty(fileUri)) {
                logger.info("Found image in {}", xpath);
                break;
            }
        }

        // Do not create pages for resourceSet elements that have no relation to images
        if (StringUtils.isEmpty(fileUri)) {
            logger.debug("No file path found for this resource set.");
            return null;
        }

        String fileName;
        if (StringUtils.isNotEmpty(fileUri) && fileUri.contains("/")) {
            if (Utils.isFileNameMatchesRegex(fileUri, IIIF_IMAGE_FILE_NAMES)) {
                // Extract correct original file name from IIIF
                fileName = Utils.getFileNameFromIiifUrl(fileUri);
            } else {
                fileName = fileUri.substring(fileUri.lastIndexOf("/") + 1);
            }
        } else {
            fileName = fileUri;
        }

        // Handle external/internal file URL
        if (StringUtils.isNotEmpty(fileUri)) {
            handleImageUrl(fileUri, ret.getDoc(), fileName, dataFolders.get(DataRepository.PARAM_MEDIA), sbImgFileNames, downloadExternalImages,
                    useOldImageFolderIfAvailable, false);
        }

        String baseFileName = FilenameUtils.getBaseName((String) ret.getDoc().getFieldValue(SolrConstants.FILENAME));
        if (dataFolders.get(DataRepository.PARAM_MIX) != null) {
            try {
                Map<String, String> mixData = TextHelper.readMix(
                        new File(dataFolders.get(DataRepository.PARAM_MIX).toAbsolutePath().toString(), baseFileName + FileTools.XML_EXTENSION));
                for (Entry<String, String> entry : mixData.entrySet()) {
                    if (!(entry.getKey().equals(SolrConstants.WIDTH) && ret.getDoc().getField(SolrConstants.WIDTH) != null)
                            && !(entry.getKey().equals(SolrConstants.HEIGHT) && ret.getDoc().getField(SolrConstants.HEIGHT) != null)) {
                        ret.getDoc().addField(entry.getKey(), entry.getValue());
                    }
                }
            } catch (JDOMException e) {
                logger.error(e.getMessage(), e);
            } catch (IOException e) {
                logger.warn(e.getMessage());
            }
        }

        addPageAdditionalTechMetadata(ret, dataFolders, fileName);

        return ret;
    }

    /**
     * @param indexObj IndexObject of the parent docstruct (usually the top level docstruct).
     * @return List<SolrInputDocument>
     * @throws FatalIndexerException
     */
    private List<SolrInputDocument> generateEvents(IndexObject indexObj) throws FatalIndexerException {
        String query = "/lido:lido/lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event";
        List<Element> eventList = xp.evaluateToElements(query, null);
        if (eventList == null || eventList.isEmpty()) {
            return Collections.emptyList();
        }

        logger.info("Found {} event(s).", eventList.size());
        String defaultFieldBackup = indexObj.getDefaultValue();
        List<SolrInputDocument> ret = new ArrayList<>(eventList.size());
        for (Element eleEvent : eventList) {
            SolrInputDocument eventDoc = new SolrInputDocument();
            String iddocEvent = getNextIddoc();
            eventDoc.addField(SolrConstants.IDDOC, iddocEvent);
            eventDoc.addField(SolrConstants.GROUPFIELD, iddocEvent);
            eventDoc.addField(SolrConstants.DOCTYPE, DocType.EVENT.name());
            List<LuceneField> dcFields = indexObj.getLuceneFieldsWithName(SolrConstants.DC);
            if (dcFields != null) {
                for (LuceneField field : dcFields) {
                    eventDoc.addField(field.getField(), field.getValue());
                }
            }
            eventDoc.setField(SolrConstants.IDDOC_OWNER, String.valueOf(indexObj.getIddoc()));
            eventDoc.addField(SolrConstants.PI_TOPSTRUCT, indexObj.getPi());

            // Add topstruct type
            if (!eventDoc.containsKey(SolrConstants.DOCSTRCT_TOP) && indexObj.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP) != null) {
                eventDoc.setField(SolrConstants.DOCSTRCT_TOP, indexObj.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP).getValue());
            }

            // Find event type
            query = "lido:eventType/lido:term/text()";
            String type = xp.evaluateToString(query, eleEvent);
            if (StringUtils.isBlank(type)) {
                // LIDO 1.1 skos:Concept fallback
                query = "lido:eventType/skos:Concept/skos:prefLabel[@xml:lang='en']/text()";
                type = xp.evaluateToString(query, eleEvent);
            }
            if (StringUtils.isNotBlank(type)) {
                eventDoc.addField(SolrConstants.EVENTTYPE, type);
                indexObj.setDefaultValue(type);
            } else {
                logger.error("Event type not found.");
            }

            // Copy access conditions
            for (String accessCondition : indexObj.getAccessConditions()) {
                eventDoc.addField(SolrConstants.ACCESSCONDITION, accessCondition);
            }

            // Create a backup of the current grouped metadata list of the parent docstruct
            List<GroupedMetadata> groupedFieldsBackup = new ArrayList<>(indexObj.getGroupedMetadataFields());
            List<LuceneField> fields = MetadataHelper.retrieveElementMetadata(eleEvent, "", indexObj, xp);

            // Add grouped metadata as separate documents
            if (indexObj.getGroupedMetadataFields().size() > groupedFieldsBackup.size()) {
                // Newly added items in IndexObject.groupedMetadataFields come from the event, so just use these new items
                List<GroupedMetadata> eventGroupedFields =
                        indexObj.getGroupedMetadataFields().subList(groupedFieldsBackup.size(), indexObj.getGroupedMetadataFields().size());
                for (GroupedMetadata gmd : eventGroupedFields) {
                    SolrInputDocument doc = SolrSearchIndex.createDocument(gmd.getFields());
                    String iddoc = getNextIddoc();
                    doc.addField(SolrConstants.IDDOC, iddoc);
                    if (!doc.getFieldNames().contains(SolrConstants.GROUPFIELD)) {
                        logger.warn("{} not set in grouped metadata doc {}, using IDDOC instead.", SolrConstants.GROUPFIELD,
                                doc.getFieldValue(SolrConstants.LABEL));
                        doc.addField(SolrConstants.GROUPFIELD, iddoc);
                    }
                    // IDDOC_OWNER should always contain the IDDOC of the lowest docstruct to which this page is mapped.
                    // Since child docstructs are added recursively, this should be the case without further conditions.
                    doc.addField(SolrConstants.IDDOC_OWNER, iddocEvent);
                    doc.addField(SolrConstants.DOCTYPE, DocType.METADATA.name());
                    doc.addField(SolrConstants.PI_TOPSTRUCT, indexObj.getTopstructPI());

                    // Add DC values to metadata doc
                    if (dcFields != null) {
                        for (LuceneField field : dcFields) {
                            doc.addField(field.getField(), field.getValue());
                        }
                    }

                    // Copy access conditions to metadata docs
                    for (String accessCondition : indexObj.getAccessConditions()) {
                        doc.addField(SolrConstants.ACCESSCONDITION, accessCondition);
                    }

                    ret.add(doc);
                }

                // Grouped metadata fields are written directly into the IndexObject, which is not desired. Replace the metadata from the backup.
                indexObj.setGroupedMetadataFields(groupedFieldsBackup);
            }

            for (LuceneField field : fields) {
                eventDoc.addField(field.getField(), field.getValue());
                logger.debug("Added {}:{} to event '{}'.", field.getField(), field.getValue(), type);

                // Check whether this field is configured to be added as a sort field to topstruct
                List<FieldConfig> fieldConfigList =
                        SolrIndexerDaemon.getInstance()
                                .getConfiguration()
                                .getMetadataConfigurationManager()
                                .getConfigurationListForField(field.getField());
                if (fieldConfigList != null && !fieldConfigList.isEmpty()) {
                    FieldConfig fieldConfig = fieldConfigList.get(0);
                    if (fieldConfig.isAddSortFieldToTopstruct()) {
                        List<LuceneField> retList = new ArrayList<>(1);
                        MetadataHelper.addSortField(field.getField(), field.getValue(), SolrConstants.PREFIX_SORT,
                                fieldConfig.getNonSortConfigurations(),
                                fieldConfig.getValueNormalizers(), retList);
                        if (!retList.isEmpty()) {
                            indexObj.addToLucene(retList.get(0), false);
                        }
                    }
                }
            }

            // Use the main IndexObject's default value field to collect default values for the events, then restore the original value
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                eventDoc.addField(SolrConstants.DEFAULT, indexObj.getDefaultValue());
                indexObj.setDefaultValue(defaultFieldBackup);
            }

            ret.add(eventDoc);
        }

        return ret;
    }

    /**
     * Sets TYPE and LABEL from the LIDO document.
     * 
     * @param indexObj {@link IndexObject}
     * @throws FatalIndexerException
     */
    protected void setSimpleData(IndexObject indexObj) {
        indexObj.setSourceDocFormat(FileFormat.LIDO);
        Element structNode = indexObj.getRootStructNode();

        // Set type
        List<String> values = xp.evaluateToStringList(
                "lido:descriptiveMetadata/lido:objectClassificationWrap/lido:objectWorkTypeWrap/lido:objectWorkType/lido:term/text()", structNode);
        if (values != null && !values.isEmpty()) {
            indexObj.setType(MetadataConfigurationManager.mapDocStrct((values.get(0)).trim()));
        }
        logger.trace("TYPE: {}", indexObj.getType());

        // Set label
        String value = structNode.getAttributeValue("LABEL");
        if (value != null) {
            indexObj.setLabel(value);
        }
        logger.trace("LABEL: {}", indexObj.getLabel());
    }

    /**
     * 
     * @return {@link FileFormat}
     */
    protected FileFormat getSourceDocFormat() {
        return FileFormat.LIDO;
    }
}
