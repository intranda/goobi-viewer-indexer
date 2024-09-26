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
import java.util.List;
import java.util.Map;

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
     * @see io.goobi.viewer.indexer.Indexer#addToIndex(java.nio.file.Path, java.util.Map)
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

            IndexObject indexObj = new IndexObject(getNextIddoc());
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
            List<LuceneField> thumbnailFields = LidoIndexer.mapPagesToDocstruct(indexObj, writeStrategy, 0);
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
            PhysicalElement page = generatePageDocument(eleImage, String.valueOf(getNextIddoc()), order, dataFolders, downloadExternalImages);
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
    PhysicalElement generatePageDocument(Element eleImage, String iddoc, Integer order, Map<String, Path> dataFolders,
            boolean downloadExternalImages) {
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
        PhysicalElement ret = createPhysicalElement(order, iddoc, String.valueOf(order));

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

        addPageAdditionalTechMetadata(ret, dataFolders, fileName);

        return ret;
    }

    @Override
    protected FileFormat getSourceDocFormat() {
        return FileFormat.DENKXWEB;
    }
}
