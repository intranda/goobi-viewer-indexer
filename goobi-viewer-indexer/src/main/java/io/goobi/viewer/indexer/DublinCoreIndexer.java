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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.Configuration;
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
    private static final Logger logger = LoggerFactory.getLogger(DublinCoreIndexer.class);

    /** Constant <code>DEFAULT_FILEGROUP_1="PRESENTATION"</code> */
    public static final String DEFAULT_FILEGROUP_1 = "PRESENTATION";
    /** Constant <code>DEFAULT_FILEGROUP_2="DEFAULT"</code> */
    public static final String DEFAULT_FILEGROUP_2 = "DEFAULT";
    /** Constant <code>OBJECT_FILEGROUP="OBJECT"</code> */
    public static final String OBJECT_FILEGROUP = "OBJECT";
    /** Constant <code>ALTO_FILEGROUP="ALTO"</code> */
    public static final String ALTO_FILEGROUP = "ALTO";
    /** Constant <code>FULLTEXT_FILEGROUP="FULLTEXT"</code> */
    public static final String FULLTEXT_FILEGROUP = "FULLTEXT";
    /** Constant <code>ANCHOR_UPDATE_EXTENSION=".UPDATED"</code> */
    public static final String ANCHOR_UPDATE_EXTENSION = ".UPDATED";

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
    public String[] index(Path dcFile, Map<String, Path> dataFolders, ISolrWriteStrategy writeStrategy,
            int pageCountStart) {
        String[] ret = { null, null };

        if (dcFile == null || !Files.exists(dcFile)) {
            throw new IllegalArgumentException("dcfile must point to an existing Dublin Core file.");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null.");
        }

        logger.debug("Indexing Dublin Core file '{}'...", dcFile.getFileName());
        try {
            initJDomXP(dcFile);
            IndexObject indexObj = new IndexObject(getNextIddoc(hotfolder.getSearchIndex()));
            logger.debug("IDDOC: {}", indexObj.getIddoc());
            indexObj.setRootStructNode(xp.getRootElement());

            // set some simple data in den indexObject
            setSimpleData(indexObj);
            setUrn(indexObj);

            // Set PI
            {
                String pi = MetadataHelper.getPIFromXML("/record/", xp);
                if (StringUtils.isNotBlank(pi)) {
                    pi = MetadataHelper.applyIdentifierModifications(pi);
                    logger.info("Record PI: {}", pi);

                    // Do not allow identifiers with characters that cannot be used in file names
                    Pattern p = Pattern.compile("[^\\w|-]");
                    Matcher m = p.matcher(pi);
                    if (m.find()) {
                        ret[1] = new StringBuilder("PI contains illegal characters: ").append(pi).toString();
                        throw new IndexerException(ret[1]);
                    }
                    indexObj.setPi(pi);
                    indexObj.setTopstructPI(pi);
                    logger.debug("PI: {}", indexObj.getPi());

                    // Determine the data repository to use
                    DataRepository[] repositories =
                            hotfolder.getDataRepositoryStrategy()
                                    .selectDataRepository(pi, dcFile, dataFolders, hotfolder.getSearchIndex(), hotfolder.getOldSearchIndex());
                    dataRepository = repositories[0];
                    previousDataRepository = repositories[1];
                    if (StringUtils.isNotEmpty(dataRepository.getPath())) {
                        indexObj.setDataRepository(dataRepository.getPath());
                    }

                    ret[0] = new StringBuilder(indexObj.getPi()).append(Indexer.XML_EXTENSION).toString();

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
                } else {
                    ret[1] = "PI not found.";
                    throw new IndexerException(ret[1]);
                }
            }

            if (writeStrategy == null) {
                // Request appropriate write strategy
                writeStrategy = AbstractWriteStrategy.create(dcFile, dataFolders, hotfolder);
            } else {
                logger.info("Solr write strategy injected by caller: {}", writeStrategy.getClass().getName());
            }

            // Set source doc format
            indexObj.addToLucene(SolrConstants.SOURCEDOCFORMAT, FileFormat.DUBLINCORE.name());
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

            // If this is a volume (= has an anchor) that has already been indexed, copy access conditions from the anchor element
            if (indexObj.isVolume() && indexObj.getAccessConditions().isEmpty()) {
                String anchorPi = MetadataHelper.getAnchorPi(xp);
                if (anchorPi != null) {
                    indexObj.setAnchorPI(anchorPi);
                    SolrDocumentList hits = hotfolder.getSearchIndex()
                            .search(SolrConstants.PI + ":" + anchorPi, Collections.singletonList(SolrConstants.ACCESSCONDITION));
                    if (hits != null && !hits.isEmpty()) {
                        Collection<Object> fields = hits.get(0).getFieldValues(SolrConstants.ACCESSCONDITION);
                        if (fields != null) {
                            for (Object o : fields) {
                                indexObj.getAccessConditions().add(o.toString());
                            }
                        } else {
                            logger.error(
                                    "Anchor document '{}' has no ACCESSCONDITION values. Please check whether it is a proper anchor and not a group!",
                                    anchorPi);
                        }
                    }
                }
            }

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // Write created/updated timestamps
            indexObj.writeDateModified(true);

            // Generate docs for all pages and add to the write strategy
            generatePageDocuments(writeStrategy, dataFolders, dataRepository, indexObj.getPi(), pageCountStart);

            // If images have been found for any page, set a boolean in the root doc indicating that the record does have images
            indexObj.addToLucene(FIELD_IMAGEAVAILABLE, String.valueOf(recordHasImages));

            // If full-text has been indexed for any page, set a boolean in the root doc indicating that the record does have full-text
            indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, String.valueOf(recordHasFulltext));

            // write all page URNs sequentially into one field
            generatePageUrns(indexObj);

            // Add THUMBNAIL,THUMBPAGENO,THUMBPAGENOLABEL (must be done AFTER writeDateMondified(), writeAccessConditions() and generatePageDocuments()!)
            List<LuceneField> thumbnailFields = mapPagesToDocstruct(indexObj, writeStrategy);
            if (thumbnailFields != null) {
                indexObj.getLuceneFields().addAll(thumbnailFields);
            }

            // ISWORK only for non-anchors
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
                            indexObj.addToLucene(SolrConstants.CMS_TEXT_ + field, value);
                            indexObj.addToLucene(SolrConstants.CMS_TEXT_ALL, value);
                        }
                    }
                }
            }

            // Create group documents if this record is part of a group and no doc exists for that group yet
            for (String groupIdField : indexObj.getGroupIds().keySet()) {
                String groupSuffix = groupIdField.replace(SolrConstants.GROUPID_, "");
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
                SolrInputDocument doc = hotfolder.getSearchIndex()
                        .checkAndCreateGroupDoc(groupIdField, indexObj.getGroupIds().get(groupIdField), moreMetadata,
                                getNextIddoc(hotfolder.getSearchIndex()));
                if (doc != null) {
                    writeStrategy.addDoc(doc);
                    logger.debug("Created group document for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                } else {
                    logger.debug("Group document already exists for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                }
            }

            // Add grouped metadata as separate documents
            addGroupedMetadataDocs(writeStrategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

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

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)
            logger.debug("Writing document to index...");
            SolrInputDocument rootDoc = SolrSearchIndex.createDocument(indexObj.getLuceneFields());
            writeStrategy.setRootDoc(rootDoc);
            writeStrategy.writeDocs(Configuration.getInstance().isAggregateRecords());
            logger.info("Successfully finished indexing '{}'.", dcFile.getFileName());
        } catch (Exception e) {
            logger.error("Indexing of '{}' could not be finished due to an error.", dcFile.getFileName());
            logger.error(e.getMessage(), e);
            ret[1] = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            hotfolder.getSearchIndex().rollback();
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
     * @return
     * @throws FatalIndexerException
     */
    private static List<LuceneField> mapPagesToDocstruct(IndexObject indexObj, ISolrWriteStrategy writeStrategy) throws FatalIndexerException {
        List<String> physIds = new ArrayList<>(writeStrategy.getPageDocsSize());
        for (int i = 1; i <= writeStrategy.getPageDocsSize(); ++i) {
            physIds.add(String.valueOf(i));
        }
        List<SolrInputDocument> pageDocs = writeStrategy.getPageDocsForPhysIdList(physIds);
        if (pageDocs.isEmpty()) {
            logger.warn("No pages found for {}", indexObj.getLogId());
            return Collections.emptyList();
        }

        List<LuceneField> ret = new ArrayList<>(5);

        SolrInputDocument firstPageDoc = pageDocs.get(0);
        if (firstPageDoc != null) {
            // Add thumbnail information from the first page
            String thumbnailFileName = (String) firstPageDoc.getFieldValue(SolrConstants.FILENAME);
            ret.add(new LuceneField(SolrConstants.THUMBNAIL, thumbnailFileName));
            if (DocType.SHAPE.name().equals(firstPageDoc.getFieldValue(SolrConstants.DOCTYPE))) {
                ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPageDoc.getFieldValue("ORDER_PARENT"))));
            } else {
                ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPageDoc.getFieldValue(SolrConstants.ORDER))));
            }
            ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) firstPageDoc.getFieldValue(SolrConstants.ORDERLABEL)));
            ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) firstPageDoc.getFieldValue(SolrConstants.MIMETYPE)));
        }

        // If this is a top struct element, look for a representative image
        for (SolrInputDocument pageDoc : pageDocs) {
            String pageFileName = pageDoc.getField(SolrConstants.FILENAME + SolrConstants._HTML_SANDBOXED) != null
                    ? (String) pageDoc.getFieldValue(SolrConstants.FILENAME + SolrConstants._HTML_SANDBOXED)
                    : (String) pageDoc.getFieldValue(SolrConstants.FILENAME);
            String pageFileBaseName = FilenameUtils.getBaseName(pageFileName);

            // Make sure IDDOC_OWNER of a page contains the iddoc of the lowest possible mapped docstruct
            if (pageDoc.getField(FIELD_OWNERDEPTH) == null || 0 > (Integer) pageDoc.getFieldValue(FIELD_OWNERDEPTH)) {
                pageDoc.setField(SolrConstants.IDDOC_OWNER, String.valueOf(indexObj.getIddoc()));
                pageDoc.setField(FIELD_OWNERDEPTH, 0);

                // Add the parent document's structure element to the page
                pageDoc.setField(SolrConstants.DOCSTRCT, indexObj.getType());

                // Add topstruct type to the page
                if (!pageDoc.containsKey(SolrConstants.DOCSTRCT_TOP) && indexObj.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP) != null) {
                    pageDoc.setField(SolrConstants.DOCSTRCT_TOP, indexObj.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP).getValue());
                }

                // Remove SORT_ fields from a previous, higher up docstruct
                Set<String> fieldsToRemove = new HashSet<>();
                for (String fieldName : pageDoc.getFieldNames()) {
                    if (fieldName.startsWith(SolrConstants.SORT_)) {
                        fieldsToRemove.add(fieldName);
                    }
                }
                for (String fieldName : fieldsToRemove) {
                    pageDoc.removeField(fieldName);
                }
                //  Add this docstruct's SORT_* fields to page
                if (indexObj.getIddoc() == Long.valueOf((String) pageDoc.getFieldValue(SolrConstants.IDDOC_OWNER))) {
                    for (LuceneField field : indexObj.getLuceneFields()) {
                        if (field.getField().startsWith(SolrConstants.SORT_)) {
                            pageDoc.addField(field.getField(), field.getValue());
                        }
                    }
                }
            }

            if (pageDoc.getField(SolrConstants.PI_TOPSTRUCT) == null) {
                pageDoc.addField(SolrConstants.PI_TOPSTRUCT, indexObj.getTopstructPI());
            }
            if (pageDoc.getField(SolrConstants.DATAREPOSITORY) == null && indexObj.getDataRepository() != null) {
                pageDoc.addField(SolrConstants.DATAREPOSITORY, indexObj.getDataRepository());
            }
            if (pageDoc.getField(SolrConstants.DATEUPDATED) == null && !indexObj.getDateUpdated().isEmpty()) {
                for (Long date : indexObj.getDateUpdated()) {
                    pageDoc.addField(SolrConstants.DATEUPDATED, date);
                }
            }
            if (pageDoc.getField(SolrConstants.DATEINDEXED) == null && !indexObj.getDateIndexed().isEmpty()) {
                for (Long date : indexObj.getDateIndexed()) {
                    pageDoc.addField(SolrConstants.DATEINDEXED, date);
                }
            }

            // Add of each docstruct access conditions (no duplicates)
            Set<String> existingAccessConditions = new HashSet<>();
            if (pageDoc.getFieldValues(SolrConstants.ACCESSCONDITION) != null) {
                for (Object obj : pageDoc.getFieldValues(SolrConstants.ACCESSCONDITION)) {
                    existingAccessConditions.add((String) obj);
                }
            }
            for (String s : indexObj.getAccessConditions()) {
                if (!existingAccessConditions.contains(s)) {
                    pageDoc.addField(SolrConstants.ACCESSCONDITION, s);
                }
            }
            if (indexObj.getAccessConditions().isEmpty()) {
                logger.warn("{}: {} has no access conditions.", pageFileBaseName, indexObj.getIddoc());
            }

            // Add owner docstruct's metadata (tokenized only!) and SORT_* fields to the page
            Set<String> existingMetadataFieldNames = new HashSet<>();
            Set<String> existingSortFieldNames = new HashSet<>();
            for (String fieldName : pageDoc.getFieldNames()) {
                if (Configuration.getInstance().getMetadataConfigurationManager().getFieldsToAddToPages().contains(fieldName)) {
                    for (Object value : pageDoc.getFieldValues(fieldName)) {
                        existingMetadataFieldNames.add(new StringBuilder(fieldName).append(String.valueOf(value)).toString());
                    }
                } else if (fieldName.startsWith(SolrConstants.SORT_)) {
                    existingSortFieldNames.add(fieldName);
                }
            }
            for (LuceneField field : indexObj.getLuceneFields()) {
                if (Configuration.getInstance().getMetadataConfigurationManager().getFieldsToAddToPages().contains(field.getField())
                        && !existingMetadataFieldNames.contains(new StringBuilder(field.getField()).append(field.getValue()).toString())) {
                    // Avoid duplicates (same field name + value)
                    pageDoc.addField(field.getField(), field.getValue());
                    logger.debug("Added {}:{} to page {}", field.getField(), field.getValue(), pageDoc.getFieldValue(SolrConstants.ORDER));
                } else if (field.getField().startsWith(SolrConstants.SORT_) && !existingSortFieldNames.contains(field.getField())) {
                    // Only one instance of each SORT_ field may exist
                    pageDoc.addField(field.getField(), field.getValue());
                }
            }

            // Update the doc in the write strategy (otherwise some implementations might ignore the changes).
            writeStrategy.updateDoc(pageDoc);
        }

        // Add the number of assigned pages and the labels of the first and last page to this structure element
        indexObj.setNumPages(pageDocs.size());
        if (!pageDocs.isEmpty()) {
            SolrInputDocument lastPagedoc = pageDocs.get(pageDocs.size() - 1);
            String firstPageLabel = (String) firstPageDoc.getFieldValue(SolrConstants.ORDERLABEL);
            String lastPageLabel = (String) lastPagedoc.getFieldValue(SolrConstants.ORDERLABEL);
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
            if (generatePageDocument(eleImage, String.valueOf(getNextIddoc(hotfolder.getSearchIndex())), order, writeStrategy,
                    dataFolders)) {
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
     * @param writeStrategy
     * @param dataFolders
     * @return
     * @throws FatalIndexerException
     */
    boolean generatePageDocument(Element eleImage, String iddoc, Integer order, ISolrWriteStrategy writeStrategy, Map<String, Path> dataFolders)
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

        // Create Solr document for this page
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(SolrConstants.IDDOC, iddoc);
        doc.addField(SolrConstants.GROUPFIELD, iddoc);
        doc.addField(SolrConstants.DOCTYPE, DocType.PAGE.name());
        doc.addField(SolrConstants.ORDER, order);
        doc.addField(SolrConstants.PHYSID, String.valueOf(order));

        doc.addField(SolrConstants.ORDERLABEL, String.valueOf(order));

        // URL
        String fileName = eleImage.getText();

        // Mime type
        parseMimeType(doc, fileName);

        // Add file size
        try {
            Path dataFolder = dataFolders.get(DataRepository.PARAM_MEDIA);
            // TODO other mime types/folders
            if (dataFolder != null) {
                Path path = Paths.get(dataFolder.toAbsolutePath().toString(), fileName);
                if (Files.isRegularFile(path)) {
                    doc.addField(FIELD_FILESIZE, Files.size(path));
                }
            }
        } catch (IllegalArgumentException | IOException e) {
            logger.warn(e.getMessage());
        }
        if (!doc.containsKey(FIELD_FILESIZE)) {
            doc.addField(FIELD_FILESIZE, -1);
        }

        // Add image dimension values from EXIF
        if (!doc.containsKey(SolrConstants.WIDTH) || !doc.containsKey(SolrConstants.HEIGHT)) {
            getSize(dataFolders.get(DataRepository.PARAM_MEDIA), (String) doc.getFieldValue(SolrConstants.FILENAME)).ifPresent(dimension -> {
                doc.addField(SolrConstants.WIDTH, dimension.width);
                doc.addField(SolrConstants.HEIGHT, dimension.height);
            });
        }

        // FIELD_IMAGEAVAILABLE indicates whether this page has an image
        if (doc.containsKey(SolrConstants.FILENAME) && doc.containsKey(SolrConstants.MIMETYPE)
                && ((String) doc.getFieldValue(SolrConstants.MIMETYPE)).startsWith("image")) {
            doc.addField(FIELD_IMAGEAVAILABLE, true);
            recordHasImages = true;
        } else {
            doc.addField(FIELD_IMAGEAVAILABLE, false);
        }

        // FULLTEXTAVAILABLE indicates whether this page has full-text
        if (doc.getField(SolrConstants.FULLTEXT) != null) {
            doc.addField(SolrConstants.FULLTEXTAVAILABLE, true);
            recordHasFulltext = true;
        } else {
            doc.addField(SolrConstants.FULLTEXTAVAILABLE, false);
        }

        writeStrategy.addPageDoc(doc);
        return true;
    }

    /**
     * Sets DMDID, ID, TYPE and LABEL from the METS document.
     * 
     * @param indexObj {@link IndexObject}
     * @throws FatalIndexerException
     */
    private static void setSimpleData(IndexObject indexObj) throws FatalIndexerException {
        logger.trace("setSimpleData(IndexObject) - start");

        // LOGID
        indexObj.setLogId("LOD_0000");
        logger.trace("LOGID: {}", indexObj.getLogId());

        // TYPE
        indexObj.setType("record");
        logger.trace("TYPE: {}", indexObj.getType());

        // LABEL
        {
            String value = TextHelper
                    .normalizeSequence(indexObj.getRootStructNode().getChildText("title", Configuration.getInstance().getNamespaces().get("dc")));
            if (value != null) {
                // Remove non-sort characters from LABEL, if configured to do so
                if (Configuration.getInstance().isLabelCleanup()) {
                    value = value.replace("<ns>", "");
                    value = value.replace("</ns>", "");
                    value = value.replace("<<", "");
                    value = value.replace(">>", "");
                    value = value.replace("¬", "");
                }
                indexObj.setLabel(value);
            }
        }
        logger.trace("LABEL: {}", indexObj.getLabel());
    }

    /**
     * Finds all physical page URNs for the given IndexObject and adds them to its metadata sequentially as one string. Should only be used with the
     * top docstruct (ISWORK). TODO get from generated pages instead of METS.
     * 
     * @param indexObj The IndexObject to find URNs for.
     * @throws FatalIndexerException
     */
    private void generatePageUrns(IndexObject indexObj) throws FatalIndexerException {
        String query1 = "/mets:mets/mets:structMap[@TYPE='PHYSICAL']/mets:div[@TYPE='physSequence']/mets:div/@CONTENTIDS";
        List<String> physUrnList = xp.evaluateToStringList(query1, null);
        if (physUrnList != null) {
            StringBuilder sbPageUrns = new StringBuilder();
            List<String> imageUrns = new ArrayList<>(physUrnList.size());
            for (String pageUrn : physUrnList) {
                String urn = null;
                if (Utils.isUrn(pageUrn)) {
                    urn = pageUrn.replaceAll("[\\\\]", "");
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
     * @return
     * @throws FatalIndexerException
     */
    private String setUrn(IndexObject indexObj) throws FatalIndexerException {
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
                // Java NIO is non-blocking, so copying a file in one call and then deleting it in a second might run into problems. Instead, move the file.
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
}
