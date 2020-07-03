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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.model.FatalIndexerException;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.IndexerException;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.LazySolrWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.SerializingSolrWriteStrategy;

/**
 * Indexer implementation for WorldViews documents.
 */
public class WorldViewsIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(WorldViewsIndexer.class);

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

    /** Constant <code>fulltextCharset="DEFAULT_FULLTEXT_CHARSET"</code> */
    public static String fulltextCharset = DEFAULT_FULLTEXT_CHARSET;

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

    /**
     * Indexes the given WorldViews file.
     *
     * @param mainFile {@link java.nio.file.Path}
     * @param fromReindexQueue a boolean.
     * @param dataFolders a {@link java.util.Map} object.
     * @param pageCountStart Order number for the first page.
     * @should index record correctly
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @return an array of {@link java.lang.String} objects.
     */
    public String[] index(Path mainFile, boolean fromReindexQueue, Map<String, Path> dataFolders, ISolrWriteStrategy writeStrategy,
            int pageCountStart) {
        String[] ret = { null, null };

        if (mainFile == null || !Files.exists(mainFile)) {
            throw new IllegalArgumentException("mainFile must point to an existing XML file.");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null.");
        }

        try {
            initJDomXP(mainFile);
            IndexObject indexObj = new IndexObject(getNextIddoc(hotfolder.getSearchIndex()));
            logger.debug("IDDOC: {}", indexObj.getIddoc());

            // Set PI
            {
                String pi = xp.evaluateToString("worldviews//identifier/text()", null);
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
                                    .selectDataRepository(pi, mainFile, dataFolders, hotfolder.getSearchIndex(), hotfolder.getOldSearchIndex());
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
                boolean useSerializingStrategy = false;
                long size = Files.size(mainFile);
                if (size >= hotfolder.metsFileSizeThreshold) {
                    useSerializingStrategy = true;
                    logger.info("WorldViews file is {} bytes, using a slower Solr write strategy to avoid memory overflows.", size);
                } else {
                    for (String key : dataFolders.keySet()) {
                        switch (key) {
                            case DataRepository.PARAM_ALTO:
                            case DataRepository.PARAM_ALTOCROWD:
                            case DataRepository.PARAM_FULLTEXT:
                            case DataRepository.PARAM_FULLTEXTCROWD:
                            case DataRepository.PARAM_ABBYY:
                            case DataRepository.PARAM_TEIWC:
                            case DataRepository.PARAM_TEIMETADATA:
                                Path dataFolder = dataFolders.get(key);
                                if (dataFolder != null) {
                                    // Files.size() does not work with directories, so use FileUtils
                                    long dataFolderSize = FileUtils.sizeOfDirectory(dataFolder.toFile());
                                    if (dataFolderSize >= hotfolder.dataFolderSizeThreshold) {
                                        useSerializingStrategy = true;
                                        logger.info("Data folder '{}' is {} bytes, using a slower Solr write strategy to avoid memory overflows.",
                                                dataFolder.toAbsolutePath().toString(), dataFolderSize);
                                        break;
                                    }
                                }
                                break;
                            default:
                                // do nothing
                        }
                    }
                }
                if (useSerializingStrategy) {
                    writeStrategy = new SerializingSolrWriteStrategy(hotfolder.getSearchIndex(), hotfolder.getTempFolder());

                }
                //                else if (IndexerConfig.getInstance().getBoolean("init.aggregateRecords")) {
                //                    writeStrategy = new HierarchicalLazySolrWriteStrategy(hotfolder.getSolrHelper());
                //                }
                else {
                    writeStrategy = new LazySolrWriteStrategy(hotfolder.getSearchIndex());
                }
            } else {
                logger.info("Solr write strategy injected by caller: {}", writeStrategy.getClass().getName());
            }

            // Set source doc format
            indexObj.addToLucene(SolrConstants.SOURCEDOCFORMAT, FileFormat.WORLDVIEWS.name());

            prepareUpdate(indexObj);

            // Docstruct
            {
                String docstructType = xp.evaluateToString("worldviews//docType/text()", null);
                if (docstructType != null) {
                    indexObj.setType(docstructType);
                } else {
                    indexObj.setType("Monograph");
                    logger.warn("<docType> not found, setting docstruct type to 'Monograph'.");
                }
            }
            // LOGID
            indexObj.setLogId("LOG_0000");
            // Collections / access conditions
            {
                List<String> collections = xp.evaluateToStringList("worldviews//collection", null);
                if (collections != null && !collections.isEmpty()) {
                    for (String collection : collections) {
                        indexObj.addToLucene(SolrConstants.DC, collection);
                        indexObj.addToLucene(SolrConstants.ACCESSCONDITION, collection);
                    }
                }
            }
            // MD_WV_*SOURCE
            {
                String sourcePi = xp.evaluateToString("worldviews//relatedItem[@type='primarySource']/identifier/text()", null);
                if (sourcePi != null) {
                    indexObj.addToLucene("MD_WV_PRIMARYSOURCE", sourcePi);
                } else {
                    // For sources use own PI
                    indexObj.addToLucene("MD_WV_PRIMARYSOURCE", indexObj.getPi());
                }
            }
            {
                String sourcePi = xp.evaluateToString("worldviews//relatedItem[@type='secondarySource']/identifier/text()", null);
                if (sourcePi != null) {
                    indexObj.addToLucene("MD_WV_SECONDARYSOURCE", sourcePi);
                }
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
            indexObj.writeDateModified(!fromReindexQueue && !noTimestampUpdate);

            // Generate docs for all pages and add to the write strategy
            generatePageDocuments(writeStrategy, dataFolders, pageCountStart);
            indexObj.setNumPages(writeStrategy.getPageDocsSize());
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

            // Add THUMBNAIL,THUMBPAGENO,THUMBPAGENOLABEL (must be done AFTER writeDateMondified(), writeAccessConditions() and generatePageDocuments()!)
            generateChildDocstructDocuments(indexObj, true, writeStrategy, dataFolders, workDepth);

            // ISWORK only for non-anchors
            indexObj.addToLucene(SolrConstants.ISWORK, "true");

            // Add DEFAULT field
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(indexObj.getDefaultValue()));
                // indexObj.getSuperDefaultBuilder().append(' ').append(indexObj.getDefaultValue().trim());
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
                            String content = TextHelper.readFileToString(file.toFile(), null);
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
                for (LuceneField field : indexObj.getLuceneFields()) {
                    if (titleField.equals(field.getField())) {
                        // Add title/label
                        moreMetadata.put("LABEL", field.getValue());
                        moreMetadata.put("MD_TITLE", field.getValue());
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

            // Add grouped metadata as separate Solr docs (remove duplicates first)
            indexObj.removeDuplicateGroupedMetadata();
            addGroupedMetadataDocs(writeStrategy, indexObj);

            boolean indexedChildrenFileList = false;

            logger.debug("reindexedChildrenFileList.size(): {}", WorldViewsIndexer.reindexedChildrenFileList.size());
            if (WorldViewsIndexer.reindexedChildrenFileList.contains(mainFile)) {
                logger.debug("{} in reindexedChildrenFileList, removing...", mainFile.toAbsolutePath());
                WorldViewsIndexer.reindexedChildrenFileList.remove(mainFile);
                indexedChildrenFileList = true;
            }

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)
            logger.debug("Writing document to index...");
            SolrInputDocument rootDoc = SolrSearchIndex.createDocument(indexObj.getLuceneFields());
            writeStrategy.setRootDoc(rootDoc);
            writeStrategy.writeDocs(Configuration.getInstance().isAggregateRecords());
            if (indexObj.isVolume() && (!indexObj.isUpdate() || indexedChildrenFileList)) {
                logger.info("Re-indexing anchor...");
                copyAndReIndexAnchor(indexObj, hotfolder, dataRepository);
            }
            logger.info("Successfully finished indexing '{}'.", mainFile.getFileName());
        } catch (Exception e) {
            logger.error("Indexing of '{}' could not be finished due to an error.", mainFile.getFileName());
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
     * Generates thumbnail info fields for the given docstruct. Also generates page docs mapped to this docstruct. <code>IndexObj.topstructPi</code>
     * must be set before calling this method.
     * 
     * @param indexObj {@link IndexObject}
     * @param isWork
     * @param writeStrategy
     * @param dataFolders
     * @param depth Depth of the current docstruct in the docstruct hierarchy.
     * @return {@link LuceneField}
     * @throws IndexerException -
     * @throws IOException
     * @throws FatalIndexerException
     */
    private void generateChildDocstructDocuments(IndexObject rootIndexObj, boolean isWork, ISolrWriteStrategy writeStrategy,
            Map<String, Path> dataFolders, int depth) throws IndexerException, IOException, FatalIndexerException {
        IndexObject currentIndexObj = null;
        String currentDocstructLabel = null;

        String firstPageFile = null;
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
            String pageFileBaseName = FilenameUtils.getBaseName((String) pageDoc.getFieldValue(SolrConstants.FILENAME));

            // New docstruct
            if (pageDocstructLabel != null && !pageDocstructLabel.equals(currentDocstructLabel)) {
                currentDocstructLabel = pageDocstructLabel;
                docstructCount++;

                // Finalize previous docstruct
                if (currentIndexObj != null) {
                    finalizeChildDocstruct(currentIndexObj, dataFolders, writeStrategy);
                }

                // Create new docstruct object
                currentIndexObj = new IndexObject(getNextIddoc(hotfolder.getSearchIndex()));
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
                    if (Configuration.getInstance().getMetadataConfigurationManager().getFieldsToAddToChildren().contains(field.getField())) {
                        existingMetadataFields.add(new StringBuilder(field.getField()).append(field.getValue()).toString());
                    } else if (field.getField().startsWith(SolrConstants.SORT_)) {
                        existingSortFieldNames.add(field.getField());
                    }
                }
                for (LuceneField field : rootIndexObj.getLuceneFields()) {
                    if (Configuration.getInstance().getMetadataConfigurationManager().getFieldsToAddToChildren().contains(field.getField())
                            && !existingMetadataFields.contains(new StringBuilder(field.getField()).append(field.getValue()).toString())) {
                        // Avoid duplicates (same field name + value)
                        currentIndexObj.addToLucene(field.getField(), field.getValue());
                        logger.debug("Added {}:{} to child element {}", field.getField(), field.getValue(), currentIndexObj.getLogId());
                    } else if (field.getField().startsWith(SolrConstants.SORT_) && !existingSortFieldNames.contains(field.getField())) {
                        // Only one instance of each SORT_ field may exist
                        currentIndexObj.addToLucene(field.getField(), field.getValue());
                    }
                }

                currentIndexObj.writeAccessConditions(rootIndexObj);

                // Add grouped metadata as separate documents
                addGroupedMetadataDocs(writeStrategy, currentIndexObj);

                // Add own and all ancestor LABEL values to the DEFAULT field
                StringBuilder sbDefaultValue = new StringBuilder();
                sbDefaultValue.append(currentIndexObj.getDefaultValue());
                String labelWithSpaces = new StringBuilder(" ").append(currentIndexObj.getLabel()).append(' ').toString();
                if (StringUtils.isNotEmpty(currentIndexObj.getLabel()) && !sbDefaultValue.toString().contains(labelWithSpaces)) {
                    // logger.info("Adding own LABEL to DEFAULT: " + indexObj.getLabel());
                    sbDefaultValue.append(labelWithSpaces);
                }
                if (Configuration.getInstance().isAddLabelToChildren()) {
                    for (String label : currentIndexObj.getParentLabels()) {
                        String parentLabelWithSpaces = new StringBuilder(" ").append(label).append(' ').toString();
                        if (StringUtils.isNotEmpty(label) && !sbDefaultValue.toString().contains(parentLabelWithSpaces)) {
                            // logger.info("Adding ancestor LABEL to DEFAULT: " + label);
                            sbDefaultValue.append(parentLabelWithSpaces);
                        }
                    }
                }

                currentIndexObj.setDefaultValue(sbDefaultValue.toString());

                // Add DEFAULT field
                if (StringUtils.isNotEmpty(currentIndexObj.getDefaultValue())) {
                    currentIndexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(currentIndexObj.getDefaultValue()));
                    // Add default value to parent doc
                    // parentIndexObject.getSuperDefaultBuilder().append(' ').append(indexObj.getDefaultValue().trim());
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
            for (LuceneField field : currentIndexObj.getLuceneFields()) {
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

            // Make sure IDDOC_OWNER of a page contains the iddoc of the lowest possible mapped docstruct
            if (pageDoc.getField("MDNUM_OWNERDEPTH") == null || depth > (Integer) pageDoc.getFieldValue("MDNUM_OWNERDEPTH")) {
                pageDoc.setField(SolrConstants.IDDOC_OWNER, String.valueOf(currentIndexObj.getIddoc()));
                pageDoc.setField("MDNUM_OWNERDEPTH", depth);

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
                if (currentIndexObj.getIddoc() == Long.valueOf((String) pageDoc.getFieldValue(SolrConstants.IDDOC_OWNER))) {
                    for (LuceneField field : currentIndexObj.getLuceneFields()) {
                        if (field.getField().startsWith(SolrConstants.SORT_)) {
                            pageDoc.addField(field.getField(), field.getValue());
                        }
                    }
                }
            }

            // Update the doc in the write strategy (otherwise some implementations might ignore the changes).
            writeStrategy.updateDoc(pageDoc);
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
     * @param pageCountStart a int.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should create documents for all mapped pages
     * @should set correct ORDER values
     * @should skip unmapped pages
     * @should switch to DEFAULT file group correctly
     * @should maintain page order after parallel processing
     */
    public void generatePageDocuments(final ISolrWriteStrategy writeStrategy, final Map<String, Path> dataFolders, int pageCountStart)
            throws FatalIndexerException {
        // Get all physical elements
        List<Element> eleListImages = xp.evaluateToElements("worldviews/resource/images/image", null);
        logger.info("Generating {} page documents (count starts at {})...", eleListImages.size(), pageCountStart);

        if (Configuration.getInstance().getThreads() > 1) {
            ExecutorService executor = Executors.newFixedThreadPool(Configuration.getInstance().getThreads());
            for (final Element eleImage : eleListImages) {

                // Generate each page document in its own thread
                Runnable r = new Runnable() {

                    @Override
                    public void run() {
                        try {
                            generatePageDocument(eleImage, String.valueOf(getNextIddoc(hotfolder.getSearchIndex())), null, writeStrategy,
                                    dataFolders);
                        } catch (FatalIndexerException e) {
                            logger.error("Should be exiting here now...");
                        } finally {
                        }
                    }
                };
                executor.execute(r);
            }
            executor.shutdown();
            while (!executor.isTerminated()) {
            }

            // TODO lambda instead of loop with Java 8
            //        eleStructMapPhysicalList.parallelStream().forEach(
            //                eleStructMapPhysical -> generatePageDocument(eleStructMapPhysical, String.valueOf(getNextIddoc(hotfolder.getSolrHelper())), null,
            //                        writeStrategy, dataFolders));
        } else {
            int order = pageCountStart;
            for (final Element eleImage : eleListImages) {
                if (generatePageDocument(eleImage, String.valueOf(getNextIddoc(hotfolder.getSearchIndex())), order, writeStrategy, dataFolders)) {
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
     * @param order
     * @param writeStrategy
     * @param dataFolders
     * @return
     * @throws FatalIndexerException
     * @should add all basic fields
     * @should add page metadata correctly
     */
    boolean generatePageDocument(Element eleImage, String iddoc, Integer order, ISolrWriteStrategy writeStrategy, Map<String, Path> dataFolders)
            throws FatalIndexerException {
        String id = eleImage.getAttributeValue("ID");
        if (order == null) {
            order = Integer.parseInt(eleImage.getChildText("sequence"));
        }
        logger.trace("generatePageDocument: {} (IDDOC {}) processed by thread {}", order, iddoc, Thread.currentThread().getId());

        // Create Solr document for this page
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(SolrConstants.IDDOC, iddoc);
        doc.addField(SolrConstants.GROUPFIELD, iddoc);
        doc.addField(SolrConstants.DOCTYPE, DocType.PAGE.name());
        doc.addField(SolrConstants.PHYSID, "PHYS_" + MetadataHelper.FORMAT_FOUR_DIGITS.get().format(order));
        doc.addField(SolrConstants.ORDER, order);
        doc.addField(SolrConstants.ORDERLABEL, Configuration.getInstance().getEmptyOrderLabelReplacement());

        boolean displayImage = Boolean.valueOf(eleImage.getChildText("displayImage"));
        if (displayImage) {
            // Add file name
            String fileName = eleImage.getChildText("fileName");
            doc.addField(SolrConstants.FILENAME, fileName);

            // Add file size
            if (dataFolders != null) {
                try {
                    Path dataFolder = dataFolders.get(DataRepository.PARAM_MEDIA);
                    // TODO other mime types/folders
                    if (dataFolder != null) {
                        Path path = Paths.get(dataFolder.toAbsolutePath().toString(), fileName);
                        doc.addField("MDNUM_FILESIZE", Files.size(path));
                    } else {
                        doc.addField("MDNUM_FILESIZE", -1);
                    }
                } catch (FileNotFoundException e) {
                    logger.error(e.getMessage());
                    doc.addField("MDNUM_FILESIZE", -1);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    doc.addField("MDNUM_FILESIZE", -1);
                } catch (IllegalArgumentException e) {
                    logger.error(e.getMessage(), e);
                    doc.addField("MDNUM_FILESIZE", -1);
                }
            }

            // Representative image
            boolean representative = Boolean.valueOf(eleImage.getChildText("representative"));
            if (representative) {
                doc.addField(SolrConstants.THUMBNAILREPRESENT, fileName);
            }
        } else {
            // TODO placeholder
            String placeholder = eleImage.getChildText("placeholder");
        }
        doc.addField(SolrConstants.MIMETYPE, "image");

        // FIELD_IMAGEAVAILABLE indicates whether this page has an image
        if (doc.containsKey(SolrConstants.FILENAME) && doc.containsKey(SolrConstants.MIMETYPE)
                && ((String) doc.getFieldValue(SolrConstants.MIMETYPE)).startsWith("image")) {
            doc.addField(FIELD_IMAGEAVAILABLE, true);
            recordHasImages = true;
        } else {
            doc.addField(FIELD_IMAGEAVAILABLE, false);
        }

        // Copyright
        String copyright = eleImage.getChildText("copyright");
        if (StringUtils.isNotEmpty(copyright)) {
            doc.addField("MD_COPYRIGHT", copyright);
        }
        // access condition
        String license = eleImage.getChildText("licence");
        if (StringUtils.isNotEmpty(license)) {
            doc.addField(SolrConstants.ACCESSCONDITION, license);
        }

        // Metadata payload for later evaluation
        String structType = eleImage.getChildText("structType");
        if (structType != null) {
            doc.addField(SolrConstants.LABEL, structType);
            doc.addField(SolrConstants.DOCSTRCT, "OtherDocStrct"); // TODO
        }

        if (dataFolders != null) {
            Map<String, Object> altoData = null;
            String baseFileName = FilenameUtils.getBaseName((String) doc.getFieldValue(SolrConstants.FILENAME));

            // Add complete crowdsourcing ALTO document and full-text generated from ALTO, if available
            boolean foundCrowdsourcingData = false;
            if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) != null) {
                try {
                    altoData = TextHelper.readAltoFile(
                            new File(dataFolders.get(DataRepository.PARAM_ALTOCROWD).toAbsolutePath().toString(), baseFileName + XML_EXTENSION));
                } catch (FileNotFoundException e) {
                    // Not all pages will have custom ALTO docs
                } catch (JDOMException | IOException e) {
                    logger.error(e.getMessage(), e);
                }
                if (altoData != null) {
                    foundCrowdsourcingData = true;
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO))) {
                        doc.addField(SolrConstants.ALTO, altoData.get(SolrConstants.ALTO));
                        doc.addField(SolrConstants.FILENAME_ALTO, baseFileName + XML_EXTENSION);
                        logger.debug("Added ALTO from crowdsourcing ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT))) {
                        doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags((String) altoData.get(SolrConstants.FULLTEXT)));
                        // doc.addField("MD_FULLTEXT", altoData.get(SolrConstants.FULLTEXT));
                        logger.debug("Added FULLTEXT from crowdsourcing ALTO for page {}", order);
                    }
                    //Getting width/height from ALTO is unreliable. Rather get them from the image itself (see below)
                    //                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                    //                        doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                    //                        logger.debug("Added WIDTH from crowdsourcing ALTO for page {}", order);
                    //                    }
                    //                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                    //                        doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                    //                        logger.debug("Added WIDTH from crowdsourcing ALTO for page {}", order);
                    //                    }
                    if (altoData.get(SolrConstants.NAMEDENTITIES) != null) {
                        addNamedEntitiesFields(altoData, doc);
                    }
                }
            }

            // Look for plain fulltext from crowdsouring, if the FULLTEXT field is still empty
            if (doc.getField(SolrConstants.FULLTEXT) == null && dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) != null) {
                String fulltext =
                        TextHelper.generateFulltext(baseFileName + TXT_EXTENSION, dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD), false);
                if (fulltext != null) {
                    foundCrowdsourcingData = true;
                    doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags(fulltext));
                    // doc.addField("MD_FULLTEXT", fulltext);
                    doc.addField(SolrConstants.FILENAME_FULLTEXT, baseFileName + TXT_EXTENSION);
                    logger.debug("Added FULLTEXT from crowdsourcing plain text for page {}", order);
                }
            }
            // Look for a regular ALTO document for this page and fill ALTO and/or FULLTEXT fields, whichever is still empty
            if (!foundCrowdsourcingData && (doc.getField(SolrConstants.ALTO) == null || doc.getField(SolrConstants.FULLTEXT) == null)
                    && dataFolders.get(DataRepository.PARAM_ALTO) != null) {
                try {
                    altoData = TextHelper.readAltoFile(
                            new File(dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), baseFileName + XML_EXTENSION));
                } catch (JDOMException e) {
                    logger.error(e.getMessage(), e);
                } catch (IOException e) {
                    logger.warn(e.getMessage());
                }
                if (altoData != null) {
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO)) && doc.getField(SolrConstants.ALTO) == null) {
                        doc.addField(SolrConstants.ALTO, altoData.get(SolrConstants.ALTO));
                        doc.addField(SolrConstants.FILENAME_ALTO, baseFileName + XML_EXTENSION);
                        logger.debug("Added ALTO from regular ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT)) && doc.getField(SolrConstants.FULLTEXT) == null) {
                        doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags((String) altoData.get(SolrConstants.FULLTEXT)));
                        // doc.addField("MD_FULLTEXT", altoData.get(SolrConstants.FULLTEXT));
                        logger.debug("Added FULLTEXT from regular ALTO for page {}", order);
                    }
                    //Getting width/height from ALTO is unreliable. Rather get them from the image itself (see below)
                    //                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                    //                        doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                    //                        logger.debug("Added WIDTH from regular ALTO for page {}", order);
                    //                    }
                    //                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                    //                        doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                    //                        logger.debug("Added WIDTH from regular ALTO for page {}", order);
                    //                    }
                    if (altoData.get(SolrConstants.NAMEDENTITIES) != null) {
                        addNamedEntitiesFields(altoData, doc);
                    }
                }
            }

            // If FULLTEXT is still empty, look for a plain full-text
            if (!foundCrowdsourcingData && doc.getField(SolrConstants.FULLTEXT) == null && dataFolders.get(DataRepository.PARAM_FULLTEXT) != null) {
                String fulltext = TextHelper.generateFulltext(baseFileName + TXT_EXTENSION, dataFolders.get(DataRepository.PARAM_FULLTEXT), true);
                if (fulltext != null) {
                    doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags(fulltext));
                    // doc.addField("MD_FULLTEXT", fulltext);
                    doc.addField(SolrConstants.FILENAME_FULLTEXT, baseFileName + TXT_EXTENSION);
                    logger.debug("Added FULLTEXT from regular plain text for page {}", order);
                }
            }

            // ABBYY XML (converted to ALTO)
            if (!foundCrowdsourcingData && dataFolders.get(DataRepository.PARAM_ABBYY) != null) {
                try {
                    try {
                        altoData = TextHelper.readAbbyyToAlto(
                                new File(dataFolders.get(DataRepository.PARAM_ABBYY).toAbsolutePath().toString(), baseFileName + XML_EXTENSION));
                        if (altoData != null) {
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO)) && doc.getField(SolrConstants.ALTO) == null) {
                                doc.addField(SolrConstants.ALTO, altoData.get(SolrConstants.ALTO));
                                logger.debug("Added ALTO from regular ALTO for page {}", order);
                            }
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT))
                                    && doc.getField(SolrConstants.FULLTEXT) == null) {
                                doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags((String) altoData.get(SolrConstants.FULLTEXT)));
                                doc.addField("MD_FULLTEXT", altoData.get(SolrConstants.FULLTEXT));
                                logger.debug("Added FULLTEXT from regular ALTO for page {}", order);
                            }
                            //Getting width/height from ALTO is unreliable. Rather get them from the image itself (see below)
                            //                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                            //                                doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                            //                                logger.debug("Added WIDTH from regular ALTO for page {}", order);
                            //                            }
                            //                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                            //                                doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                            //                                logger.debug("Added WIDTH from regular ALTO for page {}", order);
                            //                            }
                            if (altoData.get(SolrConstants.NAMEDENTITIES) != null) {
                                addNamedEntitiesFields(altoData, doc);
                            }
                        }
                    } catch (FileNotFoundException e) {
                        logger.warn(e.getMessage());
                    }
                } catch (XMLStreamException e) {
                    logger.error(e.getMessage(), e);
                } catch (IOException e) {
                    logger.warn(e.getMessage());
                }
            }

            // Read word coords from TEI only if none has been read from ALTO for this page yet
            if (!foundCrowdsourcingData && dataFolders.get(DataRepository.PARAM_TEIWC) != null) {
                try {
                    altoData = TextHelper.readTeiToAlto(
                            new File(dataFolders.get(DataRepository.PARAM_TEIWC).toAbsolutePath().toString(), baseFileName + XML_EXTENSION));
                    if (altoData != null) {
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO)) && doc.getField(SolrConstants.ALTO) == null) {
                            doc.addField(SolrConstants.ALTO, altoData.get(SolrConstants.ALTO));
                            doc.addField(SolrConstants.FILENAME_ALTO, baseFileName + XML_EXTENSION);
                            logger.debug("Added ALTO from regular ALTO for page {}", order);
                        }
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT)) && doc.getField(SolrConstants.FULLTEXT) == null) {
                            doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags((String) altoData.get(SolrConstants.FULLTEXT)));
                            // doc.addField("MD_FULLTEXT", altoData.get(SolrConstants.FULLTEXT));
                            logger.debug("Added FULLTEXT from regular ALTO for page {}", order);

                        }
                        //Getting width/height from ALTO is unreliable. Rather get them from the image itself (see below)
                        //                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                        //                            doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                        //                            logger.debug("Added WIDTH from regular ALTO for page {}", order);
                        //                        }
                        //                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                        //                            doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                        //                            logger.debug("Added WIDTH from regular ALTO for page {}", order);
                        //                        }
                        if (altoData.get(SolrConstants.NAMEDENTITIES) != null) {
                            addNamedEntitiesFields(altoData, doc);
                        }
                    }
                } catch (JDOMException e) {
                    logger.error(e.getMessage(), e);
                } catch (IOException e) {
                    logger.warn(e.getMessage());
                }
            }

            if (dataFolders.get(DataRepository.PARAM_MIX) != null) {
                try {
                    Map<String, String> mixData = TextHelper
                            .readMix(new File(dataFolders.get(DataRepository.PARAM_MIX).toAbsolutePath().toString(), baseFileName + XML_EXTENSION));
                    for (String key : mixData.keySet()) {
                        if (!(key.equals(SolrConstants.WIDTH) && doc.getField(SolrConstants.WIDTH) != null)
                                && !(key.equals(SolrConstants.HEIGHT) && doc.getField(SolrConstants.HEIGHT) != null)) {
                            doc.addField(key, mixData.get(key));
                        }
                    }
                } catch (JDOMException e) {
                    logger.error(e.getMessage(), e);
                } catch (IOException e) {
                    logger.warn(e.getMessage());
                }
            }

            //get width/height from image file if it is an actual image and width/height haven't been set
            if (doc.getField(SolrConstants.HEIGHT) == null || doc.getField(SolrConstants.WIDTH) == null) {
                getSize(dataFolders.get(DataRepository.PARAM_MEDIA), (String) doc.getFieldValue(SolrConstants.FILENAME)).ifPresent(dimension -> {
                    doc.addField(SolrConstants.WIDTH, dimension.width);
                    doc.addField(SolrConstants.HEIGHT, dimension.height);
                });
            }

            // FULLTEXTAVAILABLE indicates whether this page has full-text
            if (doc.getField(SolrConstants.FULLTEXT) != null) {
                doc.addField(SolrConstants.FULLTEXTAVAILABLE, true);
                recordHasFulltext = true;
            } else {
                doc.addField(SolrConstants.FULLTEXTAVAILABLE, false);
            }
        }

        writeStrategy.addPageDoc(doc);
        return true;
    }

    /**
     * Adds the anchor for the given volume object to the re-index queue.
     * 
     * @param indexObj {@link IndexObject}
     * @param hotfolder
     * @param dataRepository
     * @throws UnsupportedEncodingException
     */
    static void copyAndReIndexAnchor(IndexObject indexObj, Hotfolder hotfolder, DataRepository dataRepository) throws UnsupportedEncodingException {
        logger.debug("copyAndReIndexAnchor: {}", indexObj.getPi());
        if (indexObj.getParent() != null) {
            String piParent = indexObj.getParent().getPi();
            String indexedAnchorFilePath =
                    new StringBuilder(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString()).append("/")
                            .append(piParent)
                            .append(Indexer.XML_EXTENSION)
                            .toString();
            Path indexedAnchor = Paths.get(indexedAnchorFilePath);
            if (Files.exists(indexedAnchor)) {
                hotfolder.getReindexQueue().add(indexedAnchor);
            }
        } else {
            logger.warn("No anchor file has been indexed for this work yet.");
        }
    }

    /**
     * Prepares the given record for an update. Creation timestamp and representative thumbnail and anchor IDDOC are preserved. A new update timestamp
     * is added, child docs are removed.
     *
     * @param indexObj {@link io.goobi.viewer.indexer.model.IndexObject}
     * @throws java.io.IOException
     * @throws org.apache.solr.client.solrj.SolrServerException
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should keep creation timestamp
     * @should set update timestamp correctly
     * @should keep representation thumbnail
     * @should keep anchor IDDOC
     * @should delete anchor secondary docs
     */
    protected void prepareUpdate(IndexObject indexObj) throws IOException, SolrServerException, FatalIndexerException {
        String pi = indexObj.getPi().trim();
        SolrDocumentList hits = hotfolder.getSearchIndex().search(SolrConstants.PI + ":" + pi, null);
        // Retrieve record from old index, if available
        boolean fromOldIndex = false;
        if (hits.getNumFound() == 0 && hotfolder.getOldSearchIndex() != null) {
            hits = hotfolder.getOldSearchIndex().search(SolrConstants.PI + ":" + pi, null);
            if (hits.getNumFound() > 0) {
                fromOldIndex = true;
                logger.info("Retrieving data from old index for record '{}'.", pi);
            }
        }
        if (hits.getNumFound() == 0) {
            return;
        }

        logger.debug("This file has already been indexed, initiating an UPDATE instead...");
        indexObj.setUpdate(true);
        SolrDocument doc = hits.get(0);
        // Set creation timestamp, if exists (should never be updated)
        Object dateCreated = doc.getFieldValue(SolrConstants.DATECREATED);
        if (dateCreated != null) {
            // Set creation timestamp, if exists (should never be updated)
            indexObj.setDateCreated((Long) dateCreated);
        }
        // Set update timestamp
        Collection<Object> dateUpdatedValues = doc.getFieldValues(SolrConstants.DATEUPDATED);
        if (dateUpdatedValues != null) {
            for (Object date : dateUpdatedValues) {
                indexObj.getDateUpdated().add((Long) date);
            }
        }
        // Set previous representation thumbnail, if available
        Object thumbnail = doc.getFieldValue(SolrConstants.THUMBNAILREPRESENT);
        if (thumbnail != null) {
            indexObj.setThumbnailRepresent((String) thumbnail);
        }

        // Recursively delete all children, if not an anchor
        if (!fromOldIndex) {
            deleteWithPI(pi, false, hotfolder.getSearchIndex());
        }
    }
}
