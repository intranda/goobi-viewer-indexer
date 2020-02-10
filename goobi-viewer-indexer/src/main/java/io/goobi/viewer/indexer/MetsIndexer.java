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
import java.io.UnsupportedEncodingException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
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
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrHelper;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.helper.XmlTools;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.FatalIndexerException;
import io.goobi.viewer.indexer.model.GroupedMetadata;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.IndexerException;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.SolrConstants.MetadataGroupType;
import io.goobi.viewer.indexer.model.config.FieldConfig;
import io.goobi.viewer.indexer.model.config.XPathConfig;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.LazySolrWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.SerializingSolrWriteStrategy;

/**
 * Indexer implementation for METS documents.
 */
public class MetsIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(MetsIndexer.class);

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
    /** Constant <code>DEFAULT_FULLTEXT_CHARSET="UTF-8"</code> */
    public static final String DEFAULT_FULLTEXT_CHARSET = "UTF-8";

    private static List<Path> reindexedChildrenFileList = new ArrayList<>();

    private volatile String useFileGroup = null;

    /**
     * Constructor.
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public MetsIndexer(Hotfolder hotfolder) {
        this.hotfolder = hotfolder;
    }

    /**
     * Indexes the given METS file.
     *
     * @param metsFile {@link java.nio.file.Path}
     * @param fromReindexQueue a boolean.
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
    public String[] index(Path metsFile, boolean fromReindexQueue, Map<String, Path> dataFolders, ISolrWriteStrategy writeStrategy,
            int pageCountStart) {
        String[] ret = { null, null };

        if (metsFile == null || !Files.exists(metsFile)) {
            throw new IllegalArgumentException("metsFile must point to an existing METS file.");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null.");
        }

        logger.debug("Indexing METS file '{}'...", metsFile.getFileName());
        try {
            initJDomXP(metsFile);
            IndexObject indexObj = new IndexObject(getNextIddoc(hotfolder.getSolrHelper()));
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
            {
                String preQuery = "/mets:mets/mets:dmdSec[@ID='" + indexObj.getDmdid() + "']/mets:mdWrap[@MDTYPE='MODS']/";
                logger.debug("preQuery: {}", preQuery);
                String pi = MetadataHelper.getPIFromXML(preQuery, xp);
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
                            hotfolder.getDataRepositoryStrategy().selectDataRepository(pi, metsFile, dataFolders, hotfolder.getSolrHelper());
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
                long size = Files.size(metsFile);
                if (size >= hotfolder.metsFileSizeThreshold) {
                    useSerializingStrategy = true;
                    logger.info("METS file is {} bytes, using a slower Solr write strategy to avoid memory overflows.", size);
                } else {
                    for (String key : dataFolders.keySet()) {
                        switch (key) {
                            case DataRepository.PARAM_ALTO:
                            case DataRepository.PARAM_ALTOCROWD:
                            case DataRepository.PARAM_FULLTEXT:
                            case DataRepository.PARAM_FULLTEXTCROWD:
                            case DataRepository.PARAM_ABBYY:
                            case DataRepository.PARAM_TEIWC:
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
                    writeStrategy = new SerializingSolrWriteStrategy(hotfolder.getSolrHelper(), hotfolder.getTempFolder());

                }
                //                else if (IndexerConfig.getInstance().getBoolean("init.aggregateRecords")) {
                //                    writeStrategy = new HierarchicalLazySolrWriteStrategy(hotfolder.getSolrHelper());
                //                }
                else {
                    writeStrategy = new LazySolrWriteStrategy(hotfolder.getSolrHelper());
                }
            } else {
                logger.info("Solr write strategy injected by caller: {}", writeStrategy.getClass().getName());
            }

            // Set source doc format
            indexObj.addToLucene(SolrConstants.SOURCEDOCFORMAT, FileFormat.METS.name());
            prepareUpdate(indexObj);

            int workDepth = 0; // depth of the docstrct that has ISWORK (volume or monograph)
            if (indexObj.isVolume()) {
                // Find anchor document for this volume
                workDepth = 1;
                StringBuilder sbXpath = new StringBuilder(170);
                sbXpath.append("/mets:mets/mets:dmdSec[@ID='")
                        .append(structNode.getAttributeValue("DMDID"))
                        .append("']/mets:mdWrap[@MDTYPE='MODS']/mets:xmlData/mods:mods/mods:relatedItem[@type='host']/mods:recordInfo/mods:recordIdentifier");
                List<Element> piList = xp.evaluateToElements(sbXpath.toString(), null);
                if (!piList.isEmpty()) {
                    String parentPi = piList.get(0).getText().trim();
                    parentPi = MetadataHelper.applyIdentifierModifications(parentPi);
                    indexObj.setParentPI(parentPi);
                    String[] fields = { SolrConstants.IDDOC, SolrConstants.DOCSTRCT };
                    String parentIddoc = null;
                    String parentDocstrct = null;
                    SolrDocumentList hits = hotfolder.getSolrHelper()
                            .search(new StringBuilder().append(SolrConstants.PI).append(":").append(parentPi).toString(), Arrays.asList(fields));
                    if (hits != null && hits.getNumFound() > 0) {
                        parentIddoc = (String) hits.get(0).getFieldValue(SolrConstants.IDDOC);
                        parentDocstrct = (String) hits.get(0).getFieldValue(SolrConstants.DOCSTRCT);
                    }
                    // Create parent IndexObject
                    if (parentPi != null && parentIddoc != null) {
                        logger.debug("Creating anchor for '{}' (PI:{}, IDDOC:{})", indexObj.getIddoc(), parentPi, parentIddoc);
                        IndexObject anchor = new IndexObject(Long.valueOf(parentIddoc), parentPi);
                        if (anchor.getIddoc() == indexObj.getIddoc()) {
                            throw new IndexerException("Anchor and volume have the same IDDOC: " + indexObj.getIddoc());
                        }
                        // Set anchor properties manually because this IndexObject does not undergo the normal procedure
                        anchor.setAnchor(true);
                        anchor.setVolume(false);
                        if (parentDocstrct == null) {
                            logger.warn("Anchor docstruct not found in the index document, determining by volume type...");
                            switch (indexObj.getType()) {
                                case "PeriodicalVolume":
                                    parentDocstrct = "Periodical";
                                    break;
                                case "Volume":
                                    parentDocstrct = "MultiVolumeWork";
                                    break;
                                case "Record":
                                    parentDocstrct = "Record";
                                    break;
                                case "MapVolume":
                                    parentDocstrct = "MultiVolumeMap";
                                    break;
                                default:
                                    // nothing
                            }
                        }
                        anchor.setType(parentDocstrct);
                        indexObj.setParent(anchor);
                    }
                }
            }

            // write opac url
            String opacXpath =
                    "/mets:mets/mets:amdSec/mets:digiprovMD[@ID='DIGIPROV']/mets:mdWrap[@OTHERMDTYPE='DVLINKS']/mets:xmlData/dv:links/dv:reference/text()";
            String opacUrl = xp.evaluateToString(opacXpath, null);
            if (StringUtils.isEmpty(opacUrl)) {
                opacUrl = xp.evaluateToCdata(opacXpath, null);
            }
            logger.debug("OPACURL: {}", opacUrl);
            if (StringUtils.isNotEmpty(opacUrl)) {
                indexObj.addToLucene(SolrConstants.OPACURL, opacUrl);
            }

            // Process TEI files
            if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) != null) {
                MetadataHelper.processTEIMetadataFiles(indexObj, dataFolders.get(DataRepository.PARAM_TEIMETADATA));
            }

            // put some simple data in lucene array
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
                    SolrDocumentList hits = hotfolder.getSolrHelper()
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

            // Read DATECREATED from METS
            if (indexObj.getDateCreated() == -1) {
                Date dateCreated = getMetsCreateDate();
                if (dateCreated != null) {
                    indexObj.setDateCreated(dateCreated.getTime());
                    logger.info("Using creation timestamp from METS: {}", indexObj.getDateCreated());
                }
            }
            // Write created/updated timestamps
            indexObj.writeDateModified(!fromReindexQueue && !noTimestampUpdate);

            if (!indexObj.isAnchor()) {
                // Generate docs for all pages and add to the write strategy
                generatePageDocuments(writeStrategy, dataFolders, dataRepository, indexObj.getPi(), pageCountStart);

                // If full-text has been indexed for any page, set a boolean in the root doc indicating that the records does have full-text
                indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, String.valueOf(recordHasFulltext));

                // write all page URNs sequentially into one field
                generatePageUrns(indexObj);

                // Add THUMBNAIL,THUMBPAGENO,THUMBPAGENOLABEL (must be done AFTER writeDateMondified(), writeAccessConditions() and generatePageDocuments()!)
                List<LuceneField> thumbnailFields = mapPagesToDocstruct(indexObj, true, writeStrategy, dataFolders, workDepth);
                if (thumbnailFields != null) {
                    indexObj.getLuceneFields().addAll(thumbnailFields);
                }

                // ISWORK only for non-anchors
                indexObj.addToLucene(SolrConstants.ISWORK, "true");
                logger.trace("ISWORK: {}", indexObj.getLuceneFieldWithName(SolrConstants.ISWORK).getValue());
            } else {
                // Anchors
                indexObj.addToLucene(SolrConstants.ISANCHOR, "true");
                long numVolumes = hotfolder.getSolrHelper()
                        .getNumHits(new StringBuilder(SolrConstants.PI_PARENT).append(":")
                                .append(indexObj.getPi())
                                .append(" AND ")
                                .append(SolrConstants.ISWORK)
                                .append(":true")
                                .toString());
                indexObj.addToLucene(SolrConstants.NUMVOLUMES, String.valueOf(numVolumes));
                logger.info("Added number of volumes: {}", numVolumes);
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
                SolrInputDocument doc = hotfolder.getSolrHelper()
                        .checkAndCreateGroupDoc(groupIdField, indexObj.getGroupIds().get(groupIdField), moreMetadata,
                                getNextIddoc(hotfolder.getSolrHelper()));
                if (doc != null) {
                    writeStrategy.addDoc(doc);
                    logger.debug("Created group document for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                } else {
                    logger.debug("Group document already exists for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                }
            }

            // Add grouped metadata as separate documents
            addGroupedMetadataDocs(writeStrategy, indexObj);

            boolean indexedChildrenFileList = false;
            if (!indexObj.isAnchor()) {
                // Index all child elements recursively
                indexAllChildren(indexObj, workDepth + 1, writeStrategy, dataFolders);
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
            } else {
                // Create and index new anchor file that includes all currently indexed children
                logger.debug("'{}' is an anchor file.", metsFile.getFileName());
                anchorMerge(indexObj);
                // Then re-index all children
                updateAllAnchorChildren(indexObj);
            }

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)
            logger.debug("Writing document to index...");
            SolrInputDocument rootDoc = SolrHelper.createDocument(indexObj.getLuceneFields());
            writeStrategy.setRootDoc(rootDoc);
            writeStrategy.writeDocs(Configuration.getInstance().isAggregateRecords());
            if (indexObj.isVolume() && (!indexObj.isUpdate() || indexedChildrenFileList)) {
                logger.info("Re-indexing anchor...");
                copyAndReIndexAnchor(indexObj, hotfolder, dataRepository);
            }
            logger.info("Successfully finished indexing '{}'.", metsFile.getFileName());
        } catch (Exception e) {
            logger.error("Indexing of '{}' could not be finished due to an error.", metsFile.getFileName());
            logger.error(e.getMessage(), e);
            ret[1] = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            hotfolder.getSolrHelper().rollback();
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
    private List<LuceneField> mapPagesToDocstruct(IndexObject indexObj, boolean isWork, ISolrWriteStrategy writeStrategy,
            Map<String, Path> dataFolders, int depth) throws IndexerException, IOException, FatalIndexerException {
        if (StringUtils.isEmpty(indexObj.getLogId())) {
            throw new IndexerException("Object has no LOG_ID.");
        }

        // Determine all PHYSID mapped to the current LOGID
        String xpath = "/mets:mets/mets:structLink/mets:smLink[@xlink:from=\"" + indexObj.getLogId() + "\"]/@xlink:to";
        List<String> physIdList = xp.evaluateToStringList(xpath, null);
        if (physIdList == null || physIdList.isEmpty()) {
            //            throw new IndexerException("No image associated with '" + logId + "'");
            logger.warn("No pages mapped to '{}'.", indexObj.getLogId());
            return null;
        }

        List<SolrInputDocument> pageDocs = writeStrategy.getPageDocsForPhysIdList(physIdList);
        if (pageDocs.isEmpty()) {
            logger.warn("No pages found for {}", indexObj.getLogId());
        }

        // If this is a top struct element, look for a representative image
        String filePathBanner = null;
        if (isWork) {
            xpath = "/mets:mets/mets:fileSec/mets:fileGrp[@USE=\"" + useFileGroup + "\"]/mets:file[@USE=\"banner\"]/mets:FLocat/@xlink:href";
            filePathBanner = xp.evaluateToAttributeStringValue(xpath, null);
            if (StringUtils.isNotEmpty(filePathBanner)) {
                // Add thumbnail information from the representative page
                filePathBanner = FilenameUtils.getName(filePathBanner);
                logger.debug("Found representation thumbnail for {} in METS: {}", indexObj.getLogId(), filePathBanner);
            } else if (StringUtils.isNotEmpty(indexObj.getThumbnailRepresent())) {
                filePathBanner = indexObj.getThumbnailRepresent();
                logger.debug("No representation thumbnail for {} found in METS, using previous file: {}", indexObj.getLogId(), filePathBanner);
            }
        }
        boolean thumbnailSet = false;
        List<LuceneField> ret = new ArrayList<>();
        SolrInputDocument firstPageDoc = !pageDocs.isEmpty() ? pageDocs.get(0) : null;
        if (StringUtils.isEmpty(filePathBanner) && firstPageDoc != null) {
            // Add thumbnail information from the first page
            //                String thumbnailFileName = firstPageDoc.getField(SolrConstants.FILENAME + "_HTML-SANDBOXED") != null ? (String) firstPageDoc
            //                        .getFieldValue(SolrConstants.FILENAME + "_HTML-SANDBOXED") : (String) firstPageDoc.getFieldValue(SolrConstants.FILENAME);
            String thumbnailFileName = (String) firstPageDoc.getFieldValue(SolrConstants.FILENAME);
            ret.add(new LuceneField(SolrConstants.THUMBNAIL, thumbnailFileName));
            if ("SHAPE".equals(firstPageDoc.getFieldValue(SolrConstants.DOCTYPE))) {
                ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPageDoc.getFieldValue("ORDER_PARENT"))));
            } else {
                ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPageDoc.getFieldValue(SolrConstants.ORDER))));
            }
            ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) firstPageDoc.getFieldValue(SolrConstants.ORDERLABEL)));
            ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) firstPageDoc.getFieldValue(SolrConstants.MIMETYPE)));
            thumbnailSet = true;
        }
        for (SolrInputDocument pageDoc : pageDocs) {
            //                String pageFileName = pageDoc.getField(SolrConstants.FILENAME + "_HTML-SANDBOXED") != null ? (String) pageDoc.getFieldValue(
            //                        SolrConstants.FILENAME + "_HTML-SANDBOXED") : (String) pageDoc.getFieldValue(SolrConstants.FILENAME);
            String pageFileName = (String) pageDoc.getFieldValue(SolrConstants.FILENAME);
            String pageFileBaseName = FilenameUtils.getBaseName(pageFileName);
            // Add thumbnail information from the representative page
            if (!thumbnailSet && StringUtils.isNotEmpty(filePathBanner) && filePathBanner.equals(pageFileName)) {
                ret.add(new LuceneField(SolrConstants.THUMBNAIL, pageFileName));
                // THUMBNAILREPRESENT is just used to identify the presence of a custom representation thumbnail to the indexer, it is not used in the viewer
                ret.add(new LuceneField(SolrConstants.THUMBNAILREPRESENT, pageFileName));
                ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(pageDoc.getFieldValue(SolrConstants.ORDER))));
                ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) pageDoc.getFieldValue(SolrConstants.ORDERLABEL)));
                ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) pageDoc.getFieldValue(SolrConstants.MIMETYPE)));
                thumbnailSet = true;
            }

            int currentDepth = -1;
            if (pageDoc.getField("MDNUM_OWNERDEPTH") != null) {
                currentDepth = (int) pageDoc.getField("MDNUM_OWNERDEPTH").getValue();
            }

            // Make sure IDDOC_OWNER of a page contains the IDDOC of the lowest possible mapped docstruct
            if (depth > currentDepth) {
                pageDoc.setField(SolrConstants.IDDOC_OWNER, String.valueOf(indexObj.getIddoc()));
                pageDoc.setField("MDNUM_OWNERDEPTH", depth);

                // Add the parent document's structure element to the page
                pageDoc.setField(SolrConstants.DOCSTRCT, indexObj.getType());

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

            // Add PI_TOPSTRUCT
            if (pageDoc.getField(SolrConstants.PI_TOPSTRUCT) == null) {
                pageDoc.addField(SolrConstants.PI_TOPSTRUCT, indexObj.getTopstructPI());
            }
            // Add PI_ANCHOR
            if (StringUtils.isNotEmpty(indexObj.getAnchorPI()) && pageDoc.getField(SolrConstants.PI_ANCHOR) == null) {
                pageDoc.addField(SolrConstants.PI_ANCHOR, indexObj.getAnchorPI());
            }
            // Add GROUPID_*
            if (!indexObj.getGroupIds().isEmpty()) {
                for (String groupId : indexObj.getGroupIds().keySet()) {
                    if (!pageDoc.containsKey(groupId)) {
                        pageDoc.addField(groupId, indexObj.getLuceneFieldWithName(groupId).getValue());
                    }
                }
            }
            // Add DATAREPOSITORY
            if (pageDoc.getField(SolrConstants.DATAREPOSITORY) == null && indexObj.getDataRepository() != null) {
                pageDoc.addField(SolrConstants.DATAREPOSITORY, indexObj.getDataRepository());
            }
            if (pageDoc.getField(SolrConstants.DATEUPDATED) == null && !indexObj.getDateUpdated().isEmpty()) {
                for (Long date : indexObj.getDateUpdated()) {
                    pageDoc.addField(SolrConstants.DATEUPDATED, date);
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
                if (!existingAccessConditions.contains(s) && !SolrConstants.OPEN_ACCESS_VALUE.equals(s)) {
                    // Override OPENACCESS if a different access condition comes from a lower docstruct
                    if (depth > currentDepth && existingAccessConditions.contains(SolrConstants.OPEN_ACCESS_VALUE)) {
                        // Remove all instances of ACCESSCONDITION, then re-add existing values (minus OPENACCSS)
                        pageDoc.removeField(SolrConstants.ACCESSCONDITION);
                        for (String existingS : existingAccessConditions) {
                            if (!SolrConstants.OPEN_ACCESS_VALUE.equals(existingS)) {
                                pageDoc.addField(SolrConstants.ACCESSCONDITION, existingS);
                            }
                        }
                    }
                    // Add new non-OPENACCESS condition
                    pageDoc.addField(SolrConstants.ACCESSCONDITION, s);
                } else if (SolrConstants.OPEN_ACCESS_VALUE.equals(s) && depth > currentDepth) {
                    // If OPENACCESS is on a lower docstruct, however, remove all previous access conditions and override with OPENACCESS
                    pageDoc.removeField(SolrConstants.ACCESSCONDITION);
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

            // For shape page docs, create grouped metadata docs for their mapped docstruct
            if ("SHAPE".equals(pageDoc.getFieldValue(SolrConstants.DOCTYPE))) {
                GroupedMetadata shapeGmd = new GroupedMetadata();
                shapeGmd.getFields().add(new LuceneField(SolrConstants.METADATATYPE, MetadataGroupType.SHAPE.name()));
                shapeGmd.getFields().add(new LuceneField(SolrConstants.GROUPFIELD, String.valueOf(pageDoc.getFieldValue(SolrConstants.IDDOC))));
                shapeGmd.getFields().add(new LuceneField(SolrConstants.LABEL, (String) pageDoc.getFieldValue("MD_COORDS")));
                shapeGmd.getFields().add(new LuceneField("MD_COORDS", (String) pageDoc.getFieldValue("MD_COORDS")));
                shapeGmd.getFields().add(new LuceneField("MD_SHAPE", (String) pageDoc.getFieldValue("MD_SHAPE")));
                indexObj.getGroupedMetadataFields().add(shapeGmd);
                logger.debug("Mapped SHAPE document {} to {}", pageDoc.getFieldValue(SolrConstants.ORDER), indexObj.getLogId());
            }

            // Update the doc in the write strategy (otherwise some implementations might ignore the changes).
            writeStrategy.updateDoc(pageDoc);
        }

        // If a representative image is set but not mapped to any docstructs, do not use it
        if (!thumbnailSet && StringUtils.isNotEmpty(filePathBanner) && firstPageDoc != null) {
            logger.warn("Selected representative image '{}' is not mapped to any structure element - using first mapped image instead.",
                    filePathBanner);
            String pageFileName = (String) firstPageDoc.getFieldValue(SolrConstants.FILENAME);
            ret.add(new LuceneField(SolrConstants.THUMBNAIL, pageFileName));
            // THUMBNAILREPRESENT is just used to identify the presence of a custom representation thumbnail to the indexer, it is not used in the viewer
            ret.add(new LuceneField(SolrConstants.THUMBNAILREPRESENT, pageFileName));
            ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPageDoc.getFieldValue(SolrConstants.ORDER))));
            ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) firstPageDoc.getFieldValue(SolrConstants.ORDERLABEL)));
            ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) firstPageDoc.getFieldValue(SolrConstants.MIMETYPE)));
            thumbnailSet = true;
        }

        // Add the number of assigned pages and the labels of the first and last page to this structure element
        indexObj.setNumPages(pageDocs.size());
        if (firstPageDoc != null) {
            SolrInputDocument lastPagedoc = pageDocs.get(pageDocs.size() - 1);
            String firstPageLabel = (String) firstPageDoc.getFieldValue(SolrConstants.ORDERLABEL);
            String lastPageLabel = (String) lastPagedoc.getFieldValue(SolrConstants.ORDERLABEL);
            if (firstPageLabel != null && !"-".equals(firstPageLabel.trim())) {
                indexObj.setFirstPageLabel(firstPageLabel);
            }
            if (lastPageLabel != null && !"-".equals(lastPageLabel.trim())) {
                indexObj.setLastPageLabel(lastPageLabel);
            }
            // logger.info(indexObj.getLogId() + ": " + indexObj.getFirstPageLabel() + " - " + indexObj.getLastPageLabel());
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
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should create documents for all mapped pages
     * @should set correct ORDER values
     * @should skip unmapped pages
     * @should switch to DEFAULT file group correctly
     * @should maintain page order after parallel processing
     */
    public void generatePageDocuments(final ISolrWriteStrategy writeStrategy, final Map<String, Path> dataFolders,
            final DataRepository dataRepository, final String pi, int pageCountStart) throws FatalIndexerException {
        // Get all physical elements
        String xpath = "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div";
        List<Element> eleStructMapPhysicalList = xp.evaluateToElements(xpath, null);
        logger.info("Generating {} page documents (count starts at {})...", eleStructMapPhysicalList.size(), pageCountStart);

        if (Configuration.getInstance().getThreads() > 1) {
            // Generate each page document in its own thread
            ForkJoinPool pool = new ForkJoinPool(Configuration.getInstance().getThreads());
            ConcurrentHashMap<Long, Boolean> map = new ConcurrentHashMap<>();
            try {
                pool.submit(() -> eleStructMapPhysicalList.parallelStream().forEach(eleStructMapPhysical -> {
                    try {
                        long iddoc = getNextIddoc(hotfolder.getSolrHelper());
                        if (map.containsKey(iddoc)) {
                            logger.error("Duplicate IDDOC: {}", iddoc);
                        }
                        generatePageDocument(eleStructMapPhysical, String.valueOf(iddoc), pi, null, writeStrategy, dataFolders, dataRepository);
                        map.put(iddoc, true);
                    } catch (FatalIndexerException e) {
                        logger.error("Should be exiting here now...");
                    }
                })).get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error(e.getMessage(), e);
                SolrIndexerDaemon.getInstance().stop();
            }
        } else {
            // Generate pages sequentially
            int order = pageCountStart;
            for (final Element eleStructMapPhysical : eleStructMapPhysicalList) {
                if (generatePageDocument(eleStructMapPhysical, String.valueOf(getNextIddoc(hotfolder.getSolrHelper())), pi, order, writeStrategy,
                        dataFolders, dataRepository)) {
                    order++;
                }
            }
        }
        logger.info("Generated {} page documents.", writeStrategy.getPageDocsSize());
    }

    /**
     * 
     * @param eleStructMapPhysical
     * @param iddoc
     * @param pi
     * @param order
     * @param writeStrategy
     * @param dataFolders
     * @param dataRepository
     * @return
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
    boolean generatePageDocument(Element eleStructMapPhysical, String iddoc, String pi, Integer order, final ISolrWriteStrategy writeStrategy,
            final Map<String, Path> dataFolders, final DataRepository dataRepository) throws FatalIndexerException {
        if (dataFolders != null && dataRepository == null) {
            throw new IllegalArgumentException("dataRepository may not be null if dataFolders is not null");
        }

        String id = eleStructMapPhysical.getAttributeValue("ID");
        if (order == null) {
            order = Integer.parseInt(eleStructMapPhysical.getAttributeValue("ORDER"));
        }
        logger.trace("generatePageDocument: {} (IDDOC {}) processed by thread {}", id, iddoc, Thread.currentThread().getId());
        // Check whether this physical element is mapped to any logical element, skip if not
        StringBuilder sbXPath = new StringBuilder(70);
        sbXPath.append("/mets:mets/mets:structLink/mets:smLink[@xlink:to=\"").append(id).append("\"]");
        List<Element> eleStructLinkList = xp.evaluateToElements(sbXPath.toString(), null);
        if (eleStructLinkList.isEmpty()) {
            logger.warn("Page {} is not mapped to a structure element, skipping...", order);
            return false;
        }

        List<Element> eleFptrList = eleStructMapPhysical.getChildren("fptr", Configuration.getInstance().getNamespaces().get("mets"));
        String useFileGroup = null;

        // Create Solr document for this page
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(SolrConstants.IDDOC, iddoc);
        doc.addField(SolrConstants.GROUPFIELD, iddoc);
        doc.addField(SolrConstants.DOCTYPE, DocType.PAGE.name());
        doc.addField(SolrConstants.PHYSID, id);
        doc.addField(SolrConstants.ORDER, order);

        List<SolrInputDocument> shapePageDocs = Collections.emptyList();

        // Determine the FILEID root (part of the FILEID that doesn't change for different mets:fileGroups)
        String fileIdRoot = null;
        String useFileID = null;
        for (Element eleFptr : eleFptrList) {
            String fileID = eleFptr.getAttributeValue("FILEID");
            logger.trace("fileID: {}", fileID);
            if (fileID.contains(DEFAULT_FILEGROUP_1)) {
                // Always prefer PRESENTATION: override if already set to something else
                useFileGroup = DEFAULT_FILEGROUP_1;
                useFileID = fileID;
            } else if (fileID.contains(DEFAULT_FILEGROUP_2) && !DEFAULT_FILEGROUP_1.equals(useFileGroup)) {
                useFileGroup = DEFAULT_FILEGROUP_2;
                useFileID = fileID;
            } else if (fileID.contains(OBJECT_FILEGROUP) && !DEFAULT_FILEGROUP_1.equals(useFileGroup)) {
                useFileGroup = OBJECT_FILEGROUP;
                useFileID = fileID;
            }

            // Piggyback shape metadata on fake page documents to ensure their mapping to corresponding shape docstructs
            if (eleFptr.getChild("seq", Configuration.getInstance().getNamespaces().get("mets")) != null) {
                List<Element> eleListArea = eleFptr.getChild("seq", Configuration.getInstance().getNamespaces().get("mets"))
                        .getChildren("area", Configuration.getInstance().getNamespaces().get("mets"));
                if (eleListArea != null && !eleListArea.isEmpty()) {
                    int count = 1;
                    shapePageDocs = new ArrayList<>();
                    for (Element eleArea : eleListArea) {
                        String coords = eleArea.getAttributeValue("COORDS");
                        String physId = eleArea.getAttributeValue("ID");
                        String shape = eleArea.getAttributeValue("SHAPE");
                        SolrInputDocument shapePageDoc = new SolrInputDocument();
                        shapePageDoc.addField(SolrConstants.IDDOC, getNextIddoc(hotfolder.getSolrHelper()));
                        shapePageDoc.setField(SolrConstants.DOCTYPE, "SHAPE");
                        shapePageDoc.addField(SolrConstants.ORDER, Utils.generateLongOrderNumber(order, count));
                        shapePageDoc.addField(SolrConstants.PHYSID, physId);
                        ;
                        shapePageDoc.addField("MD_COORDS", coords);
                        shapePageDoc.addField("MD_SHAPE", shape);
                        shapePageDoc.addField("ORDER_PARENT", order);
                        shapePageDocs.add(shapePageDoc);
                        count++;
                        logger.debug("Added SHAPE page document: {}", shapePageDoc.getFieldValue(SolrConstants.ORDER));
                    }
                }
            }
        }
        if (useFileGroup != null && StringUtils.isEmpty(useFileID)) {
            logger.warn("FILEID not found for file group {}", useFileGroup);
            useFileID = "";
        }

        boolean fileGroupPrefix = false; // FILEID starts with the file group name
        boolean fileGroupSuffix = false; // FILEID ends with the file group name
        char fileIdSeparator = '_'; // Separator character between the group name and the rest and the file ID

        // Remove the file group part from the file ID
        if (useFileID != null && useFileGroup != null && useFileID.contains(useFileGroup)) {
            if (useFileID.startsWith(useFileGroup + '_')) {
                fileGroupPrefix = true;
            } else if (useFileID.startsWith(useFileGroup + '.')) {
                fileGroupPrefix = true;
                fileIdSeparator = '.';
            } else if (useFileID.endsWith('_' + useFileGroup)) {
                fileGroupSuffix = true;
            } else if (useFileID.endsWith('.' + useFileGroup)) {
                fileGroupSuffix = true;
                fileIdSeparator = '.';
            }
            if (fileGroupPrefix) {
                fileIdRoot = useFileID.replace(useFileGroup + fileIdSeparator, "");
            } else if (fileGroupSuffix) {
                fileIdRoot = useFileID.replace(fileIdSeparator + useFileGroup, "");
            }
            doc.addField(SolrConstants.FILEIDROOT, fileIdRoot);
        }

        String orderLabel = eleStructMapPhysical.getAttributeValue("ORDERLABEL");
        if (StringUtils.isNotEmpty(orderLabel)) {
            doc.addField(SolrConstants.ORDERLABEL, orderLabel);
        } else {
            doc.addField(SolrConstants.ORDERLABEL, Configuration.getInstance().getEmptyOrderLabelReplacement());
        }
        String contentIDs = eleStructMapPhysical.getAttributeValue("CONTENTIDS");
        if (Utils.isUrn(contentIDs)) {
            doc.addField(SolrConstants.IMAGEURN, contentIDs);
        }
        String dmdId = eleStructMapPhysical.getAttributeValue("DMDID");
        if (StringUtils.isNotEmpty(dmdId)) {
            // TODO page PURL
            IndexObject pageObj = new IndexObject(0);
            MetadataHelper.writeMetadataToObject(pageObj, xp.getMdWrap(dmdId), "", xp);
            for (LuceneField field : pageObj.getLuceneFields()) {
                doc.addField(field.getField(), field.getValue());
            }
        }

        String altoURL = null;
        // For each mets:fileGroup in the mets:fileSec
        String xpath = "/mets:mets/mets:fileSec/mets:fileGrp";
        List<Element> eleFileGrpList = xp.evaluateToElements(xpath, null);
        for (Element eleFileGrp : eleFileGrpList) {
            String fileGroup = eleFileGrp.getAttributeValue("USE");
            logger.debug("Found file group: {}", fileGroup);
            // If useFileGroup is still not set or not PRESENTATION, check whether the current group is PRESENTATION or DEFAULT and set it to that
            if ((useFileGroup == null || !DEFAULT_FILEGROUP_1.equals(useFileGroup))
                    && (DEFAULT_FILEGROUP_1.equals(fileGroup) || DEFAULT_FILEGROUP_2.equals(fileGroup) || OBJECT_FILEGROUP.equals(fileGroup))) {
                useFileGroup = fileGroup;
            }
            String fileId = null;
            if (fileGroupPrefix) {
                fileId = fileGroup + fileIdSeparator + fileIdRoot;
            } else if (fileGroupSuffix) {
                fileId = fileIdRoot + fileIdSeparator + fileGroup;
            } else {
                fileId = fileIdRoot;
            }
            logger.debug("fileId: {}", fileId);

            // If fileId is not null, use an XPath expression for the appropriate file element; otherwise get all file elements and get the one with the index of the page order
            String fileIdXPathCondition = fileId != null ? "[@ID=\"" + fileId + "\"]" : "";
            int attrListIndex = fileId != null ? 0 : order - 1;

            // Check whether the fileId_fileGroup pattern applies for this file group, otherwise just use the fileId
            xpath = "mets:file" + fileIdXPathCondition + "/mets:FLocat/@xlink:href";
            logger.debug(xpath);
            List<Attribute> filepathAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
            //            if (filepathAttrList == null || filepathAttrList.isEmpty()) {
            //                fileId = useFileID;
            //                xpath = "mets:file[@ID=\"" + fileId + "\"]/mets:FLocat/@xlink:href";
            //                filepathAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
            //            }
            logger.trace(xpath);
            if (filepathAttrList == null || filepathAttrList.size() <= attrListIndex) {
                // Skip silently
                logger.debug("Skipping file group {}", fileGroup);
                continue;
            }

            String filePath = filepathAttrList.get(attrListIndex).getValue();
            logger.trace("filePath: " + filePath);
            if (StringUtils.isEmpty(filePath)) {
                logger.warn("{}: file path not found in file group '{}'.", fileId, fileGroup);
                //                break;
            }
            String fileName = FilenameUtils.getName(filePath);

            // Mime type
            xpath = "mets:file" + fileIdXPathCondition + "/@MIMETYPE";
            List<Attribute> mimetypeAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
            if (mimetypeAttrList == null || mimetypeAttrList.isEmpty()) {
                logger.error("{}: mime type not found in file group '{}'.", fileId, fileGroup);
                break;
            }

            String mimetype = mimetypeAttrList.get(attrListIndex).getValue();
            if (StringUtils.isEmpty(mimetype)) {
                logger.error("{}: mime type is blank in file group '{}'.", fileId, fileGroup);
                break;
            }

            String[] mimetypeSplit = mimetype.split("/");
            if (mimetypeSplit.length != 2) {
                logger.error("Illegal mime type '{}' in file group '{}'.", mimetype, fileGroup);
                break;
            }

            if (fileGroup.equals(useFileGroup)) {
                // The file name from the main file group (PRESENTATION or DEFAULT) will be used for thumbnail purposes etc.
                if (filePath.startsWith("http")) {
                    // Should write the full URL into FILENAME because otherwise a PI_TOPSTRUCT+FILENAME combination may no longer be unique
                    if (doc.containsKey(SolrConstants.FILENAME)) {
                        logger.error("Page {} already contains FILENAME={}, but attempting to add another value from filegroup {}", iddoc, filePath,
                                fileGroup);
                    }
                    doc.addField(SolrConstants.FILENAME, filePath);
                    if (!shapePageDocs.isEmpty()) {
                        for (SolrInputDocument shapePageDoc : shapePageDocs) {
                            shapePageDoc.addField(SolrConstants.FILENAME, filePath);
                        }
                    }
                    // RosDok IIIF
                    if (DEFAULT_FILEGROUP_2.equals(useFileGroup) && !doc.containsKey(SolrConstants.FILENAME + "_HTML-SANDBOXED")) {
                        doc.addField(SolrConstants.FILENAME + "_HTML-SANDBOXED", filePath);
                    }
                } else {
                    if (doc.containsKey(SolrConstants.FILENAME)) {
                        logger.error("Page {} already contains FILENAME={}, but attempting to add another value from filegroup {}", iddoc, fileName,
                                fileGroup);
                    }
                    doc.addField(SolrConstants.FILENAME, fileName);
                    if (!shapePageDocs.isEmpty()) {
                        for (SolrInputDocument shapePageDoc : shapePageDocs) {
                            shapePageDoc.addField(SolrConstants.FILENAME, fileName);
                        }
                    }
                }

                // Add mime type
                doc.addField(SolrConstants.MIMETYPE, mimetypeSplit[0]);
                if (!shapePageDocs.isEmpty()) {
                    for (SolrInputDocument shapePageDoc : shapePageDocs) {
                        shapePageDoc.addField(SolrConstants.MIMETYPE, mimetypeSplit[0]);
                    }
                }
                // Add file size
                if (dataFolders != null) {
                    try {
                        Path dataFolder = dataFolders.get(DataRepository.PARAM_MEDIA);
                        if (dataFolder != null) {
                            Path path = Paths.get(dataFolder.toAbsolutePath().toString(), fileName);
                            doc.addField("MDNUM_FILESIZE", Files.size(path));
                        } else {
                            doc.addField("MDNUM_FILESIZE", -1);
                        }
                    } catch (FileNotFoundException | NoSuchFileException e) {
                        logger.error("File not found: " + e.getMessage());
                        doc.addField("MDNUM_FILESIZE", -1);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                        doc.addField("MDNUM_FILESIZE", -1);
                    } catch (IllegalArgumentException e) {
                        logger.error(e.getMessage(), e);
                        doc.addField("MDNUM_FILESIZE", -1);
                    }
                }

            } else if (fileGroup.equals(ALTO_FILEGROUP) || fileGroup.equals(FULLTEXT_FILEGROUP)) {
                altoURL = filePath;
            } else {
                String fieldName = SolrConstants.FILENAME + "_" + mimetypeSplit[1].toUpperCase();
                if (doc.getField(fieldName) == null) {
                    switch (mimetypeSplit[1]) {
                        case "html-sandboxed":
                            // Add full URL
                            doc.addField(SolrConstants.FILENAME + "_" + mimetypeSplit[1].toUpperCase(), filePath);
                            break;
                        case "object":
                            doc.addField(SolrConstants.FILENAME, fileName);
                            doc.addField(SolrConstants.MIMETYPE, mimetypeSplit[1]);
                            break;
                        default:
                            doc.addField(SolrConstants.FILENAME + "_" + mimetypeSplit[1].toUpperCase(), fileName);
                    }

                }
            }

            if (doc.getField(SolrConstants.WIDTH) == null && doc.getField(SolrConstants.HEIGHT) == null) {
                // Width + height (invalid)
                xpath = "mets:file" + fileIdXPathCondition + "/@WIDTH";
                List<Attribute> widthAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
                Integer width = null;
                Integer height = null;
                if (widthAttrList != null && !widthAttrList.isEmpty() && StringUtils.isNotBlank(widthAttrList.get(0).getValue())) {
                    width = Integer.valueOf(widthAttrList.get(0).getValue());
                    logger.warn("mets:file[@ID='{}'] contains illegal WIDTH attribute. It will still be used, though.", fileId);
                }
                xpath = "mets:file" + fileIdXPathCondition + "/@HEIGHT";
                List<Attribute> heightAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
                if (heightAttrList != null && !heightAttrList.isEmpty() && StringUtils.isNotBlank(heightAttrList.get(0).getValue())) {
                    height = Integer.valueOf(heightAttrList.get(0).getValue());
                    logger.warn("mets:file[@ID='{}'] contains illegal HEIGHT attribute. It will still be used, though.", fileId);
                }
                if (width != null && height != null) {
                    doc.addField(SolrConstants.WIDTH, width);
                    doc.addField(SolrConstants.HEIGHT, height);
                }

                // Width + height (from techMD)
                xpath = "mets:file" + fileIdXPathCondition + "/@ADMID";
                List<Attribute> amdIdAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
                if (amdIdAttrList != null && !amdIdAttrList.isEmpty() && StringUtils.isNotBlank(amdIdAttrList.get(0).getValue())) {
                    String amdId = amdIdAttrList.get(0).getValue();
                    xpath = "/mets:mets/mets:amdSec/mets:techMD[@ID='" + amdId
                            + "']/mets:mdWrap[@MDTYPE='OTHER']/mets:xmlData/pbcoreInstantiation/formatFrameSize/text()";
                    String frameSize = xp.evaluateToString(xpath, null);
                    if (StringUtils.isNotEmpty(frameSize)) {
                        String[] frameSizeSplit = frameSize.split("x");
                        if (frameSizeSplit.length == 2) {
                            doc.addField(SolrConstants.WIDTH, frameSizeSplit[0].trim());
                            doc.addField(SolrConstants.HEIGHT, frameSizeSplit[1].trim());
                            logger.debug("WIDTH: {}", doc.getFieldValue(SolrConstants.WIDTH));
                            logger.debug("HEIGHT: {}", doc.getFieldValue(SolrConstants.HEIGHT));
                        } else {
                            logger.warn("Invalid formatFrameSize value in mets:techMD[@ID='{}']: '{}'", amdId, frameSize);
                        }
                    }
                }
            }
        }

        if (dataFolders != null) {
            Map<String, Object> altoData = null;
            String baseFileName = FilenameUtils.getBaseName((String) doc.getFieldValue(SolrConstants.FILENAME));

            // Add complete crowdsourcing ALTO document and full-text generated from ALTO, if available
            boolean foundCrowdsourcingData = false;
            boolean altoWritten = false;
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
                        doc.addField(SolrConstants.FILENAME_ALTO, dataRepository.getDir(DataRepository.PARAM_ALTOCROWD).getFileName().toString() + '/'
                                + pi + '/' + baseFileName + XML_EXTENSION);
                        altoWritten = true;
                        logger.debug("Added ALTO from crowdsourcing ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT))) {
                        doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags((String) altoData.get(SolrConstants.FULLTEXT)));
                        logger.debug("Added FULLTEXT from crowdsourcing ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                        doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                        logger.debug("Added WIDTH from crowdsourcing ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                        doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                        logger.debug("Added WIDTH from crowdsourcing ALTO for page {}", order);
                    }
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
                    doc.addField(SolrConstants.FILENAME_FULLTEXT, dataRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD).getFileName().toString()
                            + '/' + pi + '/' + baseFileName + TXT_EXTENSION);
                    logger.debug("Added FULLTEXT from crowdsourcing plain text for page {}", order);
                }
            }
            // Look for a regular ALTO document for this page and fill ALTO and/or FULLTEXT fields, whichever is still empty
            if (!foundCrowdsourcingData && (doc.getField(SolrConstants.ALTO) == null || doc.getField(SolrConstants.FULLTEXT) == null)
                    && dataFolders.get(DataRepository.PARAM_ALTO) != null && !"info".equals(baseFileName)) {
                try {
                    altoData = TextHelper.readAltoFile(
                            new File(dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), baseFileName + XML_EXTENSION));
                } catch (JDOMException e) {
                    logger.error(e.getMessage(), e);
                } catch (IOException e) {
                    logger.warn(e.getMessage());
                }
                if (altoData != null) {
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO)) && doc.getField(SolrConstants.FILENAME_ALTO) == null) {
                        doc.addField(SolrConstants.FILENAME_ALTO, dataRepository.getDir(DataRepository.PARAM_ALTO).getFileName().toString() + '/' + pi
                                + '/' + baseFileName + XML_EXTENSION);
                        altoWritten = true;
                        logger.debug("Added ALTO from regular ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT)) && doc.getField(SolrConstants.FULLTEXT) == null) {
                        doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags((String) altoData.get(SolrConstants.FULLTEXT)));
                        logger.debug("Added FULLTEXT from regular ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                        doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                        logger.debug("Added WIDTH from regular ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                        doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                        logger.debug("Added WIDTH from regular ALTO for page {}", order);
                    }
                    if (altoData.get(SolrConstants.NAMEDENTITIES) != null) {
                        addNamedEntitiesFields(altoData, doc);
                    }
                }
            }

            // If FULLTEXT is still empty, look for a plain full-text
            if (!foundCrowdsourcingData && doc.getField(SolrConstants.FULLTEXT) == null && dataFolders.get(DataRepository.PARAM_FULLTEXT) != null
                    && !"info".equals(baseFileName)) {
                String fulltext = TextHelper.generateFulltext(baseFileName + TXT_EXTENSION, dataFolders.get(DataRepository.PARAM_FULLTEXT), true);
                if (fulltext != null) {
                    doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags(fulltext));
                    doc.addField(SolrConstants.FILENAME_FULLTEXT, dataRepository.getDir(DataRepository.PARAM_FULLTEXT).getFileName().toString() + '/'
                            + pi + '/' + baseFileName + TXT_EXTENSION);
                    logger.debug("Added FULLTEXT from regular plain text for page {}", order);
                }
            }

            // ABBYY XML (converted to ALTO)
            if (!altoWritten && !foundCrowdsourcingData && dataFolders.get(DataRepository.PARAM_ABBYY) != null && !"info".equals(baseFileName)) {
                try {
                    try {
                        altoData = TextHelper.readAbbyyToAlto(
                                new File(dataFolders.get(DataRepository.PARAM_ABBYY).toAbsolutePath().toString(), baseFileName + XML_EXTENSION));
                        if (altoData != null) {
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO))
                                    && doc.getField(SolrConstants.FILENAME_ALTO) == null) {
                                if (dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED) != null) {
                                    doc.addField(SolrConstants.FILENAME_ALTO,
                                            dataRepository.getDir(DataRepository.PARAM_ALTO_CONVERTED).getFileName().toString() + '/' + pi + '/'
                                                    + baseFileName + XML_EXTENSION);
                                    // Write converted ALTO file
                                    FileUtils.writeStringToFile(
                                            new File(dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED).toFile(), baseFileName + XML_EXTENSION),
                                            (String) altoData.get(SolrConstants.ALTO), "UTF-8");
                                    altoWritten = true;
                                    logger.debug("Added ALTO from regular ALTO for page {}", order);
                                } else {
                                    logger.error("Data folder not defined: {}", dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED));
                                }
                            }
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT))
                                    && doc.getField(SolrConstants.FULLTEXT) == null) {
                                doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags((String) altoData.get(SolrConstants.FULLTEXT)));
                                logger.debug("Added FULLTEXT from regular ALTO for page {}", order);
                            }
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                                doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                                logger.debug("Added WIDTH from regular ALTO for page {}", order);
                            }
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                                doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                                logger.debug("Added WIDTH from regular ALTO for page {}", order);
                            }
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
            if (!altoWritten && !foundCrowdsourcingData && dataFolders.get(DataRepository.PARAM_TEIWC) != null && !"info".equals(baseFileName)) {
                try {
                    altoData = TextHelper.readTeiToAlto(
                            new File(dataFolders.get(DataRepository.PARAM_TEIWC).toAbsolutePath().toString(), baseFileName + XML_EXTENSION));
                    if (altoData != null) {
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO)) && doc.getField(SolrConstants.FILENAME_ALTO) == null) {
                            if (dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED) != null) {
                                doc.addField(SolrConstants.FILENAME_ALTO,
                                        dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED).getParent().getFileName().toString() + '/' + pi + '/'
                                                + baseFileName + XML_EXTENSION);
                                logger.debug("Added ALTO from regular ALTO for page {}", order);
                                FileUtils.writeStringToFile(
                                        new File(dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED).toFile(), baseFileName + XML_EXTENSION),
                                        (String) altoData.get(SolrConstants.ALTO), "UTF-8");
                                altoWritten = true;
                                // Write converted ALTO file
                            } else {
                                logger.error("Data folder not defined: {}", dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED));
                            }

                        }
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT)) && doc.getField(SolrConstants.FULLTEXT) == null) {
                            doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags((String) altoData.get(SolrConstants.FULLTEXT)));
                            logger.debug("Added FULLTEXT from regular ALTO for page {}", order);
                        }
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                            doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                            logger.debug("Added WIDTH from regular ALTO for page {}", order);
                        }
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                            doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                            logger.debug("Added WIDTH from regular ALTO for page {}", order);
                        }
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

            if (dataFolders.get(DataRepository.PARAM_MIX) != null && !"info".equals(baseFileName)) {
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

            // If there is still no ALTO at this point and the METS document contains a file group for ALTO, download and use it
            if (!altoWritten && !foundCrowdsourcingData && altoURL != null && altoURL.startsWith("http")
                    && !altoURL.startsWith(Configuration.getInstance().getString("init.viewerUrl", "missing?"))) {
                try {
                    logger.debug("Downloading ALTO from {}", altoURL);
                    String alto = Utils.callUrl(altoURL);
                    if (StringUtils.isNotEmpty(alto)) {
                        Document altoDoc = XmlTools.getDocumentFromString(alto, null);
                        altoData = TextHelper.readAltoDoc(altoDoc);
                        if (altoData != null) {
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO))) {
                                // Create PARAM_ALTO_CONVERTED dir in hotfolder for download, if it doesn't yet exist
                                if (dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED) == null) {
                                    dataFolders.put(DataRepository.PARAM_ALTO_CONVERTED,
                                            Paths.get(dataRepository.getDir(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), pi));
                                    Files.createDirectory(dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED));
                                }
                                if (dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED) != null) {
                                    String fileName = MetadataHelper.FORMAT_EIGHT_DIGITS.get().format(order) + XML_EXTENSION;
                                    doc.addField(SolrConstants.FILENAME_ALTO,
                                            dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED).getParent().getFileName().toString() + '/' + pi + '/'
                                                    + fileName);
                                    // Write ALTO file
                                    File file = new File(dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED).toFile(), fileName);
                                    FileUtils.writeStringToFile(file, (String) altoData.get(SolrConstants.ALTO), "UTF-8");
                                    altoWritten = true;
                                    logger.debug("Added ALTO from downloaded ALTO for page {}", order);
                                } else {
                                    logger.error("Data folder not defined: {}", dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED));
                                }
                            }
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT))
                                    && doc.getField(SolrConstants.FULLTEXT) == null) {
                                doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags((String) altoData.get(SolrConstants.FULLTEXT)));
                                logger.debug("Added FULLTEXT from downloaded ALTO for page {}", order);
                            }
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                                doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                                logger.debug("Added WIDTH from downloaded ALTO for page {}", order);
                            }
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                                doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                                logger.debug("Added HEIGHT from downloaded ALTO for page {}", order);
                            }
                            if (altoData.get(SolrConstants.NAMEDENTITIES) != null) {
                                addNamedEntitiesFields(altoData, doc);
                            }
                        }
                    }
                } catch (JDOMException e) {
                    logger.error(e.getMessage(), e);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }

            }

            // Add image dimension values from EXIF
            if (!doc.containsKey(SolrConstants.WIDTH) || !doc.containsKey(SolrConstants.HEIGHT)) {
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
        // Add prepared shape page docs
        if (!shapePageDocs.isEmpty()) {
            for (SolrInputDocument shapePageDoc : shapePageDocs) {
                writeStrategy.addPageDoc(shapePageDoc);
            }
        }
        // Set global useFileGroup value (used for mapping later), if not yet set
        if (this.useFileGroup == null) {
            this.useFileGroup = useFileGroup;
        }
        return true;
    }

    /**
     * Updates the anchor METS file by looking up all indexed children and updating the links.
     * 
     * @param indexObj {@link IndexObject}
     * @throws IndexerException in case of errors.
     * @throws IOException in case of errors.
     * @throws SolrServerException
     * @throws FatalIndexerException
     */
    private void anchorMerge(IndexObject indexObj) throws IndexerException, IOException, SolrServerException, FatalIndexerException {
        logger.debug("anchorMerge: {}", indexObj.getPi());
        SolrDocumentList hits =
                hotfolder.getSolrHelper().search(SolrConstants.PI_PARENT + ":" + indexObj.getPi() + " AND " + SolrConstants.ISWORK + ":true", null);
        if (hits.isEmpty()) {
            logger.warn("Anchor '{}' has no volumes, no merge needed.", indexObj.getPi());
            return;
        }

        Map<Integer, String> childrenInfo = new HashMap<>();
        Map<String, String> labelInfo = new HashMap<>();
        Map<String, Integer> orderInfo = new HashMap<>();
        Map<String, String> urnInfo = new HashMap<>();
        Map<String, String> typeInfo = new HashMap<>();
        List<String> childrenInfoUnsorted = new ArrayList<>();
        List<String> collections = new ArrayList<>();
        boolean labelSort = false;
        for (SolrDocument doc : hits) {
            String pi = null;
            int num = 0;
            if (doc.getFieldValue(SolrConstants.PI) != null) {
                pi = (String) doc.getFieldValue(SolrConstants.PI);
                if (doc.getFieldValue(SolrConstants.CURRENTNOSORT) != null) {
                    try {
                        num = (Integer) doc.getFieldValue(SolrConstants.CURRENTNOSORT);
                        orderInfo.put(pi, num);
                    } catch (ClassCastException e) {
                        logger.error("'{}' is not a numerical value.", doc.getFieldValue(SolrConstants.CURRENTNOSORT).toString());
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
                labelSort = true;
            }

            // Collect all volume collections
            if (hotfolder.isAddVolumeCollectionsToAnchor() && doc.getFieldValues(SolrConstants.DC) != null) {
                for (Object obj : doc.getFieldValues(SolrConstants.DC)) {
                    String dc = (String) obj;
                    dc = dc.replace(".", "#");
                    if (!collections.contains(dc)) {
                        logger.debug("Found volume colletion: {}", dc);
                        collections.add(dc);
                    }
                }
            }
        }

        SortedMap<Integer, String> sortedChildrenMap = null;
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
                sortedChildrenMap.put(k, childrenInfoUnsorted.get(k));
            }
        }

        if (indexObj.getRootStructNode().getChildren().size() <= 0) {
            logger.error("Anchor file contains no child elements!");
            return;
        }

        // Merge anchor and volume collections and add them all to the anchor
        boolean newAnchorCollections = false;
        if (hotfolder.isAddVolumeCollectionsToAnchor()) {
            List<Element> eleDmdSecList =
                    xp.evaluateToElements("/mets:mets/mets:dmdSec[@ID='" + indexObj.getDmdid() + "']/mets:mdWrap[@MDTYPE='MODS']", null);
            if (eleDmdSecList != null && !eleDmdSecList.isEmpty()) {
                Element eleDmdSec = eleDmdSecList.get(0);
                List<Element> eleModsList = xp.evaluateToElements("mets:xmlData/mods:mods", eleDmdSec);
                if (eleModsList != null && !eleModsList.isEmpty()) {
                    Element eleMods = eleModsList.get(0);
                    List<FieldConfig> collectionConfigFields =
                            Configuration.getInstance().getMetadataConfigurationManager().getConfigurationListForField(SolrConstants.DC);
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
                                        newAnchorCollections = true;
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
        }

        // Generate volume elements
        Element firstChild = indexObj.getRootStructNode().getChildren().get(0);
        for (int j = 0; j < sortedChildrenMap.size(); j++) {
            Element child = firstChild.clone();
            int currentNo = (Integer) sortedChildrenMap.keySet().toArray()[j];
            String pi = sortedChildrenMap.get(currentNo);

            // Set URN
            if (urnInfo.get(pi) != null) {
                child.setAttribute("CONTENTIDS", urnInfo.get(pi));
            } else {
                child.removeAttribute("CONTENTIDS");
            }

            // ID Anpassen
            String strIdTail = String.valueOf(j + 1);
            String strId = strIdTail;
            if (j < 10) {
                strId = "000" + strIdTail;
            } else if (j < 100) {
                strId = "00" + strIdTail;
            } else if (j < 1000) {
                strId = "0" + strIdTail;
            }

            // Set ORDER
            if (orderInfo.get(pi) != null) {
                child.setAttribute("ORDER", String.valueOf(orderInfo.get(pi)));
            }

            // Set LOGID
            child.setAttribute("ID", "LOG_" + strId);
            // Set LABEL
            child.setAttribute("LABEL", labelInfo.get(pi));

            // Set TYPE
            if (typeInfo.get(pi) != null) {
                child.setAttribute("TYPE", typeInfo.get(pi));
            }

            // URL
            {
                Namespace nsMets = Configuration.getInstance().getNamespaces().get("mets");
                Namespace nsXlink = Configuration.getInstance().getNamespaces().get("xlink");
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
            }
            childrenE.add(child);

        }

        // Remove old children
        indexObj.getRootStructNode().removeContent();
        // Element edebug = indexObj.getRootStructNode();
        for (Element element : childrenE) {
            indexObj.getRootStructNode().addContent(element);
        }

        // Write XML file
        String extension;
        if (newAnchorCollections) {
            extension = Indexer.XML_EXTENSION;
            logger.info("Anchor document '{}' has received new collection entries and will be reindexed immediately.", indexObj.getPi());
        } else {
            extension = ANCHOR_UPDATE_EXTENSION;
        }

        Path updatedAnchorFile =
                Utils.getCollisionFreeDataFilePath(hotfolder.getHotfolder().toAbsolutePath().toString(), indexObj.getPi(), "#", extension);
        try {
            xp.writeDocumentToFile(updatedAnchorFile.toAbsolutePath().toString());
            if (Files.exists(updatedAnchorFile)) {
                hotfolder.getReindexQueue().add(updatedAnchorFile);
            }
        } catch (IOException e) {
            logger.error("Error while merging the anchor.", e);
        }
        // }
    }

    /**
     * Adds the anchor for the given volume object to the re-index queue.
     * 
     * @param indexObj {@link IndexObject}
     * @param hotfolder
     * @param dataRepository
     * @throws UnsupportedEncodingException
     */
    void copyAndReIndexAnchor(IndexObject indexObj, Hotfolder hotfolder, DataRepository dataRepository) throws UnsupportedEncodingException {
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

    /***
     * Re-indexes all child records of the given anchor document.
     * 
     * @param indexObj {@link IndexObject}
     * @throws IOException -
     * @throws SolrServerException
     */
    private void updateAllAnchorChildren(IndexObject indexObj) throws IOException, SolrServerException {
        logger.debug("Scheduling all METS files that belong to this anchor for re-indexing...");
        SolrDocumentList hits = hotfolder.getSolrHelper()
                .search(new StringBuilder(SolrConstants.PI_PARENT).append(":")
                        .append(indexObj.getPi())
                        .append(" AND ")
                        .append(SolrConstants.ISWORK)
                        .append(":true")
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
            // String indexedMetsFilePath = URLEncoder.encode(Hotfolder.getIndexedMets() + File.separator + pi + AbstractIndexer.XML_EXTENSION, "utf-8");
            String indexedMetsFilePath = dataRepository.getDir(DataRepository.PARAM_INDEXED_METS) + File.separator + pi + Indexer.XML_EXTENSION;
            Path indexedMets = Paths.get(indexedMetsFilePath);
            if (Files.exists(indexedMets)) {
                hotfolder.getReindexQueue().add(indexedMets);
                MetsIndexer.reindexedChildrenFileList.add(indexedMets);
                logger.debug("Added '{}' to reindexedChildrenPiList.", pi);
            }
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
        SolrDocumentList hits = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + pi, null);
        if (hits == null || hits.getNumFound() == 0) {
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
        if (isAnchor()) {
            // Keep old IDDOC
            indexObj.setIddoc(Long.valueOf(doc.getFieldValue(SolrConstants.IDDOC).toString()));
            // Delete old doc
            hotfolder.getSolrHelper().deleteDocument(String.valueOf(indexObj.getIddoc()));
            // Delete secondary docs (aggregated metadata, events)
            List<String> iddocsToDelete = new ArrayList<>();
            hits = hotfolder.getSolrHelper()
                    .search(SolrConstants.IDDOC_OWNER + ":" + indexObj.getIddoc(), Collections.singletonList(SolrConstants.IDDOC));
            for (SolrDocument doc2 : hits) {
                iddocsToDelete.add((String) doc2.getFieldValue(SolrConstants.IDDOC));
            }
            if (!iddocsToDelete.isEmpty()) {
                logger.info("Deleting {} secondary documents...", iddocsToDelete.size());
                hotfolder.getSolrHelper().deleteDocuments(new ArrayList<>(iddocsToDelete));
            }
        } else {
            // Recursively delete all children, if not an anchor
            deleteWithPI(pi, false, hotfolder.getSolrHelper());
        }
    }

    /**
     * Recursively re-indexes the logical docstruct subtree of the node represented by the given IndexObject.
     * 
     * @param parentIndexObject {@link IndexObject}
     * @param depth OBSOLETE
     * @param writeStrategy
     * @param dataFolders
     * @throws IOException
     * @throws FatalIndexerException
     */
    private void indexAllChildren(IndexObject parentIndexObject, int depth, ISolrWriteStrategy writeStrategy, Map<String, Path> dataFolders)
            throws IOException, FatalIndexerException {
        logger.trace("indexAllChildren");
        List<Element> childrenNodeList = xp.evaluateToElements("mets:div", parentIndexObject.getRootStructNode());
        for (int i = 0; i < childrenNodeList.size(); i++) {
            Element node = childrenNodeList.get(i);
            IndexObject indexObj = new IndexObject(getNextIddoc(hotfolder.getSolrHelper()));
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
                indexObj.addToLucene(parentIndexObject.getLuceneFieldWithName(SolrConstants.PI_ANCHOR));
            }
            // Inherit GROUPID_* fields
            if (!parentIndexObject.getGroupIds().isEmpty()) {
                for (String groupId : parentIndexObject.getGroupIds().keySet()) {
                    indexObj.addToLucene(parentIndexObject.getLuceneFieldWithName(groupId));
                }
            }

            // Add parent's metadata and SORT_* fields to this docstruct
            Set<String> existingMetadataFields = new HashSet<>();
            Set<String> existingSortFieldNames = new HashSet<>();
            for (LuceneField field : indexObj.getLuceneFields()) {
                if (Configuration.getInstance().getMetadataConfigurationManager().getFieldsToAddToPages().contains(field.getField())) {
                    existingMetadataFields.add(new StringBuilder(field.getField()).append(field.getValue()).toString());
                } else if (field.getField().startsWith(SolrConstants.SORT_)) {
                    existingSortFieldNames.add(field.getField());
                }
            }
            for (LuceneField field : parentIndexObject.getLuceneFields()) {
                if (Configuration.getInstance().getMetadataConfigurationManager().getFieldsToAddToChildren().contains(field.getField())
                        && !existingMetadataFields.contains(new StringBuilder(field.getField()).append(field.getValue()).toString())) {
                    // Avoid duplicates (same field name + value)
                    indexObj.addToLucene(field.getField(), field.getValue());
                    logger.debug("Added {}:{} to child element {}", field.getField(), field.getValue(), indexObj.getLogId());
                } else if (field.getField().startsWith(SolrConstants.SORT_) && !existingSortFieldNames.contains(field.getField())) {
                    // Only one instance of each SORT_ field may exist
                    indexObj.addToLucene(field.getField(), field.getValue());
                }
            }

            indexObj.writeAccessConditions(parentIndexObject);

            // Generate thumbnail info and page docs for this docstruct. PI_TOPSTRUCT must be set at this point!
            if (StringUtils.isNotEmpty(indexObj.getLogId())) {
                try {
                    List<LuceneField> thumbnailFields = mapPagesToDocstruct(indexObj, false, writeStrategy, dataFolders, depth);
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

            // Add grouped metadata as separate documents (must be done after mapping page docs to this docstrct)
            addGroupedMetadataDocs(writeStrategy, indexObj);

            // Add own and all ancestor LABEL values to the DEFAULT field
            StringBuilder sbDefaultValue = new StringBuilder();
            sbDefaultValue.append(indexObj.getDefaultValue());
            String labelWithSpaces = new StringBuilder(" ").append(indexObj.getLabel()).append(' ').toString();
            if (StringUtils.isNotEmpty(indexObj.getLabel()) && !sbDefaultValue.toString().contains(labelWithSpaces)) {
                // logger.info("Adding own LABEL to DEFAULT: " + indexObj.getLabel());
                sbDefaultValue.append(labelWithSpaces);
            }
            if (Configuration.getInstance().isAddLabelToChildren()) {
                for (String label : indexObj.getParentLabels()) {
                    String parentLabelWithSpaces = new StringBuilder(" ").append(label).append(' ').toString();
                    if (StringUtils.isNotEmpty(label) && !sbDefaultValue.toString().contains(parentLabelWithSpaces)) {
                        // logger.info("Adding ancestor LABEL to DEFAULT: " + label);
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
            // write to lucene
            logger.debug("Writing child document '{}'...", indexObj.getIddoc());
            writeStrategy.addDoc(SolrHelper.createDocument(indexObj.getLuceneFields()));
            indexAllChildren(indexObj, depth + 1, writeStrategy, dataFolders);
        }
    }

    /**
     * Sets DMDID, ID, TYPE and LABEL from the METS document.
     * 
     * @param indexObj {@link IndexObject}
     * @throws FatalIndexerException
     */
    private static void setSimpleData(IndexObject indexObj) throws FatalIndexerException {
        logger.trace("setSimpleData(IndexObject) - start");
        Element structNode = indexObj.getRootStructNode();

        // DMDID
        {
            indexObj.setDmdid(TextHelper.normalizeSequence(structNode.getAttributeValue("DMDID")));
            logger.trace("DMDID: {}", indexObj.getDmdid());
        }

        // LOGID
        {
            String value = TextHelper.normalizeSequence(structNode.getAttributeValue("ID"));
            if (value != null) {
                indexObj.setLogId(value);
            }
            logger.trace("LOGID: {}", indexObj.getLogId());
        }

        // TYPE
        {
            String value = TextHelper.normalizeSequence(structNode.getAttributeValue("TYPE"));
            if (value != null) {
                indexObj.setType(value);
            }
        }
        logger.trace("TYPE: {}", indexObj.getType());

        // LABEL
        {
            String value = TextHelper.normalizeSequence(structNode.getAttributeValue("LABEL"));
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
            StringBuffer sbPageUrns = new StringBuffer();
            List<String> imageUrns = new ArrayList<>(physUrnList.size());
            for (String pageUrn : physUrnList) {
                String urn = null;
                if (Utils.isUrn(pageUrn)) {
                    urn = pageUrn.replaceAll("[\\\\]", "");
                }
                if (StringUtils.isEmpty(urn)) {
                    urn = "NOURN";
                }
                //                indexObj.addToLucene(SolrConstants.IMAGEURN_OAI, urn);
                sbPageUrns.append(urn).append(' ');
                imageUrns.add(urn);
            }
            //            indexObj.addToLucene(SolrConstants.PAGEURNS, sbPageUrns.toString());
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
     * Returns the logical root node.
     * 
     * @param indexObj
     * @return {@link Element} or null
     * @throws FatalIndexerException
     * 
     */
    private Element findStructNode(IndexObject indexObj) throws FatalIndexerException {
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
    private boolean isAnchor() throws FatalIndexerException {
        String anchorQuery = "/mets:mets/mets:structMap[@TYPE='PHYSICAL']";
        List<Element> anchorList = xp.evaluateToElements(anchorQuery, null);
        // das habe ich selber hinzugefügt..
        if (anchorList == null || anchorList.isEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * Checks whether this is a volume of a multivolume work (should be false for monographs and anchors).
     * 
     * @return boolean
     * @throws IndexerException
     * @throws FatalIndexerException
     */
    private boolean isVolume() throws IndexerException, FatalIndexerException {
        String query =
                "/mets:mets/mets:dmdSec/mets:mdWrap[@MDTYPE='MODS']/mets:xmlData/mods:mods/mods:relatedItem[@type='host']/mods:recordInfo/mods:recordIdentifier";
        List<Element> relatedItemList = xp.evaluateToElements(query, null);
        if (relatedItemList != null && !relatedItemList.isEmpty()) {
            return true;
        }

        return false;
    }

    /**
     * <p>
     * getMetsCreateDate.
     * </p>
     *
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should return CREATEDATE value
     * @should return null if date does not exist in METS
     * @return a {@link java.util.Date} object.
     */
    protected Date getMetsCreateDate() throws FatalIndexerException {
        String dateString = xp.evaluateToAttributeStringValue("/mets:mets/mets:metsHdr/@CREATEDATE", null);
        if (dateString != null) {
            try {
                return MetadataHelper.formatterISO8601DateTimeFullWithTimeZone.parseDateTime(dateString).toDate();
            } catch (IllegalArgumentException e) {
                try {
                    return MetadataHelper.formatterISO8601Full.parseDateTime(dateString).toDate();
                } catch (IllegalArgumentException e1) {
                    logger.error(e1.getMessage());
                }
            }

        }

        return null;
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
                        .append(MetadataHelper.formatterBasicDateTime.print(System.currentTimeMillis()))
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
