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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Element;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.DateTools;
import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.HttpConnector;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.GroupedMetadata;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * Indexer implementation for EAD documents.
 */
public class EadIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(EadIndexer.class);

    /**
     * Constructor.
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public EadIndexer(Hotfolder hotfolder) {
        super();
        this.hotfolder = hotfolder;
    }

    /**
     * 
     * @param hotfolder
     * @param httpConnector
     */
    public EadIndexer(Hotfolder hotfolder, HttpConnector httpConnector) {
        super(httpConnector);
        this.hotfolder = hotfolder;
    }

    /**
     * Indexes the given METS file.
     * 
     * @param eadFile {@link Path}
     * @param fromReindexQueue
     * @param reindexSettings
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     * 
     */
    @Override
    public void addToIndex(Path eadFile, boolean fromReindexQueue, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        String fileNameRoot = FilenameUtils.getBaseName(eadFile.getFileName().toString());

        // Check data folders in the hotfolder
        Map<String, Path> dataFolders = checkDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

        // Use existing folders for those missing in the hotfolder
        checkReindexSettings(dataFolders, reindexSettings);

        String[] resp = index(eadFile, fromReindexQueue, dataFolders, null);
        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            String newFileName = resp[0];
            String pi = FilenameUtils.getBaseName(newFileName);
            Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_EAD).toAbsolutePath().toString(), newFileName);
            if (eadFile.equals(indexed)) {
                return;
            }

            Files.copy(eadFile, indexed, StandardCopyOption.REPLACE_EXISTING);
            dataRepository.checkOtherRepositoriesForRecordFileDuplicates(newFileName, DataRepository.PARAM_INDEXED_EAD,
                    hotfolder.getDataRepositoryStrategy().getAllDataRepositories());

            if (previousDataRepository != null) {
                // Move non-repository data folders to the selected repository
                previousDataRepository.moveDataFoldersToRepository(dataRepository, FilenameUtils.getBaseName(newFileName));
            }

            // Delete unsupported data folders
            FileTools.deleteUnsupportedDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

            try {
                Files.delete(eadFile);
            } catch (IOException e) {
                logger.warn(LOG_COULD_NOT_BE_DELETED, eadFile.toAbsolutePath());
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
            logger.info("Successfully finished indexing '{}'.", eadFile.getFileName());

            // Remove this file from lower priority hotfolders to avoid overriding changes with older version
            SolrIndexerDaemon.getInstance().removeRecordFileFromLowerPriorityHotfolders(pi, hotfolder);
        } else {
            // Error
            if (hotfolder.isDeleteContentFilesOnFailure()) {
                // Delete all data folders for this record from the hotfolder
                DataRepository.deleteDataFoldersFromHotfolder(dataFolders, reindexSettings);
            }
            handleError(eadFile, resp[1], getSourceDocFormat());
            try {
                Files.delete(eadFile);
            } catch (IOException e) {
                logger.error(LOG_COULD_NOT_BE_DELETED, eadFile.toAbsolutePath());
            }
        }
    }

    /**
     * Indexes the given METS file.
     *
     * @param metsFile {@link java.nio.file.Path}
     * @param fromReindexQueue a boolean.
     * @param dataFolders a {@link java.util.Map} object.
     * @param pageCountStart Order number for the first page.
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
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
    public String[] index(Path eadFile, boolean fromReindexQueue, Map<String, Path> dataFolders, ISolrWriteStrategy writeStrategy) {
        String[] ret = { null, null };

        if (eadFile == null || !Files.exists(eadFile)) {
            throw new IllegalArgumentException("eadFile must point to an existing METS file.");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null.");
        }

        logger.debug("Indexing EAD file '{}'...", eadFile.getFileName());
        try {
            initJDomXP(eadFile);
            IndexObject indexObj = new IndexObject(getNextIddoc(SolrIndexerDaemon.getInstance().getSearchIndex()));
            logger.debug("IDDOC: {}", indexObj.getIddoc());
            Element structNode = findStructNode(indexObj);
            if (structNode == null) {
                throw new IndexerException("STRUCT NODE not found.");
            }

            indexObj.setRootStructNode(structNode);

            // set some simple data in den indexObject
            setSimpleData(indexObj);
            setUrn(indexObj);

            // Set PI
            String[] foundPi = MetadataHelper.getPIFromXML("", xp);
            if (foundPi.length == 0 || StringUtils.isBlank(foundPi[0])) {
                ret[1] = "PI not found.";
                throw new IndexerException(ret[1]);
            }

            String pi = MetadataHelper.applyIdentifierModifications(foundPi[0]);
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

            // Add PI to default
            if (foundPi.length > 1 && "addToDefault".equals(foundPi[1])) {
                indexObj.setDefaultValue(indexObj.getDefaultValue() + " " + pi);
            }

            // Determine the data repository to use
            DataRepository[] repositories =
                    hotfolder.getDataRepositoryStrategy()
                            .selectDataRepository(pi, eadFile, dataFolders, SolrIndexerDaemon.getInstance().getSearchIndex(),
                                    SolrIndexerDaemon.getInstance().getOldSearchIndex());
            dataRepository = repositories[0];
            previousDataRepository = repositories[1];
            if (StringUtils.isNotEmpty(dataRepository.getPath())) {
                indexObj.setDataRepository(dataRepository.getPath());
            }

            ret[0] = new StringBuilder(indexObj.getPi()).append(FileTools.XML_EXTENSION).toString();

            // Check and use old data folders, if no new ones found
            checkOldDataFolder(dataFolders, DataRepository.PARAM_ANNOTATIONS, pi);

            if (writeStrategy == null) {
                // Request appropriate write strategy
                writeStrategy = AbstractWriteStrategy.create(eadFile, dataFolders, hotfolder);
            } else {
                logger.info("Solr write strategy injected by caller: {}", writeStrategy.getClass().getName());
            }

            // Set source doc format
            indexObj.addToLucene(SolrConstants.SOURCEDOCFORMAT, getSourceDocFormat().name());
            prepareUpdate(indexObj);

            // put some simple data in lucene array
            indexObj.pushSimpleDataToLuceneArray();

            // Write metadata relative to the mdWrap
            MetadataHelper.writeMetadataToObject(indexObj, xp.getMdWrap(indexObj.getDmdid()), "", xp);

            // Write root metadata (outside of MODS sections)
            MetadataHelper.writeMetadataToObject(indexObj, xp.getRootElement(), "", xp);

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // Read DATECREATED/DATEUPDATED from METS
            indexObj.populateDateCreatedUpdated(getMetsCreateDate());

            // Write created/updated timestamps
            indexObj.writeDateModified(false);

            // If images have been found for any page, set a boolean in the root doc indicating that the record does have images
            indexObj.addToLucene(FIELD_IMAGEAVAILABLE, String.valueOf(recordHasImages));

            // If full-text has been indexed for any page, set a boolean in the root doc indicating that the record does have full-text
            indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, String.valueOf(recordHasFulltext));

            // write all page URNs sequentially into one field
            generatePageUrns(indexObj);

            indexObj.addToLucene(SolrConstants.ISWORK, "true");

            // Add DEFAULT field
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(indexObj.getDefaultValue()));
                indexObj.setDefaultValue("");
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
                        .checkAndCreateGroupDoc(groupIdField, indexObj.getGroupIds().get(groupIdField), moreMetadata,
                                getNextIddoc(SolrIndexerDaemon.getInstance().getSearchIndex()));
                if (doc != null) {
                    writeStrategy.addDoc(doc);
                    logger.debug("Created group document for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                } else {
                    logger.debug("Group document already exists for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                }
            }

            // Index all child elements recursively
            List<IndexObject> childObjectList = indexAllChildren(indexObj, 1, writeStrategy);
            indexObj.addChildMetadata(childObjectList);

            // Add grouped metadata as separate documents
            addGroupedMetadataDocs(writeStrategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

            // Apply field modifications that should happen at the very end
            indexObj.applyFinalModifications();

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)

            logger.debug("Writing document to index...");
            SolrInputDocument rootDoc = SolrSearchIndex.createDocument(indexObj.getLuceneFields());
            writeStrategy.setRootDoc(rootDoc);

            writeStrategy.writeDocs(SolrIndexerDaemon.getInstance().getConfiguration().isAggregateRecords());
            logger.info("Finished writing data for '{}' to Solr.", pi);
        } catch (Exception e) {
            logger.error("Indexing of '{}' could not be finished due to an error.", eadFile.getFileName());
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
        logger.trace("indexAllChildren: {}", depth);
        List<IndexObject> ret = new ArrayList<>();

        List<Element> childrenNodeList = xp.evaluateToElements("ead:c", parentIndexObject.getRootStructNode());
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

            // TODO id, level, unitid, unittitle, physdesc, etc.

            // Set parent's DATEUPDATED value (needed for OAI)
            for (Long dateUpdated : parentIndexObject.getDateUpdated()) {
                if (!indexObj.getDateUpdated().contains(dateUpdated)) {
                    indexObj.getDateUpdated().add(dateUpdated);
                    indexObj.addToLucene(SolrConstants.DATEUPDATED, String.valueOf(dateUpdated));
                }
            }

            // write metadata
            MetadataHelper.writeMetadataToObject(indexObj, node, "", xp);

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

            // Add grouped metadata as separate documents (must be done after mapping page docs to this docstrct and after adding grouped metadata from child elements)
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
    private static void setSimpleData(IndexObject indexObj) {
        logger.trace("setSimpleData(IndexObject) - start");
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
     * @return
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
        String query = "//ead:archdesc/ead:dsc/ead:c";
        List<Element> elements = xp.evaluateToElements(query, null);
        if (!elements.isEmpty()) {
            return elements.get(0);
        }

        return null;
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
     * 
     * @return
     */
    protected FileFormat getSourceDocFormat() {
        return FileFormat.EAD;
    }
}
