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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Element;
import org.jdom2.Namespace;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
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
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * Indexer implementation for EAD documents.
 */
public class EadIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(EadIndexer.class);

    /** Constant <code>NAMESPACE_EAD2</code> */
    public static final Namespace NAMESPACE_EAD2 = Namespace.getNamespace("ead", "urn:isbn:1-931666-22-9");

    private ForkJoinPool pool;

    protected Namespace eadNamespace = SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("ead3");

    /**
     * Constructor.
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public EadIndexer(Hotfolder hotfolder) {
        super();
        this.hotfolder = hotfolder;
        SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().put("ead", NAMESPACE_EAD2);
    }

    /**
     * <p>Constructor for EadIndexer.</p>
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object
     * @param httpConnector a {@link io.goobi.viewer.indexer.helper.HttpConnector} object
     */
    public EadIndexer(Hotfolder hotfolder, HttpConnector httpConnector) {
        super(httpConnector);
        this.hotfolder = hotfolder;
        SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().put("ead", NAMESPACE_EAD2);
    }

    /** {@inheritDoc} */
    @Override
    public List<String> addToIndex(Path eadFile, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        String fileNameRoot = FilenameUtils.getBaseName(eadFile.getFileName().toString());

        // Check data folders in the hotfolder
        Map<String, Path> dataFolders = checkDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

        // Use existing folders for those missing in the hotfolder
        checkReindexSettings(dataFolders, reindexSettings);

        String[] resp = index(eadFile, dataFolders, null);
        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            String newFileName = resp[0];
            String pi = FilenameUtils.getBaseName(newFileName);
            Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_EAD).toAbsolutePath().toString(), newFileName);
            if (eadFile.equals(indexed)) {
                return Collections.singletonList(pi);
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
            prerenderPagePdfsIfRequired(pi);
            logger.info("Successfully finished indexing '{}'.", eadFile.getFileName());

            // Remove this file from lower priority hotfolders to avoid overriding changes with older version
            SolrIndexerDaemon.getInstance().removeRecordFileFromLowerPriorityHotfolders(pi, hotfolder);

            return Collections.singletonList(pi);
        }

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

        return Collections.emptyList();
    }

    /**
     * Indexes the given METS file.
     *
     * @param eadFile {@link java.nio.file.Path}
     * @param dataFolders a {@link java.util.Map} object.
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @return an array of {@link java.lang.String} objects.
     * @should index record correctly
     * @should update record correctly
     * @should set access conditions correctly
     */
    public String[] index(Path eadFile, Map<String, Path> dataFolders, ISolrWriteStrategy writeStrategy) {
        String[] ret = { null, null };

        if (eadFile == null || !Files.exists(eadFile)) {
            throw new IllegalArgumentException("eadFile must point to an existing EAD file.");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null.");
        }

        logger.debug("Indexing EAD file '{}'...", eadFile.getFileName());
        try {
            initJDomXP(eadFile);
            IndexObject indexObj = new IndexObject(getNextIddoc());
            logger.debug("IDDOC: {}", indexObj.getIddoc());
            Element structNode = findStructNode();
            if (structNode == null) {
                throw new IndexerException("STRUCT NODE not found.");
            }

            indexObj.setRootStructNode(structNode);

            // set some simple data in den indexObject
            setSimpleData(indexObj);

            // Set PI (from file name)
            String pi = validateAndApplyPI(MetadataHelper.applyIdentifierModifications(FilenameUtils.getBaseName(eadFile.getFileName().toString())),
                    indexObj, false);

            // Determine the data repository to use
            selectDataRepository(indexObj, pi, eadFile, dataFolders);

            ret[0] = new StringBuilder(indexObj.getPi()).append(FileTools.XML_EXTENSION).toString();

            // Check and use old data folders, if no new ones found
            checkOldDataFolders(dataFolders, new String[] { DataRepository.PARAM_ANNOTATIONS }, pi);

            if (writeStrategy == null) {
                // Request appropriate write strategy
                writeStrategy = AbstractWriteStrategy.create(eadFile, dataFolders, hotfolder);
            } else {
                logger.info("Solr write strategy injected by caller: {}", writeStrategy.getClass().getName());
            }

            prepareUpdate(indexObj);

            // put some simple data in lucene array
            indexObj.pushSimpleDataToLuceneArray();

            // Write metadata relative to the mdWrap
            MetadataHelper.writeMetadataToObject(indexObj, xp.getMdWrap(indexObj.getDmdid()), "", xp);

            // Write root metadata (outside of MODS sections)
            MetadataHelper.writeMetadataToObject(indexObj, xp.getRootElement(), "", xp);

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // Write created/updated timestamps
            indexObj.writeDateModified(false);

            // If images have been found for any page, set a boolean in the root doc indicating that the record does have images
            indexObj.addToLucene(FIELD_IMAGEAVAILABLE, String.valueOf(recordHasImages));

            // If full-text has been indexed for any page, set a boolean in the root doc indicating that the record does have full-text
            indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, String.valueOf(recordHasFulltext));

            indexObj.addToLucene(SolrConstants.ISWORK, "true");

            // Add SEARCHTERMS_ARCHIVE field (instead of DEFAULT)
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.SEARCHTERMS_ARCHIVE, cleanUpDefaultField(indexObj.getDefaultValue()));
                indexObj.setDefaultValue("");
            }

            // Add mime type
            indexObj.addToLucene(SolrConstants.MIMETYPE, "application/xml");

            // Index all child elements recursively
            List<IndexObject> childObjectList =
                    indexAllChildren(indexObj, 1, writeStrategy, SolrIndexerDaemon.getInstance().getConfiguration().getThreads() > 1);
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
     * @param parentIndexObject {@link io.goobi.viewer.indexer.model.IndexObject}
     * @param depth OBSOLETE
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object
     * @param allowParallelProcessing If true, this node's immediate children may be processed in parallel
     * @return List of <code>LuceneField</code>s to inherit up the hierarchy.
     * @throws java.io.IOException
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     */
    protected List<IndexObject> indexAllChildren(IndexObject parentIndexObject, int depth, ISolrWriteStrategy writeStrategy,
            boolean allowParallelProcessing) throws IOException, FatalIndexerException {
        logger.debug("indexAllChildren: {}", depth);
        List<IndexObject> ret = new ArrayList<>();

        List<Element> childrenNodeList;
        if ("c".equals(parentIndexObject.getRootStructNode().getName())) {
            childrenNodeList = parentIndexObject.getRootStructNode()
                    .getChildren("c", eadNamespace);
        } else if ("ead".equals(parentIndexObject.getRootStructNode().getName())) {
            // ead:archdesc/ead:dsc/ead:c
            childrenNodeList = parentIndexObject.getRootStructNode()
                    .getChild("archdesc", eadNamespace)
                    .getChild("dsc", eadNamespace)
                    .getChildren("c", eadNamespace);
        } else {
            logger.warn("Unknown node name: {}", parentIndexObject.getRootStructNode().getName());
            return Collections.emptyList();
        }
        if (logger.isDebugEnabled() && !childrenNodeList.isEmpty()) {
            logger.debug("{} child elements found", childrenNodeList.size());
        }

        if (allowParallelProcessing && pool == null && childrenNodeList.size() >= SolrIndexerDaemon.getInstance().getConfiguration().getThreads()) {
            // Generate each page document in its own thread
            logger.info("Processing {} nodes in parallel in {} threads (node depth={})...", childrenNodeList.size(),
                    SolrIndexerDaemon.getInstance().getConfiguration().getThreads(), depth);
            pool = new ForkJoinPool(SolrIndexerDaemon.getInstance().getConfiguration().getThreads());
            try {
                pool.submit(() -> childrenNodeList.parallelStream().forEachOrdered(node -> {
                    int order = childrenNodeList.indexOf(node); // TODO This is expensive (O(n))
                    try {
                        // Do not use parallel processing in recursion
                        IndexObject obj = indexChild(node, parentIndexObject, depth, order, writeStrategy, false);
                        if (obj != null) {
                            ret.add(obj);
                        }
                    } catch (FatalIndexerException e) {
                        logger.error("Should be exiting here now...");
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                })).get(GENERATE_PAGE_DOCUMENT_TIMEOUT_HOURS, TimeUnit.HOURS);
            } catch (ExecutionException e) {
                logger.error(e.getMessage(), e);
                SolrIndexerDaemon.getInstance().stop();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
                Thread.currentThread().interrupt();
            } catch (TimeoutException e) {
                logger.error(e.getMessage());
            } finally {
                pool.shutdown();
                pool = null;
            }
        } else {
            int order = 0;
            for (final Element node : childrenNodeList) {
                IndexObject obj = indexChild(node, parentIndexObject, depth, order++, writeStrategy, allowParallelProcessing);
                if (obj != null) {
                    ret.add(obj);
                }
            }
        }

        return ret;
    }

    /**
     * <p>indexChild.</p>
     *
     * @param node a {@link org.jdom2.Element} object
     * @param parentIndexObject a {@link io.goobi.viewer.indexer.model.IndexObject} object
     * @param depth a int
     * @param order a int
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object
     * @param allowParallelProcessing a boolean
     * @return Created {@link io.goobi.viewer.indexer.model.IndexObject} if it has metadata fields to inherit upwards; otherwise null
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @throws java.io.IOException
     */
    public IndexObject indexChild(Element node, IndexObject parentIndexObject, int depth, int order, ISolrWriteStrategy writeStrategy,
            boolean allowParallelProcessing) throws FatalIndexerException, IOException {
        IndexObject indexObj = new IndexObject(getNextIddoc());
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

        indexObj.addToLucene(SolrConstants.MIMETYPE, "application/xml");

        indexObj.addToLucene(SolrConstants.PREFIX_SORTNUM + "ARCHIVE_ORDER", String.valueOf(order));

        // TODO id, level, unitid, unittitle, physdesc, etc.

        // Set parent's DATEUPDATED value (needed for OAI)
        for (Long dateUpdated : parentIndexObject.getDateUpdated()) {
            if (!indexObj.getDateUpdated().contains(dateUpdated)) {
                indexObj.getDateUpdated().add(dateUpdated);
                indexObj.addToLucene(SolrConstants.DATEUPDATED, String.valueOf(dateUpdated));
            }
        }

        // write metadata
        logger.debug("Writing metadata");
        MetadataHelper.writeMetadataToObject(indexObj, node, "", xp);

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
        if (StringUtils.isNotEmpty(indexObj.getLogId()) && indexObj.getNumPages() > 0) {
            // Write number of pages and first/last page labels for this docstruct
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

        // Add own and all ancestor LABEL values to the DEFAULT field
        StringBuilder sbDefaultValue = new StringBuilder();
        sbDefaultValue.append(indexObj.getDefaultValue());
        String labelWithSpaces = new StringBuilder(" ").append(indexObj.getLabel()).append(' ').toString();
        if (StringUtils.isNotEmpty(indexObj.getLabel()) && !sbDefaultValue.toString().contains(labelWithSpaces)) {
            sbDefaultValue.append(labelWithSpaces);
        }
        if (SolrIndexerDaemon.getInstance().getConfiguration().isAddLabelToChildren()) {
            logger.debug("Adding label to children");
            for (String label : indexObj.getParentLabels()) {
                String parentLabelWithSpaces = new StringBuilder(" ").append(label).append(' ').toString();
                if (StringUtils.isNotEmpty(label) && !sbDefaultValue.toString().contains(parentLabelWithSpaces)) {
                    sbDefaultValue.append(parentLabelWithSpaces);
                }
            }
        }

        indexObj.setDefaultValue(sbDefaultValue.toString());

        // Add SEARCHTERMS_ARCHIVE field (instead of DEFAULT)
        if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
            indexObj.addToLucene(SolrConstants.SEARCHTERMS_ARCHIVE, cleanUpDefaultField(indexObj.getDefaultValue()));
            // Add default value to parent doc
            indexObj.setDefaultValue("");
        }

        // Recursively index child elements 
        List<IndexObject> childObjectList = indexAllChildren(indexObj, depth + 1, writeStrategy, allowParallelProcessing);

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

        // The following steps must be performed after adding child metadata and marking own metadata for skipping

        // Add grouped metadata as separate documents (must be done after mapping page docs to this docstrct and after adding grouped metadata from child elements)
        addGroupedMetadataDocs(writeStrategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

        // Apply field modifications that should happen at the very end
        indexObj.applyFinalModifications();

        // Write to Solr
        logger.debug("Writing child document '{}'...", indexObj.getIddoc());
        writeStrategy.addDoc(SolrSearchIndex.createDocument(indexObj.getLuceneFields()));

        // If there are fields to inherit up the hierarchy, add this index object to the return list
        if (!indexObj.getFieldsToInheritToParents().isEmpty()) {
            return indexObj;
        }

        return null;
    }

    /**
     * Sets DMDID, ID, TYPE and LABEL from the METS document.
     *
     * @param indexObj {@link io.goobi.viewer.indexer.model.IndexObject}
     */
    protected void setSimpleData(IndexObject indexObj) {
        logger.trace("setSimpleData(IndexObject) - start");

        indexObj.setDocType(DocType.ARCHIVE);
        indexObj.setSourceDocFormat(getSourceDocFormat());

        Element structNode = indexObj.getRootStructNode();

        // LOGID / DMDID

        String value = TextHelper.normalizeSequence(structNode.getAttributeValue("id"));
        if (value != null) {
            indexObj.setLogId(value);
            indexObj.setDmdid(value);
        } else {
            // Root element
            value = xp.evaluateToAttributeStringValue("ead:archdesc/@id", structNode);
            if (value != null) {
                indexObj.setLogId(value);
                indexObj.setDmdid(value);
            }
        }
        logger.trace("LOGID: {}", indexObj.getLogId());

        // TYPE 
        value = xp.evaluateToAttributeStringValue("ead:archdesc/@type", structNode);
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
     * Returns the logical root node.
     * 
     * @return {@link Element} or null
     * 
     */
    private Element findStructNode() {
        String query = "ead:ead";
        List<Element> elements = xp.evaluateToElements(query, null);
        if (!elements.isEmpty()) {
            return elements.get(0);
        }

        return null;
    }

    /**
     * <p>getSourceDocFormat.</p>
     *
     * @return a {@link io.goobi.viewer.indexer.helper.JDomXP.FileFormat} object
     */
    protected FileFormat getSourceDocFormat() {
        return FileFormat.EAD;
    }
}
