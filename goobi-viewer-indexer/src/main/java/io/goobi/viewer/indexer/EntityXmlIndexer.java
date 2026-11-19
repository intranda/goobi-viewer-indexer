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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Text;
import org.jdom2.filter.Filters;
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
import io.goobi.viewer.indexer.model.IndexingResult;
import io.goobi.viewer.indexer.model.IndexingResult.IndexingResultStatus;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.config.MetadataConfigurationManager;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * Indexer for entityXML files (GND entity exchange format, <code>https://sub.uni-goettingen.de/met/standards/entity-xml#</code>).
 * <p>
 * An entityXML file usually contains many entity records (e.g. <code>exml:person</code>) below <code>exml:collection/exml:data/exml:list</code>.
 * Each entity is indexed as a separate record without pages; its <code>xml:id</code> is used as the PI. All descriptive metadata is mapped via
 * the XPath-based field configuration.
 * </p>
 */
public class EntityXmlIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(EntityXmlIndexer.class);

    private static final String[] DATA_FOLDER_PARAMS = { DataRepository.PARAM_UGC, DataRepository.PARAM_CMS, DataRepository.PARAM_ANNOTATIONS };

    private static final Pattern PATTERN_WHITESPACE = Pattern.compile("\\s+");

    /** Number of indexed records after which progress is logged. */
    private static final int PROGRESS_LOG_INTERVAL = 1000;

    /**
     * Constructor.
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public EntityXmlIndexer(Hotfolder hotfolder) {
        super();
        this.hotfolder = hotfolder;
    }

    /**
     * {@inheritDoc}
     *
     * @should throw IllegalArgumentException if entityXmlFile null
     */
    @Override
    public List<String> addToIndex(Path entityXmlFile, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        currentRecordFile = entityXmlFile;
        try {
            return indexRecordFile(entityXmlFile, reindexSettings);
        } finally {
            // Release the record lock acquired in index(), which is held for the post-processing
            unlockRecord();
            currentRecordFile = null;
        }
    }

    /**
     * Indexes all entity records in the given file and moves their data into the data repository (see {@link #addToIndex(Path, Map)}).
     *
     * @param entityXmlFile Record file
     * @param reindexSettings
     * @return List of successfully indexed record identifiers
     * @throws IOException
     * @throws FatalIndexerException
     */
    private List<String> indexRecordFile(Path entityXmlFile, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        if (entityXmlFile == null) {
            throw new IllegalArgumentException("entityXmlFile may not be null");
        }

        logger.debug("Indexing entityXML file '{}'...", entityXmlFile.getFileName());
        String fileNameRoot = FilenameUtils.getBaseName(entityXmlFile.getFileName().toString());

        // Check data folders in the hotfolder
        Map<String, Path> dataFolders = checkDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

        // Use existing folders for those missing in the hotfolder
        checkReindexSettings(dataFolders, reindexSettings);

        List<Document> entityDocs = JDomXP.splitEntityXmlFile(entityXmlFile.toFile());
        logger.info("File contains {} entityXML records.", entityDocs.size());
        XMLOutputter outputter = new XMLOutputter();

        List<String> ret = new ArrayList<>(entityDocs.size());
        int count = 0;
        for (Document doc : entityDocs) {
            // Release the lock of the previous record (held for its post-processing)
            unlockRecord();
            IndexingResult result = index(doc, dataFolders, null);
            if (IndexingResultStatus.OK.equals(result.getStatus())) {
                String identifier = result.getPi();
                String newEntityXmlFileName = identifier + ".xml";

                // Write individual records as separate files
                Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_ENTITYXML).toAbsolutePath().toString(),
                        newEntityXmlFileName);
                try (FileOutputStream out = new FileOutputStream(indexed.toFile())) {
                    outputter.output(doc, out);
                }
                dataRepository.checkOtherRepositoriesForRecordFileDuplicates(newEntityXmlFileName, DataRepository.PARAM_INDEXED_ENTITYXML,
                        hotfolder.getDataRepositoryStrategy().getAllDataRepositories());

                // Move non-repository data directories to the selected repository
                if (previousDataRepository != null) {
                    previousDataRepository.moveDataFoldersToRepository(dataRepository, identifier);
                    // Update data repository cache map in the Goobi viewer
                    try {
                        Utils.updateDataRepositoryCache(identifier, dataRepository.getPath());
                    } catch (HTTPException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
                recordIndexed(result);

                // Older copies of this record in lower-priority hotfolders must not overwrite this version
                SolrIndexerDaemon.getInstance().registerIndexedRecord(identifier, hotfolder);

                // Add identifier to return list
                if (result.isSubmitPiToViewer()) {
                    ret.add(identifier);
                }
            } else if (!IndexingResultStatus.SKIPPED.equals(result.getStatus())) {
                handleError(entityXmlFile, result.getError(), FileFormat.ENTITYXML);
            }
            count++;
            if (count % PROGRESS_LOG_INTERVAL == 0) {
                logger.info("Processed {}/{} entityXML records.", count, entityDocs.size());
            }
        }

        // Copy original entityXML file into the orig folder, if configured
        if (hotfolder.getOrigEntityXml() != null) {
            Path orig = Paths.get(hotfolder.getOrigEntityXml().toAbsolutePath().toString(), entityXmlFile.getFileName().toString());
            Files.copy(entityXmlFile, orig, StandardCopyOption.REPLACE_EXISTING);
        }

        // Delete files from the hotfolder
        try {
            Files.delete(entityXmlFile);
        } catch (IOException e) {
            logger.error("'{}' could not be deleted; please delete it manually.", entityXmlFile.toAbsolutePath());
        }
        // Delete all data folders for this record from the hotfolder
        DataRepository.deleteDataFoldersFromHotfolder(dataFolders, reindexSettings);
        logger.info("Finished indexing entityXML file '{}'.", entityXmlFile.getFileName());

        return ret;
    }

    /**
     * Indexes a single entityXML record document (root element is the entity element, e.g. <code>exml:person</code>).
     *
     * @param doc Entity record document
     * @param dataFolders Data folders
     * @param inWriteStrategy Optional write strategy injected by the caller
     * @return {@link IndexingResult}
     * @should index record correctly
     * @should update record correctly
     * @should not index internal notes
     * @should index other entity types correctly
     */
    public IndexingResult index(Document doc, Map<String, Path> dataFolders, final ISolrWriteStrategy inWriteStrategy) {
        IndexingResult ret = new IndexingResult();
        String pi = null;
        ISolrWriteStrategy writeStrategy = inWriteStrategy;
        try {
            normalizeWhitespace(doc);
            this.xp = new JDomXP(doc);

            IndexObject indexObj = new IndexObject(getNextIddoc());
            logger.debug("IDDOC: {}", indexObj.getIddoc());
            indexObj.setRootStructNode(doc.getRootElement());

            // Set some simple data in the index object
            setSimpleData(indexObj);

            // Set PI
            pi = validateAndApplyPI(findPI(""), indexObj, true);

            // Serialize with other record files carrying the same PI (other hotfolders, other file names)
            if (!lockRecord(pi)) {
                return skipSupersededRecord(ret, pi);
            }

            // Determine the data repository to use
            selectDataRepository(indexObj, pi, null, dataFolders);

            ret.setPi(indexObj.getPi());

            // Check and use old data folders, if no new ones found
            checkOldDataFolders(dataFolders, DATA_FOLDER_PARAMS, pi);

            if (writeStrategy == null) {
                // Request appropriate write strategy
                writeStrategy = AbstractWriteStrategy.create(null, dataFolders, hotfolder);
            } else {
                logger.info("Solr write strategy injected by caller: {}", writeStrategy.getClass().getName());
            }

            prepareUpdate(indexObj);

            indexObj.pushSimpleDataToLuceneArray();
            MetadataHelper.writeMetadataToObject(indexObj, indexObj.getRootStructNode(), "", xp);

            // Add LABEL value
            if (StringUtils.isEmpty(indexObj.getLabel())) {
                LuceneField field = indexObj.getLuceneFieldWithName("MD_TITLE");
                if (field != null) {
                    indexObj.addToLucene(SolrConstants.LABEL, MetadataHelper.applyValueDefaultModifications(field.getValue()));
                }
            }

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // Entity records have no pages and no anchors
            indexObj.addToLucene(SolrConstants.ISWORK, "true");

            // Write created/updated timestamps
            indexObj.writeDateModified(true);

            indexObj.addToLucene(FIELD_IMAGEAVAILABLE, "false");
            indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, "false");

            // Add DEFAULT field
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(indexObj.getDefaultValue()));
                indexObj.setDefaultValue("");
            }

            // Add grouped metadata as separate documents
            addGroupedMetadataDocs(writeStrategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

            // Flag record identifier for submission to the viewer if it's linked to an archive node
            ret.setSubmitPiToViewer(indexObj.getLuceneFieldWithName(SolrConstants.EAD_NODE_ID) != null);

            // Add root doc
            SolrInputDocument rootDoc = SolrSearchIndex.createDocument(indexObj.getLuceneFields());
            writeStrategy.setRootDoc(rootDoc);

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)
            if (!iddocsToDelete.isEmpty()) {
                logger.info("Removing {} docs of the previous instance of this record from the index...", iddocsToDelete.size());
                SolrIndexerDaemon.getInstance().getSearchIndex().deleteDocuments(new ArrayList<>(iddocsToDelete));
            }

            logger.debug("Writing document to index...");
            writeStrategy.writeDocs(SolrIndexerDaemon.getInstance().getConfiguration().isAggregateRecords());
            ret.setDocStructType(indexObj.getType()).setNumPages(0).setNumSourcePages(0);
            logger.debug("Finished writing data for '{}' to Solr.", pi);
        } catch (FatalIndexerException | IndexerException | IOException | SolrServerException | SolrException e) {
            logger.error("Indexing of '{}' could not be finished due to an error.", pi);
            logger.error(e.getMessage(), e);
            ret.setError(e.getMessage());
            SolrIndexerDaemon.getInstance().getSearchIndex().rollback();
        } finally {
            unlockRecordAfterIndex();
            if (writeStrategy != null) {
                writeStrategy.cleanup();
            }
        }

        return ret;
    }

    /**
     * Collapses whitespace sequences in all text nodes of the given document into single spaces. entityXML deliveries contain pretty-printed
     * values that wrap across lines (e.g. "Frankfurt am\n Main").
     *
     * @param doc Document to modify
     * @should collapse line breaks and indentation in text values
     */
    static void normalizeWhitespace(Document doc) {
        if (doc == null) {
            return;
        }
        // Collect first: modifying text nodes while iterating over descendants is not supported
        List<Text> textNodes = new ArrayList<>();
        for (Text text : doc.getDescendants(Filters.text())) {
            textNodes.add(text);
        }
        for (Text text : textNodes) {
            String value = text.getText();
            String normalized = PATTERN_WHITESPACE.matcher(value).replaceAll(" ");
            if (!normalized.equals(value)) {
                text.setText(normalized);
            }
        }
    }

    /**
     * Sets SOURCEDOCFORMAT and TYPE (from the entity element name, e.g. "person").
     *
     * @param indexObj {@link IndexObject}
     */
    private static void setSimpleData(IndexObject indexObj) {
        indexObj.setSourceDocFormat(FileFormat.ENTITYXML);

        Element structNode = indexObj.getRootStructNode();
        indexObj.setType(MetadataConfigurationManager.mapDocStrct(structNode.getName()).trim());
        logger.trace("TYPE: {}", indexObj.getType());
    }

    @Override
    protected FileFormat getSourceDocFormat() {
        return FileFormat.ENTITYXML;
    }
}
