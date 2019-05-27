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
package de.intranda.digiverso.presentation.solr;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.helper.Configuration;
import de.intranda.digiverso.presentation.solr.helper.Hotfolder;
import de.intranda.digiverso.presentation.solr.helper.JDomXP;
import de.intranda.digiverso.presentation.solr.helper.MetadataHelper;
import de.intranda.digiverso.presentation.solr.helper.SolrHelper;
import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;
import de.intranda.digiverso.presentation.solr.model.IndexObject;
import de.intranda.digiverso.presentation.solr.model.IndexerException;
import de.intranda.digiverso.presentation.solr.model.LuceneField;
import de.intranda.digiverso.presentation.solr.model.SolrConstants;
import de.intranda.digiverso.presentation.solr.model.config.MetadataConfigurationManager;
import de.intranda.digiverso.presentation.solr.model.datarepository.DataRepository;
import de.intranda.digiverso.presentation.solr.model.writestrategy.ISolrWriteStrategy;
import de.intranda.digiverso.presentation.solr.model.writestrategy.LazySolrWriteStrategy;
import de.intranda.digiverso.presentation.solr.model.writestrategy.SerializingSolrWriteStrategy;

public class DenkXwebIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(DenkXwebIndexer.class);

    /**
     * Whitelist of file names belonging for this particular record (in case the media folder contains files for multiple records). StringBuffer is
     * thread-safe.
     */
    private StringBuffer sbImgFileNames = new StringBuffer();

    /**
     * Constructor.
     * 
     * @param hotfolder
     * @should set attributes correctly
     */
    public DenkXwebIndexer(Hotfolder hotfolder) {
        this.hotfolder = hotfolder;
    }

    /**
     * Indexes a DenkXweb file.
     * 
     * @param doc
     * @param dataFolders
     * @param writeStrategy
     * @param pageCountStart
     * @return
     * @should index record correctly
     * @should update record correctly
     */
    public String[] index(Document doc, Map<String, Path> dataFolders, ISolrWriteStrategy writeStrategy, int pageCountStart) {
        String[] ret = { "ERROR", null };
        String pi = null;
        try {
            this.xp = new JDomXP(doc);
            if (this.xp == null) {
                throw new IndexerException("Could not create XML parser.");
            }

            IndexObject indexObj = new IndexObject(getNextIddoc(hotfolder.getSolrHelper()));
            logger.debug("IDDOC: {}", indexObj.getIddoc());
            Element structNode = doc.getRootElement();
            indexObj.setRootStructNode(structNode);

            // set some simple data in den indexObject
            setSimpleData(indexObj);

            // Set PI
            {
                pi = MetadataHelper.getPIFromXML("", xp);
                if (StringUtils.isBlank(pi)) {
                    ret[1] = "PI not found.";
                    throw new IndexerException(ret[1]);
                }

                // Remove prefix
                if (pi.contains(":")) {
                    pi = pi.substring(pi.lastIndexOf(':') + 1);
                }
                if (pi.contains("/")) {
                    pi = pi.substring(pi.lastIndexOf('/') + 1);
                }
                pi = MetadataHelper.applyIdentifierModifications(pi);
                // Do not allow identifiers with illegal characters
                Pattern p = Pattern.compile("[^\\w|-]");
                Matcher m = p.matcher(pi);
                if (m.find()) {
                    ret[1] = "PI contains illegal characters: " + pi;
                    throw new IndexerException(ret[1]);
                }
                indexObj.setPi(pi);
                indexObj.setTopstructPI(pi);
                logger.debug("PI: {}", indexObj.getPi());

                // Determine the data repository to use
                DataRepository[] repositories =
                        hotfolder.getDataRepositoryStrategy().selectDataRepository(pi, null, dataFolders, hotfolder.getSolrHelper());
                dataRepository = repositories[0];
                previousDataRepository = repositories[1];
                if (StringUtils.isNotEmpty(dataRepository.getPath())) {
                    indexObj.setDataRepository(dataRepository.getPath());
                }

                ret[0] = indexObj.getPi();

                if (dataFolders.get(DataRepository.PARAM_MIX) == null) {
                    // Use the old MIX folder
                    dataFolders.put(DataRepository.PARAM_MIX,
                            Paths.get(dataRepository.getDir(DataRepository.PARAM_MIX).toAbsolutePath().toString(), pi));
                    if (!Files.isDirectory(dataFolders.get(DataRepository.PARAM_MIX))) {
                        dataFolders.put(DataRepository.PARAM_MIX, null);
                    } else {
                        logger.info("Using old MIX folder '{}'.", dataFolders.get(DataRepository.PARAM_MIX).toAbsolutePath());
                    }
                }
                if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) == null) {
                    // Use the old TEI metadata folder
                    dataFolders.put(DataRepository.PARAM_TEIMETADATA,
                            Paths.get(dataRepository.getDir(DataRepository.PARAM_TEIMETADATA).toAbsolutePath().toString(), pi));
                    if (!Files.isDirectory(dataFolders.get(DataRepository.PARAM_TEIMETADATA))) {
                        dataFolders.put(DataRepository.PARAM_TEIMETADATA, null);
                    } else {
                        logger.info("Using old TEI metadata folder '{}'.", dataFolders.get(DataRepository.PARAM_TEIMETADATA).toAbsolutePath());
                    }
                }
            }

            if (writeStrategy == null) {
                boolean useSerializingStrategy = false;
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
            indexObj.addToLucene(SolrConstants.SOURCEDOCFORMAT, SolrConstants._LIDO);

            prepareUpdate(indexObj);

            // Process TEI files
            if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) != null) {
                MetadataHelper.processTEIMetadataFiles(indexObj, dataFolders.get(DataRepository.PARAM_TEIMETADATA));
            }

            indexObj.pushSimpleDataToLuceneArray();
            MetadataHelper.writeMetadataToObject(indexObj, indexObj.getRootStructNode(), "", xp);

            // If this is a volume (= has an anchor) that has already been indexed, copy access conditions from the anchor element
            if (indexObj.isVolume()) {
                String anchorPi = MetadataHelper.getAnchorPi(xp);
                if (anchorPi != null) {
                    SolrDocumentList hits = hotfolder.getSolrHelper()
                            .search(SolrConstants.PI + ":" + anchorPi, Collections.singletonList(SolrConstants.ACCESSCONDITION));
                    if (hits != null && hits.getNumFound() > 0) {
                        Collection<Object> fields = hits.get(0).getFieldValues(SolrConstants.ACCESSCONDITION);
                        for (Object o : fields) {
                            indexObj.getAccessConditions().add(o.toString());
                        }
                    }
                }
            }

            // Add LABEL value
            if (StringUtils.isEmpty(indexObj.getLabel())) {
                LuceneField field = indexObj.getLuceneFieldWithName("MD_TITLE");
                if (field != null) {
                    indexObj.addToLucene(SolrConstants.LABEL, MetadataHelper.applyValueDefaultModifications(field.getValue()));
                }
            }

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // ISWORK only for non-anchors
            indexObj.addToLucene(SolrConstants.ISWORK, "true");
            logger.trace("ISWORK: {}", indexObj.getLuceneFieldWithName(SolrConstants.ISWORK).getValue());

            if (indexObj.getNumPages() > 0) {
                // Write number of pages
                indexObj.addToLucene(SolrConstants.NUMPAGES, String.valueOf(writeStrategy.getPageDocsSize()));

                // Add used-generated content docs
                for (int i = 1; i <= writeStrategy.getPageDocsSize(); ++i) {
                    SolrInputDocument pageDoc = writeStrategy.getPageDocForOrder(i);
                    if (pageDoc == null) {
                        logger.error("Page {} not found, cannot check for UGC contents.", i);
                        continue;
                    }
                    int order = (Integer) pageDoc.getFieldValue(SolrConstants.ORDER);
                    String pageFileBaseName = FilenameUtils.getBaseName((String) pageDoc.getFieldValue(SolrConstants.FILENAME));
                    if (dataFolders.get(DataRepository.PARAM_UGC) != null && !ugcAddedChecklist.contains(order)) {
                        writeStrategy.addDocs(generateUserGeneratedContentDocsForPage(pageDoc, dataFolders.get(DataRepository.PARAM_UGC),
                                String.valueOf(indexObj.getTopstructPI()), order, pageFileBaseName));
                        ugcAddedChecklist.add(order);
                    }
                }
            }

            // Write created/updated timestamps
            indexObj.writeDateModified(!noTimestampUpdate);

            // If full-text has been indexed for any page, set a boolean in the root doc indicating that the records does have full-text
            indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, String.valueOf(recordHasFulltext));

            // Add DEFAULT field
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(indexObj.getDefaultValue()));
                // indexObj.getSuperDefaultBuilder().append(' ').append(indexObj.getDefaultValue().trim());
                indexObj.setDefaultValue("");
            }

            // Add grouped metadata as separate documents
            addGroupedMetadataDocs(writeStrategy, indexObj);

            // Add root doc
            SolrInputDocument rootDoc = SolrHelper.createDocument(indexObj.getLuceneFields());
            writeStrategy.setRootDoc(rootDoc);

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)
            logger.debug("Writing document to index...");
            writeStrategy.writeDocs(Configuration.getInstance().isAggregateRecords());

            // Return image file names
            if (sbImgFileNames.length() > 0 && sbImgFileNames.charAt(0) == ';') {
                sbImgFileNames.deleteCharAt(0);
            }
            ret[1] = sbImgFileNames.toString();
            logger.info("Successfully finished indexing '{}'.", pi);
        } catch (Exception e) {
            if ("No image resource sets found.".equals(e.getMessage())) {
                logger.error("Indexing of '{}' could not be finished due to an error: {}", pi, e.getMessage());
            } else {
                logger.error("Indexing of '{}' could not be finished due to an error.", pi);
                logger.error(e.getMessage(), e);
            }
            ret[0] = "ERROR";
            ret[1] = e.getMessage();
            hotfolder.getSolrHelper().rollback();
        } finally {
            if (writeStrategy != null) {
                writeStrategy.cleanup();
            }
        }

        return ret;
    }

    /**
     * Prepares the given record for an update. Creation timestamp is preserved. A new update timestamp is added, child docs are removed.
     * 
     * @param indexObj {@link IndexObject}
     * @throws IOException -
     * @throws SolrServerException
     * @throws FatalIndexerException
     */
    private void prepareUpdate(IndexObject indexObj) throws IOException, SolrServerException, FatalIndexerException {
        String pi = indexObj.getPi().trim();
        SolrDocumentList hits = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + pi, null);
        if (hits != null && hits.getNumFound() > 0) {
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
            // Recursively delete all children
            deleteWithPI(pi, false, hotfolder.getSolrHelper());
        }
    }

    /**
     * Sets TYPE and LABEL from the LIDO document.
     * 
     * @param indexObj {@link IndexObject}
     * @throws FatalIndexerException
     */
    private static void setSimpleData(IndexObject indexObj) throws FatalIndexerException {
        Element structNode = indexObj.getRootStructNode();

        // Set type
        {
            String value = structNode.getAttributeValue("type");
            if (StringUtils.isNotEmpty(value)) {
                indexObj.setType(MetadataConfigurationManager.mapDocStrct(value).trim());
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

    public static FilenameFilter txt = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".txt");
        }
    };

    public static FilenameFilter xml = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".xml");
        }
    };
}
