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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.helper.XmlTools;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * Indexer implementation for CMS page metadata.
 */
public class CmsPageIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(CmsPageIndexer.class);

    /**
     * Constructor.
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public CmsPageIndexer(Hotfolder hotfolder) {
        super();
        this.hotfolder = hotfolder;
    }

    /**
     * {@inheritDoc}
     * 
     * @should add record to index correctly
     */
    @Override
    public List<String> addToIndex(Path cmsFile, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        Map<String, Path> dataFolders = new HashMap<>();

        String fileNameRoot = FilenameUtils.getBaseName(cmsFile.getFileName().toString());

        String[] resp = index(cmsFile, dataFolders, null, SolrIndexerDaemon.getInstance().getConfiguration().getPageCountStart());
        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            String newCmsFileName = resp[0];
            String pi = FilenameUtils.getBaseName(newCmsFileName);
            Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_CMS).toAbsolutePath().toString(), newCmsFileName);
            if (cmsFile.equals(indexed)) {
                return Collections.singletonList(pi);
            }
            Files.copy(cmsFile, indexed, StandardCopyOption.REPLACE_EXISTING);
            dataRepository.checkOtherRepositoriesForRecordFileDuplicates(newCmsFileName, DataRepository.PARAM_INDEXED_CMS,
                    hotfolder.getDataRepositoryStrategy().getAllDataRepositories());

            if (previousDataRepository != null) {
                // Move non-repository data folders to the selected repository
                previousDataRepository.moveDataFoldersToRepository(dataRepository, FilenameUtils.getBaseName(newCmsFileName));
            }

            // Copy and delete media folder
            if (dataRepository.checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_MEDIA,
                    hotfolder.getDataRepositoryStrategy().getAllDataRepositories()) > 0) {
                String msg = Utils.removeRecordImagesFromCache(FilenameUtils.getBaseName(resp[0]));
                if (msg != null) {
                    logger.info(msg);
                }
            }

            // Copy data folders
            dataRepository.copyAndDeleteAllDataFolders(pi, dataFolders, reindexSettings,
                    hotfolder.getDataRepositoryStrategy().getAllDataRepositories());

            // Delete unsupported data folders
            FileTools.deleteUnsupportedDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

            try {
                Files.delete(cmsFile);
            } catch (IOException e) {
                logger.warn(LOG_COULD_NOT_BE_DELETED, cmsFile.toAbsolutePath());
            }

            // Update data repository cache map in the Goobi viewer
            if (previousDataRepository != null) {
                try {
                    Utils.updateDataRepositoryCache(pi, dataRepository.getPath());
                } catch (HTTPException e) {
                    logger.error(e.getMessage(), e);
                }
            }

            return Collections.singletonList(pi);
        }

        // Error
        if (hotfolder.isDeleteContentFilesOnFailure()) {
            // Delete all data folders for this record from the hotfolder
            DataRepository.deleteDataFoldersFromHotfolder(dataFolders, reindexSettings);
        }
        handleError(cmsFile, resp[1], FileFormat.CMS);
        try {
            Files.delete(cmsFile);
        } catch (IOException e) {
            logger.error(LOG_COULD_NOT_BE_DELETED, cmsFile.toAbsolutePath());
        }

        return Collections.emptyList();
    }

    /**
     * Indexes the given CMS page file.
     *
     * @param cmsFile {@link java.nio.file.Path}
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
    public String[] index(Path cmsFile, Map<String, Path> dataFolders, final ISolrWriteStrategy writeStrategy,
            int pageCountStart) {
        String[] ret = { null, null };

        if (cmsFile == null || !Files.exists(cmsFile)) {
            throw new IllegalArgumentException("dcfile must point to an existing Dublin Core file.");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null.");
        }

        logger.debug("Indexing CMS page file '{}'...", cmsFile.getFileName());
        ISolrWriteStrategy useWriteStrategy = writeStrategy;
        try {

            if (useWriteStrategy == null) {
                // Request appropriate write strategy
                useWriteStrategy = AbstractWriteStrategy.create(cmsFile, dataFolders, hotfolder);
            } else {
                logger.info("Solr write strategy injected by caller: {}", useWriteStrategy.getClass().getName());
            }

            Document doc = XmlTools.readXmlFile(cmsFile);
            IndexObject indexObj = new IndexObject(getNextIddoc());
            logger.debug("IDDOC: {}", indexObj.getIddoc());

            indexObj.setSourceDocFormat(FileFormat.CMS);

            // LOGID
            indexObj.setLogId("LOG0000");

            // TYPE
            indexObj.setType("cms_page");

            // Set PI
            String pi = doc.getRootElement().getAttributeValue("id");
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
                selectDataRepository(indexObj, pi, cmsFile, dataFolders);

                ret[0] = new StringBuilder(indexObj.getPi()).append(FileTools.XML_EXTENSION).toString();
            } else {
                ret[1] = "PI not found.";
                throw new IndexerException(ret[1]);
            }

            prepareUpdate(indexObj);

            // Set title
            List<Element> eleListTitle = doc.getRootElement().getChildren("title");
            if (eleListTitle != null) {
                for (Element eleTitle : eleListTitle) {
                    if (StringUtils.isEmpty(indexObj.getLabel())) {
                        indexObj.setLabel(eleTitle.getTextTrim());
                    }
                    if (eleTitle.getAttribute("lang") != null) {
                        indexObj.addToLucene("MD_TITLE" + SolrConstants.MIXFIX_LANG + eleTitle.getAttributeValue("lang").toUpperCase(),
                                eleTitle.getTextTrim());
                    } else {
                        indexObj.addToLucene("MD_TITLE", eleTitle.getTextTrim());
                    }
                }
            }

            // Categories
            List<Element> eleListCategories = doc.getRootElement().getChild("categories").getChildren("category");
            if (eleListCategories != null) {
                for (Element eleCat : eleListCategories) {
                    String value = eleCat.getText();
                    if (StringUtils.isNotEmpty(value)) {
                        indexObj.addToLucene("MD_CATEGORY", value);
                    }
                }
            }

            // Texts
            List<Element> eleListTexts = doc.getRootElement().getChildren("text");
            if (eleListTexts != null) {
                StringBuilder sbDefault = new StringBuilder();
                for (Element eleText : eleListTexts) {
                    String lang = eleText.getAttributeValue("lang");
                    String value = eleText.getText();
                    if (StringUtils.isNotEmpty(value)) {
                        String fieldName = StringUtils.isNotEmpty(lang) ? "MD_TEXT" + SolrConstants.MIXFIX_LANG + lang.toUpperCase() : "MD_TEXT";
                        indexObj.addToLucene(fieldName, value);
                        sbDefault.append(' ').append(value.trim());
                    }
                }
                indexObj.setDefaultValue(sbDefault.toString());
            }

            // put some simple data to Lucene array
            indexObj.pushSimpleDataToLuceneArray();

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // Write created/updated timestamps
            indexObj.writeDateModified(true);

            // If images have been found for any page, set a boolean in the root doc indicating that the record does have images
            indexObj.addToLucene(FIELD_IMAGEAVAILABLE, String.valueOf(recordHasImages));

            // If full-text has been indexed for any page, set a boolean in the root doc indicating that the record does have full-text
            indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, String.valueOf(recordHasFulltext));

            // ISWORK only for non-anchors
            indexObj.addToLucene(SolrConstants.ISWORK, "true");
            logger.trace("ISWORK: {}", indexObj.getLuceneFieldWithName(SolrConstants.ISWORK).getValue());

            // Add DEFAULT field
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(indexObj.getDefaultValue().trim()));
                indexObj.setDefaultValue("");
            }

            SolrInputDocument rootDoc = SolrSearchIndex.createDocument(indexObj.getLuceneFields());
            useWriteStrategy.setRootDoc(rootDoc);

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)
            logger.debug("Writing document to index...");
            useWriteStrategy.writeDocs(SolrIndexerDaemon.getInstance().getConfiguration().isAggregateRecords());
            logger.info("Successfully finished indexing '{}'.", cmsFile.getFileName());
        } catch (IOException | IndexerException | FatalIndexerException | SolrServerException | JDOMException e) {
            logger.error("Indexing of '{}' could not be finished due to an error.", cmsFile.getFileName());
            logger.error(e.getMessage(), e);
            ret[1] = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            SolrIndexerDaemon.getInstance().getSearchIndex().rollback();
        } finally {
            if (useWriteStrategy != null) {
                useWriteStrategy.cleanup();
            }
        }

        return ret;
    }

    @Override
    protected FileFormat getSourceDocFormat() {
        return FileFormat.CMS;
    }
}
