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
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdom2.Element;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.helper.DateTools;
import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.HttpConnector;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.config.FieldConfig;
import io.goobi.viewer.indexer.model.config.XPathConfig;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * Indexer implementation for METS/MARC documents.
 */
public class MetsMarcIndexer extends MetsIndexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(MetsMarcIndexer.class);

    /**
     * Constructor.
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public MetsMarcIndexer(Hotfolder hotfolder) {
        super(hotfolder);
    }

    /**
     * 
     * @param hotfolder
     * @param httpConnector
     */
    public MetsMarcIndexer(Hotfolder hotfolder, HttpConnector httpConnector) {
        super(hotfolder, httpConnector);
    }

    /**
     * Indexes the given METS file.
     * 
     * @param metsFile {@link File}
     * @param fromReindexQueue
     * @param reindexSettings
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     * 
     */
    @Override
    public void addToIndex(Path metsFile, boolean fromReindexQueue, Map<String, Boolean> reindexSettings)
            throws IOException, FatalIndexerException {
        String fileNameRoot = FilenameUtils.getBaseName(metsFile.getFileName().toString());

        // Check data folders in the hotfolder
        Map<String, Path> dataFolders = checkDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

        // Use existing folders for those missing in the hotfolder
        checkReindexSettings(dataFolders, reindexSettings);

        String[] resp = index(metsFile, fromReindexQueue, dataFolders, null,
                SolrIndexerDaemon.getInstance().getConfiguration().getPageCountStart(),
                dataFolders.containsKey(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER));

        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            String newMetsFileName = resp[0];
            String pi = FilenameUtils.getBaseName(newMetsFileName);
            Path indexed = Paths.get(dataRepository.getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath().toString(), newMetsFileName);
            if (metsFile.equals(indexed)) {
                return;
            }

            if (Files.exists(indexed)) {
                // Add a timestamp to the old file name
                String oldMetsFilename =
                        FilenameUtils.getBaseName(newMetsFileName) + "_" + LocalDateTime.now().format(DateTools.formatterBasicDateTime) + ".xml";
                Path newFile = Paths.get(hotfolder.getUpdatedMets().toAbsolutePath().toString(), oldMetsFilename);
                Files.copy(indexed, newFile);
                logger.debug("Old METS file copied to '{}'.", newFile.toAbsolutePath());
            }
            Files.copy(metsFile, indexed, StandardCopyOption.REPLACE_EXISTING);
            dataRepository.checkOtherRepositoriesForRecordFileDuplicates(newMetsFileName, DataRepository.PARAM_INDEXED_METS,
                    hotfolder.getDataRepositoryStrategy().getAllDataRepositories());

            if (previousDataRepository != null) {
                // Move non-repository data folders to the selected repository
                previousDataRepository.moveDataFoldersToRepository(dataRepository, FilenameUtils.getBaseName(newMetsFileName));
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

            // success for goobi
            Path successFile = Paths.get(hotfolder.getSuccessFolder().toAbsolutePath().toString(), metsFile.getFileName().toString());
            try {
                Files.createFile(successFile);
                Files.setLastModifiedTime(successFile, FileTime.fromMillis(System.currentTimeMillis()));
            } catch (FileAlreadyExistsException e) {
                Files.delete(successFile);
                Files.createFile(successFile);
                Files.setLastModifiedTime(successFile, FileTime.fromMillis(System.currentTimeMillis()));
            }

            try {
                Files.delete(metsFile);
            } catch (IOException e) {
                logger.warn(LOG_COULD_NOT_BE_DELETED, metsFile.toAbsolutePath());
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
            logger.info("Successfully finished indexing '{}'.", metsFile.getFileName());

            // Remove this file from lower priority hotfolders to avoid overriding changes with older version
            SolrIndexerDaemon.getInstance().removeRecordFileFromLowerPriorityHotfolders(pi, hotfolder);
        } else {
            // Error
            if (hotfolder.isDeleteContentFilesOnFailure()) {
                // Delete all data folders for this record from the hotfolder
                DataRepository.deleteDataFoldersFromHotfolder(dataFolders, reindexSettings);
            }
            handleError(metsFile, resp[1], FileFormat.METS);
            try {
                Files.delete(metsFile);
            } catch (IOException e) {
                logger.error(LOG_COULD_NOT_BE_DELETED, metsFile.toAbsolutePath());
            }
        }
    }

    /**
     * @should index record correctly
     */
    @Override
    public String[] index(Path metsFile, boolean fromReindexQueue, Map<String, Path> dataFolders, ISolrWriteStrategy writeStrategy,
            int pageCountStart, boolean downloadExternalImages) {
        logger.trace("index (METS/MARC)");
        return super.index(metsFile, fromReindexQueue, dataFolders, writeStrategy, pageCountStart, downloadExternalImages);
    }

    /**
     * 
     * @param indexObj
     * @param collections
     * @return
     */
    @Override
    protected boolean addVolumeCollectionsToAnchor(IndexObject indexObj, List<String> collections) {
        boolean ret = false;
        List<Element> eleDmdSecList =
                xp.evaluateToElements(XPATH_DMDSEC + indexObj.getDmdid() + "']/mets:mdWrap @MDTYPE='MARC']", null);
        if (eleDmdSecList != null && !eleDmdSecList.isEmpty()) {
            Element eleDmdSec = eleDmdSecList.get(0);
            List<Element> eleModsList = xp.evaluateToElements("TODO", eleDmdSec); // TODO
            if (eleModsList != null && !eleModsList.isEmpty()) {
                Element eleMods = eleModsList.get(0);
                List<FieldConfig> collectionConfigFields =
                        SolrIndexerDaemon.getInstance()
                                .getConfiguration()
                                .getMetadataConfigurationManager()
                                .getConfigurationListForField(SolrConstants.DC);
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
                                    ret = true;
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

        return ret;
    }

    /**
     * Checks whether this is a volume of a multivolume work (should be false for monographs and anchors).
     * 
     * @return boolean
     */
    @Override
    protected boolean isVolume() {
        String query =
                "/mets:mets/mets:dmdSec/mets:mdWrap[@MDTYPE='MARC']/mets:xmlData/marc:bib/marc:record/marc:datafield[@tag='773']/marc:subfield[@code='w']";
        List<Element> relatedItemList = xp.evaluateToElements(query, null);

        return relatedItemList != null && !relatedItemList.isEmpty();
    }

    @Override
    protected FileFormat getSourceDocFormat() {
        return FileFormat.METS_MARC;
    }

    @Override
    protected String getAnchorPiXpath() {
        return "']/mets:mdWrap[@MDTYPE='MARC']/mets:xmlData/marc:bib/marc:record/marc:datafield[@tag='773']/marc:subfield[@code='w']";
    }
}
