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

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdom2.Element;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.HttpConnector;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.config.FieldConfig;
import io.goobi.viewer.indexer.model.config.XPathConfig;
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
    protected String getPiRootPath(String dmdId) {
        return "";
    }

    @Override
    protected String getAnchorPiXpath() {
        return "']/mets:mdWrap[@MDTYPE='MARC']/mets:xmlData/marc:bib/marc:record/marc:datafield[@tag='773']/marc:subfield[@code='w']";
    }
}
