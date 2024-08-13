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
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdom2.Element;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.HttpConnector;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * Indexer implementation for METS/MARC documents.
 */
public class MetsMarcIndexer extends MetsIndexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(MetsMarcIndexer.class);

    private static final String[] ANCHOR_PI_XPATHS = { "/mets:mets/mets:dmdSec/mets:mdWrap[@MDTYPE='OTHER']/mets:xmlData/anchorIdentifier",
            "/mets:mets/mets:dmdSec/mets:mdWrap[@MDTYPE='MARC']/mets:xmlData/"
                    + "marc:bib/marc:record/marc:datafield[@tag='773']/marc:subfield[@code='w']" };;

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
     * Checks whether this is a volume of a multivolume work (should be false for monographs and anchors).
     * 
     * @return boolean
     */
    @Override
    protected boolean isVolume() {
        return StringUtils.isNotEmpty(getAnchorPi());
    }

    @Override
    protected FileFormat getSourceDocFormat() {
        return FileFormat.METS_MARC;
    }

    @Override
    protected String getPiRootPath(String dmdId) {
        return "";
    }

    /**
     * <p>
     * getAnchorPi.
     * </p>
     *
     * @return a {@link java.lang.String} object.
     */
    @Override
    public String getAnchorPi() {
        for (String xpath : ANCHOR_PI_XPATHS) {
            List<Element> relatedItemList = xp.evaluateToElements(xpath, null);
            if (relatedItemList != null && !relatedItemList.isEmpty()) {
                return relatedItemList.get(0).getText();
            }
        }

        return null;
    }
}
