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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdom2.Namespace;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.HttpConnector;

/**
 * Indexer implementation for EAD documents.
 */
public class Ead3Indexer extends EadIndexer {
    
    public static final Namespace NAMESPACE_EAD3 = Namespace.getNamespace("ead", "http://ead3.archivists.org/schema/");

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(Ead3Indexer.class);

    /**
     * Constructor.
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public Ead3Indexer(Hotfolder hotfolder) {
        super(hotfolder);
        eadNamespace = SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("ead3");
        SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().put("ead", NAMESPACE_EAD3);
    }

    /**
     * 
     * @param hotfolder
     * @param httpConnector
     */
    public Ead3Indexer(Hotfolder hotfolder, HttpConnector httpConnector) {
        super(hotfolder, httpConnector);
        eadNamespace = SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("ead3");
        SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().put("ead", NAMESPACE_EAD3);
    }
}
