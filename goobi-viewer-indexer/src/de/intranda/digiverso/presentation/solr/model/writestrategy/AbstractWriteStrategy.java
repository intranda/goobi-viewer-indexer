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
package de.intranda.digiverso.presentation.solr.model.writestrategy;

import org.apache.solr.common.SolrInputDocument;

import de.intranda.digiverso.presentation.solr.model.SolrConstants;

public abstract class AbstractWriteStrategy implements ISolrWriteStrategy {

    /**
     * Adds OPENACCESS access condition field value to the give doc, if it has none.
     * 
     * @param doc
     */
    void checkAndAddAccessCondition(SolrInputDocument doc) {
        if (!doc.containsKey(SolrConstants.ACCESSCONDITION)) {
            doc.addField(SolrConstants.ACCESSCONDITION, SolrConstants.OPEN_ACCESS_VALUE);
        }
    }
}
