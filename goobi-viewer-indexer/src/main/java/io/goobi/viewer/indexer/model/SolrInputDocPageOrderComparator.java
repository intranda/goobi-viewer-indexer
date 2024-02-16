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
package io.goobi.viewer.indexer.model;

import java.util.Collection;
import java.util.Comparator;

import org.apache.solr.common.SolrInputDocument;

import io.goobi.viewer.indexer.helper.SolrSearchIndex;

/**
 * Comparator for sorting {@link SolrInputDocument}s by their ORDER field value.
 */
public class SolrInputDocPageOrderComparator implements Comparator<SolrInputDocument> {

    /**
     * {@inheritDoc}
     * 
     * @should compare correctly
     */
    @Override
    public int compare(SolrInputDocument doc1, SolrInputDocument doc2) {
        Object obj1 = getSingleFieldValue(doc1, SolrConstants.ORDER);
        Integer order1 = SolrSearchIndex.getAsInt(obj1);

        Object obj2 = getSingleFieldValue(doc2, SolrConstants.ORDER);
        Integer order2 = SolrSearchIndex.getAsInt(obj2);

        if (order1 != null && order2 != null) {
            return order1.compareTo(order2);
        } else if (order1 != null) {
            return 1;
        } else if (order2 != null) {
            return -1;
        } else {
            return 1;
        }
    }

    /**
     * 
     * @param doc
     * @param field
     * @return {@link Object}
     */
    static Object getSingleFieldValue(SolrInputDocument doc, String field) {
        Collection<Object> valueList = doc.getFieldValues(field);
        if (valueList != null && !valueList.isEmpty()) {
            return valueList.iterator().next();
        }

        return null;
    }
}
