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

import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SolrInputDocPageOrderComparatorTest {

    /**
     * @see SolrInputDocPageOrderComparator#compare(SolrInputDocument,SolrInputDocument)
     * @verifies compareCorrectly
     */
    @Test
    void compare_shouldCompareCorrectly() throws Exception {
        SolrInputDocument doc1 = new SolrInputDocument();
        doc1.setField(SolrConstants.ORDER, 1);
        SolrInputDocument doc2 = new SolrInputDocument();
        doc2.setField(SolrConstants.ORDER, 2);
        
        Assertions.assertEquals(-1, new SolrInputDocPageOrderComparator().compare(doc1, doc2));
        Assertions.assertEquals(0, new SolrInputDocPageOrderComparator().compare(doc2, doc2));
        Assertions.assertEquals(1, new SolrInputDocPageOrderComparator().compare(doc2, doc1));
    }
}
