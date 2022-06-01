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
package io.goobi.viewer.indexer.model.writestrategy;

import org.apache.solr.common.SolrInputDocument;
import org.junit.Assert;
import org.junit.Test;

import io.goobi.viewer.indexer.model.SolrConstants;

public class AbstractWriteStrategyTest {
    
    /**
     * @see AbstractWriteStrategy#sanitizeDoc(SolrInputDocument)
     * @verifies trim to single value correctly
     */
    @Test
    public void sanitizeDoc_shouldTrimToSingleValueCorrectly() throws Exception {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("BOOL_FOO", false);
        doc.addField("BOOL_FOO", true);
        doc.addField(SolrConstants.DATECREATED, 123L);
        doc.addField(SolrConstants.DATECREATED, 456L);
        
        AbstractWriteStrategy.sanitizeDoc(doc);
        
        Assert.assertNotNull(doc.getFieldValues("BOOL_FOO"));
        Assert.assertEquals(1, doc.getFieldValues("BOOL_FOO").size());
        Assert.assertFalse((boolean) doc.getFieldValue("BOOL_FOO"));
        
        Assert.assertNotNull(doc.getFieldValues(SolrConstants.DATECREATED));
        Assert.assertEquals(1, doc.getFieldValues(SolrConstants.DATECREATED).size());
        Assert.assertEquals(123L, doc.getFieldValue(SolrConstants.DATECREATED));
    }
}