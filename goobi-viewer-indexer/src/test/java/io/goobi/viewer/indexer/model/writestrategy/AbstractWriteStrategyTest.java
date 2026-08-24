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

import java.util.Collection;

import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.model.SolrConstants;

class AbstractWriteStrategyTest {
    
    /**
     * @see AbstractWriteStrategy#sanitizeDoc(SolrInputDocument)
     * @verifies trim to single value correctly
     */
    @Test
    void sanitizeDoc_shouldTrimToSingleValueCorrectly() {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("BOOL_FOO", false);
        doc.addField("BOOL_FOO", true);
        doc.addField("MDNUM_YEARPUBLISH", "2000");
        doc.addField("MDNUM_YEARPUBLISH", "2001");
        doc.addField(SolrConstants.DATECREATED, 123L);
        doc.addField(SolrConstants.DATECREATED, 456L);
        
        AbstractWriteStrategy.sanitizeDoc(doc);
        
        Assertions.assertNotNull(doc.getFieldValues("BOOL_FOO"));
        Assertions.assertEquals(1, doc.getFieldValues("BOOL_FOO").size());
        Assertions.assertFalse((boolean) doc.getFieldValue("BOOL_FOO"));
        
        Assertions.assertNotNull(doc.getFieldValues("MDNUM_YEARPUBLISH"));
        Assertions.assertEquals(1, doc.getFieldValues("MDNUM_YEARPUBLISH").size());
        Assertions.assertEquals("2000", doc.getFieldValue("MDNUM_YEARPUBLISH"));
        
        Assertions.assertNotNull(doc.getFieldValues(SolrConstants.DATECREATED));
        Assertions.assertEquals(1, doc.getFieldValues(SolrConstants.DATECREATED).size());
        Assertions.assertEquals(123L, doc.getFieldValue(SolrConstants.DATECREATED));
    }

    /**
     * @see AbstractWriteStrategy#addLayoutTagsToRootDoc(SolrInputDocument,SolrInputDocument)
     * @verifies add distinct layout tag values to root doc
     */
    @Test
    void addLayoutTagsToRootDoc_shouldAddDistinctLayoutTagValuesToRootDoc() {
        SolrInputDocument rootDoc = new SolrInputDocument();

        SolrInputDocument page1 = new SolrInputDocument();
        page1.addField(SolrConstants.MD_LAYOUTTAG, "gr_jacob_grimm");
        page1.addField(SolrConstants.MD_LAYOUTTAG, "gr_unterstrichen");
        SolrInputDocument page2 = new SolrInputDocument();
        page2.addField(SolrConstants.MD_LAYOUTTAG, "gr_jacob_grimm"); // duplicate across pages
        page2.addField(SolrConstants.MD_LAYOUTTAG, "gr_wilhelm_grimm");

        AbstractWriteStrategy.addLayoutTagsToRootDoc(page1, rootDoc);
        AbstractWriteStrategy.addLayoutTagsToRootDoc(page2, rootDoc);

        Collection<Object> values = rootDoc.getFieldValues(SolrConstants.MD_LAYOUTTAG);
        Assertions.assertNotNull(values);
        // Distinct union across pages (the duplicate gr_jacob_grimm is not repeated)
        Assertions.assertEquals(3, values.size());
        Assertions.assertTrue(values.contains("gr_jacob_grimm"));
        Assertions.assertTrue(values.contains("gr_unterstrichen"));
        Assertions.assertTrue(values.contains("gr_wilhelm_grimm"));
    }
}