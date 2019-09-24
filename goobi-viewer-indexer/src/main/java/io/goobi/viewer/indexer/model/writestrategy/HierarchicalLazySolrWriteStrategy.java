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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.helper.SolrHelper;
import io.goobi.viewer.indexer.model.FatalIndexerException;
import io.goobi.viewer.indexer.model.IndexerException;
import io.goobi.viewer.indexer.model.SolrConstants;

public class HierarchicalLazySolrWriteStrategy extends LazySolrWriteStrategy {

    private static final Logger logger = LoggerFactory.getLogger(HierarchicalLazySolrWriteStrategy.class);

    /**
     * Constructor.
     * 
     * @param solrHelper
     * @should set attributes correctly
     */
    public HierarchicalLazySolrWriteStrategy(SolrHelper solrHelper) {
        super(solrHelper);
    }

    /**
     * @param aggregateRecords
     * @throws IndexerException
     * @throws FatalIndexerException
     * @see io.goobi.viewer.indexer.model.ISolrWriteStrategy#writeDocs()
     * @should write all structure docs correctly
     * @should write all page docs correctly
     */
    @Override
    public void writeDocs(boolean aggregateRecords) throws IndexerException, FatalIndexerException {
        if (rootDoc == null) {
            throw new IndexerException("rootDoc may not be null");
        }

        for (int order : pageOrderMap.keySet()) {
            SolrInputDocument pageDoc = pageOrderMap.get(order);
            checkAndAddAccessCondition(pageDoc);
            docsToAdd.add(pageDoc);
        }

        for (SolrInputDocument doc : docsToAdd) {
            if (doc.getFieldValue("GROUPFIELD") == null) {
                logger.error("Field has no GROUPFIELD: {}", doc.toString());
            }
            rootDoc.addChildDocument(doc);
            if (aggregateRecords) {
                // Add SUPER* fields to root doc
                if (doc.containsKey(SolrConstants.DEFAULT)) {
                    rootDoc.addField(SolrConstants.SUPERDEFAULT, (doc.getFieldValue(SolrConstants.DEFAULT)));
                }
                if (doc.containsKey(SolrConstants.FULLTEXT)) {
                    rootDoc.addField(SolrConstants.SUPERFULLTEXT, (doc.getFieldValue(SolrConstants.FULLTEXT)));
                }
                if (doc.containsKey(SolrConstants.UGCTERMS)) {
                    rootDoc.addField(SolrConstants.SUPERUGCTERMS, doc.getFieldValue(SolrConstants.UGCTERMS));
                }
            }
        }

        solrHelper.writeToIndex(rootDoc);
        solrHelper.commit(SolrHelper.optimize);
        logger.debug("{} new doc(s) added.", docsToAdd.size());
    }
}
