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

import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.model.PhysicalElement;
import io.goobi.viewer.indexer.model.SolrConstants;

/**
 * <p>
 * HierarchicalLazySolrWriteStrategy class.
 * </p>
 *
 */
public class HierarchicalLazySolrWriteStrategy extends LazySolrWriteStrategy {

    private static final Logger logger = LogManager.getLogger(HierarchicalLazySolrWriteStrategy.class);

    /**
     * Constructor.
     *
     * @param searchIndex a {@link io.goobi.viewer.indexer.helper.SolrSearchIndex} object.
     * @should set attributes correctly
     */
    protected HierarchicalLazySolrWriteStrategy(SolrSearchIndex searchIndex) {
        super(searchIndex);
    }

    /** {@inheritDoc} */
    @Override
    public void writeDocs(boolean aggregateRecords) throws IndexerException, FatalIndexerException {
        if (rootDoc == null) {
            throw new IndexerException("rootDoc may not be null");
        }

        sanitizeDoc(rootDoc);

        String pi = (String) rootDoc.getFieldValue(SolrConstants.PI);

        // Check for duplicate URNs
        checkForValueCollisions(SolrConstants.URN, pi);

        for (Entry<Integer, PhysicalElement> entry : pageOrderMap.entrySet()) {
            PhysicalElement page = entry.getValue();
            checkAndAddAccessCondition(page.getDoc());
            docsToAdd.add(page.getDoc());
        }

        for (SolrInputDocument doc : docsToAdd) {
            if (doc.getFieldValue("GROUPFIELD") == null) {
                logger.error("Field has no GROUPFIELD: {}", doc);
            }
            sanitizeDoc(doc);
            rootDoc.addChildDocument(doc);
            if (aggregateRecords) {
                // Add SUPER* fields to root doc
                addSuperSearchFields(doc, rootDoc);
            }
            // Add FACET_DEFAULT
            addFacetDefaultField(doc);
        }

        searchIndex.writeToIndex(rootDoc);
        searchIndex.commit(searchIndex.isOptimize());
        logger.debug("{} new doc(s) added.", docsToAdd.size());
    }
}
