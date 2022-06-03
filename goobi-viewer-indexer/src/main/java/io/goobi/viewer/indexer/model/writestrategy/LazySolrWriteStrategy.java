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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.model.SolrConstants;

/**
 * <p>
 * LazySolrWriteStrategy class.
 * </p>
 *
 */
public class LazySolrWriteStrategy extends AbstractWriteStrategy {

    private static final Logger logger = LoggerFactory.getLogger(LazySolrWriteStrategy.class);

    protected SolrSearchIndex searchIndex;
    protected SolrInputDocument rootDoc;
    protected List<SolrInputDocument> docsToAdd = new CopyOnWriteArrayList<>();
    protected Map<Integer, SolrInputDocument> pageOrderMap = new ConcurrentHashMap<>();
    /** Map for fast doc retrieval via its PHYSID. */
    private Map<String, SolrInputDocument> physIdPageMap = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param searchIndex a {@link io.goobi.viewer.indexer.helper.SolrSearchIndex} object.
     * @should set attributes correctly
     */
    protected LazySolrWriteStrategy(SolrSearchIndex searchIndex) {
        this.searchIndex = searchIndex;
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy#setRootDoc(org.apache.solr.common.SolrInputDocument)
     */
    /** {@inheritDoc} */
    @Override
    public void setRootDoc(SolrInputDocument doc) {
        this.rootDoc = doc;

    }

    /**
     * For unit testing purposes.
     */
    public List<SolrInputDocument> getDocsToAdd() {
        return docsToAdd;
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.ISolrWriteStrategy#addDoc(org.apache.solr.common.SolrInputDocument)
     */
    /** {@inheritDoc} */
    @Override
    public void addDoc(SolrInputDocument doc) {
        addDocs(Collections.singletonList(doc));

    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.ISolrWriteStrategy#addDocs(java.util.List)
     */
    /** {@inheritDoc} */
    @Override
    public void addDocs(List<SolrInputDocument> docs) {
        docsToAdd.addAll(docs);
        logger.debug("Docs to add: {}", docsToAdd.size());
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.ISolrWriteStrategy#addPageDoc(org.apache.solr.common.SolrInputDocument)
     */
    /** {@inheritDoc} */
    @Override
    public void addPageDoc(SolrInputDocument doc) {
        int order = (Integer) doc.getFieldValue(SolrConstants.ORDER);
        if (pageOrderMap.get(order) != null) {
            logger.error("Collision for page order {}", order);
        }
        pageOrderMap.put(order, doc);
        String key = (String) doc.getFieldValue(SolrConstants.PHYSID);
        physIdPageMap.put(key, doc);
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy#updateDoc(org.apache.solr.common.SolrInputDocument)
     */
    /** {@inheritDoc} */
    @Override
    public void updateDoc(SolrInputDocument doc) {
        // This implementation doesn't need to do anything, since all docs are kept in memory
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.ISolrWriteStrategy#getPageDocsSize()
     */
    /** {@inheritDoc} */
    @Override
    public int getPageDocsSize() {
        return pageOrderMap.size();
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy#getPageOrderNumbers()
     */
    @Override
    public List<Integer> getPageOrderNumbers() {
        List<Integer> ret = new ArrayList<>(pageOrderMap.keySet());
        Collections.sort(ret);
        return ret;
    }

    /**
     * {@inheritDoc}
     *
     * Ftegy.ISolrWriteStrategy#getPageDocForOrder(int)
     * 
     * @should return correct doc
     * @should return null if order out of range
     */
    @Override
    public SolrInputDocument getPageDocForOrder(int order) {
        if (order > 0) {
            return pageOrderMap.get(order);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public List<SolrInputDocument> getPageDocsForPhysIdList(List<String> physIdList) {
        List<SolrInputDocument> ret = new ArrayList<>();

        for (String physId : physIdList) {
            SolrInputDocument doc = physIdPageMap.get(physId);
            if (doc != null) {
                ret.add(doc);
            }
        }

        return ret;
    }

    /** {@inheritDoc} */
    @Override
    public void writeDocs(boolean aggregateRecords) throws IndexerException, FatalIndexerException {
        if (rootDoc == null) {
            throw new IndexerException("topDoc may not be null");
        }
        docsToAdd.add(rootDoc);
        String pi = (String) rootDoc.getFieldValue(SolrConstants.PI);
        for (int order : pageOrderMap.keySet()) {
            SolrInputDocument pageDoc = pageOrderMap.get(order);
            // Do not add shape docs
            if ("SHAPE".equals(pageDoc.getFieldValue(SolrConstants.DOCTYPE))) {
                continue;
            }
            checkAndAddAccessCondition(pageDoc);
            docsToAdd.add(pageDoc);
            if (!pageDoc.containsKey(SolrConstants.PI_TOPSTRUCT) && pi != null) {
                pageDoc.addField(SolrConstants.PI_TOPSTRUCT, pi);
                logger.warn("Page document {} has no PI_TOPSTRUCT fields, adding now...", pageDoc.getFieldValue(SolrConstants.ORDER));
            }
            // Remove ALTO field (easier than removing all the logic involved in adding the ALTO field)
            if (pageDoc.containsKey(SolrConstants.ALTO)) {
                pageDoc.removeField(SolrConstants.ALTO);
            }
        }

        for (SolrInputDocument doc : docsToAdd) {
            if (doc.getFieldValue("GROUPFIELD") == null) {
                logger.error("Field has no GROUPFIELD: {}", doc.toString());
            }
            if (aggregateRecords) {
                // Add SUPER* fields to root doc
                if (doc.containsKey(SolrConstants.DEFAULT)) {
                    rootDoc.addField(SolrConstants.SUPERDEFAULT, doc.getFieldValue(SolrConstants.DEFAULT));
                }
                if (doc.containsKey(SolrConstants.FULLTEXT)) {
                    rootDoc.addField(SolrConstants.SUPERFULLTEXT, doc.getFieldValue(SolrConstants.FULLTEXT));
                }
                if (doc.containsKey(SolrConstants.UGCTERMS)) {
                    rootDoc.addField(SolrConstants.SUPERUGCTERMS, doc.getFieldValue(SolrConstants.UGCTERMS));
                }
            }
            sanitizeDoc(doc);
        }

        if (!docsToAdd.isEmpty()) {
            searchIndex.writeToIndex(docsToAdd);
            searchIndex.commit(SolrSearchIndex.optimize);
            logger.debug("{} new doc(s) added.", docsToAdd.size());
        } else {
            throw new IndexerException("No docs to write");
        }
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy#cleanup()
     */
    /** {@inheritDoc} */
    @Override
    public void cleanup() {
        rootDoc = null;
        docsToAdd.clear();
    }
}
