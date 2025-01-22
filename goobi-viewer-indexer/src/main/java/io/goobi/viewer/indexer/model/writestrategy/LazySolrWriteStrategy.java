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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.model.PhysicalElement;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;

/**
 * <p>
 * LazySolrWriteStrategy class.
 * </p>
 *
 */
public class LazySolrWriteStrategy extends AbstractWriteStrategy {

    private static final Logger logger = LogManager.getLogger(LazySolrWriteStrategy.class);

    protected SolrInputDocument rootDoc;
    protected List<SolrInputDocument> docsToAdd = new CopyOnWriteArrayList<>();
    protected Map<Integer, PhysicalElement> pageOrderMap = new ConcurrentHashMap<>();
    /** Map for fast doc retrieval via its PHYSID. */
    private Map<String, PhysicalElement> physIdPageMap = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param searchIndex a {@link io.goobi.viewer.indexer.helper.SolrSearchIndex} object.
     * @should set attributes correctly
     */
    protected LazySolrWriteStrategy(SolrSearchIndex searchIndex) {
        this.searchIndex = searchIndex;
    }

    /** {@inheritDoc} */
    @Override
    public void setRootDoc(SolrInputDocument doc) {
        this.rootDoc = doc;

        // Add URN to set to check for duplicates later
        if (doc != null && doc.getField(SolrConstants.URN) != null) {
            String urn = (String) doc.getFieldValue(SolrConstants.URN);
            if (StringUtils.isNotEmpty(urn)) {
                List<String> urns = collectedValues.computeIfAbsent(SolrConstants.URN, k -> new ArrayList<>());
                urns.add(urn);
            }
        }
    }

    /**
     * For unit testing purposes.
     * 
     * @return the docsToAdd
     */
    public List<SolrInputDocument> getDocsToAdd() {
        return docsToAdd;
    }

    /** {@inheritDoc} */
    @Override
    public void addDoc(SolrInputDocument doc) {
        addDocs(Collections.singletonList(doc));

    }

    /** {@inheritDoc} */
    @Override
    public void addDocs(List<SolrInputDocument> docs) {
        docsToAdd.addAll(docs);
        logger.debug("Docs to add: {}", docsToAdd.size());

        for (SolrInputDocument doc : docs) {
            // Add URN to set to check for duplicates later
            if (doc.getField(SolrConstants.URN) != null) {
                String urn = (String) doc.getFieldValue(SolrConstants.URN);
                if (StringUtils.isNotEmpty(urn)) {
                    List<String> urns = collectedValues.computeIfAbsent(SolrConstants.URN, k -> new ArrayList<>());
                    urns.add(urn);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void addPage(PhysicalElement page) {
        if (pageOrderMap.get(page.getOrder()) != null) {
            logger.error("Collision for page order {}", page.getOrder());
        }
        pageOrderMap.put(page.getOrder(), page);
        String key = (String) page.getDoc().getFieldValue(SolrConstants.PHYSID);
        physIdPageMap.put(key, page);

        // Add URN to set to check for duplicates later
        if (page.getDoc().getField(SolrConstants.IMAGEURN) != null) {
            String urn = (String) page.getDoc().getFieldValue(SolrConstants.IMAGEURN);
            if (StringUtils.isNotEmpty(urn)) {
                List<String> urns = collectedValues.computeIfAbsent(SolrConstants.URN, k -> new ArrayList<>());
                urns.add(urn);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void updatePage(PhysicalElement page) {
        // This implementation doesn't need to do anything, since all docs are kept in memory
    }

    /** {@inheritDoc} */
    @Override
    public int getPageDocsSize() {
        return pageOrderMap.size();
    }

    /** {@inheritDoc} */
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
            return pageOrderMap.get(order) != null ? pageOrderMap.get(order).getDoc() : null;
        }

        return null; //NOSONAR Returning empty map would complicate things
    }

    /** {@inheritDoc} */
    @Override
    public List<PhysicalElement> getPagesForPhysIdList(List<String> physIdList) {
        List<PhysicalElement> ret = new ArrayList<>();

        for (String physId : physIdList) {
            PhysicalElement page = physIdPageMap.get(physId);
            if (page != null) {
                ret.add(page);
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

        // Check for duplicate URNs
        checkForValueCollisions(SolrConstants.URN, pi);

        for (Entry<Integer, PhysicalElement> entry : pageOrderMap.entrySet()) {
            PhysicalElement page = entry.getValue();
            // Do not add shape docs
            if (DocType.SHAPE.name().equals(page.getDoc().getFieldValue(SolrConstants.DOCTYPE))) {
                // TODO make sure shape docs are handled separately in all formats, then remove this check
                continue;
            }
            checkAndAddAccessCondition(page.getDoc());
            docsToAdd.add(page.getDoc());
            if (!page.getDoc().containsKey(SolrConstants.PI_TOPSTRUCT) && pi != null) {
                page.getDoc().addField(SolrConstants.PI_TOPSTRUCT, pi);
                logger.warn("Page document {} has no PI_TOPSTRUCT fields, adding now...", page.getDoc().getFieldValue(SolrConstants.ORDER));
            }
            // Remove ALTO field (easier than removing all the logic involved in adding the ALTO field)
            if (page.getDoc().containsKey(SolrConstants.ALTO)) {
                page.getDoc().removeField(SolrConstants.ALTO);
            }
        }

        for (SolrInputDocument doc : docsToAdd) {
            if (doc.getFieldValue("GROUPFIELD") == null) {
                logger.error("Field has no GROUPFIELD: {}", doc);
            }
            if (aggregateRecords) {
                // Add SUPER* fields to root doc
                addSuperSearchFields(doc, rootDoc);
            }
            sanitizeDoc(doc);
        }

        if (!docsToAdd.isEmpty()) {
            searchIndex.writeToIndex(docsToAdd);
            searchIndex.commit(searchIndex.isOptimize());
            logger.debug("{} new doc(s) added.", docsToAdd.size());
        } else {
            throw new IndexerException("No docs to write");
        }
    }

    /** {@inheritDoc} */
    @Override
    public void cleanup() {
        rootDoc = null;
        docsToAdd.clear();
    }
}
