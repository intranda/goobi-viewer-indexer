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

import java.util.List;

import org.apache.solr.common.SolrInputDocument;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.model.PhysicalElement;

/**
 * <p>
 * ISolrWriteStrategy interface.
 * </p>
 *
 */
public interface ISolrWriteStrategy {

    /**
     * <p>
     * setRootDoc.
     * </p>
     *
     * @param doc a {@link org.apache.solr.common.SolrInputDocument} object.
     */
    public void setRootDoc(SolrInputDocument doc);

    /**
     * <p>
     * addDoc.
     * </p>
     *
     * @param doc a {@link org.apache.solr.common.SolrInputDocument} object.
     */
    public void addDoc(SolrInputDocument doc);

    /**
     * <p>
     * addDocs.
     * </p>
     *
     * @param docs a {@link java.util.List} object.
     */
    public void addDocs(List<SolrInputDocument> docs);

    /**
     * <p>
     * addPage.
     * </p>
     *
     * @param page {@link io.goobi.viewer.indexer.model.PhysicalElement}
     */
    public void addPage(PhysicalElement page);

    /**
     * If a page document has been updated, it may have to be updated internally. The implementation of this interface must make sure the changes to the
     * doc are not lost.
     *
     * @param doc a {@link io.goobi.viewer.indexer.model.PhysicalElement} object.
     */
    public void updatePage(PhysicalElement page);

    /**
     * <p>
     * getPageDocsSize.
     * </p>
     *
     * @return a int.
     */
    public int getPageDocsSize();

    /**
     * <p>
     * getPageOrderNumbers.
     * </p>
     * 
     * @return Ordered list of ORDER values for all pages
     */
    public List<Integer> getPageOrderNumbers();

    /**
     * <p>
     * getPageDocForOrder.
     * </p>
     *
     * @param order a int.
     * @return a {@link org.apache.solr.common.SolrInputDocument} object.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException if any.
     */
    public SolrInputDocument getPageDocForOrder(int order) throws FatalIndexerException;

    /**
     * <p>
     * getPageDocsForPhysIdList.
     * </p>
     *
     * @param physIdList a {@link java.util.List} object.
     * @return a {@link java.util.List} object.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException if any.
     */
    public List<PhysicalElement> getPagesForPhysIdList(List<String> physIdList) throws FatalIndexerException;

    /**
     * <p>
     * writeDocs.
     * </p>
     *
     * @param aggregateRecords a boolean.
     * @throws io.goobi.viewer.indexer.exceptions.IndexerException if any.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException if any.
     */
    public void writeDocs(boolean aggregateRecords) throws IndexerException, FatalIndexerException;

    /**
     * <p>
     * cleanup.
     * </p>
     */
    public void cleanup();
}
