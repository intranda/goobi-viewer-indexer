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

import java.util.List;

import org.apache.solr.common.SolrInputDocument;

import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;
import de.intranda.digiverso.presentation.solr.model.IndexerException;

public interface ISolrWriteStrategy {

    public void setRootDoc(SolrInputDocument doc);

    public void addDoc(SolrInputDocument doc);

    public void addDocs(List<SolrInputDocument> docs);

    public void addPageDoc(SolrInputDocument doc);

    /**
     * If a document has been updated, it may have to be updated internally. The implementation of this interface must make sure the changes to the
     * doc are not lost.
     * 
     * @param doc
     */
    public void updateDoc(SolrInputDocument doc);

    public int getPageOrderOffset();

    public int getPageDocsSize();

    public SolrInputDocument getPageDocForOrder(int order) throws FatalIndexerException;

    public List<SolrInputDocument> getPageDocsForPhysIdList(List<String> physIdList) throws FatalIndexerException;

    public void writeDocs(boolean aggregateRecords) throws IndexerException, FatalIndexerException;

    public void cleanup();
}
