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
package de.intranda.digiverso.presentation.solr.model.datarepository.strategy;

import java.nio.file.Path;

import de.intranda.digiverso.presentation.solr.helper.SolrHelper;
import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;
import de.intranda.digiverso.presentation.solr.model.datarepository.DataRepository;

public interface IDataRepositoryStrategy {

    /**
     * Selects available data repository for the given record. If no repository could be selected, the indexer MUST be halted.
     * 
     * @param file
     * @param pi
     * @param solrHelper
     * @return DataReopository array with index 0 containing the selected repository and 1 containing the previous repository, if not equal to
     *         selected repository
     * @throws FatalIndexerException
     */
    public DataRepository[] selectDataRepository(Path file, String pi, SolrHelper solrHelper) throws FatalIndexerException;
}
