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
package io.goobi.viewer.indexer.model.datarepository.strategy;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.model.FatalIndexerException;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

/**
 * <p>
 * IDataRepositoryStrategy interface.
 * </p>
 *
 */
public interface IDataRepositoryStrategy {

    /**
     * <p>
     * getAllDataRepositories.
     * </p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<DataRepository> getAllDataRepositories();

    /**
     * Selects available data repository for the given record. If no repository could be selected, the indexer MUST be halted.
     *
     * @param pi a {@link java.lang.String} object.
     * @param dataFile a {@link java.nio.file.Path} object.
     * @param dataFolders a {@link java.util.Map} object.
     * @param searchIndex a {@link io.goobi.viewer.indexer.helper.SolrSearchIndex} object.
     * @return DataReopository array with index 0 containing the selected repository and 1 containing the previous repository, if not equal to
     *         selected repository
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     */
    public DataRepository[] selectDataRepository(String pi, final Path dataFile, final Map<String, Path> dataFolders,
            final SolrSearchIndex searchIndex)
            throws FatalIndexerException;
}
