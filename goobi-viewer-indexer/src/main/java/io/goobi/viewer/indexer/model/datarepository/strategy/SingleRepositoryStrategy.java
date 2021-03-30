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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.MetsIndexer;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.FatalIndexerException;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

/**
 * <p>
 * SingleRepositoryStrategy class.
 * </p>
 */
public class SingleRepositoryStrategy extends AbstractDataRepositoryStrategy {

    private static final Logger logger = LoggerFactory.getLogger(SingleRepositoryStrategy.class);

    /**
     * Protected constructor.
     *
     * @param config a {@link io.goobi.viewer.indexer.helper.Configuration} object.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     */
    protected SingleRepositoryStrategy(Configuration config) throws FatalIndexerException {
        // Load data repositories
        dataRepositories = DataRepository.loadDataRepositories(config, false);
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.datarepository.strategy.IDataRepositoryStrategy#getAllDataRepositories()
     */
    /** {@inheritDoc} */
    @Override
    public List<DataRepository> getAllDataRepositories() {
        return dataRepositories;
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.datarepository.strategy.IDataRepositoryStrategy#selectDataRepository(java.lang.String, java.nio.file.Path, java.util.Map, io.goobi.viewer.indexer.helper.searchIndex)
     */
    /** {@inheritDoc} */
    @Override
    public DataRepository[] selectDataRepository(String pi, final Path dataFile, final Map<String, Path> dataFolders,
            final SolrSearchIndex searchIndex,
            final SolrSearchIndex oldSearchIndex)
            throws FatalIndexerException {
        DataRepository[] ret = new DataRepository[] { null, null };

        // Extract PI from the file name, if not value was passed (e.g. when deleting a record)
        if (StringUtils.isEmpty(pi) && dataFile != null) {
            String fileExtension = FilenameUtils.getExtension(dataFile.getFileName().toString());
            if (MetsIndexer.ANCHOR_UPDATE_EXTENSION.equals("." + fileExtension) || "delete".equals(fileExtension) || "purge".equals(fileExtension)) {
                pi = Utils.extractPiFromFileName(dataFile);
            }
        }
        if (StringUtils.isBlank(pi)) {
            if (dataFile != null) {
                logger.error("Could not parse PI from '{}'", dataFile.getFileName().toString());
            }
            return ret;
        }

        String previousRepository = null;
        try {
            // Look up previous repository in the index
            previousRepository = searchIndex.findCurrentDataRepository(pi);
            if (previousRepository == null && oldSearchIndex != null) {
                previousRepository = oldSearchIndex.findCurrentDataRepository(pi);
                if (previousRepository != null) {
                    logger.info("Data repository found in old index: {}", previousRepository);
                }
            }
        } catch (SolrServerException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        if (previousRepository != null) {
            if ("?".equals(previousRepository)) {
                // Record is already indexed, but not in a data repository
                ret[0] = new DataRepository("", true);
                return ret;
            }
            // Make sure previous repository name is converted to an absolute path
            previousRepository = DataRepository.getAbsolutePath(previousRepository);
            // Find previous repository
            for (DataRepository repository : dataRepositories) {
                if (Paths.get(previousRepository).equals(Paths.get(repository.getPath()))) {
                    logger.info(
                            "'{}' is currently indexed in data repository '{}'. Since 'SingleRepositoryStrategy' is configured, the record will be moved to out of the repository.",
                            pi, previousRepository);
                    ret[0] = new DataRepository("", true);
                    ret[1] = repository;
                    return ret;
                }
            }
            logger.warn("Previous data repository for '{}' does not exist: {}", pi, previousRepository);
        }

        ret[0] = new DataRepository("", true);
        return ret;
    }
}
