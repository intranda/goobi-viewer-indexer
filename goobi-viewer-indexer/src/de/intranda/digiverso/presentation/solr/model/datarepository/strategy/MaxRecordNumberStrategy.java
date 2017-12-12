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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.MetsIndexer;
import de.intranda.digiverso.presentation.solr.helper.Configuration;
import de.intranda.digiverso.presentation.solr.helper.SolrHelper;
import de.intranda.digiverso.presentation.solr.helper.Utils;
import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;
import de.intranda.digiverso.presentation.solr.model.datarepository.DataRepository;

/**
 * Data repository strategy that avoids full repositories where the number of records equals <code>dataRepositoriesMaxRecords</code>.
 */
public class MaxRecordNumberStrategy implements IDataRepositoryStrategy {

    private static final Logger logger = LoggerFactory.getLogger(MaxRecordNumberStrategy.class);

    private final List<DataRepository> dataRepositories = new ArrayList<>();

    private final int dataRepositoriesMaxRecords;

    @SuppressWarnings("unchecked")
    public MaxRecordNumberStrategy(Configuration config) throws FatalIndexerException {
        // Load data repositories
        List<String> dataRepositoryPaths = config.getList("init.dataRepositories.dataRepository");
        if (dataRepositoryPaths != null) {
            for (String path : dataRepositoryPaths) {
                DataRepository dataRepository = new DataRepository(path);
                if (dataRepository.isValid()) {
                    dataRepositories.add(dataRepository);
                    logger.info("Found configured data repository: {}", path);
                }
            }
        }
        if (dataRepositories.isEmpty()) {
            throw new FatalIndexerException("No data repositories found, exiting...");
        }

        dataRepositoriesMaxRecords = config.getInt("init.dataRepositories.maxRecords", 10000);
    }

    /* (non-Javadoc)
     * @see de.intranda.digiverso.presentation.solr.model.datarepository.strategy.IDataRepositoryStrategy#selectDataRepository(java.lang.String, java.nio.file.Path, java.util.Map, de.intranda.digiverso.presentation.solr.helper.SolrHelper)
     */
    @Override
    public DataRepository[] selectDataRepository(String pi, Path dataFile, Map<String, Path> dataFolders, SolrHelper solrHelper)
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
            previousRepository = solrHelper.findCurrentDataRepository(pi);
        } catch (SolrServerException e) {
            logger.error(e.getMessage(), e);
        }
        if (previousRepository != null) {
            if ("?".equals(previousRepository)) {
                // Record is already indexed, but not in a data repository
                ret[1] = new DataRepository("");
                logger.info(
                        "This record is already indexed, but its data files are not in a repository. The data files will be moved to the selected repository.");
            } else {
                // Find previous repository
                for (DataRepository repository : dataRepositories) {
                    if (previousRepository.equals(repository.getPath())) {
                        logger.info("Using previous data repository for '{}': {}", pi, previousRepository);
                        ret[0] = repository;
                        return ret;
                    }
                }
                logger.warn("Previous data repository for '{}' does not exist: {}", pi, previousRepository);
            }
        }

        // Record not yet indexed; find available repository
        try {
            for (DataRepository repository : dataRepositories) {
                int records = repository.getNumRecords();
                if (records < dataRepositoriesMaxRecords) {
                    logger.info("Repository selected for '{}': {} (currently contains {} records)", pi, repository.getPath(), records);
                    ret[0] = repository;
                    return ret;
                } else if (records > dataRepositoriesMaxRecords) {
                    logger.warn("Repository '{}' contains {} records, the limit is {}, though.", repository.getPath(), records,
                            dataRepositoriesMaxRecords);
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        logger.error("No data repository available for indexing. Please configure additional repositories. Exiting...");
        throw new FatalIndexerException("No data repository available for indexing. Please configure additional repositories. Exiting...");
    }
}
