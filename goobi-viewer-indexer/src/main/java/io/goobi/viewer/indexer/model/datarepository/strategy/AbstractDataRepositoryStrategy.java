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

import java.util.List;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

public abstract class AbstractDataRepositoryStrategy implements IDataRepositoryStrategy {

    private static final Logger logger = LogManager.getLogger(RemainingSpaceStrategy.class);

    protected List<DataRepository> dataRepositories;

    /**
     * Creates a data repository strategy object based on the configuration.
     * 
     * @param config Configuration object
     * @return Instance of a class implementing {@link IDataRepositoryStrategy}
     * @throws FatalIndexerException
     * @should return correct type
     */
    public static IDataRepositoryStrategy create(Configuration config) throws FatalIndexerException {
        if (config == null) {
            throw new IllegalArgumentException("config may not be null");
        }

        logger.info("Data repository strategy: {}", config.getDataRepositoryStrategy());
        switch (config.getDataRepositoryStrategy()) {
            case "SingleRepositoryStrategy":
                return new SingleRepositoryStrategy(config);
            case "MaxRecordNumberStrategy":
                return new MaxRecordNumberStrategy(config);
            case "RemainingSpaceStrategy":
                return new RemainingSpaceStrategy(config);
            default:
                logger.error("Unknown data repository strategy: '{}', using SingleRepositoryStrategy instead.");
                return new SingleRepositoryStrategy(config);
        }
    }

}
