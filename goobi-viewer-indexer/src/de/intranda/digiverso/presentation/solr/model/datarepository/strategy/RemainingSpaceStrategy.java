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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
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
 * 
 */
public class RemainingSpaceStrategy implements IDataRepositoryStrategy {

    private static final Logger logger = LoggerFactory.getLogger(RemainingSpaceStrategy.class);

    private final List<DataRepository> dataRepositories = new ArrayList<>();

    private final Path viewerHomePath;

    @SuppressWarnings("unchecked")
    public RemainingSpaceStrategy(Configuration config) throws FatalIndexerException {
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

        try {
            viewerHomePath = Paths.get(config.getConfiguration("viewerHome"));
            if (!Files.isDirectory(viewerHomePath)) {
                logger.error("Path defined in <viewerHome> does not exist, exiting...");
                throw new FatalIndexerException("Configuration error, see log for details.");
            }
        } catch (Exception e) {
            logger.error("<viewerHome> not defined, exiting...");
            throw new FatalIndexerException("Configuration error, see log for details.");
        }
    }

    /* (non-Javadoc)
     * @see de.intranda.digiverso.presentation.solr.model.datarepository.strategy.IDataRepositoryStrategy#selectDataRepository(java.lang.String, java.nio.file.Path, java.util.Map, de.intranda.digiverso.presentation.solr.helper.SolrHelper)
     */
    @Override
    public DataRepository[] selectDataRepository(String pi, final Path dataFile, final Map<String, Path> dataFolders, final SolrHelper solrHelper)
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

        long recordSize = 0;
        if (dataFile != null || dataFolders != null) {
            try {
                recordSize = getRecordTotalSize(dataFile, dataFolders);
                logger.info("Total record size is {} Bytes.", recordSize);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                throw new FatalIndexerException(e.getMessage());
            }
        } else {
            logger.warn("dataFile is null, skipping record size calculation.");
        }

        SortedMap<Long, DataRepository> repositorySpaceMap = new TreeMap<>();
        for (DataRepository repository : dataRepositories) {
            long size = repository.getUsableSpace();
            if (size > 0) {
                repositorySpaceMap.put(size, repository);
            }
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
                boolean found = false;
                for (DataRepository repository : dataRepositories) {
                    if (previousRepository.equals(repository.getPath())) {
                        found = true;
                        if (recordSize < repository.getUsableSpace()) {
                            logger.info("Using previous data repository for '{}': {}", pi, previousRepository);
                            ret[0] = repository;
                            return ret;
                        }
                        logger.info(
                                "Record is currently in repository '{}', but its space is insufficient. Record will be moved to a new repository.");
                        ret[1] = repository;
                    }
                }
                if (!found) {
                    logger.warn("Previous data repository for '{}' does not exist: {}", pi, previousRepository);
                }
            }
        }

        // Record not yet indexed; find available repository
        DataRepository repository = selectRepository(repositorySpaceMap, recordSize);
        if (repository != null) {
            logger.info("Repository selected for '{}': {} ({} bytes available).", pi, repository.getPath(), repository.getUsableSpace());
            ret[0] = repository;
            return ret;
        }

        logger.error("No data repository available for indexing. Please configure additional repositories. Exiting...");
        throw new FatalIndexerException("No data repository available for indexing. Please configure additional repositories. Exiting...");
    }

    /**
     * 
     * @param repositorySpaceMap
     * @param recordSize
     * @return
     * @should select repository with the smallest sufficient space
     * @should return null if recordSize is larger than any available repository space
     */
    static DataRepository selectRepository(SortedMap<Long, DataRepository> repositorySpaceMap, long recordSize) {
        if (repositorySpaceMap == null) {
            throw new IllegalArgumentException("repositorySpaceMap may not be null");
        }

        for (Iterator<Long> iterator = repositorySpaceMap.keySet().iterator(); iterator.hasNext();) {
            long storageSize = iterator.next();
            if (recordSize < storageSize) {
                return repositorySpaceMap.get(storageSize);
            }
        }

        return null;
    }

    /**
     * Counts the total size of the record represented by the given <code>dataFile</code>, including all data folders.
     * 
     * @param dataFile
     * @param dataFolders
     * @return total size in bytes
     * @throws IOException
     */
    long getRecordTotalSize(final Path dataFile, final Map<String, Path> dataFolders) throws IOException {
        long ret = 0;

        if (dataFile != null) {
            ret += Files.size(dataFile);
        } else {
            logger.info("No data file passed, assuming an extra 1 MB.");
            ret += 1048576;
        }

        if (dataFolders != null) {
            // Count data folders' size
            for (String key : dataFolders.keySet()) {
                if (dataFolders.get(key) != null) {
                    Path dataFolder = dataFolders.get(key);
                    if (Files.isDirectory(dataFolder)) {
                        long dataFolderSize = FileUtils.sizeOfDirectory(dataFolder.toFile());
                        if (dataFolderSize > 0) {
                            ret += dataFolderSize;
                        } else {
                            logger.error("Data folder '{}' has a size of {} bytes.", dataFolder.getFileName().toString());
                        }
                    } else {
                        logger.error("Data folder not found: {}", dataFolder.toAbsolutePath().toString());
                    }
                }
            }
        } else {
            // Look for data folders in hotfolder
            logger.info("No data folders passed, scanning hotfolder...");
            String fileNameRoot = FilenameUtils.getBaseName(dataFile.getFileName().toString());
            Path hotfolderPath = dataFile.getParent();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new StringBuilder(fileNameRoot).append("_*").toString())) {
                for (Path dataFolder : stream) {
                    logger.debug("Found data folder: {}", dataFolder.getFileName());
                    long dataFolderSize = FileUtils.sizeOfDirectory(dataFolder.toFile());
                    if (dataFolderSize > 0) {
                        ret += dataFolderSize;
                    } else {
                        logger.error("Data folder '{}' has a size of {} bytes.", dataFolder.getFileName().toString());
                    }
                }
            }
        }

        return ret;
    }

}
