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
package io.goobi.viewer.indexer.model.datarepository;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.StringConstants;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.datarepository.strategy.IDataRepositoryStrategy;

/**
 * <p>
 * DataRepository class.
 * </p>
 *
 */
public class DataRepository {

    private static final Logger logger = LogManager.getLogger(DataRepository.class);

    /** Constant <code>PARAM_INDEXED_METS="indexedMets"</code> */
    public static final String PARAM_INDEXED_METS = "indexedMets";
    /** Constant <code>PARAM_INDEXED_LIDO="indexedLido"</code> */
    public static final String PARAM_INDEXED_LIDO = "indexedLido";
    /** Constant <code>PARAM_INDEXED_DENKXWEB="indexedDenkXweb"</code> */
    public static final String PARAM_INDEXED_DENKXWEB = "indexedDenkXweb";
    /** Constant <code>PARAM_INDEXED_DC="indexedDC"</code> */
    public static final String PARAM_INDEXED_DUBLINCORE = "indexedDublinCore";
    /** Constant <code>PARAM_INDEXED_CMS="indexedCMS"</code> */
    public static final String PARAM_INDEXED_CMS = "indexedCMS";
    /** Constant <code>PARAM_MEDIA="mediaFolder"</code> */
    public static final String PARAM_MEDIA = "mediaFolder";
    /** Constant <code>PARAM_ALTO="altoFolder"</code> */
    public static final String PARAM_ALTO = "altoFolder";
    /** Constant <code>PARAM_ALTO_CONVERTED="altoFolder"</code> */
    public static final String PARAM_ALTO_CONVERTED = "altoFolder";
    /** Constant <code>PARAM_ALTOCROWD="altoCrowdsourcingFolder"</code> */
    public static final String PARAM_ALTOCROWD = "altoCrowdsourcingFolder";
    /** Constant <code>PARAM_ABBYY="abbyyFolder"</code> */
    public static final String PARAM_ABBYY = "abbyyFolder";
    /** Constant <code>PARAM_CMS="cmsFolder"</code> */
    public static final String PARAM_CMS = "cmsFolder";
    /** Constant <code>PARAM_DOWNLOAD_IMAGES_TRIGGER="downloadImages"</code> */
    public static final String PARAM_DOWNLOAD_IMAGES_TRIGGER = "downloadImages";
    /** Constant <code>PARAM_FULLTEXT="fulltextFolder"</code> */
    public static final String PARAM_FULLTEXT = "fulltextFolder";
    /** Constant <code>PARAM_FULLTEXTCROWD="fulltextCrowdsourcingFolder"</code> */
    public static final String PARAM_FULLTEXTCROWD = "fulltextCrowdsourcingFolder";
    /** Constant <code>PARAM_CMDI="cmdiFolder"</code> */
    public static final String PARAM_CMDI = "cmdiFolder";
    /** Constant <code>PARAM_TEIMETADATA="teiFolder"</code> */
    public static final String PARAM_TEIMETADATA = "teiFolder";
    /** Constant <code>PARAM_TEIWC="wcFolder"</code> */
    public static final String PARAM_TEIWC = "wcFolder";
    /** Constant <code>PARAM_PAGEPDF="pagePdfFolder"</code> */
    public static final String PARAM_PAGEPDF = "pagePdfFolder";
    /** Constant <code>PARAM_SOURCE="sourceContentFolder"</code> */
    public static final String PARAM_SOURCE = "sourceContentFolder";
    /** Constant <code>PARAM_UGC="userGeneratedContentFolder"</code> */
    public static final String PARAM_UGC = "userGeneratedContentFolder";
    /** Constant <code>PARAM_MIX="mixFolder"</code> */
    public static final String PARAM_MIX = "mixFolder";
    /** Constant <code>PARAM_ANNOTATIONS="annotationFolder"</code> */
    public static final String PARAM_ANNOTATIONS = "annotationFolder";

    private boolean valid = false;
    private String path;
    private Path rootDir;
    private long buffer = 0;
    private final Map<String, Path> dirMap = new HashMap<>();

    /**
     * Constructor for unit tests.
     *
     * @param path a {@link java.lang.String} object.
     */
    public DataRepository(final String path) {
        this.path = path;
    }

    /**
     * <p>
     * Constructor for DataRepository.
     * </p>
     *
     * @param path Absolute path to the repository; empty string means the default folder structure in init.viewerHome will be used
     * @param createFolders If true, the data subfolders will be automatically created
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should create dummy repository correctly
     * @should create real repository correctly
     * @should set rootDir to viewerHome path if empty string was given
     * @should add each data directory to the dirMap of dummy repository
     * @should add each data directory to the dirMap of real repository
     */
    public DataRepository(final String path, final boolean createFolders) throws FatalIndexerException {
        this.path = path;
        rootDir = "".equals(path) ? Paths.get(SolrIndexerDaemon.getInstance().getConfiguration().getViewerHome()) : Paths.get(path);

        if (Files.exists(rootDir)) {
            if (Files.isRegularFile(rootDir)) {
                logger.error("Data repository '{}' is defined but is a file, not a directory. This repository will not be used.",
                        rootDir.getFileName());
                return;
            }
        } else if (createFolders) {
            try {
                Files.createDirectories(rootDir);
                logger.info("Repository directory '{}' created.", rootDir.toAbsolutePath());
            } catch (IOException e) {
                logger.error("Repository directory '{}' could not be created.", rootDir.toAbsolutePath());
                return;
            }
        }

        checkAndCreateDataSubdir(PARAM_INDEXED_METS, createFolders);
        checkAndCreateDataSubdir(PARAM_INDEXED_LIDO, createFolders);
        checkAndCreateDataSubdir(PARAM_INDEXED_DENKXWEB, createFolders);
        checkAndCreateDataSubdir(PARAM_INDEXED_DUBLINCORE, createFolders);
        checkAndCreateDataSubdir(PARAM_INDEXED_CMS, createFolders);
        checkAndCreateDataSubdir(PARAM_MEDIA, createFolders);
        checkAndCreateDataSubdir(PARAM_ALTO, createFolders);
        checkAndCreateDataSubdir(PARAM_ALTOCROWD, createFolders);
        checkAndCreateDataSubdir(PARAM_FULLTEXT, createFolders);
        checkAndCreateDataSubdir(PARAM_FULLTEXTCROWD, createFolders);
        checkAndCreateDataSubdir(PARAM_CMDI, createFolders);
        checkAndCreateDataSubdir(PARAM_TEIMETADATA, createFolders);
        checkAndCreateDataSubdir(PARAM_TEIWC, createFolders);
        checkAndCreateDataSubdir(PARAM_ABBYY, createFolders);
        checkAndCreateDataSubdir(PARAM_PAGEPDF, createFolders);
        checkAndCreateDataSubdir(PARAM_SOURCE, createFolders);
        checkAndCreateDataSubdir(PARAM_UGC, createFolders);
        checkAndCreateDataSubdir(PARAM_MIX, createFolders);
        checkAndCreateDataSubdir(PARAM_CMS, createFolders);
        checkAndCreateDataSubdir(PARAM_ANNOTATIONS, createFolders);

        if (Files.exists(rootDir)) {
            valid = true;
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((path == null) ? 0 : path.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DataRepository other = (DataRepository) obj;
        if (path == null) {
            if (other.path != null)
                return false;
        } else if (!path.equals(other.path))
            return false;
        return true;
    }

    /**
     * 
     * @param dataFolder Data folder Path
     * @param paramName Data folder type
     * @return true if given data folder path represents the data folder in this repository; false otherwise
     */
    public boolean isRepositoryDataFolder(Path dataFolder, String paramName) {
        if (dataFolder == null) {
            return false;
        }
        if (paramName == null) {
            return false;
        }

        return dataFolder.equals(getDir(paramName));
    }

    /**
     * 
     * @param dataDirName
     * @param create
     * @throws FatalIndexerException
     */
    private void checkAndCreateDataSubdir(String dataDirName, boolean create) throws FatalIndexerException {
        if (dataDirName == null) {
            throw new IllegalArgumentException("name may not be null");
        }
        String config = SolrIndexerDaemon.getInstance().getConfiguration().getConfiguration(dataDirName);
        if (StringUtils.isEmpty(config)) {
            switch (dataDirName) {
                case PARAM_INDEXED_METS:
                case PARAM_INDEXED_LIDO:
                case PARAM_INDEXED_DENKXWEB:
                case PARAM_INDEXED_DUBLINCORE:
                case PARAM_INDEXED_CMS:
                    return;
                default:
                    throw new FatalIndexerException("No configuration found for '" + dataDirName + "', exiting...");
            }

        }
        Path dataSubdir = Paths.get(rootDir.toAbsolutePath().toString(), config);
        if (!Files.isDirectory(dataSubdir) && create) {
            try {
                Files.createDirectory(dataSubdir);
                logger.info("Created directory: {}", dataSubdir.toAbsolutePath());
            } catch (IOException e) {
                logger.error("Could not create directory: {}", dataSubdir.toAbsolutePath());
                throw new FatalIndexerException("Data directories could not be created, please check directory ownership.");
            }
        }
        dirMap.put(dataDirName, dataSubdir);
    }

    /**
     * Deletes all
     *
     * @param baseFileName a {@link java.lang.String} object.
     * @should delete ALTO folder correctly
     * @should delete ALTO crowdsourcing folder correctly
     * @should delete fulltext folder correctly
     * @should delete fulltext crowdsourcing folder correctly
     * @should delete CMDI folder correctly
     * @should delete TEI folder correctly
     * @should delete word coords folder correctly
     * @should delete ABBYY folder correctly
     * @should delete media folder correctly
     * @should delete source folder correctly
     * @should delete user generated content folder correctly
     * @should delete MIX folder correctly
     * @should delete page PDF folder correctly
     * @should delete CMS folder correctly
     * @should delete annotations folder correctly
     */
    public void deleteDataFoldersForRecord(String baseFileName) {
        deleteFolder(Paths.get(getDir(PARAM_ALTO).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_ALTOCROWD).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_FULLTEXT).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_FULLTEXTCROWD).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_CMDI).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_TEIMETADATA).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_TEIWC).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_ABBYY).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_MEDIA).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_SOURCE).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_UGC).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_MIX).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_PAGEPDF).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_CMS).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_ANNOTATIONS).toAbsolutePath().toString(), baseFileName));
    }

    /**
     * Deletes the given folder.
     *
     * @param folder a {@link java.nio.file.Path} object.
     * @should delete folder correctly
     */
    protected static void deleteFolder(Path folder) {
        if (!Files.isDirectory(folder)) {
            logger.debug("File '{}' is not a directory.", folder.toAbsolutePath());
            return;
        }
        try {
            FileUtils.deleteDirectory(folder.toFile());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Counts the total number of records in this data repository by adding METS, LIDO, DenkXWeb, DC and CMS page records.
     *
     * @throws java.io.IOException
     * @return Number of records of all types
     * @should calculate number correctly
     */
    public int getNumRecords() throws IOException {
        int metsRecords = countFiles(getDir(PARAM_INDEXED_METS));
        logger.info("Data repository '{}' contains {} METS records.", path, metsRecords);
        int lidoRecords = countFiles(getDir(PARAM_INDEXED_LIDO));
        logger.info("Data repository '{}' contains {} LIDO records.", path, lidoRecords);
        int denkxwebRecords = countFiles(getDir(PARAM_INDEXED_DENKXWEB));
        logger.info("Data repository '{}' contains {} DenkXweb records.", path, denkxwebRecords);
        int dcRecords = countFiles(getDir(PARAM_INDEXED_DUBLINCORE));
        logger.info("Data repository '{}' contains {} Dublin Core records.", path, dcRecords);
        int cmsRecords = countFiles(getDir(PARAM_INDEXED_CMS));
        logger.info("Data repository '{}' contains {} CMS page records.", path, cmsRecords);

        return metsRecords + lidoRecords + denkxwebRecords + dcRecords + cmsRecords;
    }

    /**
     * <p>
     * getUsableSpace.
     * </p>
     *
     * @return Remaining space in bytes
     */
    public long getUsableSpace() {
        try {
            return Files.getFileStore(rootDir.toRealPath()).getUsableSpace();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return 0;
    }

    /**
     * Migrates data folders from this repository to the given repository.
     *
     * @param toRepository a {@link io.goobi.viewer.indexer.model.datarepository.DataRepository} object.
     * @param pi a {@link java.lang.String} object.
     */
    public void moveDataFoldersToRepository(DataRepository toRepository, String pi) {
        if (toRepository == null) {
            throw new IllegalArgumentException("toRepository may not be null");
        }

        logger.info("Moving data from '{}' to '{}'...", path, toRepository.getPath());
        if (toRepository.equals(this)) {
            logger.error("Source and destination repositories are the same, cannot move data.");
            return;
        }

        moveDataFolderToRepository(toRepository, pi, PARAM_MEDIA);
        moveDataFolderToRepository(toRepository, pi, PARAM_ALTO);
        moveDataFolderToRepository(toRepository, pi, PARAM_ALTOCROWD);
        moveDataFolderToRepository(toRepository, pi, PARAM_ABBYY);
        moveDataFolderToRepository(toRepository, pi, PARAM_FULLTEXT);
        moveDataFolderToRepository(toRepository, pi, PARAM_FULLTEXTCROWD);
        moveDataFolderToRepository(toRepository, pi, PARAM_CMDI);
        moveDataFolderToRepository(toRepository, pi, PARAM_TEIMETADATA);
        moveDataFolderToRepository(toRepository, pi, PARAM_TEIWC);
        moveDataFolderToRepository(toRepository, pi, PARAM_PAGEPDF);
        moveDataFolderToRepository(toRepository, pi, PARAM_SOURCE);
        moveDataFolderToRepository(toRepository, pi, PARAM_UGC);
        moveDataFolderToRepository(toRepository, pi, PARAM_MIX);
        moveDataFolderToRepository(toRepository, pi, PARAM_CMS);
        moveDataFolderToRepository(toRepository, pi, PARAM_ANNOTATIONS);

        // METS
        if (getDir(PARAM_INDEXED_METS) != null) {
            Path oldRecordFile = Paths.get(getDir(PARAM_INDEXED_METS).toAbsolutePath().toString(), pi + ".xml");
            if (Files.isRegularFile(oldRecordFile)) {
                try {
                    Files.delete(oldRecordFile);
                    logger.info("Deleted old repository METS file: {}", oldRecordFile.toAbsolutePath());
                } catch (IOException e) {
                    logger.error("Could not delete old repository METS file: {}", oldRecordFile.toAbsolutePath());
                }
            }
        }
        // LIDO
        if (getDir(PARAM_INDEXED_LIDO) != null) {
            Path oldRecordFile = Paths.get(getDir(PARAM_INDEXED_LIDO).toAbsolutePath().toString(), pi + ".xml");
            if (Files.isRegularFile(oldRecordFile)) {
                try {
                    Files.delete(oldRecordFile);
                    logger.info("Deleted old repository LIDO file: {}", oldRecordFile.toAbsolutePath());
                } catch (IOException e) {
                    logger.error("Could not delete old repository LIDO file: {}", oldRecordFile.toAbsolutePath());
                }
            }
        }
        // DENKXWEB
        if (getDir(PARAM_INDEXED_DENKXWEB) != null) {
            Path oldRecordFile = Paths.get(getDir(PARAM_INDEXED_DENKXWEB).toAbsolutePath().toString(), pi + ".xml");
            if (Files.isRegularFile(oldRecordFile)) {
                try {
                    Files.delete(oldRecordFile);
                    logger.info("Deleted old repository DenkXweb file: {}", oldRecordFile.toAbsolutePath());
                } catch (IOException e) {
                    logger.error("Could not delete old repository DenkXweb file: {}", oldRecordFile.toAbsolutePath());
                }
            }
        }
        // DUBLIN CORE
        if (getDir(PARAM_INDEXED_DUBLINCORE) != null) {
            Path oldRecordFile = Paths.get(getDir(PARAM_INDEXED_DUBLINCORE).toAbsolutePath().toString(), pi + ".xml");
            if (Files.isRegularFile(oldRecordFile)) {
                try {
                    Files.delete(oldRecordFile);
                    logger.info("Deleted old repository Dublin Core file: {}", oldRecordFile.toAbsolutePath());
                } catch (IOException e) {
                    logger.error("Could not delete old repository Dublin Core file: {}", oldRecordFile.toAbsolutePath());
                }
            }
        }
        // CMS PAGES
        if (getDir(PARAM_INDEXED_CMS) != null) {
            Path oldRecordFile = Paths.get(getDir(PARAM_INDEXED_CMS).toAbsolutePath().toString(), pi + ".xml");
            if (Files.isRegularFile(oldRecordFile)) {
                try {
                    Files.delete(oldRecordFile);
                    logger.info("Deleted old repository CMS page file: {}", oldRecordFile.toAbsolutePath());
                } catch (IOException e) {
                    logger.error("Could not delete old repository CMS page file: {}", oldRecordFile.toAbsolutePath());
                }
            }
        }
        logger.info("Moving data completed.");
    }

    /**
     * <p>
     * moveDataFolderToRepository.
     * </p>
     *
     * @param toRepository a {@link io.goobi.viewer.indexer.model.datarepository.DataRepository} object.
     * @param pi a {@link java.lang.String} object.
     * @param type a {@link java.lang.String} object.
     * @should move data folder correctly
     */
    public void moveDataFolderToRepository(DataRepository toRepository, String pi, String type) {
        if (getDir(type) == null) {
            logger.warn("Data folder not configured in repository '{}': {}", path, type);
            return;
        }

        Path oldDataFolder = Paths.get(getDir(type).toAbsolutePath().toString(), pi);
        if (oldDataFolder.toFile().isDirectory()) {
            Path newDataDir = Paths.get(toRepository.getDir(type).toAbsolutePath().toString(), pi);
            try {
                int copied = FileTools.copyDirectory(oldDataFolder, newDataDir);
                if (copied > 0) {
                    logger.info("Copied {} files to repository directory: {}", copied, newDataDir.toAbsolutePath());
                }
                if (Utils.deleteDirectory(oldDataFolder)) {
                    logger.info("Old repository directory deleted: {}", oldDataFolder);
                } else {
                    logger.warn("Could not delete old repository directory: {}", oldDataFolder);
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        } else {
            logger.debug("{} does not exist.", oldDataFolder.toAbsolutePath());
        }
    }

    /**
     * <p>
     * deleteDataFolders.
     * </p>
     *
     * @param dataFolders a {@link java.util.Map} object.
     * @should delete ALTO folder correctly
     * @should delete ALTO crowdsourcing folder correctly
     * @should delete fulltext folder correctly
     * @should delete fulltext crowdsourcing folder correctly
     * @should delete CMDI folder correctly
     * @should delete TEI folder correctly
     * @should delete word coords folder correctly
     * @should delete ABBYY folder correctly
     * @should delete media folder correctly
     * @should delete source folder correctly
     * @should delete user generated content folder correctly
     * @should delete MIX folder correctly
     * @should delete page PDF folder correctly
     * @should delete CMS folder correctly
     * @should delete annotations folder correctly
     * @should delete download images trigger folder correctly
     */
    public static void deleteDataFoldersFromHotfolder(Map<String, Path> dataFolders, Map<String, Boolean> reindexSettings) {
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_ALTO);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_ALTOCROWD);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_FULLTEXT);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_FULLTEXTCROWD);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_CMDI);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_TEIMETADATA);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_TEIWC);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_ABBYY);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_MEDIA);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_MIX);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_UGC);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_CMS);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_PAGEPDF);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_SOURCE);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_ANNOTATIONS);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER);
    }

    /**
     * Deletes the data folder with the given parameter name, but only if it's not being re-indexed.
     * 
     * @param dataFolders
     * @param reindexSettings
     * @param param Data folder parameter name
     * @should delete folders correctly
     * @should not delete reindexed folders
     */
    static void deleteDataFolder(Map<String, Path> dataFolders, Map<String, Boolean> reindexSettings, String param) {
        if (param == null) {
            throw new IllegalArgumentException("param may not be null");
        }

        if ((reindexSettings == null || reindexSettings.get(param) == null || !reindexSettings.get(param)) && dataFolders.get(param) != null) {
            logger.info("Deleting data folder: {}", dataFolders.get(param).toAbsolutePath());
            Utils.deleteDirectory(dataFolders.get(param));
        }
    }

    /**
     * Copies data folders (all except for _media) from the hotfolder to their respective destination and deletes the folders from the hotfolder.
     *
     * @param pi Record identifier
     * @param dataFolders Map with source data folders
     * @param reindexSettings Boolean map for data folders which are mapped for re-indexing (i.e. no new data folder in the hotfolder)
     * @param dataRepositories a {@link java.util.List} object.
     * @throws java.io.IOException
     */
    public void copyAndDeleteAllDataFolders(String pi, Map<String, Path> dataFolders, Map<String, Boolean> reindexSettings,
            List<DataRepository> dataRepositories) throws IOException {
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null");
        }
        if (reindexSettings == null) {
            throw new IllegalArgumentException("reindexSettings may not be null");
        }

        // Copy and delete ALTO folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_ALTO, dataRepositories);
        // Copy and delete crowdsourcing ALTO folder (AFTER regular ALTO!)
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_ALTOCROWD, dataRepositories);
        // Copy and delete fulltext folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_FULLTEXT, dataRepositories);
        // Copy and delete crowdsourcing fulltext folder (AFTER regular FULLTEXT!)
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_FULLTEXTCROWD, dataRepositories);
        // Copy and delete CMDI metadata folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_CMDI, dataRepositories);
        // Copy and delete TEI metadata folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_TEIMETADATA, dataRepositories);
        // Copy and delete TEI word coordinates folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_TEIWC, dataRepositories);
        // Copy and delete ABBYY word coordinates folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_ABBYY, dataRepositories);
        // Copy and delete MIX files
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_MIX, dataRepositories);
        // Copy and delete page PDF files
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_PAGEPDF, dataRepositories);
        // Copy and delete original content files
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_SOURCE, dataRepositories);
        // Copy and delete user generated content files
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_UGC, dataRepositories);
        // Copy and delete CMS page text files
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_CMS, dataRepositories);
        // Copy and delete WebAnnotation files
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_ANNOTATIONS, dataRepositories);
        // Delete image download trigger folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER, dataRepositories);
    }

    /**
     * Searches and deletes copies of a file with the given fileName in repositories that are not this one.
     * 
     * @param fileName File to check
     * @param param Data folder name parameter
     * @param dataRepositories List of all available data repositories
     * @throws IOException
     * @should delete only misplaced files
     * 
     */
    public void checkOtherRepositoriesForRecordFileDuplicates(String fileName, String param, List<DataRepository> dataRepositories)
            throws IOException {
        for (DataRepository repo : dataRepositories) {
            if (!repo.equals(this) && repo.getDir(param) != null) {
                Path misplacedRecordFile = Paths.get(repo.getDir(param).toAbsolutePath().toString(), fileName);
                if (Files.isRegularFile(misplacedRecordFile)) {
                    logger.warn("Found {} file this record in different data repository: {}, deleting file...", param,
                            misplacedRecordFile.toAbsolutePath());
                    Files.delete(misplacedRecordFile);
                }
            }
        }
    }

    /**
     * Checks whether the folder with the given param name exists and is in the hotfolder, then proceeds to copy it to the destination folder and
     * delete the source.
     *
     * @param pi Record identifier
     * @param dataFolders Map with source data folders
     * @param reindexSettings Boolean map for data folders which are mapped for re-indexing (i.e. no new data folder in the hotfolder)
     * @param param Folder parameter name
     * @param dataRepositories a {@link java.util.List} object.
     * @return Number of copied files
     * @throws java.io.IOException
     */
    public int checkCopyAndDeleteDataFolder(String pi, Map<String, Path> dataFolders, Map<String, Boolean> reindexSettings, String param,
            List<DataRepository> dataRepositories) throws IOException {
        if (param == null) {
            throw new IllegalArgumentException("param may not be null");
        }

        if ((reindexSettings.get(param) == null || !reindexSettings.get(param)) && dataFolders.get(param) != null) {
            return copyAndDeleteDataFolder(dataFolders.get(param), param, pi);
        } else if (reindexSettings.get(param) != null && reindexSettings.get(param) && dataRepositories != null) {
            // Check for a data folder in different repositories (fixing broken migration from old-style data repositories to new)
            for (DataRepository repo : dataRepositories) {
                if (!repo.equals(this) && repo.getDir(param) != null) {
                    Path misplacedDataDir = Paths.get(repo.getDir(param).toAbsolutePath().toString(), pi);
                    if (Files.isDirectory(misplacedDataDir)) {
                        logger.warn("Found data folder for this record in different data repository: {}",
                                misplacedDataDir.toAbsolutePath());
                        logger.warn("Moving data folder to new repository...");
                        repo.moveDataFolderToRepository(this, pi, param);
                    }
                }
            }
        }

        return 0;
    }

    /**
     * <p>
     * copyAndDeleteDataFolder.
     * </p>
     *
     * @param srcFolder a {@link java.nio.file.Path} object.
     * @param paramName a {@link java.lang.String} object.
     * @param identifier a {@link java.lang.String} object.
     * @return Number of files copied.
     * @throws java.io.IOException
     */
    public int copyAndDeleteDataFolder(Path srcFolder, String paramName, String identifier) throws IOException {
        if (srcFolder == null) {
            throw new IllegalArgumentException("srcFolder may not be null");
        }
        if (paramName == null) {
            throw new IllegalArgumentException("paramName may not be null");
        }
        if (identifier == null) {
            throw new IllegalArgumentException("identifier may not be null");
        }

        int counter = 0;
        if (!DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER.equals(paramName)) {
            logger.info("Copying {} files from '{}' to '{}'...", paramName, srcFolder, getDir(paramName).toAbsolutePath());
            counter = FileTools.copyDirectory(srcFolder, new File(getDir(paramName).toFile(), identifier).toPath());
            logger.info("{} {} files copied.", counter, paramName);
        }
        if (!Utils.deleteDirectory(srcFolder)) {
            logger.warn(StringConstants.LOG_COULD_NOT_BE_DELETED, srcFolder.toAbsolutePath());
        }

        return counter;
    }

    /**
     * Copies image files for the given record identifier from a media folder that may contain images for more than one record.
     * 
     * @param sourceMediaFolder Media folder in hotfolder
     * @param identifier Record identifier
     * @param recordFileName File name of the source file
     * @param dataRepositoryStrategy
     * @param fileNames Semicolon-separated file name list
     * @param reindexing true if using old media folder; false otherwise
     * @return Number of copied files
     * @throws IOException
     */
    public int copyImagesFromMultiRecordMediaFolder(Path sourceMediaFolder, String identifier, String recordFileName,
            IDataRepositoryStrategy dataRepositoryStrategy, String fileNames, boolean reindexing) throws IOException {
        int imageCounter = 0;
        if (!reindexing) {
            // copy media files
            Path destMediaFolder = Paths.get(getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), identifier);
            if (!Files.exists(destMediaFolder)) {
                Files.createDirectory(destMediaFolder);
            }
            if (StringUtils.isNotEmpty(fileNames) && sourceMediaFolder != null) {
                logger.info("Copying image files...");
                String[] imgFileNamesSplit = fileNames.split(";");
                Set<String> imgFileNames = new HashSet<>(Arrays.asList(imgFileNamesSplit));
                try (DirectoryStream<Path> mediaFileStream = Files.newDirectoryStream(sourceMediaFolder)) {
                    for (Path mediaFile : mediaFileStream) {
                        if (Files.isRegularFile(mediaFile) && imgFileNames.contains(mediaFile.getFileName().toString())) {
                            logger.info("Copying file {} to {}", mediaFile.toAbsolutePath(), destMediaFolder.toAbsolutePath());
                            Files.copy(mediaFile, Paths.get(destMediaFolder.toAbsolutePath().toString(), mediaFile.getFileName().toString()),
                                    StandardCopyOption.REPLACE_EXISTING);
                            imageCounter++;
                        }
                    }
                }
            } else {
                logger.warn("No media file names could be extracted for record '{}'.", identifier);
            }
            if (imageCounter > 0) {
                logger.info("{} media file(s) copied.", imageCounter);
                return imageCounter;
            }
        }

        if (imageCounter == 0) {
            // Check for a data folder in different repositories (fixing broken migration from old-style data repositories to new)
            if (reindexing) {
                for (DataRepository repo : dataRepositoryStrategy.getAllDataRepositories()) {
                    if (!repo.equals(this) && repo.getDir(DataRepository.PARAM_MEDIA) != null) {
                        Path misplacedDataDir =
                                Paths.get(repo.getDir(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), identifier);
                        if (Files.isDirectory(misplacedDataDir)) {
                            logger.warn("Found data folder for this record in different data repository: {}",
                                    misplacedDataDir.toAbsolutePath());
                            logger.warn("Moving data folder to new repository...");
                            repo.moveDataFolderToRepository(this, identifier, DataRepository.PARAM_MEDIA);
                        }
                    }
                }
            } else {
                logger.warn("No media folder found for '{}'.", recordFileName);
            }
        }

        return imageCounter;
    }

    /**
     * <p>
     * countFiles.
     * </p>
     *
     * @param dir a {@link java.nio.file.Path} object.
     * @throws java.io.IOException
     * @return a int.
     */
    public static int countFiles(Path dir) throws IOException {
        if (dir == null || !Files.isDirectory(dir)) {
            return 0;
        }

        int c = 0;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
            for (Path file : files) {
                if (Files.isRegularFile(file) || Files.isSymbolicLink(file)) {
                    c++;
                }
            }
        }

        return c;
    }

    /**
     * Returns (probable) absolute path to the data repository with the given name. Used to properly detect old-style data repositories.
     *
     * @param dataRepository a {@link java.lang.String} object.
     * @return Absolute path to the given data repository name
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should return correct path
     */
    public static String getAbsolutePath(String dataRepository) throws FatalIndexerException {
        if (dataRepository != null && !Paths.get(dataRepository).isAbsolute()) {
            logger.warn("Data repository value type is deprecated: {}", dataRepository);
            dataRepository = SolrIndexerDaemon.getInstance().getConfiguration().getViewerHome() + "/data/" + dataRepository;
            dataRepository = dataRepository.replace("//", "/");
            logger.warn("Assuming absolute path to be: {}", dataRepository);
        }

        return dataRepository;
    }

    /**
     * <p>
     * loadDataRepositories.
     * </p>
     *
     * @param config a {@link io.goobi.viewer.indexer.helper.Configuration} object.
     * @param createFolders a boolean.
     * @return List of properly configured data repositories
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     */
    public static List<DataRepository> loadDataRepositories(Configuration config, boolean createFolders) throws FatalIndexerException {
        if (config == null) {
            throw new IllegalArgumentException("config may not be null");
        }

        List<DataRepository> dataRepositoryConfigs = config.getDataRepositoryConfigurations();
        if (dataRepositoryConfigs.isEmpty()) {
            return Collections.emptyList();
        }

        List<DataRepository> ret = new ArrayList<>(dataRepositoryConfigs.size());
        for (DataRepository conf : dataRepositoryConfigs) {
            DataRepository dataRepository = new DataRepository(conf.getPath(), createFolders);
            if (dataRepository.isValid()) {
                dataRepository.setBuffer(conf.getBuffer());
                ret.add(dataRepository);
                logger.info("Found configured data repository: {}", dataRepository.getPath());
            }
        }

        return ret;
    }

    /**
     * <p>
     * isValid.
     * </p>
     *
     * @return the valid
     */
    public boolean isValid() {
        return valid;
    }

    /**
     * <p>
     * Getter for the field <code>path</code>.
     * </p>
     *
     * @return the name
     */
    public String getPath() {
        return path;
    }

    /**
     * <p>
     * Getter for the field <code>rootDir</code>.
     * </p>
     *
     * @return the rootDir
     */
    public Path getRootDir() {
        return rootDir;
    }

    /**
     * <p>
     * Getter for the field <code>buffer</code>.
     * </p>
     *
     * @return the buffer
     */
    public long getBuffer() {
        return buffer;
    }

    /**
     * <p>
     * Setter for the field <code>buffer</code>.
     * </p>
     *
     * @param buffer the buffer to set
     */
    public void setBuffer(long buffer) {
        this.buffer = buffer;
    }

    /**
     * <p>
     * getDir.
     * </p>
     *
     * @param name a {@link java.lang.String} object.
     * @return a {@link java.nio.file.Path} object.
     */
    public Path getDir(String name) {
        return dirMap.get(name);
    }
}
