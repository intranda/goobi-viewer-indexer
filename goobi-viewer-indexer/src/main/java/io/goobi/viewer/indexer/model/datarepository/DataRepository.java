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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.FatalIndexerException;

public class DataRepository {

    private static final Logger logger = LoggerFactory.getLogger(DataRepository.class);

    public static final String PARAM_INDEXED_METS = "indexedMets";
    public static final String PARAM_INDEXED_LIDO = "indexedLido";
    public static final String PARAM_INDEXED_DENKXWEB = "indexedDenkXweb";
    public static final String PARAM_MEDIA = "mediaFolder";
    public static final String PARAM_ALTO = "altoFolder";
    public static final String PARAM_ALTO_CONVERTED = "altoFolder";
    public static final String PARAM_ALTOCROWD = "altoCrowdsourcingFolder";
    public static final String PARAM_ABBYY = "abbyyFolder";
    public static final String PARAM_CMS = "cmsFolder";
    public static final String PARAM_DOWNLOAD_IMAGES_TRIGGER = "downloadImages";
    public static final String PARAM_FULLTEXT = "fulltextFolder";
    public static final String PARAM_FULLTEXTCROWD = "fulltextCrowdsourcingFolder";
    public static final String PARAM_CMDI = "cmdiFolder";
    public static final String PARAM_TEIMETADATA = "teiFolder";
    public static final String PARAM_TEIWC = "wcFolder";
    public static final String PARAM_PAGEPDF = "pagePdfFolder";
    public static final String PARAM_SOURCE = "sourceContentFolder";
    public static final String PARAM_UGC = "userGeneratedContentFolder";
    public static final String PARAM_MIX = "mixFolder";

    //    public static int dataRepositoriesMaxRecords = 10000;

    private boolean valid = false;
    private String path;
    private Path rootDir;
    private long buffer = 0;
    private final Map<String, Path> dirMap = new HashMap<>();

    /**
     * Constructor for unit tests.
     */
    public DataRepository(final String path) {
        this.path = path;
    }

    /**
     * 
     * @param path Absolute path to the repository; empty string means the default folder structure in init.viewerHome will be used
     * @param createFolders If true, the data subfolders will be automatically created
     * @throws FatalIndexerException
     * @should create dummy repository correctly
     * @should create real repository correctly
     * @should set rootDir to viewerHome path if empty string was given
     */
    public DataRepository(final String path, final boolean createFolders) throws FatalIndexerException {
        this.path = path;
        rootDir = "".equals(path) ? Paths.get(Configuration.getInstance().getViewerHome()) : Paths.get(path);

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
        if (createFolders) {
            checkAndCreateDataSubdir(PARAM_INDEXED_METS);
            checkAndCreateDataSubdir(PARAM_INDEXED_LIDO);
            checkAndCreateDataSubdir(PARAM_INDEXED_DENKXWEB);
            checkAndCreateDataSubdir(PARAM_MEDIA);
            checkAndCreateDataSubdir(PARAM_ALTO);
            checkAndCreateDataSubdir(PARAM_ALTOCROWD);
            checkAndCreateDataSubdir(PARAM_FULLTEXT);
            checkAndCreateDataSubdir(PARAM_FULLTEXTCROWD);
            checkAndCreateDataSubdir(PARAM_CMDI);
            checkAndCreateDataSubdir(PARAM_TEIMETADATA);
            checkAndCreateDataSubdir(PARAM_TEIWC);
            checkAndCreateDataSubdir(PARAM_ABBYY);
            checkAndCreateDataSubdir(PARAM_PAGEPDF);
            checkAndCreateDataSubdir(PARAM_SOURCE);
            checkAndCreateDataSubdir(PARAM_UGC);
            checkAndCreateDataSubdir(PARAM_MIX);
            checkAndCreateDataSubdir(PARAM_CMS);
        }
        if (Files.exists(rootDir)) {
            valid = true;
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
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
     * @param name
     * @throws FatalIndexerException
     */
    private void checkAndCreateDataSubdir(String name) throws FatalIndexerException {
        if (name == null) {
            throw new IllegalArgumentException("name may not be null");
        }
        String config = Configuration.getInstance().getConfiguration(name);
        if (StringUtils.isEmpty(config)) {
            switch (name) {
                case PARAM_INDEXED_METS:
                case PARAM_INDEXED_LIDO:
                case PARAM_INDEXED_DENKXWEB:
                    return;
                default:
                    throw new FatalIndexerException("No configuration found for '" + name + "', exiting...");
            }

        }
        //        File dataSubdir = new File(rootDir, config);
        Path dataSubdir = Paths.get(rootDir.toAbsolutePath().toString(), config);
        if (!Files.isDirectory(dataSubdir)) {
            try {
                Files.createDirectory(dataSubdir);
                logger.info("Created directory: {}", dataSubdir.toAbsolutePath());
            } catch (IOException e) {
                logger.error("Could not create directory: {}", dataSubdir.toAbsolutePath());
                throw new FatalIndexerException("Data directories could not be created, please check directory ownership.");
            }
        }
        dirMap.put(name, dataSubdir);
    }

    /**
     * Deletes all
     * 
     * @param baseFileName
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
    }

    /**
     * Deletes the given folder.
     * 
     * @param folder
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
     * Counts the total number of records in this data repository by adding METS and LIDO records.
     * 
     * @return
     * @throws IOException
     * @should calculate number correctly
     */
    public int getNumRecords() throws IOException {
        int metsRecords = countFiles(getDir(PARAM_INDEXED_METS));
        logger.info("Data repository '{}' contains {} METS records.", path, metsRecords);
        int lidoRecords = countFiles(getDir(PARAM_INDEXED_LIDO));
        logger.info("Data repository '{}' contains {} LIDO records.", path, lidoRecords);
        int denkxwebRecords = countFiles(getDir(PARAM_INDEXED_DENKXWEB));
        logger.info("Data repository '{}' contains {} DenkXweb records.", path, denkxwebRecords);

        return metsRecords + lidoRecords + denkxwebRecords;
    }

    /**
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
     * @param toRepository
     * @param pi
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

        // METS
        Path oldRecordFile = Paths.get(getDir(PARAM_INDEXED_METS).toAbsolutePath().toString(), pi + ".xml");
        if (Files.isRegularFile(oldRecordFile)) {
            try {
                Files.delete(oldRecordFile);
                logger.info("Deleted old repository METS file: {}", oldRecordFile.toAbsolutePath());
            } catch (IOException e) {
                logger.error("Could not delete old repository METS file: {}", oldRecordFile.toAbsolutePath());
            }
        }
        // LIDO
        oldRecordFile = Paths.get(getDir(PARAM_INDEXED_LIDO).toAbsolutePath().toString(), pi + ".xml");
        if (Files.isRegularFile(oldRecordFile)) {
            try {
                Files.delete(oldRecordFile);
                logger.info("Deleted old repository LIDO file: {}", oldRecordFile.toAbsolutePath());
            } catch (IOException e) {
                logger.error("Could not delete old repository LIDO file: {}", oldRecordFile.toAbsolutePath());
            }
        }
        // DENKXWEB
        oldRecordFile = Paths.get(getDir(PARAM_INDEXED_DENKXWEB).toAbsolutePath().toString(), pi + ".xml");
        if (Files.isRegularFile(oldRecordFile)) {
            try {
                Files.delete(oldRecordFile);
                logger.info("Deleted old repository DenkXweb file: {}", oldRecordFile.toAbsolutePath());
            } catch (IOException e) {
                logger.error("Could not delete old repository DenkXweb file: {}", oldRecordFile.toAbsolutePath());
            }
        }
    }

    /**
     * 
     * @param toRepository
     * @param pi
     * @param type
     * @should move data folder correctly
     */
    public void moveDataFolderToRepository(DataRepository toRepository, String pi, String type) {
        Path oldDataFolder = Paths.get(getDir(type).toAbsolutePath().toString(), pi);
        if (oldDataFolder.toFile().isDirectory()) {
            Path newDataDir = Paths.get(toRepository.getDir(type).toAbsolutePath().toString(), pi);
            try {
                int copied = Hotfolder.copyDirectory(oldDataFolder.toFile(), newDataDir.toFile());
                if (copied > 0) {
                    logger.info("Copied {} files to repository directory: {}", copied, newDataDir.toAbsolutePath());
                }
                if (Utils.deleteDirectory(oldDataFolder)) {
                    logger.info("Old repository directory deleted: {}", oldDataFolder.toString());
                } else {
                    logger.warn("Could not delete old repository directory: {}", oldDataFolder.toString());
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        } else {
            logger.warn("{} does not exist.", oldDataFolder.toAbsolutePath().toString());
        }
    }

    /**
     * 
     * @param dataFolders
     * @param reindexSettings
     */
    public static void deleteDataFolders(Map<String, Path> dataFolders, Map<String, Boolean> reindexSettings) {
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_ALTO);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_ALTOCROWD);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_FULLTEXT);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_FULLTEXTCROWD);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_CMDI);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_TEIMETADATA);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_TEIWC);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_ABBYY);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_MIX);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_UGC);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_CMS);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_PAGEPDF);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_SOURCE);

        // Delete unsupported data folders
        //                List<File> unknownDirs = Arrays.asList(hotfolderPath.listFiles(getDataFolderFilter(fileNameRoot + "_")));
        //                if (unknownDirs != null) {
        //                    for (File f : unknownDirs) {
        //                        if (!deleteDirectory(f)) {
        //                            logger.warn("'" + f.getAbsolutePath() + "' could not be deleted.");
        //                        }
        //                    }
        //                }
    }

    private static void deleteDataFolder(Map<String, Path> dataFolders, Map<String, Boolean> reindexSettings, String param) {
        if (param == null) {
            throw new IllegalArgumentException("param may notbe null");
        }

        if ((reindexSettings.get(param) == null || !reindexSettings.get(param)) && dataFolders.get(param) != null) {
            Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_CMS));
        }
    }

    /**
     * Copies data folders (all except for _media) from the hotfolder to their respective destination and deletes the folders from the hotfolder.
     * 
     * @param pi Record identifier
     * @param dataFolders Map with source data folders
     * @param reindexSettings Boolean map for data folders which are mapped for re-indexing (i.e. no new data folder in the hotfolder)
     * @param dataRepositories
     * @throws IOException
     * @throws FatalIndexerException
     */
    public void copyAndDeleteAllDataFolders(String pi, Map<String, Path> dataFolders, Map<String, Boolean> reindexSettings,
            List<DataRepository> dataRepositories) throws IOException, FatalIndexerException {
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
    }

    /**
     * Checks whether the folder with the given param name exists and is in the hotfolder, then proceeds to copy it to the destination folder and
     * delete the source.
     * 
     * @param pi Record identifier
     * @param dataFolders Map with source data folders
     * @param reindexSettings Boolean map for data folders which are mapped for re-indexing (i.e. no new data folder in the hotfolder)
     * @param param Folder parameter name
     * @param dataRepositories
     * @return
     * @throws IOException
     * @throws FatalIndexerException
     */
    public int checkCopyAndDeleteDataFolder(String pi, Map<String, Path> dataFolders, Map<String, Boolean> reindexSettings, String param,
            List<DataRepository> dataRepositories) throws IOException, FatalIndexerException {
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
                                misplacedDataDir.toAbsolutePath().toString());
                        logger.warn("Moving data folder to new repository...");
                        repo.moveDataFolderToRepository(this, pi, param);
                    }
                }
            }
        }

        return 0;
    }

    /**
     * 
     * @param srcFolder
     * @param paramName
     * @param identifier
     * @return Number of files copied.
     * @throws IOException
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

        logger.info("Copying {} files from '{}' to '{}'...", paramName, srcFolder, getDir(paramName).toAbsolutePath().toString());
        int counter = Hotfolder.copyDirectory(srcFolder.toFile(), new File(getDir(paramName).toFile(), identifier));
        logger.info("{} {} files copied.", counter, paramName);
        if (!Utils.deleteDirectory(srcFolder)) {
            logger.warn("'{}' could not be deleted.", srcFolder.toAbsolutePath());
        }

        return counter;
    }

    /**
     * 
     * @param dir
     * @return
     * @throws IOException
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
     * @param dataRepository
     * @return Absolute path to the given data repository name
     * @throws FatalIndexerException
     * @should return correct path
     */
    public static String getAbsolutePath(String dataRepository) throws FatalIndexerException {
        if (dataRepository != null && !Paths.get(dataRepository).isAbsolute()) {
            logger.warn("Data repository value type is deprecated: {}", dataRepository);
            dataRepository = Configuration.getInstance().getViewerHome() + "/data/" + dataRepository;
            dataRepository = dataRepository.replaceAll("//", "/");
            logger.warn("Assuming absolute path to be: {}", dataRepository);
        }

        return dataRepository;
    }

    /**
     * @param config
     * @param createFolders
     * @return List of properly configured data repositories
     * @throws FatalIndexerException
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
     * @return the valid
     */
    public boolean isValid() {
        return valid;
    }

    /**
     * @return the name
     */
    public String getPath() {
        return path;
    }

    /**
     * @return the rootDir
     */
    public Path getRootDir() {
        return rootDir;
    }

    /**
     * @return the buffer
     */
    public long getBuffer() {
        return buffer;
    }

    /**
     * @param buffer the buffer to set
     */
    public void setBuffer(long buffer) {
        this.buffer = buffer;
    }

    public Path getDir(String name) {
        return dirMap.get(name);
    }
}