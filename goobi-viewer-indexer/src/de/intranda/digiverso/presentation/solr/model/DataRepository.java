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
package de.intranda.digiverso.presentation.solr.model;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.helper.Hotfolder;
import de.intranda.digiverso.presentation.solr.helper.Configuration;
import de.intranda.digiverso.presentation.solr.helper.Utils;

public class DataRepository {

    private static final Logger logger = LoggerFactory.getLogger(DataRepository.class);

    public static final String PARAM_INDEXED_METS = "indexedMets";
    public static final String PARAM_INDEXED_LIDO = "indexedLido";
    public static final String PARAM_MEDIA = "mediaFolder";
    public static final String PARAM_ALTO = "altoFolder";
    public static final String PARAM_ALTOCROWD = "altoCrowdsourcingFolder";
    public static final String PARAM_ABBYY = "abbyyFolder";
    public static final String PARAM_DOWNLOAD_IMAGES_TRIGGER = "downloadImages";
    public static final String PARAM_FULLTEXT = "fulltextFolder";
    public static final String PARAM_FULLTEXTCROWD = "fulltextCrowdsourcingFolder";
    public static final String PARAM_TEIMETADATA = "teiFolder";
    public static final String PARAM_TEIWC = "wcFolder";
    public static final String PARAM_PAGEPDF = "pagePdfFolder";
    public static final String PARAM_SOURCE = "sourceContentFolder";
    public static final String PARAM_UGC = "userGeneratedContentFolder";
    public static final String PARAM_MIX = "mixFolder";
    public static final String PARAM_OVERVIEW = "overviewFolder";

    public static int dataRepositoriesMaxRecords = 10000;

    private boolean valid = false;
    private String name;
    private Path rootDir;
    private Map<String, Path> dirMap = new HashMap<>();

    /**
     * 
     * @param repositoriesRootDir
     * @param name
     * @throws FatalIndexerException
     * @should create dummy repository correctly
     * @should create real repository correctly
     */
    public DataRepository(Path repositoriesRootDir, String name) throws FatalIndexerException {
        this.name = name;
        //        rootDir = new File(repositoriesRootDir, name);
        rootDir = Paths.get(repositoriesRootDir.toAbsolutePath().toString(), name);
        if (Files.exists(rootDir)) {
            if (Files.isRegularFile(rootDir)) {
                logger.error("Data repository '{}' is defined but is a file, not a directory. This repository will not be used.", rootDir
                        .getFileName());
                return;
            }
        } else {
            try {
                Files.createDirectories(rootDir);
                logger.info("Repository directory '{}' created.", rootDir.toAbsolutePath());
            } catch (IOException e) {
                logger.error("Repository directory '{}' could not be created.", rootDir.toAbsolutePath());
                return;
            }
        }
        checkAndCreateDataSubdir(PARAM_INDEXED_METS);
        checkAndCreateDataSubdir(PARAM_INDEXED_LIDO);
        checkAndCreateDataSubdir(PARAM_MEDIA);
        checkAndCreateDataSubdir(PARAM_ALTO);
        checkAndCreateDataSubdir(PARAM_ALTOCROWD);
        checkAndCreateDataSubdir(PARAM_FULLTEXT);
        checkAndCreateDataSubdir(PARAM_FULLTEXTCROWD);
        checkAndCreateDataSubdir(PARAM_TEIMETADATA);
        checkAndCreateDataSubdir(PARAM_TEIWC);
        checkAndCreateDataSubdir(PARAM_ABBYY);
        checkAndCreateDataSubdir(PARAM_PAGEPDF);
        checkAndCreateDataSubdir(PARAM_SOURCE);
        checkAndCreateDataSubdir(PARAM_UGC);
        checkAndCreateDataSubdir(PARAM_MIX);
        checkAndCreateDataSubdir(PARAM_OVERVIEW);
        valid = true;
    }

    private void checkAndCreateDataSubdir(String name) throws FatalIndexerException {
        String config = Configuration.getInstance().getConfiguration(name);
        if (StringUtils.isEmpty(config)) {
            throw new FatalIndexerException("No configuration found for '" + name + "', exiting...");
        }
        //        File dataSubdir = new File(rootDir, config);
        Path dataSubdir = Paths.get(rootDir.toAbsolutePath().toString(), config);
        if (!Files.isDirectory(dataSubdir)) {
            try {
                Files.createDirectory(dataSubdir);
                logger.info("Created directory: {}", dataSubdir.toAbsolutePath());
            } catch (IOException e) {
                logger.error("Could not create directory: {}", dataSubdir.toAbsolutePath());
                return;
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
     * @should delete TEI folder correctly
     * @should delete word coords folder correctly
     * @should delete ABBYY folder correctly
     * @should delete media folder correctly
     * @should delete source folder correctly
     * @should delete user generated content folder correctly
     * @should delete MIX folder correctly
     * @should delete page PDF folder correctly
     * @should delete overview folder correctly
     */
    public void deleteDataFoldersForRecord(String baseFileName) {
        deleteFolder(Paths.get(getDir(PARAM_ALTO).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_ALTOCROWD).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_FULLTEXT).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_FULLTEXTCROWD).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_TEIMETADATA).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_TEIWC).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_ABBYY).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_MEDIA).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_SOURCE).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_UGC).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_MIX).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_PAGEPDF).toAbsolutePath().toString(), baseFileName));
        deleteFolder(Paths.get(getDir(PARAM_OVERVIEW).toAbsolutePath().toString(), baseFileName));
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
        Path metsDir = getDir(PARAM_INDEXED_METS);
        Path lidoDir = getDir(PARAM_INDEXED_LIDO);
        int metsRecords = countFiles(metsDir);
        logger.info("Data repository '{}' contains {} METS records.", name, metsRecords);
        int lidoRecords = countFiles(lidoDir);
        logger.info("Data repository '{}' contains {} LIDO records.", name, lidoRecords);

        return metsRecords + lidoRecords;
    }

    /**
     * Migrates data folders from this repository to the given repository.
     * 
     * @param toRepository
     * @param pi
     */
    public void moveDataFoldersToRepository(DataRepository toRepository, String pi) {
        moveDataFolderToRepository(toRepository, pi, PARAM_MEDIA);
        moveDataFolderToRepository(toRepository, pi, PARAM_ALTO);
        moveDataFolderToRepository(toRepository, pi, PARAM_ALTOCROWD);
        moveDataFolderToRepository(toRepository, pi, PARAM_ABBYY);
        moveDataFolderToRepository(toRepository, pi, PARAM_FULLTEXT);
        moveDataFolderToRepository(toRepository, pi, PARAM_FULLTEXTCROWD);
        moveDataFolderToRepository(toRepository, pi, PARAM_TEIMETADATA);
        moveDataFolderToRepository(toRepository, pi, PARAM_TEIWC);
        moveDataFolderToRepository(toRepository, pi, PARAM_PAGEPDF);
        moveDataFolderToRepository(toRepository, pi, PARAM_SOURCE);
        moveDataFolderToRepository(toRepository, pi, PARAM_UGC);
        moveDataFolderToRepository(toRepository, pi, PARAM_MIX);
        moveDataFolderToRepository(toRepository, pi, PARAM_OVERVIEW);

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
    }

    /**
     * 
     * @param toRepository
     * @param pi
     * @param type
     * @should move data folder correctly
     */
    protected void moveDataFolderToRepository(DataRepository toRepository, String pi, String type) {
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
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_TEIMETADATA);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_TEIWC);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_ABBYY);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_MIX);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_UGC);
        deleteDataFolder(dataFolders, reindexSettings, DataRepository.PARAM_OVERVIEW);
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
            Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_OVERVIEW));
        }
    }

    /**
     * Copies data folders (all except for _media) from the hotfolder to their respective destination and deletes the folders from the hotfolder.
     * 
     * @param pi Record identifier
     * @param dataFolders Map with source data folders
     * @param reindexSettings Boolean map for data folders which are mapped for re-indexing (i.e. no new data folder in the hotfolder)
     * @throws IOException
     */
    public void copyAndDeleteAllDataFolders(String pi, Map<String, Path> dataFolders, Map<String, Boolean> reindexSettings) throws IOException {
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null");
        }
        if (reindexSettings == null) {
            throw new IllegalArgumentException("reindexSettings may not be null");
        }

        // Copy and delete ALTO folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_ALTO);
        // Copy and delete crowdsourcing ALTO folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_ALTOCROWD);
        // Copy and delete fulltext folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_FULLTEXT);
        // Copy and delete crowdsourcing fulltext folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_FULLTEXTCROWD);
        // Copy and delete TEI metadata folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_TEIMETADATA);
        // Copy and delete TEI word coordinates folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_TEIWC);
        // Copy and delete ABBYY word coordinates folder
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_ABBYY);
        // Copy and delete MIX files
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_MIX);
        // Copy and delete page PDF files
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_PAGEPDF);
        // Copy and delete original content files
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_SOURCE);
        // Copy and delete user generated content files
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_UGC);
        // Copy and delete overview page text files
        checkCopyAndDeleteDataFolder(pi, dataFolders, reindexSettings, DataRepository.PARAM_OVERVIEW);
    }

    /**
     * Checks whether the folder with the given param name exists and is in the hotfolder, then proceeds to copy it to the destination folder and
     * delete the source.
     * 
     * @param pi Record identifier
     * @param dataFolders Map with source data folders
     * @param reindexSettings Boolean map for data folders which are mapped for re-indexing (i.e. no new data folder in the hotfolder)
     * @param param Folder parameter name
     * @return
     * @throws IOException
     */
    public int checkCopyAndDeleteDataFolder(String pi, Map<String, Path> dataFolders, Map<String, Boolean> reindexSettings, String param)
            throws IOException {
        if (param == null) {
            throw new IllegalArgumentException("param may not be null");
        }

        if ((reindexSettings.get(param) == null || !reindexSettings.get(param)) && dataFolders.get(param) != null) {
            return copyAndDeleteDataFolder(dataFolders.get(param), param, pi);
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

        logger.info("Copying {} files from '{}'...", paramName, srcFolder);
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
        int c = 0;
        if (Files.isDirectory(dir)) {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
                for (Path file : files) {
                    if (Files.isRegularFile(file) || Files.isSymbolicLink(file)) {
                        c++;
                    }
                }
            }
        }

        return c;
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
    public String getName() {
        return name;
    }

    /**
     * @return the rootDir
     */
    public Path getRootDir() {
        return rootDir;
    }

    public Path getDir(String name) {
        return dirMap.get(name);
    }
}
