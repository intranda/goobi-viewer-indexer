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
package io.goobi.viewer.indexer;

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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.JDOMException;
import org.jsoup.Jsoup;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

/**
 * <p>
 * DocUpdateIndexer class.
 * </p>
 *
 */
public class DocUpdateIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(DocUpdateIndexer.class);

    /** Constant <code>FILE_EXTENSION=".docupdate"</code> */
    public static final String FILE_EXTENSION = ".docupdate";

    /**
     * Constructor.
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public DocUpdateIndexer(Hotfolder hotfolder) {
        super();
        this.hotfolder = hotfolder;
    }

    /**
     * Updates the Solr document described by the given data file with content from data folders in the hotfolder.
     * 
     * @param dataFile {@link File}
     * @param fromReindexQueue
     * @param reindexSettings
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     * 
     */
    @Override
    public void addToIndex(Path dataFile, boolean fromReindexQueue, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        String fileNameRoot = FilenameUtils.getBaseName(dataFile.getFileName().toString());

        // Check data folders in the hotfolder
        Map<String, Path> dataFolders = checkDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

        if (dataFolders.isEmpty()) {
            logger.info("No data folders found for '{}', file won't be processed.", dataFile.getFileName());
            try {
                Files.delete(dataFile);
            } catch (IOException e) {
                logger.error(LOG_COULD_NOT_BE_DELETED, dataFile.toAbsolutePath());
            }
            return;
        }

        String[] resp = index(dataFile, dataFolders);

        if (StringUtils.isNotBlank(resp[0]) && resp[1] == null) {
            String pi = resp[0];

            dataRepository.copyAndDeleteAllDataFolders(pi, dataFolders, new HashMap<>(),
                    hotfolder.getDataRepositoryStrategy().getAllDataRepositories());

            // Delete unsupported data folders
            FileTools.deleteUnsupportedDataFolders(hotfolder.getHotfolderPath(), fileNameRoot);

            try {
                Files.delete(dataFile);
            } catch (IOException e) {
                logger.error(LOG_COULD_NOT_BE_DELETED, dataFile.toAbsolutePath());
            }

        } else {
            // Error
            logger.error(resp[1]);
            if (hotfolder.isDeleteContentFilesOnFailure()) {
                // Delete all data folders in hotfolder
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolder.getHotfolderPath(), new DirectoryStream.Filter<Path>() {

                    @Override
                    public boolean accept(Path entry) throws IOException {
                        return Files.isDirectory(entry)
                                && (entry.getFileName().toString().endsWith("_tif") || entry.getFileName().toString().endsWith(FOLDER_SUFFIX_MEDIA));
                    }
                });) {
                    for (Path path : stream) {
                        logger.info(LOG_FOUND_DATA_FOLDER, path.getFileName());
                        Utils.deleteDirectory(path);
                    }
                }

                if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) != null) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_ALTOCROWD));
                }
                if (dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) != null) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD));
                }
                if (dataFolders.get(DataRepository.PARAM_UGC) != null) {
                    Utils.deleteDirectory(dataFolders.get(DataRepository.PARAM_UGC));
                }
            }
            handleError(dataFile, resp[1], FileFormat.UNKNOWN);
            try {
                Files.delete(dataFile);
            } catch (IOException e) {
                logger.error(LOG_COULD_NOT_BE_DELETED, dataFile.toAbsolutePath());
            }
        }
    }

    /**
     * Updates the Solr document with the IDDOC contained in the data file name.
     *
     * @param dataFile a {@link java.nio.file.Path} object.
     * @param dataFolders a {@link java.util.Map} object.
     * @return an array of {@link java.lang.String} objects.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should update document correctly
     */
    @SuppressWarnings("unchecked")
    public String[] index(Path dataFile, Map<String, Path> dataFolders) throws FatalIndexerException {
        String[] ret = { STATUS_ERROR, null };
        String baseFileName = FilenameUtils.getBaseName(dataFile.getFileName().toString());
        String[] fileNameSplit = baseFileName.split("#");
        if (fileNameSplit.length < 2) {
            ret[1] = "Faulty data file name: '" + dataFile.getFileName().toString() + "'; cannot extract PI and IDDOC.";
            return ret;
        }

        String pi = fileNameSplit[0];
        Integer order = null;
        String iddoc = null;
        String anchorPi = null;
        Map<String, String> groupIds = new HashMap<>();
        String query = null;
        try {
            order = Integer.valueOf(fileNameSplit[1]);
            query = new StringBuilder().append('+')
                    .append(SolrConstants.PI_TOPSTRUCT)
                    .append(':')
                    .append(pi)
                    .append(" +")
                    .append(SolrConstants.ORDER)
                    .append(':')
                    .append(order)
                    .append(" +")
                    .append(SolrConstants.DOCTYPE)
                    .append(':')
                    .append(DocType.PAGE.name())
                    .toString();
        } catch (NumberFormatException e) {
            logger.warn("Could not parse page number '{}', attempting to use as IDDOC. Please update your Goobi viewer core.", fileNameSplit[1]);
            iddoc = fileNameSplit[1];
            query = new StringBuilder().append('+')
                    .append(SolrConstants.PI_TOPSTRUCT)
                    .append(':')
                    .append(pi)
                    .append(" +")
                    .append(SolrConstants.IDDOC)
                    .append(':')
                    .append(iddoc)
                    .toString();
        }
        dataRepository = hotfolder.getDataRepositoryStrategy()
                .selectDataRepository(pi, null, null, SolrIndexerDaemon.getInstance().getSearchIndex(),
                        SolrIndexerDaemon.getInstance().getOldSearchIndex())[0];

        try {
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search(query, null);
            if (docList == null || docList.isEmpty()) {
                if (order != null) {
                    ret[1] = "Page not found in index: " + order;
                } else {
                    ret[1] = "IDDOC not found in index: " + iddoc;
                }
                return ret;
            }
            SolrDocument doc = docList.get(0);
            iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            order = (int) doc.getFieldValue(SolrConstants.ORDER);
            anchorPi = (String) doc.getFieldValue(SolrConstants.PI_ANCHOR);
            for (String fieldName : doc.getFieldNames()) {
                if (fieldName.startsWith(SolrConstants.PREFIX_GROUPID)) {
                    groupIds.put(fieldName, (String) doc.getFieldValue(fieldName));
                }
            }
            String pageFileName = doc.containsKey(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED)
                    ? (String) doc.getFieldValue(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED)
                    : (String) doc.getFieldValue(SolrConstants.FILENAME);
            if (pageFileName == null) {
                ret[1] = "Document " + iddoc + " contains no " + SolrConstants.FILENAME + " field, please checks the index.";
                return ret;
            }
            String pageFileBaseName = FilenameUtils.getBaseName(pageFileName);
            logger.info("Updating doc {} ({}, page {})...", iddoc, pi, doc.getFieldValue(SolrConstants.ORDER));

            //  Find files in data folders and populate update map
            Map<String, Map<String, Object>> partialUpdates = new HashMap<>();

            if (dataFolders.get(DataRepository.PARAM_CMS) != null) {
                Path staticPageFolder = dataFolders.get(DataRepository.PARAM_CMS);
                if (Files.isDirectory(staticPageFolder)) {
                    File[] files = staticPageFolder.toFile().listFiles(FileTools.FILENAME_FILTER_XML);
                    if (files.length > 0) {
                        for (File file : files) {
                            String field = FilenameUtils.getBaseName(file.getName()).toUpperCase();
                            String content = FileTools.readFileToString(file, null);
                            Map<String, Object> update = new HashMap<>();
                            update.put("set", TextHelper.cleanUpHtmlTags(content));
                            partialUpdates.put(SolrConstants.PREFIX_CMS_TEXT + field, update);
                            partialUpdates.put(SolrConstants.CMS_TEXT_ALL, update);

                        }
                    }
                }
            }

            // Crowdsourcing ALTO
            if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) != null) {
                List<Object> values = getFileContents(dataFolders.get(DataRepository.PARAM_ALTOCROWD));
                if (!values.isEmpty()) {
                    Object o = values.get(0);
                    if (o instanceof Map<?, ?>) {
                        Map<String, Object> altoData = (Map<String, Object>) o;
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO))) {
                            // Write ALTO file
                            Path repositoryPath = dataRepository.getRootDir();

                            // Update FILENAME_ALTO, if it doesn't exist yet
                            String altoFileName = (String) doc.getFieldValue(SolrConstants.FILENAME);
                            if (altoFileName != null && !partialUpdates.containsKey(SolrConstants.FILENAME_ALTO)) {
                                altoFileName =
                                        new StringBuilder().append(dataRepository.getDir(DataRepository.PARAM_ALTOCROWD).getFileName().toString())
                                                .append('/')
                                                .append(pi)
                                                .append('/')
                                                .append(FilenameUtils.getBaseName(altoFileName))
                                                .append(FileTools.XML_EXTENSION)
                                                .toString();
                                Map<String, Object> update = new HashMap<>();
                                update.put("set", altoFileName);
                                partialUpdates.put(SolrConstants.FILENAME_ALTO, update);
                            }

                            Path altoFile = Paths.get(repositoryPath.toAbsolutePath().toString(), altoFileName);
                            Utils.checkAndCreateDirectory(altoFile.getParent());
                            FileUtils.writeStringToFile(altoFile.toFile(), (String) altoData.get(SolrConstants.ALTO), TextHelper.DEFAULT_CHARSET);
                        }
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT))) {
                            String fulltext = ((String) altoData.get(SolrConstants.FULLTEXT)).trim();
                            Map<String, Object> update = new HashMap<>();
                            update.put("set", Jsoup.parse(fulltext).text());
                            partialUpdates.put(SolrConstants.FULLTEXT, update);
                        }

                    }
                }
            }
            // Crowdsourcing FULLTEXT
            if (dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) != null) {
                List<Object> values = getFileContents(dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD));
                if (!values.isEmpty()) {
                    Object o = values.get(0);
                    if (o instanceof String s) {
                        String fulltext = s.trim();
                        if (!partialUpdates.containsKey(SolrConstants.FULLTEXT)) {
                            Map<String, Object> update = new HashMap<>();
                            update.put("set", Jsoup.parse(fulltext).text());
                            partialUpdates.put(SolrConstants.FULLTEXT, update);
                        }
                        // Write text file
                        Path repositoryPath = dataRepository.getRootDir();
                        String fulltextFileName = (String) doc.getFieldValue(SolrConstants.FILENAME_FULLTEXT);
                        if (fulltextFileName == null) {
                            // Add FILENAME_FULLTEXT, if it doesn't exist yet
                            fulltextFileName = (String) doc.getFieldValue(SolrConstants.FILENAME);
                            if (fulltextFileName != null && !partialUpdates.containsKey(SolrConstants.FILENAME_FULLTEXT)) {
                                fulltextFileName =
                                        new StringBuilder().append(dataRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD).getFileName().toString())
                                                .append('/')
                                                .append(pi)
                                                .append('/')
                                                .append(FilenameUtils.getBaseName(fulltextFileName))
                                                .append(FileTools.TXT_EXTENSION)
                                                .toString();
                                Map<String, Object> update = new HashMap<>();
                                update.put("set", fulltextFileName);
                                partialUpdates.put(SolrConstants.FILENAME_FULLTEXT, update);
                            }
                        }
                        Path fulltextFile = Paths.get(repositoryPath.toAbsolutePath().toString(), fulltextFileName);
                        Utils.checkAndCreateDirectory(fulltextFile.getParent());
                        FileUtils.writeStringToFile(fulltextFile.toFile(), fulltext, TextHelper.DEFAULT_CHARSET);
                    }
                }
            }

            // Remove old UGC
            query = SolrConstants.DOCTYPE + ":UGC AND " + SolrConstants.PI_TOPSTRUCT + ":" + pi + SolrConstants.SOLR_QUERY_AND + SolrConstants.ORDER
                    + ":" + order;
            SolrDocumentList ugcDocList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(SolrConstants.DOCTYPE + ":UGC AND " + SolrConstants.PI_TOPSTRUCT + ":" + pi + SolrConstants.SOLR_QUERY_AND
                            + SolrConstants.ORDER + ":" + order,
                            Collections.singletonList(SolrConstants.IDDOC));
            if (ugcDocList != null && !ugcDocList.isEmpty()) {
                // Collect delete old UGC docs for deletion
                List<String> oldIddocs = new ArrayList<>(ugcDocList.size());
                for (SolrDocument oldDoc : ugcDocList) {
                    oldIddocs.add((String) oldDoc.getFieldValue(SolrConstants.IDDOC));
                }
                if (!oldIddocs.isEmpty()) {
                    SolrIndexerDaemon.getInstance().getSearchIndex().deleteDocuments(oldIddocs);
                } else {
                    logger.error("No IDDOC values found in docs matching query: {}", query);
                }
            }

            if (dataFolders.get(DataRepository.PARAM_UGC) != null) {
                // Create a dummy input doc with relevant field values from the page doc
                SolrInputDocument dummyDoc = new SolrInputDocument();
                dummyDoc.setField(SolrConstants.IDDOC_OWNER, doc.getFieldValue(SolrConstants.IDDOC_OWNER));
                dummyDoc.setField(SolrConstants.DOCSTRCT_TOP, doc.getFieldValue(SolrConstants.DOCSTRCT_TOP));

                // Add new UGC docs
                List<SolrInputDocument> newUgcDocList =
                        generateUserGeneratedContentDocsForPage(dummyDoc, dataFolders.get(DataRepository.PARAM_UGC),
                                pi, anchorPi, groupIds, order, pageFileBaseName);
                if (!newUgcDocList.isEmpty()) {
                    SolrIndexerDaemon.getInstance().getSearchIndex().writeToIndex(newUgcDocList);
                } else {
                    logger.warn("No user generated content values found for page {}.", order);
                }

                // Add comments
                List<SolrInputDocument> newCommentDocList = generateUserCommentDocsForPage(dummyDoc, dataFolders.get(DataRepository.PARAM_UGC),
                        pi, anchorPi, groupIds, order);
                if (!newCommentDocList.isEmpty()) {
                    SolrIndexerDaemon.getInstance().getSearchIndex().writeToIndex(newCommentDocList);
                } else {
                    logger.warn("No user comments found for page {}.", order);
                }

            }

            if (!partialUpdates.isEmpty()) {
                // Update doc (only if partialUpdates is not empty, otherwise all fields but IDDOC will be deleted!)
                SolrIndexerDaemon.getInstance().getSearchIndex().updateDoc(doc, partialUpdates);
            } else {
                // Otherwise just commit the new UGC docs
                SolrIndexerDaemon.getInstance().getSearchIndex().commit(false);
            }

            ret[0] = pi;
            logger.info("Successfully finished updating IDDOC={}", iddoc);
        } catch (IOException | SolrServerException e) {
            logger.error("Indexing of IDDOC={} could not be finished due to an error.", iddoc);
            logger.error(e.getMessage(), e);
            ret[0] = STATUS_ERROR;
            ret[1] = e.getMessage();
            SolrIndexerDaemon.getInstance().getSearchIndex().rollback();
        }

        return ret;
    }

    /**
     * 
     * @param folder
     * @return List<Object>
     */
    static List<Object> getFileContents(Path folder) {
        if (folder == null) {
            throw new IllegalArgumentException("folder may not be null");
        }

        List<Object> ret = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(folder)) {
            for (Path path : stream) {
                Path recordFile = path;
                logger.info("Found file: {}/{}", recordFile.getParent().getFileName(), recordFile.getFileName());
                Object o = readTextFile(recordFile);
                if (o != null) {
                    ret.add(o);
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return ret;
    }

    /**
     * 
     * @param recordFile
     * @return
     * @throws IOException
     */
    static Object readTextFile(Path recordFile) throws IOException {
        try {
            if (recordFile.getFileName().toString().endsWith(".xml")) {
                Map<String, Object> altoData = TextHelper.readAltoFile(recordFile.toFile());
                if (altoData != null) {
                    return altoData;
                }
            } else if (recordFile.getFileName().toString().endsWith(".txt")) {
                return FileTools.readFileToString(recordFile.toFile(), null);
            } else {
                logger.warn("Incompatible data file found: {}", recordFile.toAbsolutePath());
            }
        } catch (JDOMException e) {
            if (!e.getMessage().contains("Premature end of file")) {
                logger.warn("Could not read ALTO file '{}': {}", recordFile.getFileName(), e.getMessage());
            }
        }

        return null;
    }
}
