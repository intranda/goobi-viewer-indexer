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
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.JDOMException;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.FatalIndexerException;
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
    private static final Logger logger = LoggerFactory.getLogger(DocUpdateIndexer.class);

    /** Constant <code>FILE_EXTENSION=".docupdate"</code> */
    public static String FILE_EXTENSION = ".docupdate";

    /**
     * Constructor.
     *
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public DocUpdateIndexer(Hotfolder hotfolder) {
        this.hotfolder = hotfolder;
    }

    /**
     * Updates the Solr document with the IDDOC contained in the data file name.
     *
     * @param dataFile a {@link java.nio.file.Path} object.
     * @param dataFolders a {@link java.util.Map} object.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should update document correctly
     * @return an array of {@link java.lang.String} objects.
     */

    @SuppressWarnings("unchecked")
    public String[] index(Path dataFile, Map<String, Path> dataFolders) throws FatalIndexerException {
        String[] ret = { "ERROR", null };
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
                .selectDataRepository(pi, null, null, hotfolder.getSearchIndex(), hotfolder.getOldSearchIndex())[0];

        try {
            SolrDocumentList docList = hotfolder.getSearchIndex().search(query, null);
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
                if (fieldName.startsWith(SolrConstants.GROUPID_)) {
                    groupIds.put(fieldName, (String) doc.getFieldValue(fieldName));
                }
            }
            String pageFileName = doc.containsKey(SolrConstants.FILENAME + "_HTML-SANDBOXED")
                    ? (String) doc.getFieldValue(SolrConstants.FILENAME + "_HTML-SANDBOXED") : (String) doc.getFieldValue(SolrConstants.FILENAME);
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
                    File[] files = staticPageFolder.toFile().listFiles(xml);
                    if (files.length > 0) {
                        for (File file : files) {
                            String field = FilenameUtils.getBaseName(file.getName()).toUpperCase();
                            String content = FileTools.readFileToString(file, null);
                            Map<String, Object> update = new HashMap<>();
                            update.put("set", TextHelper.cleanUpHtmlTags(content));
                            partialUpdates.put(SolrConstants.CMS_TEXT_ + field, update);
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
                                                .append(XML_EXTENSION)
                                                .toString();
                                Map<String, Object> update = new HashMap<>();
                                update.put("set", altoFileName);
                                partialUpdates.put(SolrConstants.FILENAME_ALTO, update);
                            } else {
                                throw new RuntimeException(altoFileName);
                            }

                            Path altoFile = Paths.get(repositoryPath.toAbsolutePath().toString(), altoFileName);
                            Utils.checkAndCreateDirectory(altoFile.getParent());
                            FileUtils.writeStringToFile(altoFile.toFile(), (String) altoData.get(SolrConstants.ALTO), "UTF-8");
                        }
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT))) {
                            String fulltext = ((String) altoData.get(SolrConstants.FULLTEXT)).trim();
                            {
                                Map<String, Object> update = new HashMap<>();
                                update.put("set", Jsoup.parse(fulltext).text());
                                partialUpdates.put(SolrConstants.FULLTEXT, update);
                            }
                        }

                    }
                }
            }
            // Crowdsourcing FULLTEXT
            if (dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) != null) {
                List<Object> values = getFileContents(dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD));
                if (!values.isEmpty()) {
                    Object o = values.get(0);
                    if (o instanceof String) {
                        String fulltext = ((String) o).trim();
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
                                                .append(TXT_EXTENSION)
                                                .toString();
                                Map<String, Object> update = new HashMap<>();
                                update.put("set", fulltextFileName);
                                partialUpdates.put(SolrConstants.FILENAME_FULLTEXT, update);
                            }
                        }
                        Path fulltextFile = Paths.get(repositoryPath.toAbsolutePath().toString(), fulltextFileName);
                        Utils.checkAndCreateDirectory(fulltextFile.getParent());
                        FileUtils.writeStringToFile(fulltextFile.toFile(), fulltext, "UTF-8");
                    }
                }
            }

            // Remove old UGC
            query = SolrConstants.DOCTYPE + ":UGC AND " + SolrConstants.PI_TOPSTRUCT + ":" + pi + " AND " + SolrConstants.ORDER + ":" + order;
            SolrDocumentList ugcDocList = hotfolder.getSearchIndex()
                    .search(SolrConstants.DOCTYPE + ":UGC AND " + SolrConstants.PI_TOPSTRUCT + ":" + pi + " AND " + SolrConstants.ORDER + ":" + order,
                            Collections.singletonList(SolrConstants.IDDOC));
            if (ugcDocList != null && !ugcDocList.isEmpty()) {
                // Collect delete old UGC docs for deletion
                List<String> oldIddocs = new ArrayList<>(ugcDocList.size());
                for (SolrDocument oldDoc : ugcDocList) {
                    oldIddocs.add((String) oldDoc.getFieldValue(SolrConstants.IDDOC));
                }
                if (!oldIddocs.isEmpty()) {
                    hotfolder.getSearchIndex().deleteDocuments(oldIddocs);
                } else {
                    logger.error("No IDDOC values found in docs matching query: {}", query);
                }
            }
            // Add new UGC docs
            if (dataFolders.get(DataRepository.PARAM_UGC) != null) {
                SolrInputDocument dummyDoc = new SolrInputDocument();
                List<SolrInputDocument> newUgcDocList = generateUserGeneratedContentDocsForPage(dummyDoc, dataFolders.get(DataRepository.PARAM_UGC),
                        pi, anchorPi, groupIds, order, pageFileBaseName);
                if (!newUgcDocList.isEmpty()) {
                    hotfolder.getSearchIndex().writeToIndex(newUgcDocList);
                } else {
                    logger.warn("No user generated content values found for page {}.", order);
                }

                //                SolrInputDocument dummyDoc = new SolrInputDocument();
                //                generateUserGeneratedContentDocsForPage(dummyDoc, dataFolders.get(DataRepository.PARAM_UGC), pi, order, pageFileBaseName);
                //                Map<String, Object> update = new HashMap<>();
                //                update.put("set", dummyDoc.getFieldValue(SolrConstants.UGCTERMS));
                //                partialUpdates.put(SolrConstants.UGCTERMS, update);
            }

            if (!partialUpdates.isEmpty()) {
                // Update doc (only if partialUpdates is not empty, otherwise all fields but IDDOC will be deleted!)
                if (!hotfolder.getSearchIndex().updateDoc(doc, partialUpdates)) {
                    ret[1] = "Could not update document for IDDOC=" + iddoc;
                    return ret;
                }
            } else {
                // Otherwise just commit the new UGC docs
                hotfolder.getSearchIndex().commit(false);
            }

            ret[0] = pi;
            logger.info("Successfully finished updating IDDOC={}", iddoc);
        } catch (IOException | SolrServerException e) {
            logger.error("Indexing of IDDOC={} could not be finished due to an error.", iddoc);
            logger.error(e.getMessage(), e);
            ret[0] = "ERROR";
            ret[1] = e.getMessage();
            hotfolder.getSearchIndex().rollback();
        }

        return ret;
    }

    /**
     * 
     * @param folder
     * @return
     */
    static List<Object> getFileContents(Path folder) {
        if (folder == null) {
            throw new IllegalArgumentException("folder may not be null");
        }

        List<Object> ret = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(folder)) {
            for (Path path : stream) {
                Path recordFile = path;
                try {
                    logger.info("Found file: {}/{}", recordFile.getParent().getFileName(), recordFile.getFileName());
                    if (recordFile.getFileName().toString().endsWith(".xml")) {
                        Map<String, Object> altoData = TextHelper.readAltoFile(recordFile.toFile());
                        if (altoData != null) {
                            ret.add(altoData);
                        }
                    } else if (recordFile.getFileName().toString().endsWith(".txt")) {
                        String value = FileTools.readFileToString(path.toFile(), null);
                        ret.add(value);
                    } else {
                        logger.warn("Incompatible data file found: {}", recordFile.toAbsolutePath());
                    }
                } catch (JDOMException e) {
                    if (!e.getMessage().contains("Premature end of file")) {
                        logger.warn("Could not read ALTO file '{}': {}", recordFile.getFileName().toString(), e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return ret;
    }
}
