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
package de.intranda.digiverso.presentation.solr;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.JDOMException;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.helper.Hotfolder;
import de.intranda.digiverso.presentation.solr.helper.TextHelper;
import de.intranda.digiverso.presentation.solr.model.DataRepository;
import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;
import de.intranda.digiverso.presentation.solr.model.SolrConstants;

public class DocUpdateIndexer extends AbstractIndexer {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(DocUpdateIndexer.class);

    public static String FILE_EXTENSION = ".docupdate";

    /**
     * Constructor.
     * 
     * @param hotfolder
     * @should set attributes correctly
     */
    public DocUpdateIndexer(Hotfolder hotfolder) {
        this.hotfolder = hotfolder;
    }

    /**
     * Updates the Solr document with the IDDOC contained in the data file name.
     * 
     * @param dataFile
     * @param dataFolders
     * @param mixFolder
     * @return
     * @throws FatalIndexerException
     * @should update document correctly
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
        String iddoc = fileNameSplit[1];
        hotfolder.selectDataRepository(null, pi);

        try {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.IDDOC + ":" + iddoc, null);
            if (docList == null || docList.isEmpty()) {
                ret[1] = "IDDOC not found in index: " + iddoc;
                return ret;
            }
            SolrDocument doc = docList.get(0);
            int order = (int) doc.getFieldValue(SolrConstants.ORDER);
            String pageFileName = doc.containsKey(SolrConstants.FILENAME + "_HTML-SANDBOXED") ? (String) doc.getFieldValue(SolrConstants.FILENAME
                    + "_HTML-SANDBOXED") : (String) doc.getFieldValue(SolrConstants.FILENAME);
            String pageFileBaseName = FilenameUtils.getBaseName(pageFileName);
            logger.info("Updating doc {} ({}, page {})...", iddoc, pi, doc.getFieldValue(SolrConstants.ORDER));

            //  Find files in data folders and populate update map
            Map<String, Map<String, Object>> partialUpdates = new HashMap<>();

            // Crowdsourcing ALTO
            if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) != null) {
                List<Object> values = getFileContents(dataFolders.get(DataRepository.PARAM_ALTOCROWD));
                if (!values.isEmpty()) {
                    Object o = values.get(0);
                    if (o instanceof Map<?, ?>) {
                        Map<String, Object> altoData = (Map<String, Object>) o;
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO))) {
                            Map<String, Object> update = new HashMap<>();
                            update.put("set", altoData.get(SolrConstants.ALTO));
                            partialUpdates.put(SolrConstants.ALTO, update);
                        }
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT))) {
                            String fulltext = ((String) altoData.get(SolrConstants.FULLTEXT)).trim();
                            {
                                Map<String, Object> update = new HashMap<>();
                                update.put("set", Jsoup.parse(fulltext).text());
                                partialUpdates.put(SolrConstants.FULLTEXT, update);
                            }
                            {
                                // Map<String, Object> update = new HashMap<>();
                                // update.put("set", fulltext);
                                // partialUpdates.put("MD_FULLTEXT", update);
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
                        //                        if (!partialUpdates.containsKey("MD_FULLTEXT")) {
                        //                            Map<String, Object> update = new HashMap<>();
                        //                            update.put("set", fulltext);
                        //                            partialUpdates.put("MD_FULLTEXT", update);
                        //                        }
                    }
                }
            }
            // UGC
            if (dataFolders.get(DataRepository.PARAM_UGC) != null) {
                SolrInputDocument dummyDoc = new SolrInputDocument();
                generateUserGeneratedContentDocsForPage(dummyDoc, dataFolders.get(DataRepository.PARAM_UGC), pi, order, pageFileBaseName);
                Map<String, Object> update = new HashMap<>();
                update.put("set", dummyDoc.getFieldValue(SolrConstants.UGCTERMS));
                partialUpdates.put(SolrConstants.UGCTERMS, update);
            }

            // Update doc
            if (!hotfolder.getSolrHelper().updateDoc(doc, partialUpdates)) {
                ret[1] = "Could not update document for IDDOC=" + iddoc;
                return ret;
            }

            ret[0] = pi;
            logger.info("Successfully finished updating IDDOC={}", iddoc);
        } catch (FatalIndexerException | SolrServerException e) {
            logger.error("Indexing of IDDOC={} could not be finished due to an error.", iddoc);
            logger.error(e.getMessage(), e);
            ret[0] = "ERROR";
            ret[1] = e.getMessage();
            hotfolder.getSolrHelper().rollback();
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
                try {
                    Path recordFile = path;
                    logger.info("Found file: {}/{}", recordFile.getParent().getFileName(), recordFile.getFileName());
                    if (recordFile.getFileName().toString().endsWith(".xml")) {
                        Map<String, Object> altoData = TextHelper.readAltoFile(recordFile.toFile());
                        if (altoData != null) {
                            ret.add(altoData);
                        }
                    } else if (recordFile.getFileName().toString().endsWith(".txt")) {
                        String value = TextHelper.readFileToString(path.toFile());
                        ret.add(value);
                    } else {
                        logger.warn("Incompatible data file found: {}", recordFile.toAbsolutePath());
                    }
                } catch (JDOMException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return ret;
    }
}
