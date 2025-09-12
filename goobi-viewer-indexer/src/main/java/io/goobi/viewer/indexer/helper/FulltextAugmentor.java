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
package io.goobi.viewer.indexer.helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.JDOMException;

import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

/**
 * Augments a solr document with data from fulltext-files (alto and plaintxt)
 */
public class FulltextAugmentor {

    private static final Logger logger = LogManager.getLogger(FulltextAugmentor.class);

    private static final String LOG_ADDED_FULLTEXT_FROM_REGULAR_ALTO = "Added FULLTEXT from regular ALTO for page {}";

    private final DataRepository dataRepository;

    /**
     * Default constructor
     * 
     * @param dataRepository the data repository to use
     */
    public FulltextAugmentor(DataRepository dataRepository) {
        this.dataRepository = dataRepository;
    }

    /**
     * Add fulltext data to given document
     * 
     * @param doc Page Solr input document
     * @param dataFolders Folder paths containing full-text files
     * @param pi Record identifier
     * @param order Page number
     * @param altoURL Optional URL for ALTO download
     * @return true if any fulltext data was found, false otherwise
     */
    public boolean addFullTextToPageDoc(SolrInputDocument doc, Map<String, Path> dataFolders, String pi, int order,
            String altoURL) {
        if (doc == null || dataFolders == null) {
            return false;
        }

        Map<String, Object> altoData = null;
        String baseFileName = FilenameUtils.getBaseName((String) doc.getFieldValue(SolrConstants.FILENAME));
        // If main image file is a IIIF URL or anything with no unique file name, look for alternatives
        if (!isBaseFileNameUsable(baseFileName)) {
            if (doc.getFieldValue("FILENAME_JPEG") != null) {
                baseFileName = FilenameUtils.getBaseName((String) doc.getFieldValue("FILENAME_JPEG"));
            } else if (doc.getFieldValue("FILENAME_TIFF") != null) {
                baseFileName = FilenameUtils.getBaseName((String) doc.getFieldValue("FILENAME_TIFF"));
            }
        }

        // Add complete crowdsourcing ALTO document and full-text generated from ALTO, if available
        boolean foundCrowdsourcingData = false;
        boolean altoWritten = false;
        if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) != null) {
            File altoFile =
                    new File(dataFolders.get(DataRepository.PARAM_ALTOCROWD).toAbsolutePath().toString(), baseFileName + FileTools.XML_EXTENSION);
            try {
                altoData = TextHelper.readAltoFile(altoFile);
                altoWritten =
                        addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTOCROWD, pi, baseFileName, order, false);
                if (altoWritten) {
                    foundCrowdsourcingData = true;
                }
            } catch (FileNotFoundException e) {
                // Not all pages will have custom ALTO docs
            } catch (IOException | JDOMException e) {
                if (!(e instanceof FileNotFoundException) && !e.getMessage().contains("Premature end of file")) {
                    logger.warn("Could not read ALTO file '{}': {}", altoFile.getName(), e.getMessage());
                }
            }
        }

        // Look for plain fulltext from crowdsouring, if the FULLTEXT field is still empty
        if (doc.getField(SolrConstants.FULLTEXT) == null && dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) != null) {
            String fulltext =
                    TextHelper.generateFulltext(baseFileName + FileTools.TXT_EXTENSION, dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD),
                            false, SolrIndexerDaemon.getInstance().getConfiguration().getBoolean("init.fulltextForceUTF8", true));
            if (fulltext != null) {
                foundCrowdsourcingData = true;
                doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags(fulltext));
                doc.addField(SolrConstants.FILENAME_FULLTEXT, dataRepository.getDir(DataRepository.PARAM_FULLTEXTCROWD).getFileName().toString()
                        + '/' + pi + '/' + baseFileName + FileTools.TXT_EXTENSION);
                logger.debug("Added FULLTEXT from crowdsourcing plain text for page {}", order);
            }
        }

        // Look for a regular ALTO document for this page and fill ALTO and/or FULLTEXT fields, whichever is still empty
        if (!foundCrowdsourcingData && (doc.getField(SolrConstants.ALTO) == null || doc.getField(SolrConstants.FULLTEXT) == null)
                && dataFolders.get(DataRepository.PARAM_ALTO) != null && isBaseFileNameUsable(baseFileName)) {
            File altoFile = new File(dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), baseFileName + FileTools.XML_EXTENSION);
            try {
                altoData = TextHelper.readAltoFile(altoFile);
                altoWritten = addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, pi, baseFileName, order, false);
            } catch (IOException | JDOMException e) {
                if (!(e instanceof FileNotFoundException) && !e.getMessage().contains("Premature end of file")) {
                    logger.warn("Could not read ALTO file '{}': {}", altoFile.getName(), e.getMessage());
                }
            }
        }

        // If FULLTEXT is still empty, look for a plain full-text
        if (!foundCrowdsourcingData && doc.getField(SolrConstants.FULLTEXT) == null && dataFolders.get(DataRepository.PARAM_FULLTEXT) != null
                && isBaseFileNameUsable(baseFileName)) {
            String fulltext =
                    TextHelper.generateFulltext(baseFileName + FileTools.TXT_EXTENSION, dataFolders.get(DataRepository.PARAM_FULLTEXT), true,
                            SolrIndexerDaemon.getInstance().getConfiguration().getBoolean("init.fulltextForceUTF8", true));
            if (fulltext != null) {
                doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags(fulltext));
                doc.addField(SolrConstants.FILENAME_FULLTEXT, dataRepository
                        .getDir(DataRepository.PARAM_FULLTEXT)
                        .getFileName()
                        .toString() + '/'
                        + pi + '/' + baseFileName + FileTools.TXT_EXTENSION);
                logger.debug("Added FULLTEXT from regular plain text for page {}", order);
            }
        }

        // Convert ABBYY XML to ALTO
        if (!altoWritten && !foundCrowdsourcingData && dataFolders.get(DataRepository.PARAM_ABBYY) != null && isBaseFileNameUsable(baseFileName)) {
            try {
                altoData = TextHelper.readAbbyyToAlto(
                        new File(dataFolders.get(DataRepository.PARAM_ABBYY).toAbsolutePath().toString(),
                                baseFileName + FileTools.XML_EXTENSION));
                altoWritten = addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO_CONVERTED, pi, baseFileName,
                        order, true);
            } catch (IOException e) {
                logger.warn(e.getMessage());
            } catch (XMLStreamException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // Convert TEI to ALTO
        if (!altoWritten && !foundCrowdsourcingData && dataFolders.get(DataRepository.PARAM_TEIWC) != null && isBaseFileNameUsable(baseFileName)) {
            try {
                altoData = TextHelper.readTeiToAlto(
                        new File(dataFolders.get(DataRepository.PARAM_TEIWC).toAbsolutePath().toString(), baseFileName + FileTools.XML_EXTENSION));
                altoWritten = addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO_CONVERTED, pi, baseFileName, order,
                        true);
            } catch (IOException e) {
                logger.warn(e.getMessage());
            }
        }

        if (dataFolders.get(DataRepository.PARAM_MIX) != null && isBaseFileNameUsable(baseFileName)) {
            try {
                Map<String, String> mixData = TextHelper
                        .readMix(new File(dataFolders.get(DataRepository.PARAM_MIX).toAbsolutePath().toString(),
                                baseFileName + FileTools.XML_EXTENSION));
                for (Entry<String, String> entry : mixData.entrySet()) {
                    if (!(entry.getKey().equals(SolrConstants.WIDTH) && doc.getField(SolrConstants.WIDTH) != null)
                            && !(entry.getKey().equals(SolrConstants.HEIGHT) && doc.getField(SolrConstants.HEIGHT) != null)) {
                        doc.addField(entry.getKey(), entry.getValue());
                    }
                }
            } catch (JDOMException e) {
                logger.error(e.getMessage(), e);
            } catch (IOException e) {
                logger.warn(e.getMessage());
            }
        }

        // If there is still no ALTO at this point and the METS document contains a file group for ALTO, download and use it
        if (!altoWritten && !foundCrowdsourcingData && altoURL != null && Utils.isValidURL(altoURL)
                && SolrIndexerDaemon.getInstance().getConfiguration().getViewerUrl() != null
                && !altoURL.startsWith(SolrIndexerDaemon.getInstance().getConfiguration().getViewerUrl())) {
            try {
                String alto = null;
                if (Strings.CI.startsWith(altoURL, "http")) {
                    // HTTP(S)
                    logger.debug("Downloading ALTO from {}", altoURL);
                    alto = Utils.getWebContentGET(altoURL);
                } else if (Strings.CI.startsWith(altoURL, "file:/")) {
                    // FILE
                    logger.debug("Reading ALTO from {}", altoURL);
                    alto = FileTools.readFileToString(new File(URI.create(altoURL).toURL().getPath()), StandardCharsets.UTF_8.name());
                }
                if (StringUtils.isNotEmpty(alto)) {
                    Document altoDoc = XmlTools.getDocumentFromString(alto, null);
                    altoData = TextHelper.readAltoDoc(altoDoc);
                    if (altoData != null) {
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO))) {
                            // Create PARAM_ALTO_CONVERTED dir in hotfolder for download, if it doesn't yet exist
                            if (dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED) == null) {
                                dataFolders.put(DataRepository.PARAM_ALTO_CONVERTED,
                                        Paths.get(dataRepository.getDir(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), pi));
                                Files.createDirectory(dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED));
                            }
                            if (dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED) != null) {
                                String fileName = MetadataHelper.FORMAT_EIGHT_DIGITS.get().format(order) + FileTools.XML_EXTENSION;
                                doc.addField(SolrConstants.FILENAME_ALTO,
                                        dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED).getParent().getFileName().toString() + '/' + pi + '/'
                                                + fileName);
                                // Write ALTO file
                                File file = new File(dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED).toFile(), fileName);
                                FileUtils.writeStringToFile(file, (String) altoData.get(SolrConstants.ALTO), TextHelper.DEFAULT_CHARSET);
                                logger.info("Added ALTO from external URL for page {}", order);
                            } else {
                                logger.error("Data folder not defined: {}", dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED));
                            }
                        } else {
                            logger.warn("No ALTO found");
                        }
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT))
                                && doc.getField(SolrConstants.FULLTEXT) == null) {
                            doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags((String) altoData.get(SolrConstants.FULLTEXT)));
                            logger.info("Added FULLTEXT from downloaded ALTO for page {}", order);
                        } else {
                            logger.warn("No FULLTEXT found");
                        }
                        if (altoData.get(SolrConstants.NAMEDENTITIES) != null) {
                            addNamedEntitiesFields(altoData, doc);
                        }
                    }
                }
            } catch (FileNotFoundException e) {
                logger.error(e.getMessage());
            } catch (JDOMException | IOException e) {
                logger.error(e.getMessage(), e);
            } catch (HTTPException e) {
                logger.warn("{}: {}", e.getMessage(), altoURL);
            }
        }

        // Add image dimension values from EXIF
        if (!doc.containsKey(SolrConstants.WIDTH) || !doc.containsKey(SolrConstants.HEIGHT)
                || ("0".equals(doc.getFieldValue(SolrConstants.WIDTH)) && "0".equals(doc.getFieldValue(SolrConstants.HEIGHT)))) {
            doc.removeField(SolrConstants.WIDTH);
            doc.removeField(SolrConstants.HEIGHT);
            ImageSizeReader.getSize(dataFolders.get(DataRepository.PARAM_MEDIA), (String) doc.getFieldValue(SolrConstants.FILENAME))
                    .ifPresent(dimension -> {
                        doc.addField(SolrConstants.WIDTH, dimension.width);
                        doc.addField(SolrConstants.HEIGHT, dimension.height);
                    });
        }

        // FULLTEXTAVAILABLE indicates whether this page has full-text
        if (doc.getField(SolrConstants.FULLTEXT) != null) {
            doc.addField(SolrConstants.FULLTEXTAVAILABLE, true);
            return true;
        }
        doc.addField(SolrConstants.FULLTEXTAVAILABLE, false);
        return false;
    }

    /**
     * 
     * @param baseFileName
     * @return true if baseFileName is not one of the keywords; false otherwise
     */
    private static boolean isBaseFileNameUsable(String baseFileName) {
        return !("default".equals(baseFileName) || "info".equals(baseFileName) || "native".equals(baseFileName));
    }

    /**
     * 
     * @param doc Solr input document
     * @param altoData Parsed ALTO data
     * @param dataFolders Map containing data folders
     * @param altoParamName name of the data repository folder containing the alto file
     * @param pi Record identifier
     * @param baseFileName Base name of the page data file
     * @param order Page order
     * @param converted
     * @return true if any fields were added; false otherwise
     * @throws IOException
     * @should return false if altodata null
     * @should throw IllegalArgumentException if doc null
     * @should throw IllegalArgumentException if dataFolders null
     * @should throw IllegalArgumentException if pi null
     * @should throw IllegalArgumentException if baseFileName null
     * @should add filename for native alto file
     * @should add filename for crowdsourcing alto file
     * @should add filename for converted alto file
     * @should add fulltext
     * @should add width and height
     * @should add named entities
     */
    boolean addIndexFieldsFromAltoData(final SolrInputDocument doc, final Map<String, Object> altoData, final Map<String, Path> dataFolders,
            final String altoParamName, final String pi, final String baseFileName, final int order, final boolean converted) throws IOException {
        if (altoData == null) {
            return false;
        }
        if (doc == null) {
            throw new IllegalArgumentException(StringConstants.ERROR_DOC_MAY_NOT_BE_NULL);
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null");
        }
        if (pi == null) {
            throw new IllegalArgumentException(StringConstants.ERROR_PI_MAY_NOT_BE_NULL);
        }
        if (baseFileName == null) {
            throw new IllegalArgumentException("baseFileName may not be null");
        }

        boolean ret = false;
        // Write ALTO converted from ABBYY/TEI
        if (converted) {
            if (dataFolders.get(altoParamName) != null) {
                FileUtils.writeStringToFile(
                        new File(dataFolders.get(altoParamName).toFile(), baseFileName + FileTools.XML_EXTENSION),
                        (String) altoData.get(SolrConstants.ALTO), TextHelper.DEFAULT_CHARSET);
                ret = true;
            } else {
                logger.error("Data folder not defined: {}", dataFolders.get(altoParamName));
            }
        }

        // FILENAME_ALTO
        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO)) && doc.getField(SolrConstants.FILENAME_ALTO) == null
                && dataRepository != null) {
            doc.addField(SolrConstants.FILENAME_ALTO,
                    dataRepository.getDir(altoParamName)
                            .getFileName()
                            .toString() + '/' + pi
                            + '/' + baseFileName + FileTools.XML_EXTENSION);
            ret = true;
            logger.debug("Added ALTO from {} for page {}", dataRepository.getDir(altoParamName).getFileName(), order);
        }
        // FULLTEXT
        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT))
                && doc.getField(SolrConstants.FULLTEXT) == null) {
            doc.addField(SolrConstants.FULLTEXT, TextHelper.cleanUpHtmlTags((String) altoData.get(SolrConstants.FULLTEXT)));
            logger.debug(LOG_ADDED_FULLTEXT_FROM_REGULAR_ALTO, order);
        }
        // NAMEDENTITIES
        if (altoData.get(SolrConstants.NAMEDENTITIES) != null) {
            addNamedEntitiesFields(altoData, doc);
        }

        return ret;
    }

    /**
     * Adds named entity fields from the given list to the given SolrInputDocument.
     *
     * @param altoData a {@link java.util.Map} object.
     * @param doc a {@link org.apache.solr.common.SolrInputDocument} object.
     * @should add field
     * @should add untokenized field
     */
    @SuppressWarnings("unchecked")
    static void addNamedEntitiesFields(Map<String, Object> altoData, SolrInputDocument doc) {
        List<String> neList = (List<String>) altoData.get(SolrConstants.NAMEDENTITIES);
        if (neList == null || neList.isEmpty()) {
            return;
        }

        for (String ne : neList) {
            String[] splitString = ne.split("###", 3);
            if (splitString.length > 1 && splitString[1] != null) {
                splitString[1] = cleanUpNamedEntityValue(splitString[1]);
                String fieldName = new StringBuilder("NE_").append(splitString[0]).toString();
                doc.addField(fieldName, splitString[1]);
                doc.addField(new StringBuilder(fieldName).append(SolrConstants.SUFFIX_UNTOKENIZED).toString(), splitString[1]);
            }
            // Extract NORM_IDENTIFIER from URI for searches
            if (splitString.length > 2 && splitString[2] != null) {
                String identifier = de.intranda.digiverso.normdataimporter.Utils.getIdentifierFromURI(splitString[2]);
                doc.addField("NORM_IDENTIFIER", identifier);
            }
        }
    }

    /**
     * Removes any non-alphanumeric trailing characters from the given string.
     *
     * @param value a {@link java.lang.String} object.
     * @return Cleaned up value.
     * @should clean up value correctly
     * @should throw IllegalArgumentException given null
     */
    static String cleanUpNamedEntityValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException("value may not be null");
        }
        StringBuilder sb = new StringBuilder(value);
        while (sb.length() > 1 && !CharUtils.isAsciiAlphanumeric(sb.charAt(sb.length() - 1))) {
            sb.deleteCharAt(sb.length() - 1);
        }
        while (sb.length() > 1 && !CharUtils.isAsciiAlphanumeric(sb.charAt(0))) {
            sb.deleteCharAt(0);
        }

        return sb.toString();
    }

}
