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

import java.awt.Dimension;
import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.exif.ExifIFD0Directory;
import com.drew.metadata.jpeg.JpegDirectory;
import com.drew.metadata.png.PngDirectory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import de.intranda.api.annotation.GeoLocation;
import de.intranda.api.annotation.ISelector;
import de.intranda.api.annotation.SimpleResource;
import de.intranda.api.annotation.wa.FragmentSelector;
import de.intranda.api.annotation.wa.SpecificResource;
import de.intranda.api.annotation.wa.TextualResource;
import de.intranda.api.annotation.wa.WebAnnotation;
import de.intranda.digiverso.normdataimporter.model.Record;
import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.HttpConnector;
import io.goobi.viewer.indexer.helper.ImageSizeReader;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.StringConstants;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.helper.WebAnnotationTools;
import io.goobi.viewer.indexer.helper.XmlTools;
import io.goobi.viewer.indexer.model.GroupedMetadata;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.PhysicalElement;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * <p>
 * Abstract Indexer class.
 * </p>
 *
 */
public abstract class Indexer {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(Indexer.class);

    private static final int HTTP_CONNECTION_TIMEOUT = 4000;

    protected static final int GENERATE_PAGE_DOCUMENT_TIMEOUT_HOURS = 6;

    protected static final String FIELD_COORDS = "MD_COORDS";
    protected static final String FIELD_IMAGEAVAILABLE = "BOOL_IMAGEAVAILABLE";
    protected static final String FIELD_FILESIZE = "MDNUM_FILESIZE";
    protected static final String FIELD_OWNERDEPTH = "MDNUM_OWNERDEPTH";
    protected static final String FIELD_SHAPE = "MD_SHAPE";
    protected static final String FIELD_TEXT = "MD_TEXT";
    protected static final String LOG_ADDED_FULLTEXT_FROM_REGULAR_ALTO = "Added FULLTEXT from regular ALTO for page {}";

    public static final String STATUS_ERROR = "ERROR";

    static final String FOLDER_SUFFIX_ALTOCROWD = "_altocrowd";
    static final String FOLDER_SUFFIX_DOWNLOADIMAGES = "_downloadimages";
    static final String FOLDER_SUFFIX_MEDIA = "_media";
    static final String FOLDER_SUFFIX_TXTCROWD = "_txtcrowd";

    static final String LOG_COULD_NOT_BE_DELETED = "'{}' could not be deleted! Please delete it manually!";
    static final String LOG_FOUND_DATA_FOLDER = "Found data folder: {}";

    public static final String[] IIIF_IMAGE_FILE_NAMES =
            { ".*bitonal.(jpg|png|tif|jp2)$", ".*color.(jpg|png|tif|jp2)$", ".*default.(jpg|png|tif|jp2)$", ".*gray.(jpg|png|tif|jp2)$",
                    ".*native.(jpg|png|tif|jp2)$" };

    // TODO cyclic dependency; find a more elegant way to select a repository w/o passing the hotfolder instance to the indexer
    protected Hotfolder hotfolder;

    /** XPath Parser. */
    protected JDomXP xp;

    protected DataRepository dataRepository;

    protected DataRepository previousDataRepository;

    protected StringBuilder sbLog = new StringBuilder();

    protected final Set<Integer> ugcAddedChecklist = new HashSet<>();
    /** Indicates whether any of this record's pages has images. */
    protected boolean recordHasImages = false;
    /** Indicates whether any of this record's pages has full-text. */
    protected boolean recordHasFulltext = false;

    protected final HttpConnector httpConnector;

    private final ObjectMapper mapper = new ObjectMapper();

    protected Set<String> iddocsToDelete = new HashSet<>();

    protected Indexer() {
        httpConnector = new HttpConnector(HTTP_CONNECTION_TIMEOUT);
        mapper.registerModule(new JavaTimeModule());
    }

    protected Indexer(HttpConnector httpConnector) {
        this.httpConnector = httpConnector;
        mapper.registerModule(new JavaTimeModule());
    }

    /**
     * Indexes given record file.
     * 
     * @param recordFile
     * @param reindexSettings
     * @return List of successfully indexed record identifiers
     */
    public abstract List<String> addToIndex(Path recordFile, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException;

    /**
     * 
     * @param queryPrefix
     * @return Found PI value; otherwise null
     * @throws IndexerException
     * @should find identifier correctly
     * @should throw IndexerException if no identifier found
     */
    protected String findPI(String queryPrefix) throws IndexerException {
        String ret = MetadataHelper.getPIFromXML(queryPrefix, xp);
        if (StringUtils.isBlank(ret)) {
            throw new IndexerException("PI not found.");
        }

        return ret;
    }

    /**
     * 
     * @param foundPi
     * @param indexObj
     * @param removeIdentifierPrefix
     * @return The PI
     * @throws IndexerException
     * @should set identifier correctly
     * @should remove prefix correctly
     * @should add identifier to default
     * @should throw IndexerException if identifier invalid
     */
    protected String validateAndApplyPI(String foundPi, IndexObject indexObj, boolean removeIdentifierPrefix) throws IndexerException {
        String pi = foundPi;
        // Remove identifier prefix
        if (removeIdentifierPrefix) {
            if (pi.contains(":")) {
                pi = pi.substring(pi.lastIndexOf(':') + 1);
            }
            if (pi.contains("/")) {
                pi = pi.substring(pi.lastIndexOf('/') + 1);
            }
        }

        pi = MetadataHelper.applyIdentifierModifications(pi);
        logger.info("Record PI: {}", pi);

        // Do not allow identifiers with characters that cannot be used in file names
        if (!Utils.validatePi(pi)) {
            throw new IndexerException("PI contains illegal characters: " + pi);
        }

        indexObj.setPi(pi);
        indexObj.setTopstructPI(pi);

        // Add PI to default
        if (MetadataHelper.isPiAddToDefault(SolrIndexerDaemon.getInstance()
                .getConfiguration()
                .getMetadataConfigurationManager()
                .getConfigurationListForField(SolrConstants.PI))) {
            indexObj.setDefaultValue(indexObj.getDefaultValue() + " " + pi);
        }

        return pi;
    }

    /**
     * Move data file to the error folder.
     * 
     * @param dataFile {@link File}
     * @param error
     * @param format
     * @should write log file and copy of mets file into errorMets
     */
    void handleError(Path dataFile, String error, FileFormat format) {
        logger.error("Failed to process '{}'.", dataFile.getFileName());
        // Error log file
        if (FileFormat.METS.equals(format) || FileFormat.METS_MARC.equals(format)) {
            File logFile = new File(hotfolder.getErrorMets().toFile(), FilenameUtils.getBaseName(dataFile.getFileName().toString()) + ".log");
            try (FileWriter fw = new FileWriter(logFile); BufferedWriter out = new BufferedWriter(fw)) {
                Files.copy(dataFile, Paths.get(hotfolder.getErrorMets().toAbsolutePath().toString(), dataFile.getFileName().toString()),
                        StandardCopyOption.REPLACE_EXISTING);
                if (error != null) {
                    out.write(error);
                }
            } catch (IOException e) {
                logger.error("Data file could not be moved to errorMets!", e);
            }
        }
    }

    /**
     * Removes the document represented by the given METS or LIDO file from the index.
     *
     * @param pi {@link java.lang.String} Record identifier.
     * @param trace A Lucene document with DATEDELETED timestamp will be created if true.
     * @param searchIndex a {@link io.goobi.viewer.indexer.helper.SolrSearchIndex} object.
     * @return {@link java.lang.Boolean}
     * @throws java.io.IOException
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should throw IllegalArgumentException if pi empty
     * @should throw IllegalArgumentException if searchIndex null
     * @should return false if pi not found
     * @should delete METS record from index completely
     * @should delete LIDO record from index completely
     * @should leave trace document for METS record if requested
     * @should leave trace document for LIDO record if requested
     */
    public static boolean delete(String pi, boolean trace, SolrSearchIndex searchIndex) throws IOException, FatalIndexerException {
        if (StringUtils.isEmpty(pi)) {
            throw new IllegalArgumentException("pi may not be empty or null.");
        }
        if (searchIndex == null) {
            throw new IllegalArgumentException("searchIndex may not be null.");
        }
        // Check whether this is an anchor record
        try {
            SolrDocumentList hits = searchIndex.search(new StringBuilder(SolrConstants.PI).append(":").append(pi).toString(),
                    Collections.singletonList(SolrConstants.ISANCHOR));
            if (!hits.isEmpty() && hits.get(0).getFieldValue(SolrConstants.ISANCHOR) != null
                    && Boolean.TRUE.equals(hits.get(0).getFieldValue(SolrConstants.ISANCHOR))) {
                hits = searchIndex.search(SolrConstants.PI_PARENT + ":" + pi, Collections.singletonList(SolrConstants.PI));
                if (hits.getNumFound() > 0) {
                    // Only empty anchors may be deleted
                    logger.error("This is a multi-volume work that has indexed children. It may not be deleted at this moment!");
                    return false;
                }
            }
        } catch (SolrServerException e) {
            logger.error(e.getMessage(), e);
            return false;
        }

        // Delete
        try {
            if (!deleteWithPI(pi, trace, true, searchIndex).isEmpty()) {
                searchIndex.commit(searchIndex.isOptimize());

                // Clear cache for record
                String msg = Utils.removeRecordImagesFromCache(pi);
                if (msg != null) {
                    logger.info(msg);
                }

                return true;
            }
        } catch (SolrServerException e) {
            logger.error(e.getMessage(), e);
            searchIndex.rollback();
        }

        return false;
    }

    /**
     * Deletes the entire document hierarchy that belong to the given PI, as well as any orphaned docs that don't belong to the current indexed
     * instance but might still exist.
     *
     * @param pi String
     * @param createTraceDoc a boolean.
     * @param deleteImmediately
     * @param searchIndex a {@link io.goobi.viewer.indexer.helper.SolrSearchIndex} object.
     * @throws java.io.IOException
     * @throws org.apache.solr.client.solrj.SolrServerException
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @return Set<String>
     */
    protected static Set<String> deleteWithPI(String pi, boolean createTraceDoc, boolean deleteImmediately, SolrSearchIndex searchIndex)
            throws IOException, SolrServerException, FatalIndexerException {
        String query = SolrConstants.PI + ":" + pi;
        SolrDocumentList hits = searchIndex.search(query, null);
        if (hits.isEmpty()) {
            logger.error("Not found: {}", pi);
            return Collections.emptySet();
        }

        if (hits.getNumFound() == 1) {
            logger.info("Found a previous instance of this volume in the index.");
        } else {
            logger.warn(
                    "{} previous instances of this volume have been found in the index. This shouldn't ever be the case."
                            + " Check whether there is more than one indexer instance running! All instances will be removed...",
                    hits.getNumFound());
        }
        String queryPageUrns = new StringBuilder(SolrConstants.PI_TOPSTRUCT).append(":")
                .append(pi)
                .append(SolrConstants.SOLR_QUERY_AND)
                .append(SolrConstants.DOCTYPE)
                .append(":PAGE")
                .toString();

        // Unless the index is broken, there should be only one hit
        Set<String> ret = new HashSet<>();
        for (SolrDocument doc : hits) {
            String iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            if (iddoc == null) {
                continue;
            }
            logger.debug("Removing instance: {}", iddoc);
            ret.add(iddoc);

            // Build replacement document that is marked as deleted
            if (createTraceDoc && doc.getFieldValue(SolrConstants.DATEDELETED) == null) {
                String urn = null;
                if (doc.getFieldValue(SolrConstants.URN) != null) {
                    urn = (String) doc.getFieldValue(SolrConstants.URN);
                }
                // Collect page URNs
                hits = searchIndex.search(queryPageUrns, Collections.singletonList(SolrConstants.IMAGEURN));
                List<String> pageUrns = new ArrayList<>(hits.size());
                if (!hits.isEmpty()) {
                    for (SolrDocument hit : hits) {
                        String pageUrn = (String) hit.getFieldValue(SolrConstants.IMAGEURN);
                        if (pageUrn != null) {
                            pageUrns.add(pageUrn);
                        }
                    }
                }
                String now = String.valueOf(System.currentTimeMillis());
                createDeletedDoc(pi, urn, pageUrns, now, now, searchIndex);
            }
        }

        // Retrieve all docs for this record via PI_TOPSTRUCT
        hits = searchIndex.search(new StringBuilder(SolrConstants.PI_TOPSTRUCT).append(":").append(pi).toString(),
                Collections.singletonList(SolrConstants.IDDOC));
        for (SolrDocument doc : hits) {
            String iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            if (iddoc != null) {
                ret.add(iddoc);
            }
        }

        if (deleteImmediately && searchIndex.deleteDocuments(new ArrayList<>(ret))) {
            logger.info("Immediate deletion requested - {} docs deleted.", ret.size());
        }

        return ret;
    }

    /**
     * Build replacement document that is marked as deleted.
     * 
     * @param pi
     * @param urn
     * @param pageUrns
     * @param dateDeleted
     * @param dateUpdated
     * @param searchIndex
     * @throws NumberFormatException
     * @throws FatalIndexerException
     */
    private static void createDeletedDoc(String pi, String urn, List<String> pageUrns, String dateDeleted, String dateUpdated,
            SolrSearchIndex searchIndex) throws NumberFormatException, FatalIndexerException {
        // Build replacement document that is marked as deleted
        logger.info("Creating 'DELETED' document for {}...", pi);
        List<LuceneField> fields = new ArrayList<>();
        String iddoc = String.valueOf(getNextIddoc());
        fields.add(new LuceneField(SolrConstants.IDDOC, iddoc));
        fields.add(new LuceneField(SolrConstants.GROUPFIELD, iddoc));

        fields.add(new LuceneField(SolrConstants.PI, pi));
        if (urn != null) {
            fields.add(new LuceneField(SolrConstants.URN, urn));
        }
        if (pageUrns != null) {
            for (String pageUrn : pageUrns) {
                fields.add(new LuceneField(SolrConstants.IMAGEURN_OAI, pageUrn.replace("\\\\", "")));
            }
        }
        fields.add(new LuceneField(SolrConstants.DATEDELETED, dateDeleted));
        fields.add(new LuceneField(SolrConstants.DATEUPDATED, dateUpdated));
        searchIndex.writeToIndex(SolrSearchIndex.createDocument(fields));
    }

    /**
     * Returns the next available IDDOC value.
     *
     * @return Generated UUID as {@link String}
     */
    protected static synchronized String getNextIddoc() {
        return UUID.randomUUID().toString();
    }

    /**
     * Replaces irrelevant characters in the DEFAULT field value with spaces.
     *
     * @param field a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     * @should replace irrelevant chars with spaces correctly
     * @should return null if field null
     */
    public static String cleanUpDefaultField(String field) {
        if (field != null) {
            return field.replace(",", " ").replace(";", " ").replace(":", " ").replace("  ", " ").trim();
        }

        return null;
    }

    /**
     * Parses the user generated content XML file for the given page and creates a Solr doc for each content entry. In addition, all content strings
     * are written into the UGC doc's own search field. To make sure the IDDOC_OWNER value is matches the one in the corresponding page, make sure
     * this method is called after all docstruct to page mapping is finished.
     * 
     * @param pageDoc
     * @param dataFolder
     * @param pi
     * @param anchorPi
     * @param groupIds
     * @param order
     * @param fileNameRoot
     * @return List of Solr input documents for the UGC contents
     * @throws FatalIndexerException
     * @should return empty list if dataFolder null
     */
    List<SolrInputDocument> generateUserGeneratedContentDocsForPage(SolrInputDocument pageDoc, Path dataFolder, String pi, String anchorPi,
            Map<String, String> groupIds, int order, String fileNameRoot) throws FatalIndexerException {
        if (dataFolder == null || !Files.isDirectory(dataFolder)) {
            logger.info("UGC folder is empty.");
            return Collections.emptyList();
        }

        Path file = Paths.get(dataFolder.toAbsolutePath().toString(), fileNameRoot + FileTools.XML_EXTENSION);
        if (!Files.isRegularFile(file)) {
            logger.debug("'{}' does not exist or is not a file.", file.getFileName());
            return Collections.emptyList();
        }

        try (FileInputStream fis = new FileInputStream(file.toFile())) {
            Document xmlDoc = XmlTools.getSAXBuilder().build(fis);
            if (xmlDoc == null || xmlDoc.getRootElement() == null) {
                logger.info("Invalid XML in file '{}'.", file.getFileName());
                return Collections.emptyList();
            }
            List<SolrInputDocument> ret = new ArrayList<>();
            List<Element> eleContentList = xmlDoc.getRootElement().getChildren();
            if (eleContentList == null || eleContentList.isEmpty()) {
                logger.info("No data found in file '{}'.", file.getFileName());
                return Collections.emptyList();
            }
            logger.info("Found {} user generated contents in  file '{}'.", eleContentList.size(), file.getFileName());
            for (Element eleContent : eleContentList) {
                SolrInputDocument doc = generateUserGeneratedContentDocForPage(eleContent, pageDoc, pi, anchorPi, groupIds, order);
                if (doc != null) {
                    ret.add(doc);
                }
            }
            return ret;
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        } catch (IOException | JDOMException e) {
            logger.error(e.getMessage(), e);
        }

        return Collections.emptyList();
    }

    /**
     * Generates a Solr document for a single user generated content element.
     * 
     * @param eleContent
     * @param pageDoc
     * @param pi
     * @param anchorPi
     * @param groupIds
     * @param order
     * @return Generated {@link SolrInputDocument}
     * @should throw IllegalArgumentException if eleContent null
     */
    SolrInputDocument generateUserGeneratedContentDocForPage(Element eleContent, SolrInputDocument pageDoc, String pi,
            String anchorPi, Map<String, String> groupIds, int order) {
        if (eleContent == null) {
            throw new IllegalArgumentException("eleContent may not be null");
        }

        StringBuilder sbTerms = new StringBuilder();
        SolrInputDocument doc = new SolrInputDocument();
        String iddoc = getNextIddoc();
        doc.addField(SolrConstants.IDDOC, iddoc);
        if (pageDoc != null) {
            if (pageDoc.containsKey(SolrConstants.IDDOC_OWNER)) {
                doc.addField(SolrConstants.IDDOC_OWNER, pageDoc.getFieldValue(SolrConstants.IDDOC_OWNER));
            }
            // Add topstruct type
            if (!doc.containsKey(SolrConstants.DOCSTRCT_TOP) && pageDoc.containsKey(SolrConstants.DOCSTRCT_TOP)) {
                doc.setField(SolrConstants.DOCSTRCT_TOP, pageDoc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
            }
        }
        doc.addField(SolrConstants.GROUPFIELD, iddoc);
        doc.addField(SolrConstants.DOCTYPE, DocType.UGC.name());
        doc.addField(SolrConstants.PI_TOPSTRUCT, pi);
        doc.addField(SolrConstants.ORDER, order);
        if (StringUtils.isNotEmpty(anchorPi)) {
            doc.addField(SolrConstants.PI_ANCHOR, anchorPi);
        }
        // Add GROUPID_* fields
        if (groupIds != null && !groupIds.isEmpty()) {
            for (Entry<String, String> entry : groupIds.entrySet()) {
                doc.addField(entry.getKey(), entry.getValue());
            }
        }
        List<Element> eleFieldList = eleContent.getChildren();
        switch (eleContent.getName()) {
            case "UserGeneratedPerson":
                doc.addField(SolrConstants.UGCTYPE, SolrConstants.UGC_TYPE_PERSON);
                break;
            case "UserGeneratedCorporation":
                doc.addField(SolrConstants.UGCTYPE, SolrConstants.UGC_TYPE_CORPORATION);
                break;
            case "UserGeneratedAddress":
                doc.addField(SolrConstants.UGCTYPE, SolrConstants.UGC_TYPE_ADDRESS);
                break;
            case "UserGeneratedComment":
                doc.addField(SolrConstants.UGCTYPE, SolrConstants.UGC_TYPE_COMMENT);
                break;
            default:
                // nothing
        }
        for (Element eleField : eleFieldList) {
            if (StringUtils.isNotEmpty(eleField.getValue())) {
                switch (eleField.getName()) {
                    case "area":
                        doc.addField(SolrConstants.UGCCOORDS, MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "firstname":
                        doc.addField(SolrConstants.MD_FIRSTNAME, MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "lastname":
                        doc.addField(SolrConstants.MD_LASTNAME, MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "personIdentifier":
                        doc.addField("MD_PERSONIDENTIFIER", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "title":
                        doc.addField("MD_CORPORATION", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "address":
                        doc.addField("MD_ADDRESS", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "occupation":
                        doc.addField("MD_OCCUPATION", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "corporationIdentifier":
                        doc.addField("MD_CORPORATIONIDENTIFIER", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "street":
                        doc.addField("MD_STREET", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "houseNumber":
                        doc.addField("MD_HOUSENUMBER", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "district":
                        doc.addField("MD_DISTRICT", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "city":
                        doc.addField("MD_CITY", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "country":
                        doc.addField("MD_COUNTRY", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "coordinateX":
                        doc.addField("MD_COORDX", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "coordinateY":
                        doc.addField("MD_COORDY", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    case "text":
                        doc.addField(FIELD_TEXT, MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                        break;
                    default:
                        // nothing

                }
                // Collect all terms in one string
                if (!"area".equals(eleField.getName())) {
                    String terms = MetadataHelper.applyValueDefaultModifications(eleField.getValue().trim());
                    String termsWithSpaces = " " + terms + " ";
                    if (!sbTerms.toString().contains(termsWithSpaces)) {
                        sbTerms.append(termsWithSpaces);
                    }
                }
            }
        }
        if (StringUtils.isNotBlank(sbTerms.toString())) {
            doc.addField(SolrConstants.UGCTERMS, sbTerms.toString());
        }

        return doc;
    }

    /**
     * Parses the comment annotation JSON file for the given page and creates a Solr doc. In addition, all content strings are written into the UGC
     * doc's own search field. To make sure the IDDOC_OWNER value is matches the one in the corresponding page, make sure this method is called after
     * all docstruct to page mapping is finished.
     * 
     * @param pageDoc
     * @param dataFolder
     * @param pi
     * @param anchorPi
     * @param groupIds
     * @param order
     * @return List of Solr input documents for the comment annotations
     * @should return empty list if dataFolder null
     * @should construct doc correctly
     * @should skip comment for other pages
     */
    List<SolrInputDocument> generateUserCommentDocsForPage(SolrInputDocument pageDoc, Path dataFolder, String pi, String anchorPi,
            Map<String, String> groupIds, int order) {
        if (dataFolder == null || !Files.isDirectory(dataFolder)) {
            logger.info("UGC folder not found.");
            return Collections.emptyList();
        }

        List<SolrInputDocument> ret = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataFolder, "*.{json}")) {
            for (Path file : stream) {
                if (!Files.isRegularFile(file)) {
                    logger.debug("'{}' is not a file.", file.getFileName());
                    continue;
                }

                logger.debug("JSON file: {}", file.getFileName());
                try (FileInputStream fis = new FileInputStream(file.toFile())) {
                    WebAnnotation anno = new ObjectMapper().registerModule(new JavaTimeModule()).readValue(fis, WebAnnotation.class);
                    if (anno == null) {
                        logger.warn("Invalid JSON in file '{}'.", file.getFileName());
                        continue;
                    }
                    if (anno.getBody() == null || !anno.getBody().getClass().equals(TextualResource.class)) {
                        logger.warn("Missing or invalid body in JSON '{}'.", file.getFileName());
                        continue;
                    }

                    Long extractedOrder = null;
                    String uri = null;

                    if (anno.getTarget() instanceof SimpleResource sr) {
                        uri = sr.getId().toString();
                    } else if (anno.getTarget() instanceof TextualResource tr) {
                        uri = tr.getText();
                    }
                    if (uri != null) {
                        Pattern p = Pattern.compile("pages\\/(\\d+)\\/canvas");
                        Matcher m = p.matcher(uri);
                        if (m.find()) {
                            extractedOrder = Long.parseLong(m.group(1));
                        }
                    }
                    // Skip pages that don't match
                    if (extractedOrder == null || extractedOrder != order) {
                        continue;
                    }

                    SolrInputDocument doc = new SolrInputDocument();
                    String iddoc = getNextIddoc();
                    doc.addField(SolrConstants.IDDOC, iddoc);

                    if (pageDoc != null) {
                        if (pageDoc.containsKey(SolrConstants.IDDOC_OWNER)) {
                            doc.addField(SolrConstants.IDDOC_OWNER, pageDoc.getFieldValue(SolrConstants.IDDOC_OWNER));
                        }
                        // Add topstruct type
                        if (!doc.containsKey(SolrConstants.DOCSTRCT_TOP) && pageDoc.containsKey(SolrConstants.DOCSTRCT_TOP)) {
                            doc.setField(SolrConstants.DOCSTRCT_TOP, pageDoc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
                        }
                    }
                    doc.addField(SolrConstants.GROUPFIELD, iddoc);
                    doc.addField(SolrConstants.DOCTYPE, DocType.UGC.name());
                    doc.addField(SolrConstants.PI_TOPSTRUCT, pi);
                    doc.addField(SolrConstants.ORDER, order);
                    if (StringUtils.isNotEmpty(anchorPi)) {
                        doc.addField(SolrConstants.PI_ANCHOR, anchorPi);
                    }
                    // Add GROUPID_* fields
                    if (groupIds != null && !groupIds.isEmpty()) {
                        for (Entry<String, String> entry : groupIds.entrySet()) {
                            doc.addField(entry.getKey(), entry.getValue());
                        }
                    }
                    doc.addField(SolrConstants.UGCTYPE, SolrConstants.UGC_TYPE_COMMENT);

                    TextualResource body = (TextualResource) anno.getBody();
                    if (StringUtils.isNotEmpty(body.getText())) {
                        doc.addField(FIELD_TEXT, body.getText());
                        doc.addField(SolrConstants.UGCTERMS, "COMMENT  " + body.getText());
                    }

                    doc.addField("MD_ANNOTATION_ID", anno.getId().toString());

                    ret.add(doc);
                }
            }

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return ret;
    }

    /**
     * 
     * @param pageDocs
     * @param dataFolder
     * @param pi
     * @param anchorPi
     * @param groupIds
     * @return List<SolrInputDocument>
     * @should return empty list if dataFolder null
     * @should create docs correctly
     */
    List<SolrInputDocument> generateAnnotationDocs(Map<Integer, SolrInputDocument> pageDocs, Path dataFolder, String pi, String anchorPi,
            Map<String, String> groupIds) {
        if (dataFolder == null || !Files.isDirectory(dataFolder)) {
            logger.info("Annotation folder is empty.");
            return Collections.emptyList();
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataFolder, "*.{json}")) {
            List<SolrInputDocument> ret = new ArrayList<>();
            for (Path path : stream) {
                String iddoc = getNextIddoc();
                ret.add(readAnnotation(path, iddoc, pi, anchorPi, pageDocs, groupIds));
            }

            return ret;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return Collections.emptyList();
    }

    /**
     * <p>
     * readAnnotation.
     * </p>
     *
     * @param pageDocs a {@link java.util.Map} object.
     * @param pi a {@link java.lang.String} object.
     * @param anchorPi a {@link java.lang.String} object.
     * @param groupIds a {@link java.util.Map} object.
     * @param path a {@link java.nio.file.Path} object.
     * @param iddoc a long.
     * @return a {@link org.apache.solr.common.SolrInputDocument} object.
     */
    public SolrInputDocument readAnnotation(Path path, String iddoc, String pi, String anchorPi, Map<Integer, SolrInputDocument> pageDocs,
            Map<String, String> groupIds) {
        try (FileInputStream fis = new FileInputStream(path.toFile())) {
            String json = FileTools.readFileToString(path.toFile(), TextHelper.DEFAULT_CHARSET);
            WebAnnotation annotation = mapper.readValue(json, WebAnnotation.class);
            if (annotation == null) {
                return null; //NOSONAR Returning empty map would complicate things
            }

            String annotationId = Paths.get(annotation.getId().getPath()).getFileName().toString();

            StringBuilder sbTerms = new StringBuilder();
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField(SolrConstants.IDDOC, iddoc);
            doc.addField(SolrConstants.GROUPFIELD, iddoc);
            doc.addField(SolrConstants.DOCTYPE, DocType.UGC.name());
            doc.addField(SolrConstants.PI_TOPSTRUCT, pi);
            doc.addField(SolrConstants.MD_ANNOTATION_ID, annotationId);
            if (StringUtils.isNotBlank(annotation.getRights())) {
                doc.addField(SolrConstants.ACCESSCONDITION, annotation.getRights());
            } else {
                doc.addField(SolrConstants.ACCESSCONDITION, "OPENACCESS");

            }
            Integer pageOrder = WebAnnotationTools.parsePageOrder(annotation.getTarget().getId());
            if (pageOrder != null) {
                doc.setField(SolrConstants.ORDER, pageOrder);

                // Look up owner page doc
                SolrInputDocument pageDoc = pageDocs.get(pageOrder);
                if (pageDoc != null) {
                    if (pageDoc.containsKey(SolrConstants.IDDOC)) {
                        doc.addField(SolrConstants.IDDOC_OWNER, pageDoc.getFieldValue(SolrConstants.IDDOC));
                    }
                    // Add topstruct type
                    if (!doc.containsKey(SolrConstants.DOCSTRCT_TOP) && pageDoc.containsKey(SolrConstants.DOCSTRCT_TOP)) {
                        doc.setField(SolrConstants.DOCSTRCT_TOP, pageDoc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
                    }
                }
            } else {
                //TODO: add doc directly to work
                if (pageDocs != null && !pageDocs.isEmpty()) {
                    SolrInputDocument pageDoc = pageDocs.values().iterator().next();
                    if (pageDoc != null) {
                        doc.setField(SolrConstants.DOCSTRCT_TOP, pageDoc.getFieldValue(SolrConstants.DOCSTRCT_TOP));
                    }
                }
            }

            if (StringUtils.isNotEmpty(anchorPi)) {
                doc.addField(SolrConstants.PI_ANCHOR, anchorPi);
            }
            // Add GROUPID_* fields
            if (groupIds != null && !groupIds.isEmpty()) {
                for (Entry<String, String> entry : groupIds.entrySet()) {
                    doc.addField(entry.getKey(), entry.getValue());
                }
            }

            // Value
            if (annotation.getBody() instanceof TextualResource tr) {
                doc.setField(SolrConstants.UGCTYPE, SolrConstants.UGC_TYPE_COMMENT);
                doc.addField(FIELD_TEXT, tr.getText());
            } else if (annotation.getBody() instanceof GeoLocation geoLocation) {
                doc.setField(SolrConstants.UGCTYPE, SolrConstants.UGC_TYPE_ADDRESS);
                // Add searchable coordinates
                if (geoLocation.getGeometry() != null) {
                    double[] coords = geoLocation.getGeometry().getCoordinates();
                    if (coords.length == 2) {
                        doc.addField(FIELD_COORDS, coords[0] + " " + coords[1]);
                    }
                }
            } else if (annotation.getBody() != null) {
                logger.warn("Cannot interpret annotation body of type '{}'.", annotation.getBody().getClass());
            } else {
                logger.warn("Annotaton has no body: {}", annotation);

            }
            // Add annotation body as JSON, always!
            if (annotation.getBody() != null) {
                doc.addField("MD_BODY", annotation.getBody().toString());
            }

            if (annotation.getTarget() instanceof SpecificResource sr) {
                // Coords
                ISelector selector = sr.getSelector();
                if (selector instanceof FragmentSelector fs) {
                    String coords = fs.getValue();
                    doc.addField(SolrConstants.UGCCOORDS, MetadataHelper.applyValueDefaultModifications(coords));
                    doc.setField(SolrConstants.UGCTYPE, SolrConstants.UGC_TYPE_ADDRESS);
                }
            }

            sbTerms.append(doc.getFieldValue(SolrConstants.UGCTYPE)).append(" ");
            sbTerms.append(doc.getFieldValue(FIELD_TEXT)).append(" ");

            if (StringUtils.isNotBlank(sbTerms.toString())) {
                doc.setField(SolrConstants.UGCTERMS, sbTerms.toString().trim());
            }

            return doc;
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return null; //NOSONAR Returning empty map would complicate things
    }

    /**
     * Generates Solr docs for legacy UGC and WebAnnotation files and adds them to the write strategy.
     *
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @param dataFolders a {@link java.util.Map} object.
     * @param indexObj a {@link io.goobi.viewer.indexer.model.IndexObject} object.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     */
    protected void writeUserGeneratedContents(ISolrWriteStrategy writeStrategy, Map<String, Path> dataFolders, IndexObject indexObj)
            throws FatalIndexerException {
        // Collect page docs for annotation<->page mapping
        Map<Integer, SolrInputDocument> pageDocs = HashMap.newHashMap(writeStrategy.getPageDocsSize());

        // Add used-generated content docs from legacy crowdsourcing
        for (int i : writeStrategy.getPageOrderNumbers()) {
            SolrInputDocument pageDoc = writeStrategy.getPageDocForOrder(i);
            if (pageDoc == null) {
                logger.warn("Page {} not found, cannot check for UGC contents.", i);
                continue;
            }
            int order = (Integer) pageDoc.getFieldValue(SolrConstants.ORDER);
            String pageFileBaseName = FilenameUtils.getBaseName((String) pageDoc.getFieldValue(SolrConstants.FILENAME));
            if (dataFolders.get(DataRepository.PARAM_UGC) != null && !ugcAddedChecklist.contains(order)) {
                // UGC
                writeStrategy.addDocs(generateUserGeneratedContentDocsForPage(pageDoc, dataFolders.get(DataRepository.PARAM_UGC),
                        indexObj.getTopstructPI(), indexObj.getAnchorPI(), indexObj.getGroupIds(), order, pageFileBaseName));
                // Comment annotations
                writeStrategy.addDocs(generateUserCommentDocsForPage(pageDoc, dataFolders.get(DataRepository.PARAM_UGC),
                        indexObj.getTopstructPI(), indexObj.getAnchorPI(), indexObj.getGroupIds(), order));
                ugcAddedChecklist.add(order);
            }

            pageDocs.put(order, pageDoc);
        }

        // Add user generated content docs from annotations
        if (dataFolders.get(DataRepository.PARAM_ANNOTATIONS) != null) {
            writeStrategy.addDocs(generateAnnotationDocs(pageDocs, dataFolders.get(DataRepository.PARAM_ANNOTATIONS), indexObj.getTopstructPI(),
                    indexObj.getAnchorPI(), indexObj.getGroupIds()));
        }
    }

    /**
     * Creates the JDomXP instance for this indexer using the given XML file.
     *
     * @param xmlFile a {@link java.nio.file.Path} object.
     * @throws java.io.IOException
     * @throws org.jdom2.JDOMException
     */
    public void initJDomXP(Path xmlFile) throws IOException, JDOMException {
        xp = new JDomXP(xmlFile.toFile());
    }

    /**
     * <p>
     * delta.
     * </p>
     *
     * @param n a int.
     * @param m a int.
     * @return a int.
     */
    protected static int delta(int n, int m) {
        return Math.abs(n - m);
    }

    /**
     * Retrieves the image size (width/height) for the image referenced in the given page document The image sizes are retrieved from image metadata.
     * if this doesn't work, no image sizes are set
     * 
     * @param mediaFolder
     * @param filename
     * @return Optional<Dimension>
     * @should return size correctly
     */
    static Optional<Dimension> getSize(Path mediaFolder, String filename) {
        logger.trace("getSize: {}", filename);
        if (filename == null || mediaFolder == null) {
            return Optional.empty();
        }
        File imageFile = new File(filename);
        imageFile = new File(mediaFolder.toAbsolutePath().toString(), imageFile.getName());
        if (!imageFile.isFile()) {
            return Optional.empty();
        }
        logger.debug("Found image file {}", imageFile.getAbsolutePath());
        return readDimension(imageFile);
    }

    /**
     * 
     * @param imageFile
     * @return Optional<Dimension>
     */
    static Optional<Dimension> readDimension(File imageFile) {
        Dimension imageSize = new Dimension(0, 0);
        try {
            Metadata imageMetadata = ImageMetadataReader.readMetadata(imageFile);
            Directory jpegDirectory = imageMetadata.getFirstDirectoryOfType(JpegDirectory.class);
            Directory exifDirectory = imageMetadata.getFirstDirectoryOfType(ExifIFD0Directory.class);
            Directory pngDirectory = imageMetadata.getFirstDirectoryOfType(PngDirectory.class);
            try {
                imageSize.width = Integer.valueOf(pngDirectory.getDescription(1).replaceAll("\\D", ""));
                imageSize.height = Integer.valueOf(pngDirectory.getDescription(2).replaceAll("\\D", ""));
            } catch (NullPointerException e) {
                //
            }
            try {
                imageSize.width = Integer.valueOf(exifDirectory.getDescription(256).replaceAll("\\D", ""));
                imageSize.height = Integer.valueOf(exifDirectory.getDescription(257).replaceAll("\\D", ""));
            } catch (NullPointerException e) {
                //
            }
            try {
                imageSize.width = Integer.valueOf(jpegDirectory.getDescription(3).replaceAll("\\D", ""));
                imageSize.height = Integer.valueOf(jpegDirectory.getDescription(1).replaceAll("\\D", ""));
            } catch (NullPointerException e) {
                //
            }

            if (imageSize.getHeight() * imageSize.getHeight() > 0) {
                return Optional.of(imageSize);
            }
        } catch (ImageProcessingException | IOException e) {
            try {
                imageSize = getSizeForJp2(imageFile.toPath());
                return Optional.ofNullable(imageSize);
            } catch (IOException e2) {
                logger.warn(e2.toString());
                try {
                    BufferedImage image = ImageIO.read(imageFile);
                    if (image != null) {
                        return Optional.of(new Dimension(image.getWidth(), image.getHeight()));
                    }
                } catch (NullPointerException | IOException e1) {
                    logger.error("Unable to read image size: {}: {}", e.getMessage(), imageFile.getName());
                }
            } catch (UnsatisfiedLinkError e3) {
                logger.error("Unable to load jpeg2000 ImageReader: {}", e.toString());
            }
        }

        return Optional.empty();
    }

    /**
     * <p>
     * getSizeForJp2.
     * </p>
     *
     * @param image a {@link java.nio.file.Path} object.
     * @return a {@link java.awt.Dimension} object.
     * @throws java.io.IOException if any.
     */
    public static Dimension getSizeForJp2(Path image) throws IOException {
        if (image.getFileName().toString().matches("(?i).*\\.jp(2|x|2000)")) {
            logger.debug("Reading with jpeg2000 ImageReader");
            Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("jpeg2000");

            while (readers.hasNext()) {
                ImageReader reader = readers.next();
                logger.trace("Found reader: {}", reader);
                if (reader != null) {
                    try (InputStream inStream = Files.newInputStream(image); ImageInputStream iis = ImageIO.createImageInputStream(inStream);) {
                        reader.setInput(iis);
                        int width = reader.getWidth(0);
                        int height = reader.getHeight(0);
                        if (width * height > 0) {
                            return new Dimension(width, height);
                        }
                        logger.error("Error reading image dimensions of {} with image reader {}", image, reader.getClass().getSimpleName());
                    } catch (IOException e) {
                        logger.error("Error reading {} with image reader {}", image, reader.getClass().getSimpleName());
                    }
                }
            }
            ImageReader reader = getOpenJpegReader();
            if (reader != null) {
                logger.trace("found openjpeg reader");
                try (InputStream inStream = Files.newInputStream(image); ImageInputStream iis = ImageIO.createImageInputStream(inStream);) {
                    reader.setInput(iis);
                    int width = reader.getWidth(0);
                    int height = reader.getHeight(0);
                    if (width * height > 0) {
                        return new Dimension(width, height);
                    }
                    logger.error("Error reading image dimensions of {} with image reader {}", image, reader.getClass().getSimpleName());
                } catch (IOException e) {
                    logger.error("Error reading {} with image reader {}", image, reader.getClass().getSimpleName());
                }
            } else {
                logger.debug("Not openjpeg image reader found");
            }
        }

        throw new IOException("No valid image reader found for 'jpeg2000'");

    }

    /**
     * <p>
     * getOpenJpegReader.
     * </p>
     *
     * @return a {@link javax.imageio.ImageReader} object.
     */
    public static ImageReader getOpenJpegReader() {
        ImageReader reader;
        try {
            Object readerSpi = Class.forName("de.digitalcollections.openjpeg.imageio.OpenJp2ImageReaderSpi").getConstructor().newInstance();
            reader = ((ImageReaderSpi) readerSpi).createReaderInstance();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        } catch (NoClassDefFoundError | ClassNotFoundException | IllegalAccessException | InstantiationException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            logger.warn("No openjpeg reader");
            return null;
        }
        return reader;
    }

    /**
     * 
     * @param indexObj
     * @param writeStrategy
     * @should add group id as anchor pi to certain docstructs
     * @should not add group doc if group id equals anchor pi
     */
    protected void addGroupDocs(IndexObject indexObj, ISolrWriteStrategy writeStrategy) {
        logger.trace("addGroupDocs");
        if (indexObj == null || writeStrategy == null) {
            return;
        }

        for (Entry<String, String> entry : indexObj.getGroupIds().entrySet()) {
            String groupIdField = entry.getKey();
            logger.info("groupIdField: {}", groupIdField);
            String groupSuffix = groupIdField.replace(SolrConstants.PREFIX_GROUPID, "");
            Map<String, String> moreMetadata = new HashMap<>();
            String titleField = "MD_TITLE_" + groupSuffix;
            for (LuceneField field : indexObj.getLuceneFields()) {
                if (field.getField().endsWith(groupSuffix) && !field.getField().startsWith(SolrConstants.PREFIX_GROUPID)
                        && !field.getField().startsWith("GROUPORDER_")) {
                    if (titleField.equals(field.getField())) {
                        moreMetadata.put(SolrConstants.LABEL, field.getValue());
                    }
                    // Add any MD_*_GROUPSUFFIX field to the group doc
                    moreMetadata.put(field.getField().replace("_" + groupSuffix, ""), field.getValue());
                }
            }
            String docstructType = groupSuffix.toLowerCase();
            moreMetadata.put(SolrConstants.DOCSTRCT, docstructType);
            if (indexObj.getAnchorPI() != null && indexObj.getAnchorPI().equals(entry.getValue())) {
                // Avoid overwriting an actual anchor with a group if the identifier is identical
                logger.info("Group ID equals Anchor PI '{}', skipping group doc...", indexObj.getAnchorPI());
                continue;
            }
            // TODO make configurable or smt
            if ("newspaper".equals(docstructType)) {
                indexObj.setAnchorPI(entry.getValue());
            }
            SolrInputDocument doc = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .checkAndCreateGroupDoc(groupIdField, indexObj.getGroupIds().get(groupIdField), moreMetadata, getNextIddoc());
            if (doc != null) {
                writeStrategy.addDoc(doc);
                if (logger.isDebugEnabled()) {
                    logger.debug("Created group document for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                }
            } else if (logger.isDebugEnabled()) {
                logger.debug("Group document already exists for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
            }
        }
    }

    /**
     * Adds the given {@link PhysicalElement}'s grouped metadata to the write strategy. Must be called after the page has been mapped to a docstruct,
     * so that all relevant metadata has been copied from the structure element.
     * 
     * @param page {@link PhysicalElement}
     * @param pi Record identifier
     * @param writeStrategy
     * @return Number of added group docs
     * @should add grouped metadata docs from given page to writeStrategy correctly
     */
    public int addGroupedMetadataDocsForPage(PhysicalElement page, String pi, ISolrWriteStrategy writeStrategy) {
        if (page == null) {
            throw new IllegalArgumentException("page may not be null");
        }
        if (writeStrategy == null) {
            throw new IllegalArgumentException("writeStrategy may not be null");
        }

        int count = 0;
        for (GroupedMetadata gmd : page.getGroupedMetadata()) {
            SolrInputDocument doc = SolrSearchIndex.createDocument(gmd.getFields());
            String iddoc = getNextIddoc();
            doc.addField(SolrConstants.IDDOC, iddoc);
            if (!doc.getFieldNames().contains(SolrConstants.GROUPFIELD)) {
                logger.warn("{} not set in grouped metadata doc {}, using IDDOC instead.", SolrConstants.GROUPFIELD,
                        doc.getFieldValue(SolrConstants.LABEL));
                doc.addField(SolrConstants.GROUPFIELD, iddoc);
            }
            doc.addField(SolrConstants.IDDOC_OWNER, page.getDoc().getFieldValue(SolrConstants.IDDOC));
            doc.addField(SolrConstants.DOCTYPE, DocType.METADATA.name());
            doc.addField(SolrConstants.PI_TOPSTRUCT, pi);

            // Add DC values to metadata doc
            for (String dc : SolrSearchIndex.getMetadataValues(page.getDoc(), SolrConstants.DC)) {
                doc.addField(SolrConstants.DC, dc);
            }

            // Copy access conditions to metadata docs
            for (String accessCondition : SolrSearchIndex.getMetadataValues(page.getDoc(), SolrConstants.ACCESSCONDITION)) {
                doc.addField(SolrConstants.ACCESSCONDITION, accessCondition);
            }

            writeStrategy.addDoc(doc);
            count++;
        }

        return count;
    }

    /**
     * <p>
     * Adds grouped metadata to the given write strategy as separate Solr documents. This method should be called AFTER
     * <code>IndexObject.groupedMetadataFields</code> has been populated completely.
     * </p>
     *
     * @param writeStrategy WriteStrategy that holds the added Solr documets
     * @param indexObj a {@link io.goobi.viewer.indexer.model.IndexObject} object.
     * @param groupedMetadataList The list of grouped metadata objects to add as Solr fields
     * @param ownerIddoc Value for IDDOC_OWNER
     * @return number of created docs
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should add docs correctly
     * @should set PI_TOPSTRUCT to child docstruct metadata
     * @should set DOCSTRCT_TOP
     * @should skip fields correctly
     * @should add authority metadata to group metadata docs correctly
     * @should add authority metadata to docstruct doc correctly except coordinates
     * @should add coordinates to docstruct doc correctly
     * @should recursively add child metadata
     */
    public int addGroupedMetadataDocs(ISolrWriteStrategy writeStrategy, IndexObject indexObj, List<GroupedMetadata> groupedMetadataList,
            String ownerIddoc) throws FatalIndexerException {
        if (groupedMetadataList == null || groupedMetadataList.isEmpty()) {
            return 0;
        }

        int count = 0;
        List<LuceneField> dcFields = indexObj.getLuceneFieldsWithName(SolrConstants.DC);
        Set<String> skipFields = new HashSet<>();
        logger.debug("{} has {} grouped metadata elements.", indexObj.getLogId(), groupedMetadataList.size());
        for (GroupedMetadata gmd : groupedMetadataList) {
            count += addGroupedMetadataDocs(gmd, writeStrategy, indexObj, ownerIddoc, skipFields, dcFields);
        }

        return count;
    }

    /**
     * 
     * @param gmd
     * @param writeStrategy
     * @param indexObj
     * @param ownerIddoc
     * @param skipFields
     * @param dcFields
     * @return Number of added Solr docs
     * @throws FatalIndexerException
     * @should throw IllegalArgumentException if gmd null
     * @should throw IllegalArgumentException indexObj null
     * @should throw IllegalArgumentException if writeStrategy null
     * @should add BOOL_WKT_COORDINATES true to docstruct if WKT_COORDS found
     */
    int addGroupedMetadataDocs(GroupedMetadata gmd, ISolrWriteStrategy writeStrategy, IndexObject indexObj, String ownerIddoc, Set<String> skipFields,
            List<LuceneField> dcFields) throws FatalIndexerException {
        if (gmd == null) {
            throw new IllegalArgumentException("gmd may not be null");
        }
        if (indexObj == null) {
            throw new IllegalArgumentException("indexObj may not be null");
        }
        if (writeStrategy == null) {
            throw new IllegalArgumentException("writeStrategy may not be null");
        }

        if (gmd.isSkip()) {
            return 0;
        }

        // Skip if no MD_VALUE found
        if (gmd.getMainValue() == null) {
            logger.debug("No main value found on grouped field {}, skipping...", gmd.getLabel());
            return 0;
        }

        int count = 0;
        List<LuceneField> fieldsToAddToGroupDoc = new ArrayList<>(gmd.getFields().size() + gmd.getAuthorityDataFields().size());
        fieldsToAddToGroupDoc.addAll(gmd.getFields());
        if (gmd.isAddAuthorityDataToDocstruct() || gmd.isAddCoordsToDocstruct()) {
            // Add authority data to docstruct doc instead of grouped metadata
            for (LuceneField field : gmd.getAuthorityDataFields()) {
                if (gmd.isAddAuthorityDataToDocstruct() && (field.getField().startsWith("BOOL_") || field.getField().startsWith("SORT_"))) {
                    // Only add single valued fields once

                    // Skip BOOL_WKT_COORDS, if not explicitly configured to add coordinate fields
                    if (field.getField().equals(MetadataHelper.FIELD_HAS_WKT_COORDS) && !gmd.isAddCoordsToDocstruct()) {
                        fieldsToAddToGroupDoc.add(field);
                        continue;
                    }

                    if (skipFields.contains(field.getField())) {
                        continue;
                    }
                    skipFields.add(field.getField());
                } else if ((field.getField().startsWith("WKT_") || field.getField().startsWith(Record.AUTOCOORDS_FIELD)
                        || field.getField().startsWith("NORM_COORDS_")) && !gmd.isAddCoordsToDocstruct()) {
                    // Do not add coordinates to docstruct field, unless explicitly configured
                    fieldsToAddToGroupDoc.add(field);
                    continue;
                } else {
                    // Avoid field+value duplicates for all other fields
                    if (skipFields.contains(field.getField() + field.getValue())) {
                        continue;
                    }
                    skipFields.add(field.getField() + field.getValue());
                }
                switch (field.getField()) {
                    case MetadataHelper.FIELD_HAS_WKT_COORDS:
                        // Skip BOOL_WKT_COORDS
                        continue;
                    case MetadataHelper.FIELD_WKT_COORDS:
                        // Add BOOL_WKT_COORDS=true manually, instead
                        boolean boolAlreadySet = false;
                        for (LuceneField f : indexObj.getLuceneFields()) {
                            if (f.getField().equals(MetadataHelper.FIELD_HAS_WKT_COORDS)) {
                                f.setValue("true");
                                boolAlreadySet = true;
                                break;
                                // Just make sure the first instance of this field is "true", the rest will be sanitized eventually
                            }
                        }
                        if (!boolAlreadySet) {
                            indexObj.getLuceneFields().add(new LuceneField(MetadataHelper.FIELD_HAS_WKT_COORDS, "true"));
                        }
                        break;
                    case "NORM_TYPE":
                        // Fields to add to the metadata doc anyway
                        fieldsToAddToGroupDoc.add(field);
                        continue;
                    default:
                        break;
                }
                indexObj.getLuceneFields().add(field);
                logger.debug("added field: {}:{}", field.getField(), field.getValue());
            }
        } else {
            fieldsToAddToGroupDoc.addAll(gmd.getAuthorityDataFields());
        }
        SolrInputDocument doc = SolrSearchIndex.createDocument(fieldsToAddToGroupDoc);
        String iddoc = getNextIddoc();
        doc.addField(SolrConstants.IDDOC, iddoc);
        if (!doc.getFieldNames().contains(SolrConstants.GROUPFIELD)) {
            logger.warn("{} not set in grouped metadata doc {}, using IDDOC instead.", SolrConstants.GROUPFIELD,
                    doc.getFieldValue(SolrConstants.LABEL));
            doc.addField(SolrConstants.GROUPFIELD, iddoc);
        }
        doc.addField(SolrConstants.IDDOC_OWNER, ownerIddoc);
        doc.addField(SolrConstants.DOCTYPE, DocType.METADATA.name());
        doc.addField(SolrConstants.PI_TOPSTRUCT, indexObj.getTopstructPI());

        // Add topstruct type
        if (!doc.containsKey(SolrConstants.DOCSTRCT_TOP) && indexObj.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP) != null) {
            doc.setField(SolrConstants.DOCSTRCT_TOP, indexObj.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP).getValue());
        }

        // Add access conditions
        // Commented out because inherited access conditions trigger metadata locking where not wanted
        //        if (!doc.containsKey(SolrConstants.ACCESSCONDITION)) {
        //            for (String s : indexObj.getAccessConditions()) {
        //                doc.addField(SolrConstants.ACCESSCONDITION, s);
        //            }
        //        }

        // Add DC values to metadata doc
        if (dcFields != null) {
            for (LuceneField field : dcFields) {
                doc.addField(field.getField(), field.getValue());
            }
        }

        writeStrategy.addDoc(doc);
        count++;

        // Recursively add children
        if (!gmd.getChildren().isEmpty()) {
            logger.debug("{}:{} has {} child metadata entries.", gmd.getLabel(), gmd.getMainValue(), gmd.getChildren().size());
            count += addGroupedMetadataDocs(writeStrategy, indexObj, gmd.getChildren(), iddoc);
        }

        return count;
    }

    /**
     * 
     * @param dataFolders
     * @param paramNames
     * @param pi
     * @throws IOException
     * @should throw IllegalArgumentException if pi null
     */
    protected void checkOldDataFolders(Map<String, Path> dataFolders, String[] paramNames, String pi) throws IOException {
        if (pi == null) {
            throw new IllegalArgumentException(StringConstants.ERROR_PI_MAY_NOT_BE_NULL);
        }
        if (dataFolders == null || paramNames == null) {
            return;
        }

        for (String paramName : paramNames) {
            checkOldDataFolder(dataFolders, paramName, pi);
        }
    }

    /**
     * Checks for old data folder of the <code>paramName</code> type and puts it into <code>dataFolders</code>, if none yet present.
     *
     * @param dataFolders a {@link java.util.Map} object.
     * @param paramName a {@link java.lang.String} object.
     * @param pi a {@link java.lang.String} object.
     * @throws java.io.IOException
     * @should throw IllegalArgumentException if dataFolders null
     * @should throw IllegalArgumentException if paramName null
     * @should throw IllegalArgumentException if pi null
     */
    void checkOldDataFolder(Map<String, Path> dataFolders, String paramName, String pi) throws IOException {
        if (dataFolders == null) {
            throw new IllegalArgumentException(StringConstants.ERROR_DATAFOLDERS_MAY_NOT_BE_NULL);
        }
        if (paramName == null) {
            throw new IllegalArgumentException("paramName may not be null");
        }
        if (pi == null) {
            throw new IllegalArgumentException(StringConstants.ERROR_PI_MAY_NOT_BE_NULL);
        }

        // New data folder found in hotfolder
        if (dataFolders.get(paramName) != null) {
            return;
        }

        // Use the old data folder (old repository, if present, otherwise new)
        DataRepository useDataRepository = previousDataRepository != null ? previousDataRepository : dataRepository;
        // If there is an inconsistency in data folder distribution due to previous errors, look in both repositories
        if (useDataRepository.getDir(paramName) == null && useDataRepository.equals(previousDataRepository)) {
            useDataRepository = dataRepository;
        }
        // No old data folder found
        if (useDataRepository.getDir(paramName) == null) {
            return;
        }

        dataFolders.put(paramName, Paths.get(useDataRepository.getDir(paramName).toAbsolutePath().toString(), pi));
        if (!Files.isDirectory(dataFolders.get(paramName))) {
            dataFolders.put(paramName, null);

            // Create ALTO dir for converted ABBYY or TEI files
            if (DataRepository.PARAM_ALTO.equals(paramName) && useDataRepository.getDir(DataRepository.PARAM_ALTO) != null
                    && (dataFolders.get(DataRepository.PARAM_ABBYY) != null || dataFolders.get(DataRepository.PARAM_TEIWC) != null)) {
                dataFolders.put(DataRepository.PARAM_ALTO_CONVERTED,
                        Paths.get(useDataRepository.getDir(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), pi));
                Files.createDirectory(dataFolders.get(DataRepository.PARAM_ALTO_CONVERTED));
            }
        } else {
            logger.info("Using old '{}' data folder '{}'.", paramName, dataFolders.get(paramName).toAbsolutePath());
        }
    }

    /**
     * <p>
     * Getter for the field <code>dataRepository</code>.
     * </p>
     *
     * @return the dataRepository
     */
    public DataRepository getDataRepository() {
        return dataRepository;
    }

    /**
     * <p>
     * Setter for the field <code>dataRepository</code>.
     * </p>
     *
     * @param dataRepository the dataRepository to set
     */
    public void setDataRepository(DataRepository dataRepository) {
        this.dataRepository = dataRepository;
    }

    /**
     * <p>
     * Getter for the field <code>previousDataRepository</code>.
     * </p>
     *
     * @return the previousDataRepository
     */
    public DataRepository getPreviousDataRepository() {
        return previousDataRepository;
    }

    /**
     * <p>
     * Setter for the field <code>previousDataRepository</code>.
     * </p>
     *
     * @param previousDataRepository the previousDataRepository to set
     */
    public void setPreviousDataRepository(DataRepository previousDataRepository) {
        this.previousDataRepository = previousDataRepository;
    }

    /**
     * Download the content if the given fileUrl into the given Path target. If target denotes a directory, create a file within using the filename
     * from the URI to write the content.
     * 
     * @param fileUrl
     * @param targetPath
     * @param targetFileName
     * @return The file the data was written into
     * @throws IOException
     * @throws MalformedURLException
     */
    protected String downloadExternalImage(String fileUrl, final Path targetPath, String targetFileName) throws URISyntaxException, IOException {
        Path useTargetPath = targetPath;
        if (Files.isDirectory(useTargetPath)) {
            if (StringUtils.isNotEmpty(targetFileName)) {
                useTargetPath = useTargetPath.resolve(targetFileName);
            } else {
                String fileName = Path.of(URI.create(fileUrl).getPath()).getFileName().toString();
                useTargetPath = useTargetPath.resolve(fileName);
            }
        }
        httpConnector.downloadFile(new URI(fileUrl), useTargetPath);
        if (Files.isRegularFile(useTargetPath)) {
            logger.info("Downloaded {}", useTargetPath);
            return useTargetPath.toAbsolutePath().toString();
        }

        throw new IOException("Failed to write file '" + useTargetPath + "' from url '" + fileUrl + "'");
    }

    /**
     * 
     * @param url
     * @return int[]
     * @should fetch dimensions correctly
     */
    protected static int[] getImageDimensionsFromIIIF(String url) {
        if (StringUtils.isEmpty(url)) {
            return new int[0];
        }

        try {
            URI uri = new URI(url);
            JSONTokener tokener = new JSONTokener(uri.toURL().openStream());
            JSONObject root = new JSONObject(tokener);
            int width = root.getInt("width");
            int height = root.getInt("height");
            return new int[] { width, height };
        } catch (IOException | JSONException | URISyntaxException e) {
            logger.error("Could not fetch JSON from '{}': {}", url, e.getMessage());
        }

        return new int[0];
    }

    /**
     * Checks whether the document represents an anchor.
     * 
     * 
     * @return Always false in the default implementation
     * @throws FatalIndexerException
     */
    boolean isAnchor() throws FatalIndexerException {
        return false;
    }

    /**
     * Checks whether this is a volume of a multivolume work (should be false for monographs and anchors).
     * 
     * @return boolean
     * 
     */
    protected boolean isVolume() {
        return false;
    }

    /**
     * Selects the appropriate data repository for the given record.
     * 
     * @param indexObj
     * @param pi
     * @param recordFile
     * @param dataFolders
     * @throws FatalIndexerException
     */
    protected void selectDataRepository(IndexObject indexObj, String pi, Path recordFile, Map<String, Path> dataFolders)
            throws FatalIndexerException {
        DataRepository[] repositories =
                hotfolder.getDataRepositoryStrategy()
                        .selectDataRepository(pi, recordFile, dataFolders, SolrIndexerDaemon.getInstance().getSearchIndex(),
                                SolrIndexerDaemon.getInstance().getOldSearchIndex());
        dataRepository = repositories[0];
        previousDataRepository = repositories[1];
        if (StringUtils.isNotEmpty(dataRepository.getPath())) {
            indexObj.setDataRepository(dataRepository.getPath());
        }
    }

    /**
     * Prepares the given record for an update. Creation timestamp and representative thumbnail and anchor IDDOC are preserved. A new update timestamp
     * is added, child docs are removed.
     *
     * @param indexObj {@link io.goobi.viewer.indexer.model.IndexObject}
     * @throws java.io.IOException
     * @throws org.apache.solr.client.solrj.SolrServerException
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should keep creation timestamp
     * @should set update timestamp correctly
     * @should keep representation thumbnail
     * @should keep anchor IDDOC
     * @should delete anchor secondary docs
     */
    protected void prepareUpdate(IndexObject indexObj) throws IOException, SolrServerException, FatalIndexerException {
        String pi = indexObj.getPi().trim();
        SolrDocumentList hits = SolrIndexerDaemon.getInstance().getSearchIndex().search(SolrConstants.PI + ":" + pi, null);
        // Retrieve record from old index, if available
        boolean fromOldIndex = false;
        if (hits.getNumFound() == 0 && SolrIndexerDaemon.getInstance().getOldSearchIndex() != null) {
            hits = SolrIndexerDaemon.getInstance().getOldSearchIndex().search(SolrConstants.PI + ":" + pi, null);
            if (hits.getNumFound() > 0) {
                fromOldIndex = true;
                logger.info("Retrieving data from old index for record '{}'.", pi);
            }
        }
        if (hits.getNumFound() == 0) {
            return;
        }

        logger.debug("This file has already been indexed, initiating an UPDATE instead...");
        indexObj.setUpdate(true);
        SolrDocument doc = hits.get(0);
        // Set creation timestamp, if exists (should never be updated)
        Object dateCreated = doc.getFieldValue(SolrConstants.DATECREATED);
        if (dateCreated != null) {
            // Set creation timestamp, if exists (should never be updated)
            indexObj.setDateCreated((Long) dateCreated);
        }
        // Collect update timestamps
        Collection<Object> dateUpdatedValues = doc.getFieldValues(SolrConstants.DATEUPDATED);
        if (dateUpdatedValues != null) {
            for (Object date : dateUpdatedValues) {
                indexObj.getDateUpdated().add((Long) date);
            }
        }
        // Collect index timestamps
        Collection<Object> dateIndexedValues = doc.getFieldValues(SolrConstants.DATEINDEXED);
        if (dateIndexedValues != null) {
            for (Object date : dateIndexedValues) {
                indexObj.getDateIndexed().add((Long) date);
            }
        }
        // Set previous representation thumbnail, if available
        Object thumbnail = doc.getFieldValue(SolrConstants.THUMBNAILREPRESENT);
        if (thumbnail != null) {
            indexObj.setThumbnailRepresent((String) thumbnail);
        }
        if (isAnchor()) {
            // Keep old IDDOC
            indexObj.setIddoc(String.valueOf(doc.getFieldValue(SolrConstants.IDDOC)));
            // Delete old doc
            iddocsToDelete.add(indexObj.getIddoc());
            // Delete secondary docs (grouped metadata, events)
            hits = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(SolrConstants.IDDOC_OWNER + ":" + indexObj.getIddoc() + " " + SolrConstants.PI_TOPSTRUCT + ":" + indexObj.getPi(),
                            Collections.singletonList(SolrConstants.IDDOC));
            for (SolrDocument doc2 : hits) {
                iddocsToDelete.add((String) doc2.getFieldValue(SolrConstants.IDDOC));
            }
        } else if (!fromOldIndex) {
            // Recursively delete all children, if not an anchor
            iddocsToDelete = deleteWithPI(pi, false, false, SolrIndexerDaemon.getInstance().getSearchIndex());
        }
    }

    /**
     * Handles remote and local image file URLs (including optional download).
     * 
     * @param url
     * @param doc
     * @param fileName
     * @param mediaTargetPath
     * @param sbImgFileNames
     * @param downloadExternalImages
     * @param useOldImageFolderIfAvailable
     * @param representative
     */
    protected void handleImageUrl(String url, SolrInputDocument doc, String fileName, Path mediaTargetPath, StringBuilder sbImgFileNames,
            boolean downloadExternalImages, boolean useOldImageFolderIfAvailable, boolean representative) {
        if (StringUtils.isEmpty(url)) {
            return;
        }
        if (doc == null) {
            throw new IllegalArgumentException(StringConstants.ERROR_DOC_MAY_NOT_BE_NULL);
        }
        if (fileName == null) {
            throw new IllegalArgumentException("fileName may not be null");
        }

        // External image
        if (url.startsWith("http")) {
            // Download image, if so requested (and not a local resource)
            String viewerUrl = SolrIndexerDaemon.getInstance().getConfiguration().getViewerUrl();
            logger.debug("media folder: {}", mediaTargetPath);
            if (downloadExternalImages && mediaTargetPath != null && viewerUrl != null && !url.startsWith(viewerUrl)) {
                // Download image and use locally
                try {
                    File file = new File(downloadExternalImage(url, mediaTargetPath, fileName));
                    if (file.isFile()) {
                        logger.info("Downloaded {}", file);
                        sbImgFileNames.append(';').append(fileName);
                        doc.addField(SolrConstants.FILENAME, fileName);

                        // Representative image (local)
                        if (representative) {
                            doc.addField(SolrConstants.THUMBNAILREPRESENT, fileName);
                        }
                    } else {
                        logger.warn("Could not download file: {}", url);
                    }
                } catch (IOException | URISyntaxException e) {
                    logger.error("Could not download image: {}: {}", url, e.getMessage());
                }
            } else if (mediaTargetPath != null && useOldImageFolderIfAvailable) {
                // If image previously downloaded, use local version, when re-indexing
                doc.addField(SolrConstants.FILENAME, fileName);
                // Representative image (local)
                if (representative) {
                    doc.addField(SolrConstants.THUMBNAILREPRESENT, fileName);
                }
            } else {
                // Add external image URL
                doc.addField(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED, url);
                // Representative image (external)
                if (representative) {
                    doc.addField(SolrConstants.THUMBNAILREPRESENT, url);
                }
            }
        } else {
            // For non-remote file, add the file name to the list
            sbImgFileNames.append(';').append(fileName);
            // Representative image (local)
            if (representative) {
                doc.addField(SolrConstants.THUMBNAILREPRESENT, fileName);
            }
        }

        // Mime type
        parseMimeType(doc, fileName);
    }

    /**
     * 
     * @param doc {@link SolrInputDocument}
     * @param filePath
     * @should parse mime type from mp4 file correctly
     */
    static void parseMimeType(SolrInputDocument doc, String filePath) {
        if (doc == null) {
            throw new IllegalArgumentException(StringConstants.ERROR_DOC_MAY_NOT_BE_NULL);
        }
        if (StringUtils.isEmpty(filePath)) {
            return;
        }
        // Add full path if this is a local file or download has failed or is disabled
        if (!doc.containsKey(SolrConstants.FILENAME)) {
            doc.addField(SolrConstants.FILENAME, filePath);
        }

        // fileName: base name without path if file; full if URL
        String fileName = (String) doc.getFieldValue(SolrConstants.FILENAME);
        if (!fileName.startsWith("http")) {
            fileName = FilenameUtils.getName(fileName);
        }
        String mimetype = "image";
        String subMimetype = "";
        if (doc.containsKey(SolrConstants.FILENAME)) {
            // Determine mime type from file content
            try {
                mimetype = Files.probeContentType(Paths.get(filePath));
                if (StringUtils.isBlank(mimetype)) {
                    mimetype = "image";
                } else if (mimetype.contains("/")) {
                    subMimetype = mimetype.substring(mimetype.indexOf("/") + 1);
                }
            } catch (IOException e) {
                logger.warn("Cannot determine mime type from '{}', using 'image'.", filePath);
            }
        }

        if (StringUtils.isNotBlank(mimetype)) {
            doc.addField(SolrConstants.MIMETYPE, mimetype);
            if (StringUtils.isNotBlank(subMimetype)) {
                doc.addField(SolrConstants.FILENAME + "_" + subMimetype.toUpperCase(), fileName);
            }
        }
    }

    /**
     * 
     * @param doc
     * @param dataFolder
     * @param fileName
     */
    protected static void addFileSizeToDoc(SolrInputDocument doc, Path dataFolder, String fileName) {
        if (doc == null) {
            throw new IllegalArgumentException("doc may not be null");
        }

        try {
            // TODO other mime types/folders
            if (dataFolder != null && fileName != null) {
                Path path = Paths.get(dataFolder.toAbsolutePath().toString(), fileName);
                if (Files.isRegularFile(path)) {
                    doc.addField(FIELD_FILESIZE, Files.size(path));
                }
            }
        } catch (IllegalArgumentException | IOException e) {
            logger.warn(e.getMessage());
        }
        if (!doc.containsKey(FIELD_FILESIZE)) {
            doc.addField(FIELD_FILESIZE, -1);
        }
    }

    /**
     * 
     * @param hotfolderPath
     * @param fileNameRoot
     * @return Map<String, Path>
     * @throws IOException
     * @should add data folder paths correctly
     */
    static Map<String, Path> checkDataFolders(Path hotfolderPath, String fileNameRoot) throws IOException {
        Map<String, Path> dataFolders = new HashMap<>();

        try (DirectoryStream<Path> stream =
                Files.newDirectoryStream(hotfolderPath, new StringBuilder(fileNameRoot).append("_*").toString())) {
            for (Path path : stream) {
                logger.info(LOG_FOUND_DATA_FOLDER, path.getFileName());
                String fileNameSansRoot = path.getFileName().toString().substring(fileNameRoot.length());
                switch (fileNameSansRoot) {
                    case "_tif", FOLDER_SUFFIX_MEDIA: // GBVMETSAdapter uses _media folders
                        dataFolders.put(DataRepository.PARAM_MEDIA, path);
                        break;
                    case "_txt":
                        dataFolders.put(DataRepository.PARAM_FULLTEXT, path);
                        break;
                    case FOLDER_SUFFIX_TXTCROWD:
                        dataFolders.put(DataRepository.PARAM_FULLTEXTCROWD, path);
                        break;
                    case "_wc":
                        dataFolders.put(DataRepository.PARAM_TEIWC, path);
                        break;
                    case "_neralto":
                        dataFolders.put(DataRepository.PARAM_ALTO, path);
                        logger.info("NER ALTO folder found: {}", path.getFileName());
                        break;
                    case "_alto":
                        // Only add regular ALTO path if no NER ALTO folder is found
                        if (dataFolders.get(DataRepository.PARAM_ALTO) == null) {
                            dataFolders.put(DataRepository.PARAM_ALTO, path);
                        }
                        break;
                    case FOLDER_SUFFIX_ALTOCROWD:
                        dataFolders.put(DataRepository.PARAM_ALTOCROWD, path);
                        break;
                    case "_xml":
                        dataFolders.put(DataRepository.PARAM_ABBYY, path);
                        break;
                    case "_pdf":
                        dataFolders.put(DataRepository.PARAM_PAGEPDF, path);
                        break;
                    case "_mix":
                        dataFolders.put(DataRepository.PARAM_MIX, path);
                        break;
                    case "_src":
                        dataFolders.put(DataRepository.PARAM_SOURCE, path);
                        break;
                    case "_ugc":
                        dataFolders.put(DataRepository.PARAM_UGC, path);
                        break;
                    case "_cms":
                        dataFolders.put(DataRepository.PARAM_CMS, path);
                        break;
                    case "_tei":
                        dataFolders.put(DataRepository.PARAM_TEIMETADATA, path);
                        break;
                    case "_annotations":
                        dataFolders.put(DataRepository.PARAM_ANNOTATIONS, path);
                        break;
                    case FOLDER_SUFFIX_DOWNLOADIMAGES:
                        dataFolders.put(DataRepository.PARAM_DOWNLOAD_IMAGES_TRIGGER, path);
                        break;
                    default:
                        // nothing
                }
            }
        }

        return dataFolders;
    }

    /**
     * Checks
     * 
     * @param dataFolders
     * @param reindexSettings
     * @should throw IllegalArgumentException if dataFolders null
     * @should throw IllegalArgumentException if reindexSettings null
     * @should add reindex flags correctly if data folders missing
     * @should not add reindex flags if data folders present
     */
    static void checkReindexSettings(Map<String, Path> dataFolders, Map<String, Boolean> reindexSettings) {
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null");
        }
        if (reindexSettings == null) {
            throw new IllegalArgumentException("reindexSettings may not be null");
        }

        // Use existing folders for those missing in the hotfolder
        if (dataFolders.get(DataRepository.PARAM_MEDIA) == null) {
            reindexSettings.put(DataRepository.PARAM_MEDIA, true);
        }
        if (dataFolders.get(DataRepository.PARAM_ALTO) == null) {
            reindexSettings.put(DataRepository.PARAM_ALTO, true);
        }
        if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) == null) {
            reindexSettings.put(DataRepository.PARAM_ALTOCROWD, true);
        }
        if (dataFolders.get(DataRepository.PARAM_FULLTEXT) == null) {
            reindexSettings.put(DataRepository.PARAM_FULLTEXT, true);
        }
        if (dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) == null) {
            reindexSettings.put(DataRepository.PARAM_FULLTEXTCROWD, true);
        }
        if (dataFolders.get(DataRepository.PARAM_TEIWC) == null) {
            reindexSettings.put(DataRepository.PARAM_TEIWC, true);
        }
        if (dataFolders.get(DataRepository.PARAM_ABBYY) == null) {
            reindexSettings.put(DataRepository.PARAM_ABBYY, true);
        }
        if (dataFolders.get(DataRepository.PARAM_MIX) == null) {
            reindexSettings.put(DataRepository.PARAM_MIX, true);
        }
        if (dataFolders.get(DataRepository.PARAM_UGC) == null) {
            reindexSettings.put(DataRepository.PARAM_UGC, true);
        }
        if (dataFolders.get(DataRepository.PARAM_CMS) == null) {
            reindexSettings.put(DataRepository.PARAM_CMS, true);
        }
        if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) == null) {
            reindexSettings.put(DataRepository.PARAM_TEIMETADATA, true);
        }
        if (dataFolders.get(DataRepository.PARAM_ANNOTATIONS) == null) {
            reindexSettings.put(DataRepository.PARAM_ANNOTATIONS, true);
        }
    }

    /**
     * Send a request to the viewer rest api to start a task to prerender page PDF files if it is required. The task is required if either
     * hasNewMediaFiles is true, of if a non-empty _media folder exists in the {@link #getDataRepository()}. In the first case, always overwrite any
     * existing page PDFs; in the second case, only do so if {@link Configuration#isForcePrerenderPdfs()} is true
     * 
     * @param pi The identifier of the process to create pdfs for
     */
    void prerenderPagePdfsIfRequired(String pi) {
        try {
            Path mediaFolder = this.dataRepository.getDir(DataRepository.PARAM_MEDIA);
            if (mediaFolder != null && !FileTools.isFolderEmpty(mediaFolder)) {
                boolean force = SolrIndexerDaemon.getInstance().getConfiguration().isForcePrerenderPdfs();
                logger.debug("Reindexed process with media files: Trigger prerenderPDFs task in viewer; overwrite existing files: {}", pi);
                Utils.prerenderPdfs(pi, force);
            }
        } catch (IOException | HTTPException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * 
     * @param fileName File name to check
     * @param pageDoc
     * @return Alternative file name if fileName not image and alternatives available; otherwise fileName
     * @should replace value correctly
     * @should return fileName if image
     * @should return fileNAme if url
     */
    static String checkThumbnailFileName(String fileName, SolrInputDocument pageDoc) {
        String ret = fileName;
        if (!Utils.isValidImageOrIiifURI(fileName) && !FileTools.isImageFile(fileName) && pageDoc != null) {
            String filenameTiffField = SolrConstants.FILENAME + "_TIFF";
            String filenameJpegField = SolrConstants.FILENAME + "_JPEG";
            if (pageDoc.getFieldValue(filenameTiffField) != null) {
                ret = (String) pageDoc.getFieldValue(filenameTiffField);
                logger.info("Using {}:{} for {}", filenameTiffField, ret, SolrConstants.THUMBNAIL);
            } else if (pageDoc.getFieldValue(filenameJpegField) != null) {
                ret = (String) pageDoc.getFieldValue(filenameJpegField);
                logger.info("Using {}:{} for {}", filenameJpegField, ret, SolrConstants.THUMBNAIL);
            } else {
                logger.warn("'{}' is not a valid thumbnail file name, but no alternative was found.", fileName);
            }
        }

        return ret;
    }

    /**
     * 
     * @param order
     * @param iddoc
     * @param physId
     * @return {@link PhysicalElement}
     */
    protected static PhysicalElement createPhysicalElement(int order, String iddoc, String physId) {
        PhysicalElement ret = new PhysicalElement(order);
        ret.getDoc().addField(SolrConstants.IDDOC, iddoc);
        ret.getDoc().addField(SolrConstants.GROUPFIELD, iddoc);
        ret.getDoc().addField(SolrConstants.DOCTYPE, DocType.PAGE.name());
        ret.getDoc().addField(SolrConstants.ORDER, order);
        ret.getDoc().addField(SolrConstants.PHYSID, physId);

        return ret;
    }

    /**
     * Adds image size, mime type and image/full-text availability metadata fields.
     * 
     * @param page
     * @param dataFolders
     * @param fileName
     */
    protected void addPageAdditionalTechMetadata(PhysicalElement page, Map<String, Path> dataFolders, String fileName) {
        if (page == null) {
            throw new IllegalArgumentException("page may not be null");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null");
        }

        // Add file size
        addFileSizeToDoc(page.getDoc(), dataFolders.get(DataRepository.PARAM_MEDIA), fileName);

        // Add image dimension values from EXIF
        if (!page.getDoc().containsKey(SolrConstants.WIDTH) || !page.getDoc().containsKey(SolrConstants.HEIGHT)) {
            ImageSizeReader.getSize(dataFolders.get(DataRepository.PARAM_MEDIA), (String) page.getDoc().getFieldValue(SolrConstants.FILENAME))
                    .ifPresent(dimension -> {
                        page.getDoc().addField(SolrConstants.WIDTH, dimension.width);
                        page.getDoc().addField(SolrConstants.HEIGHT, dimension.height);
                    });
        }

        // FIELD_IMAGEAVAILABLE indicates whether this page has an image
        if (page.getDoc().containsKey(SolrConstants.FILENAME) && page.getDoc().containsKey(SolrConstants.MIMETYPE)
                && ((String) page.getDoc().getFieldValue(SolrConstants.MIMETYPE)).startsWith("image")) {
            page.getDoc().addField(FIELD_IMAGEAVAILABLE, true);
            recordHasImages = true;
        } else {
            page.getDoc().addField(FIELD_IMAGEAVAILABLE, false);
        }

        // FULLTEXTAVAILABLE indicates whether this page has full-text
        if (page.getDoc().getField(SolrConstants.FULLTEXT) != null) {
            page.getDoc().addField(SolrConstants.FULLTEXTAVAILABLE, true);
            recordHasFulltext = true;
        } else {
            page.getDoc().addField(SolrConstants.FULLTEXTAVAILABLE, false);
        }
    }

    protected abstract FileFormat getSourceDocFormat();
}
