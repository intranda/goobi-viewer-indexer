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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.exif.ExifIFD0Directory;
import com.drew.metadata.jpeg.JpegDirectory;
import com.drew.metadata.png.PngDirectory;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.intranda.api.annotation.GeoLocation;
import de.intranda.api.annotation.ISelector;
import de.intranda.api.annotation.wa.FragmentSelector;
import de.intranda.api.annotation.wa.SpecificResource;
import de.intranda.api.annotation.wa.TextualResource;
import de.intranda.api.annotation.wa.WebAnnotation;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrHelper;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.helper.WebAnnotationTools;
import io.goobi.viewer.indexer.model.FatalIndexerException;
import io.goobi.viewer.indexer.model.GroupedMetadata;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.IndexerException;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

public abstract class Indexer {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(Indexer.class);

    public static final String XML_EXTENSION = ".xml";
    public static final String TXT_EXTENSION = ".txt";

    public static boolean noTimestampUpdate = false;

    private static long nextIddoc = -1;

    // TODO cyclic dependency; find a more elegant way to select a repository w/o passing the hotfolder instance to the indexer
    protected Hotfolder hotfolder;

    /** XPath Parser. */
    protected JDomXP xp;

    protected DataRepository dataRepository;

    protected DataRepository previousDataRepository;

    protected StringBuilder sbLog = new StringBuilder();

    protected final Set<Integer> ugcAddedChecklist = new HashSet<>();

    /** Indicates whether any of this record's pages has full-text. */
    protected boolean recordHasFulltext = false;

    /**
     * Removes the document represented by the given METS or LIDO file from the index.
     * 
     * @param pi {@link String} Record identifier.
     * @param trace A Lucene document with DATEDELETED timestamp will be created if true.
     * @param hotfolder
     * @return {@link Boolean}
     * @throws IOException
     * @throws FatalIndexerException
     * @should delete METS record from index completely
     * @should delete LIDO record from index completely
     * @should leave trace document for METS record if requested
     * @should leave trace document for LIDO record if requested
     */
    public static boolean delete(String pi, boolean trace, SolrHelper solrHelper) throws IOException, FatalIndexerException {
        if (StringUtils.isEmpty(pi)) {
            throw new IllegalArgumentException("pi may not be empty or null.");
        }
        if (solrHelper == null) {
            throw new IllegalArgumentException("solrHelper may not be null.");
        }
        // Check whether this is an anchor record
        try {
            SolrDocumentList hits = solrHelper.search(new StringBuilder(SolrConstants.PI).append(":").append(pi).toString(),
                    Collections.singletonList(SolrConstants.ISANCHOR));
            if (!hits.isEmpty() && hits.get(0).getFieldValue(SolrConstants.ISANCHOR) != null
                    && (Boolean) hits.get(0).getFieldValue(SolrConstants.ISANCHOR)) {
                hits = solrHelper.search(SolrConstants.PI_PARENT + ":" + pi, Collections.singletonList(SolrConstants.PI));
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
            if (deleteWithPI(pi, trace, solrHelper)) {
                solrHelper.commit(SolrHelper.optimize);
                return true;
            }
        } catch (SolrServerException e) {
            logger.error(e.getMessage(), e);
            solrHelper.rollback();
        }

        return false;
    }

    /**
     * Löscht aus dem Index alle Documente die zu folgendem PI gehören. Das Löschen ist rekursiv. Unterelemente werden auch gelöscht.
     * 
     * @param pi String
     * @param createTraceDoc
     * @param solrHelper
     * @throws IOException
     * @throws SolrServerException
     * @throws FatalIndexerException
     */
    protected static boolean deleteWithPI(String pi, boolean createTraceDoc, SolrHelper solrHelper)
            throws IOException, SolrServerException, FatalIndexerException {
        Set<String> iddocsToDelete = new HashSet<>();

        String query = SolrConstants.PI + ":" + pi;
        SolrDocumentList hits = solrHelper.search(query, null);
        if (!hits.isEmpty()) {
            if (hits.getNumFound() == 1) {
                logger.info("Removing previous instance of this volume from the index...");
            } else {
                logger.warn(
                        "{} previous instances of this volume have been found in the index. This shouldn't ever be the case. Check whether there is more than one indexer instance running! All instances will be removed...",
                        hits.getNumFound());
            }
            String queryPageUrns = new StringBuilder(SolrConstants.PI_TOPSTRUCT).append(":")
                    .append(pi)
                    .append(" AND ")
                    .append(SolrConstants.DOCTYPE)
                    .append(":PAGE")
                    .toString();
            for (SolrDocument doc : hits) {
                String iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
                if (iddoc != null) {
                    logger.debug("Removing instance: {}", iddoc);
                    iddocsToDelete.add(iddoc);
                    if (createTraceDoc && doc.getFieldValue(SolrConstants.DATEDELETED) == null) {
                        // Build replacement document that is marked as deleted
                        String urn = null;
                        if (doc.getFieldValue(SolrConstants.URN) != null) {
                            urn = (String) doc.getFieldValue(SolrConstants.URN);
                        }
                        // Collect page URNs
                        hits = solrHelper.search(queryPageUrns, Collections.singletonList(SolrConstants.IMAGEURN));
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
                        createDeletedDoc(pi, urn, pageUrns, now, now, solrHelper);
                    }
                }
            }
        } else {
            logger.error("Not found: {}", pi);
        }

        // Retrieve all docs for this record via PI_TOPSTRUCT
        hits = solrHelper.search(new StringBuilder(SolrConstants.PI_TOPSTRUCT).append(":").append(pi).toString(),
                Collections.singletonList(SolrConstants.IDDOC));
        for (SolrDocument doc : hits) {
            String iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
            if (iddoc != null) {
                iddocsToDelete.add(iddoc);
            }
        }

        //        // This cleans up any docs that for some reason weren't deleted via the IDDOC route. Also, docs for user generated contents.
        //        {
        //            hits = solrHelper.search(SolrConstants.PI_TOPSTRUCT + ":" + pi, Collections.singletonList(SolrConstants.IDDOC));
        //            if (!hits.isEmpty()) {
        //                int numRegularDocsToDelete = iddocsToDelete.size();
        //                logger.debug("Removing " + hits.getNumFound() + " (possibly) lost subelements of this volume from the index...");
        //                for (SolrDocument doc : hits) {
        //                    String iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
        //                    if (iddoc != null && !iddocsToDelete.contains(iddoc)) {
        //                        iddocsToDelete.addAll(deleteWithIDDOC(iddoc, solrHelper));
        //                    }
        //                }
        //                int numLostDocs = iddocsToDelete.size() - numRegularDocsToDelete;
        //                if (numLostDocs > 0) {
        //                    logger.warn("Found " + numLostDocs
        //                            + " lost documents belonging to this record, but having no connection to the currently indexed document structure.");
        //                }
        //            }
        //        }

        boolean success = solrHelper.deleteDocuments(new ArrayList<>(iddocsToDelete));
        logger.info("{} docs deleted.", iddocsToDelete.size());

        return success;
    }

    /**
     * Recursively adds the given IDDOC and all IDDOCs of child documents to 'iddocsToDelete'. Does not perform the actual deletion, so it should only
     * be called by 'deleteWithPI()'! TODO This method is (almost) identical to MetsIndexer.deleteWithIDDOC()
     * 
     * @param inIddoc {@link String}
     * @param solrHelper
     * @return List of all IDDOC values from the document hierarchy.
     * @throws IOException -
     * @throws SolrServerException
     */
    @Deprecated
    protected static Set<String> deleteWithIDDOC(String inIddoc, SolrHelper solrHelper) throws IOException, SolrServerException {
        Set<String> ret = new HashSet<>();

        String iddoc = inIddoc.trim();
        // Add this IDDOC to the deletion list
        ret.add(iddoc);

        Set<String> childIddocs = new HashSet<>();

        // Child docstructs
        SolrDocumentList hits = solrHelper.search(SolrConstants.IDDOC_PARENT + ":" + iddoc, Collections.singletonList(SolrConstants.IDDOC));
        for (SolrDocument doc : hits) {
            childIddocs.add((String) doc.getFieldValue(SolrConstants.IDDOC));
        }

        // Pages and events
        hits = solrHelper.search(SolrConstants.IDDOC_OWNER + ":" + iddoc, Collections.singletonList(SolrConstants.IDDOC));
        for (SolrDocument doc : hits) {
            String iddoc2 = (String) doc.getFieldValue(SolrConstants.IDDOC);
            // Page and event docs don't have children - add directly
            if (StringUtils.isNotEmpty(iddoc2)) {
                ret.add(iddoc2);
            }
        }

        // Delete child documents recursively
        for (String cIddoc : childIddocs) {
            ret.addAll(deleteWithIDDOC(cIddoc, solrHelper));
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
     * @param dateCreated
     * @throws IOException
     * @throws NumberFormatException
     * @throws FatalIndexerException
     */
    private static void createDeletedDoc(String pi, String urn, List<String> pageUrns, String dateDeleted, String dateUpdated, SolrHelper solrHelper)
            throws NumberFormatException, IOException, FatalIndexerException {
        // Build replacement document that is marked as deleted
        logger.info("Creating 'DELETED' document for {}...", pi);
        List<LuceneField> fields = new ArrayList<>();
        String iddoc = String.valueOf(getNextIddoc(solrHelper));
        fields.add(new LuceneField(SolrConstants.IDDOC, iddoc));
        fields.add(new LuceneField(SolrConstants.GROUPFIELD, iddoc));

        fields.add(new LuceneField(SolrConstants.PI, pi));
        if (urn != null) {
            fields.add(new LuceneField(SolrConstants.URN, urn));
        }
        if (pageUrns != null) {
            // StringBuilder sbPageUrns = new StringBuilder();
            for (String pageUrn : pageUrns) {
                fields.add(new LuceneField(SolrConstants.IMAGEURN_OAI, pageUrn.replaceAll("[\\\\]", "")));
                // sbPageUrns.append(imageUrn).append(' ');
            }
            // fields.add(new LuceneField(SolrConstants.PAGEURNS, sbPageUrns.toString()));
        }
        fields.add(new LuceneField(SolrConstants.DATEDELETED, dateDeleted));
        fields.add(new LuceneField(SolrConstants.DATEUPDATED, dateUpdated));
        solrHelper.writeToIndex(SolrHelper.createDocument(fields));
    }

    /**
     * Returns the next available IDDOC value.
     * 
     * @param solrHelper
     * @return
     * @throws FatalIndexerException
     */
    protected static synchronized long getNextIddoc(SolrHelper solrHelper) throws FatalIndexerException {
        if (nextIddoc < 0) {
            // Only determine the next IDDOC from Solr once per indexer lifetime, otherwise it might return numbers that already exist
            nextIddoc = System.currentTimeMillis();
        }

        while (!solrHelper.checkIddocAvailability(nextIddoc)) {
            logger.debug("IDDOC '{}' is already taken.", nextIddoc);
            nextIddoc = System.currentTimeMillis();
        }

        long ret = nextIddoc;
        nextIddoc++;

        return ret;
    }

    /**
     * Replaces irrelevant characters in the DEFAULT field value with spaces.
     * 
     * @param field
     * @return
     * @should replace irrelevant chars with spaces correctly
     */
    public static String cleanUpDefaultField(String field) {
        if (field != null) {
            return field.replace(",", " ").replace(";", " ").replace(":", " ").replace("  ", " ").trim();
        }

        return null;
    }

    /**
     * Adds named entity fields from the given list to the given SolrInputDocument.
     * 
     * @param altoData
     * @param doc
     */
    @SuppressWarnings("unchecked")
    protected void addNamedEntitiesFields(Map<String, Object> altoData, SolrInputDocument doc) {
        List<String> neList = (List<String>) altoData.get("NAMEDENTITIES");
        if (neList != null && !neList.isEmpty()) {
            for (String ne : neList) {
                String[] splitString = ne.split("_", 2);
                if (splitString[1] != null) {
                    splitString[1] = cleanUpNamedEntityValue(splitString[1]);
                    String fieldName = new StringBuilder("NE_").append(splitString[0]).toString();
                    doc.addField(fieldName, splitString[1]);
                    doc.addField(new StringBuilder(fieldName).append(SolrConstants._UNTOKENIZED).toString(), splitString[1]);
                }
            }
        }
    }

    /**
     * Removes any non-alphanumeric trailing characters from the given string.
     * 
     * @param value
     * @return Cleaned up value.
     * @should clean up value correctly
     * @should throw IllegalArgumentException given null
     */
    protected static String cleanUpNamedEntityValue(String value) {
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

    /**
     * Parses the user generated content XML file for the given page and creates a Solr doc for each content entry. In addition, all content strings
     * are written into the UGC doc's own search field. To make sure the IDDOC_OWNER value is matches the one in the corresponding page, make sure
     * this method is called after all docstruct to page mapping is finished.
     * 
     * @param pageDoc
     * @param dataFolder
     * @param pi
     * @param anchorPi
     * @param order
     * @param fileNameRoot
     * @return List of Solr input documents for the UGC contents
     * @throws FatalIndexerException
     */
    List<SolrInputDocument> generateUserGeneratedContentDocsForPage(SolrInputDocument pageDoc, Path dataFolder, String pi, String anchorPi,
            Map<String, String> groupIds, int order, String fileNameRoot) throws FatalIndexerException {
        if (dataFolder == null || !Files.isDirectory(dataFolder)) {
            logger.info("UGC folder is empty.");
            return Collections.emptyList();
        }

        Path file = Paths.get(dataFolder.toAbsolutePath().toString(), fileNameRoot + Indexer.XML_EXTENSION);
        if (!Files.isRegularFile(file)) {
            logger.warn("'{}' is not a file.", file.getFileName().toString());
            return Collections.emptyList();
        }

        try (FileInputStream fis = new FileInputStream(file.toFile())) {
            Document xmlDoc = new SAXBuilder().build(fis);
            if (xmlDoc == null || xmlDoc.getRootElement() == null) {
                logger.info("Invalid XML in file '{}'.", file.getFileName().toString());
                return Collections.emptyList();
            }
            List<SolrInputDocument> ret = new ArrayList<>();
            List<Element> eleContentList = xmlDoc.getRootElement().getChildren();
            if (eleContentList == null || eleContentList.isEmpty()) {
                logger.info("No data found in file '{}'.", file.getFileName().toString());
                return Collections.emptyList();
            }
            logger.info("Found {} user generated contents in  file '{}'.", eleContentList.size(), file.getFileName().toString());
            for (Element eleContent : eleContentList) {
                StringBuilder sbTerms = new StringBuilder();
                SolrInputDocument doc = new SolrInputDocument();
                long iddoc = getNextIddoc(hotfolder.getSolrHelper());
                doc.addField(SolrConstants.IDDOC, iddoc);
                if (pageDoc != null && pageDoc.containsKey(SolrConstants.IDDOC_OWNER)) {
                    doc.addField(SolrConstants.IDDOC_OWNER, pageDoc.getFieldValue(SolrConstants.IDDOC_OWNER));
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
                    for (String fieldName : groupIds.keySet()) {
                        doc.addField(fieldName, groupIds.get(fieldName));
                    }
                }
                List<Element> eleFieldList = eleContent.getChildren();
                switch (eleContent.getName()) {
                    case "UserGeneratedPerson":
                        doc.addField(SolrConstants.UGCTYPE, SolrConstants._UGC_TYPE_PERSON);
                        break;
                    case "UserGeneratedCorporation":
                        doc.addField(SolrConstants.UGCTYPE, SolrConstants._UGC_TYPE_CORPORATION);
                        break;
                    case "UserGeneratedAddress":
                        doc.addField(SolrConstants.UGCTYPE, SolrConstants._UGC_TYPE_ADDRESS);
                        break;
                    case "UserGeneratedComment":
                        doc.addField(SolrConstants.UGCTYPE, SolrConstants._UGC_TYPE_COMMENT);
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
                                doc.addField("MD_FIRSTNAME", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                                break;
                            case "lastname":
                                doc.addField("MD_LASTNAME", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
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
                                doc.addField("MD_TEXT", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
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
                ret.add(doc);
            }
            // Add plaintext terms to a search field in the page doc
            //                            if (StringUtils.isNotBlank(sbAllTerms.toString())) {
            // pageDoc.addField(SolrConstants.UGCTERMS, sbAllTerms.toString());
            // logger.info("Added search terms to page " + order + " :" + sbTerms.toString().trim());
            //                            }

            return ret;
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        } catch (JDOMException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return Collections.emptyList();
    }

    /**
     * 
     * @param pageDocs
     * @param dataFolder
     * @param pi
     * @param anchorPi
     * @param groupIds
     * @param order
     * @return
     * @throws FatalIndexerException
     * @should create docs correctly
     */
    List<SolrInputDocument> generateAnnotationDocs(Map<Integer, SolrInputDocument> pageDocs, Path dataFolder, String pi, String anchorPi,
            Map<String, String> groupIds) throws FatalIndexerException {
        if (dataFolder == null || !Files.isDirectory(dataFolder)) {
            logger.info("Annotation folder is empty.");
            return Collections.emptyList();
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataFolder, "*.{json}")) {
            List<SolrInputDocument> ret = new ArrayList<>();
            for (Path path : stream) {
                long iddoc = getNextIddoc(hotfolder.getSolrHelper());
                ret.add(readAnnotation(path, iddoc, pi, anchorPi, pageDocs, groupIds));
            }

            return ret;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return Collections.emptyList();
    }

    /**
     * @param pageDocs
     * @param pi
     * @param anchorPi
     * @param groupIds
     * @param ret
     * @param path
     * @param iddoc
     * @return
     */
    public SolrInputDocument readAnnotation(Path path, long iddoc, String pi, String anchorPi, Map<Integer, SolrInputDocument> pageDocs,
            Map<String, String> groupIds) {
        try (FileInputStream fis = new FileInputStream(path.toFile())) {
            String json = TextHelper.readFileToString(path.toFile(), TextHelper.DEFAULT_ENCODING);
            WebAnnotation annotation = new ObjectMapper().readValue(json, WebAnnotation.class);
            if (annotation == null) {
                return null;
            }

            StringBuilder sbTerms = new StringBuilder();
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField(SolrConstants.IDDOC, iddoc);
            doc.addField(SolrConstants.GROUPFIELD, iddoc);
            doc.addField(SolrConstants.DOCTYPE, DocType.UGC.name());
            doc.addField(SolrConstants.PI_TOPSTRUCT, pi);
            Integer pageOrder = WebAnnotationTools.parsePageOrder(annotation.getTarget().getId());
            if (pageOrder == null) {
                // Map all non-page-specific annotations to page 1 for now
                pageOrder = 1;
            }
            doc.setField(SolrConstants.ORDER, pageOrder);

            // Look up owner page doc
            SolrInputDocument pageDoc = pageDocs.get(pageOrder);
            if (pageDoc != null && pageDoc.containsKey(SolrConstants.IDDOC)) {
                doc.addField(SolrConstants.IDDOC_OWNER, pageDoc.getFieldValue(SolrConstants.IDDOC));
            }

            if (StringUtils.isNotEmpty(anchorPi)) {
                doc.addField(SolrConstants.PI_ANCHOR, anchorPi);
            }
            // Add GROUPID_* fields
            if (groupIds != null && !groupIds.isEmpty()) {
                for (String fieldName : groupIds.keySet()) {
                    doc.addField(fieldName, groupIds.get(fieldName));
                }
            }

            // Value
            if (annotation.getBody() instanceof TextualResource) {
                doc.setField(SolrConstants.UGCTYPE, SolrConstants._UGC_TYPE_COMMENT);
                doc.addField("MD_TEXT", ((TextualResource) annotation.getBody()).getText());
            } else if (annotation.getBody() instanceof GeoLocation) {
                doc.setField(SolrConstants.UGCTYPE, SolrConstants._UGC_TYPE_ADDRESS);
                // Add searchable coordinates
                GeoLocation geoLocation = (GeoLocation) annotation.getBody();
                if (geoLocation.getGeometry() != null) {
                    double[] coords = geoLocation.getGeometry().getCoordinates();
                    if (coords.length == 2) {
                        doc.addField("MD_COORDS", coords[0] + " " + coords[1]);
                    }
                }
                // Add annotation body as JSON
                doc.addField("MD_BODY", annotation.getBody().toString());
            } else {
                logger.warn("Cannot interpret annotation body of type " + annotation.getBody().getClass());
            }

            if (annotation.getTarget() instanceof SpecificResource) {
                // Coords
                ISelector selector = ((SpecificResource) annotation.getTarget()).getSelector();
                if (selector instanceof FragmentSelector) {
                    String coords = ((FragmentSelector) selector).getValue();
                    doc.addField(SolrConstants.UGCCOORDS, MetadataHelper.applyValueDefaultModifications(coords));
                    doc.setField(SolrConstants.UGCTYPE, SolrConstants._UGC_TYPE_ADDRESS);
                }
            }

            sbTerms.append(doc.getFieldValue(SolrConstants.UGCTYPE)).append(" ");
            sbTerms.append(doc.getFieldValue("MD_TEXT")).append(" ");

            if (StringUtils.isNotBlank(sbTerms.toString())) {
                doc.setField(SolrConstants.UGCTERMS, sbTerms.toString().trim());
            }

            return doc;
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * Generates Solr docs for legacy UGC and WebAnnotation files and adds them to the write strategy.
     * 
     * @param writeStrategy
     * @param dataFolders
     * @param indexObj
     * @throws FatalIndexerException
     */
    protected void writeUserGeneratedContents(ISolrWriteStrategy writeStrategy, Map<String, Path> dataFolders, IndexObject indexObj)
            throws FatalIndexerException {
        // Collect page docs for annotation<->page mapping
        Map<Integer, SolrInputDocument> pageDocs = new HashMap<>(writeStrategy.getPageDocsSize());

        // Add used-generated content docs from legacy crowdsourcing
        for (int i = 1; i <= writeStrategy.getPageDocsSize(); ++i) {
            SolrInputDocument pageDoc = writeStrategy.getPageDocForOrder(i);
            if (pageDoc == null) {
                logger.warn("Page {} not found, cannot check for UGC contents.", i);
                continue;
            }
            int order = (Integer) pageDoc.getFieldValue(SolrConstants.ORDER);
            String pageFileBaseName = FilenameUtils.getBaseName((String) pageDoc.getFieldValue(SolrConstants.FILENAME));
            if (dataFolders.get(DataRepository.PARAM_UGC) != null && !ugcAddedChecklist.contains(order)) {
                writeStrategy.addDocs(generateUserGeneratedContentDocsForPage(pageDoc, dataFolders.get(DataRepository.PARAM_UGC),
                        indexObj.getTopstructPI(), indexObj.getAnchorPI(), indexObj.getGroupIds(), order, pageFileBaseName));
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
     * @param xmlFile
     * @throws IOException
     * @throws JDOMException
     * @throws IndexerException
     * @throws FatalIndexerException
     */
    public void initJDomXP(Path xmlFile) throws IOException, JDOMException, IndexerException, FatalIndexerException {
        xp = new JDomXP(xmlFile.toFile());
        if (xp == null) {
            throw new IndexerException("Could not create XML parser.");
        }
    }

    /**
     * @param width
     * @param parseInt
     * @return
     */
    protected static int delta(int n, int m) {
        return Math.abs(n - m);
    }

    /**
     * Retrieves the image size (width/height) for the image referenced in the given page document The image sizes are retrieved from image metadata.
     * if this doesn't work, no image sizes are set
     * 
     * @param dataFolders The data folders which must include the {@link DataRepository#PARAM_MEDIA} folder containing the image
     * @param doc the page document pertaining to the image
     * @return
     * @should return size correctly
     */
    static Optional<Dimension> getSize(Path mediaFolder, String filename) {
        logger.trace("getSize");
        if (filename == null || mediaFolder == null) {
            return Optional.empty();
        }
        File imageFile = new File(filename);
        imageFile = new File(mediaFolder.toAbsolutePath().toString(), imageFile.getName());
        if (!imageFile.isFile()) {
            return Optional.empty();
        }
        logger.trace("Found image file {}", imageFile.getAbsolutePath());
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
            }
            try {
                imageSize.width = Integer.valueOf(exifDirectory.getDescription(256).replaceAll("\\D", ""));
                imageSize.height = Integer.valueOf(exifDirectory.getDescription(257).replaceAll("\\D", ""));
            } catch (NullPointerException e) {
            }
            try {
                imageSize.width = Integer.valueOf(jpegDirectory.getDescription(3).replaceAll("\\D", ""));
                imageSize.height = Integer.valueOf(jpegDirectory.getDescription(1).replaceAll("\\D", ""));
            } catch (NullPointerException e) {
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
                    logger.error("Unable to read image size: {}: {}", e.getMessage(), filename);
                }
            } catch (UnsatisfiedLinkError e3) {
                logger.error("Unable to load jpeg2000 ImageReader: " + e.toString());
            }
        }

        return Optional.empty();
    }

    public static Dimension getSizeForJp2(Path image) throws IOException {

        if (image.getFileName().toString().matches("(?i).*\\.jp(2|x|2000)")) {
            logger.debug("Reading with jpeg2000 ImageReader");
            Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("jpeg2000");

            while (readers.hasNext()) {
                ImageReader reader = readers.next();
                logger.trace("Found reader " + reader);
                if (reader != null) {
                    try (InputStream inStream = Files.newInputStream(image); ImageInputStream iis = ImageIO.createImageInputStream(inStream);) {
                        reader.setInput(iis);
                        int width = reader.getWidth(0);
                        int height = reader.getHeight(0);
                        if (width * height > 0) {
                            return new Dimension(width, height);
                        }
                        logger.error("Error reading image dimensions of " + image + " with image reader " + reader.getClass().getSimpleName());
                        continue;
                    } catch (IOException e) {
                        logger.error("Error reading " + image + " with image reader " + reader.getClass().getSimpleName());
                        continue;
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
                    logger.error("Error reading image dimensions of " + image + " with image reader " + reader.getClass().getSimpleName());
                } catch (IOException e) {
                    logger.error("Error reading " + image + " with image reader " + reader.getClass().getSimpleName());
                }
            } else {
                logger.debug("Not openjpeg image reader found");
            }
        }

        throw new IOException("No valid image reader found for 'jpeg2000'");

    }

    public static ImageReader getOpenJpegReader() {
        ImageReader reader;
        try {
            Object readerSpi = Class.forName("de.digitalcollections.openjpeg.imageio.OpenJp2ImageReaderSpi").newInstance();
            reader = ((ImageReaderSpi) readerSpi).createReaderInstance();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        } catch (NoClassDefFoundError | ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            logger.warn("No openjpeg reader");
            return null;
        }
        return reader;
    }

    /**
     * 
     * @param writeStrategy
     * @param indexObj
     * @return number of created docs
     * @throws FatalIndexerException
     * @should add docs correctly
     */
    public int addGroupedMetadataDocs(ISolrWriteStrategy writeStrategy, IndexObject indexObj) throws FatalIndexerException {
        // Add grouped metadata as separate documents
        int count = 0;
        for (GroupedMetadata gmd : indexObj.getGroupedMetadataFields()) {
            SolrInputDocument doc = SolrHelper.createDocument(gmd.getFields());
            long iddoc = getNextIddoc(hotfolder.getSolrHelper());
            doc.addField(SolrConstants.IDDOC, iddoc);
            if (!doc.getFieldNames().contains(SolrConstants.GROUPFIELD)) {
                logger.warn("{} not set in grouped metadata doc {}, using IDDOC instead.", SolrConstants.GROUPFIELD,
                        doc.getFieldValue(SolrConstants.LABEL));
                doc.addField(SolrConstants.GROUPFIELD, iddoc);
            }
            doc.addField(SolrConstants.IDDOC_OWNER, indexObj.getIddoc());
            doc.addField(SolrConstants.DOCTYPE, DocType.METADATA.name());
            doc.addField(SolrConstants.PI_TOPSTRUCT, indexObj.getPi());
            // Add access conditions
            for (String s : indexObj.getAccessConditions()) {
                doc.addField(SolrConstants.ACCESSCONDITION, s);
            }

            writeStrategy.addDoc(doc);
            count++;
        }

        return count;
    }

    /**
     * Checks for old data folder of the <code>paramName</code> type and puts it into <code>dataFolders</code>, if none yet present.
     * 
     * @param dataFolders
     * @param paramName
     * @param pi
     * @throws IOException
     */
    protected void checkOldDataFolder(Map<String, Path> dataFolders, String paramName, String pi) throws IOException {
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null");
        }
        if (paramName == null) {
            throw new IllegalArgumentException("paramName may not be null");
        }
        if (pi == null) {
            throw new IllegalArgumentException("pi may not be null");
        }

        if (dataFolders.get(paramName) == null) {
            // Use the old data folder
            DataRepository useDataRepository = previousDataRepository != null ? previousDataRepository : dataRepository;
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
                logger.info("Using old '{}' content folder '{}'.", paramName, dataFolders.get(paramName).toAbsolutePath());
            }
        }
    }

    /**
     * @return the dataRepository
     */
    public DataRepository getDataRepository() {
        return dataRepository;
    }

    /**
     * @param dataRepository the dataRepository to set
     */
    public void setDataRepository(DataRepository dataRepository) {
        this.dataRepository = dataRepository;
    }

    /**
     * @return the previousDataRepository
     */
    public DataRepository getPreviousDataRepository() {
        return previousDataRepository;
    }

    /**
     * @param previousDataRepository the previousDataRepository to set
     */
    public void setPreviousDataRepository(DataRepository previousDataRepository) {
        this.previousDataRepository = previousDataRepository;
    }

    public static FilenameFilter txt = new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
            return (name.endsWith(".txt"));
        }
    };

    public static FilenameFilter xml = new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(Indexer.XML_EXTENSION);
        }
    };
}
