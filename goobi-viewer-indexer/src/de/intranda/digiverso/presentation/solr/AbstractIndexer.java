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

import java.awt.Dimension;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

import de.intranda.digiverso.ocr.alto.model.structureclasses.logical.AltoDocument;
import de.intranda.digiverso.ocr.alto.utils.AltoDeskewer;
import de.intranda.digiverso.presentation.solr.helper.Hotfolder;
import de.intranda.digiverso.presentation.solr.helper.JDomXP;
import de.intranda.digiverso.presentation.solr.helper.MetadataHelper;
import de.intranda.digiverso.presentation.solr.helper.SolrHelper;
import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;
import de.intranda.digiverso.presentation.solr.model.IndexerException;
import de.intranda.digiverso.presentation.solr.model.LuceneField;
import de.intranda.digiverso.presentation.solr.model.SolrConstants;
import de.intranda.digiverso.presentation.solr.model.SolrConstants.DocType;
import de.intranda.digiverso.presentation.solr.model.datarepository.DataRepository;

public abstract class AbstractIndexer {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(AbstractIndexer.class);

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
     * are written into the given page's own search field.
     * 
     * @param pageDoc
     * @param folder
     * @param order
     * @param fileNameRoot
     * @return
     * @throws FatalIndexerException
     */
    List<SolrInputDocument> generateUserGeneratedContentDocsForPage(SolrInputDocument pageDoc, Path folder, String pi, int order, String fileNameRoot)
            throws FatalIndexerException {
        List<SolrInputDocument> ret = new ArrayList<>();

        if (folder != null && Files.isDirectory(folder)) {
            Path file = Paths.get(folder.toAbsolutePath().toString(), fileNameRoot + AbstractIndexer.XML_EXTENSION);
            if (Files.isRegularFile(file)) {
                try (FileInputStream fis = new FileInputStream(file.toFile())) {
                    Document xmlDoc = new SAXBuilder().build(fis);
                    if (xmlDoc != null && xmlDoc.getRootElement() != null) {
                        List<Element> eleContentList = xmlDoc.getRootElement().getChildren();
                        if (eleContentList != null) {
                            //                            logger.info("Found " + eleContentList.size() + " user generated contents for file " + file.getName());
                            StringBuilder sbTerms = new StringBuilder();
                            for (Element eleContent : eleContentList) {
                                SolrInputDocument doc = new SolrInputDocument();
                                long iddoc = getNextIddoc(hotfolder.getSolrHelper());
                                doc.addField(SolrConstants.IDDOC, iddoc);
                                doc.addField(SolrConstants.GROUPFIELD, iddoc);
                                doc.addField(SolrConstants.DOCTYPE, DocType.UGC.name());
                                doc.addField(SolrConstants.PI_TOPSTRUCT, pi);
                                doc.addField(SolrConstants.ORDER, order);
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
                                                doc.addField(SolrConstants.UGCCOORDS,
                                                        MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                                                break;
                                            case "firstname":
                                                doc.addField("MD_FIRSTNAME", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                                                break;
                                            case "lastname":
                                                doc.addField("MD_LASTNAME", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                                                break;
                                            case "personIdentifier":
                                                doc.addField("MD_PERSONIDENTIFIER",
                                                        MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                                                break;
                                            case "title":
                                                doc.addField("MD_CORPORATION", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                                                break;
                                            case "address":
                                                doc.addField("MD_ADDRESS", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                                                break;
                                            case "corporationIdentifier":
                                                doc.addField("MD_CORPORATIONIDENTIFIER",
                                                        MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                                                break;
                                            case "street":
                                                doc.addField("MD_STREET", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
                                                break;
                                            case "houseNumber":
                                                doc.addField("MD_HOUSENUMBER", MetadataHelper.applyValueDefaultModifications(eleField.getValue()));
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
                                ret.add(doc);
                            }
                            // Add plaintext terms to a search field in the page doc
                            if (StringUtils.isNotBlank(sbTerms.toString())) {
                                pageDoc.addField(SolrConstants.UGCTERMS, sbTerms.toString());
                                //                                logger.info("Added search terms to page " + order + " :" + sbTerms.toString().trim());
                            }
                        }
                    }
                } catch (FileNotFoundException e) {
                    logger.error(e.getMessage());
                } catch (JDOMException e) {
                    logger.error(e.getMessage(), e);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        return ret;
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
     * Retrieves the image size (width/height) for the image referenced in the given page document
     * The image sizes are retrieved from image metadata. if this doesn't work, no image sizes are set
     * 
     * @param dataFolders   The data folders which must include the {@link DataRepository#PARAM_MEDIA} folder containing the image
     * @param doc           the page document pertaining to the image
     * @return
     */
    static Optional<Dimension> getSize(Map<String, Path> dataFolders, SolrInputDocument doc) {
        logger.trace("deskewAlto");
        String filename = (String) doc.getFieldValue(SolrConstants.FILENAME);
        if (filename != null && dataFolders.get(DataRepository.PARAM_MEDIA) != null) {
            File imageFile = new File(filename);
            imageFile = new File(dataFolders.get(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), imageFile.getName());
            if (!imageFile.isFile()) {
                return Optional.empty();
            }
            logger.trace("Found image file {}", imageFile.getAbsolutePath());
            Dimension imageSize = new Dimension(0, 0);
            try {
                Metadata imageMetadata = ImageMetadataReader.readMetadata(imageFile);
                Directory jpegDirectory = imageMetadata.getFirstDirectoryOfType(JpegDirectory.class);
                Directory exifDirectory = imageMetadata.getFirstDirectoryOfType(ExifIFD0Directory.class);
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
                
                if(imageSize.getHeight()*imageSize.getHeight() > 0) {
                    return Optional.of(imageSize);
                }

            } catch (ImageProcessingException | IOException e) {
                if (e.getMessage().contains("File format is not supported")) {
                    logger.warn("{}: {}", e.getMessage(), filename);
                } else {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return Optional.empty();
    }
    
    /**
     * 
     * @param dataFolders
     * @param doc
     */
    static void deskewAlto(Map<String, Path> dataFolders, SolrInputDocument doc) {
        logger.trace("deskewAlto");
        String alto = (String) doc.getFieldValue(SolrConstants.ALTO);
        String filename = (String) doc.getFieldValue(SolrConstants.FILENAME);
        if (filename != null && alto != null && dataFolders.get(DataRepository.PARAM_MEDIA) != null) {
            File imageFile = new File(filename);
            imageFile = new File(dataFolders.get(DataRepository.PARAM_MEDIA).toAbsolutePath().toString(), imageFile.getName());
            if (!imageFile.isFile()) {
                return;
            }
            logger.trace("Found image file {}", imageFile.getAbsolutePath());
            Dimension imageSize = new Dimension(0, 0);
            try {
                AltoDocument altoDoc = AltoDocument.getDocumentFromString(alto);
                Metadata imageMetadata = ImageMetadataReader.readMetadata(imageFile);
                Directory jpegDirectory = imageMetadata.getFirstDirectoryOfType(JpegDirectory.class);
                Directory exifDirectory = imageMetadata.getFirstDirectoryOfType(ExifIFD0Directory.class);
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

                String solrWidthString = (String) doc.getFieldValue(SolrConstants.WIDTH);
                if (solrWidthString == null) {
                    logger.debug("{} not found, cannot deskew ALTO.", SolrConstants.WIDTH);
                    return;
                }
                int solrWidth = Integer.parseInt(solrWidthString);
                //                int solrHeight = Integer.parseInt((String) doc.getFieldValue(SolrConstants.HEIGHT));

                if (imageSize.width > 0 && doc.getFieldValue(SolrConstants.WIDTH) != null && delta(imageSize.width, solrWidth) > 2) {

                    if (delta(imageSize.width, solrWidth) > 0.15 * imageSize.width) {
                        return; //if alto-size differs more than 15% in width from image size, don't deskew
                    }

                    logger.trace("Rotating alto coordinates to size {}x{}", imageSize.width, imageSize.height);
                    AltoDeskewer deskewer = new AltoDeskewer();
                    AltoDocument outputDoc = deskewer.deskewAlto(altoDoc, imageSize, "tr");
                    String output = AltoDocument.getStringFromDomDocument(new Document(outputDoc.writeToDom()));
                    if (output != null && !output.isEmpty()) {
                        doc.setField(SolrConstants.ALTO, output);
                        doc.setField(SolrConstants.WIDTH, outputDoc.getFirstPage().getWidth());
                        doc.setField(SolrConstants.HEIGHT, outputDoc.getFirstPage().getHeight());
                    }
                }

            } catch (ImageProcessingException | IOException e) {
                if (e.getMessage().contains("File format is not supported")) {
                    logger.warn("{}: {}", e.getMessage(), filename);
                } else {
                    logger.error("Could not deskew ALTO for image '{}', please check the image file.", imageFile.getAbsolutePath());
                    logger.error(e.getMessage(), e);
                }
            } catch (JDOMException e) {
                logger.error(e.getMessage(), e);
            }
        } else {
            logger.trace("Cannot deskew ALTO: Image file is {} and alto text has length of {}", filename, alto != null ? alto.length() : "0");
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
            return name.endsWith(AbstractIndexer.XML_EXTENSION);
        }
    };
}
