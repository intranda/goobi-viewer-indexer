package io.goobi.viewer.indexer;

import java.awt.Dimension;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Attribute;
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

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.HttpConnector;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.StringConstants;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.helper.XmlTools;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.PhysicalElement;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.file.FileId;

public class ResourceDocumentBuilder {

    private static final Logger logger = LogManager.getLogger(Indexer.class);

    private static final int GENERATE_PAGE_DOCUMENT_TIMEOUT_HOURS = 6;

    private static final String FIELD_COORDS = "MD_COORDS";
    private static final String FIELD_IMAGEAVAILABLE = "BOOL_IMAGEAVAILABLE";
    private static final String FIELD_FILESIZE = "MDNUM_FILESIZE";
    private static final String FIELD_SHAPE = "MD_SHAPE";
    private static final String LOG_ADDED_FULLTEXT_FROM_REGULAR_ALTO = "Added FULLTEXT from regular ALTO for page {}";

    private static final String[] IIIF_IMAGE_FILE_NAMES =
            { ".*bitonal.(jpg|png|tif|jp2)$", ".*color.(jpg|png|tif|jp2)$", ".*default.(jpg|png|tif|jp2)$", ".*gray.(jpg|png|tif|jp2)$",
                    ".*native.(jpg|png|tif|jp2)$" };

    /** Constant <code>DEFAULT_FILEGROUP="DEFAULT"</code> */
    private static final String DEFAULT_FILEGROUP = "DEFAULT";
    /** Constant <code>ALTO_FILEGROUP="ALTO"</code> */
    private static final String ALTO_FILEGROUP = "ALTO";
    /** Constant <code>FULLTEXT_FILEGROUP="FULLTEXT"</code> */
    private static final String FULLTEXT_FILEGROUP = "FULLTEXT";

    private static final String ATTRIBUTE_CONTENTIDS = "CONTENTIDS";

    private static final String XPATH_FILE = "mets:file";

    private final String useFileGroupGlobal;
    private final JDomXP xp;
    private final HttpConnector httpConnector;
    private final DataRepository dataRepository;
    private final DocType docType;

    private boolean hasImages = false;
    private boolean hasFulltext = false;

    public ResourceDocumentBuilder(String fileGroup, JDomXP xp, HttpConnector httpConnector, DataRepository dataRepository, DocType docType) {
        this.useFileGroupGlobal = fileGroup;
        this.xp = xp;
        this.httpConnector = httpConnector;
        this.dataRepository = dataRepository;
        this.docType = docType;
    }

    /**
     * Generates a SolrInputDocument for each page that is mapped to a docstruct. Adds all page metadata except those that come from the owning
     * docstruct (such as docstruct iddoc, type, collection, etc.).
     *
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @param dataFolders a {@link java.util.Map} object.
     * @param dataRepository a {@link io.goobi.viewer.indexer.model.datarepository.DataRepository} object.
     * @param pi a {@link java.lang.String} object.
     * @param pageCountStart a int.
     * @param downloadExternalImages
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should create documents for all mapped pages
     * @should set correct ORDER values
     * @should skip unmapped pages
     * @should switch to DEFAULT file group correctly
     * @should maintain page order after parallel processing
     */
    public Collection<PhysicalElement> generatePageDocuments(final Map<String, Path> dataFolders, final String pi,
            Integer pageCountStart, boolean downloadExternalImages)
            throws InterruptedException, FatalIndexerException {
        // Get all physical elements
        String xpath = buildPagesXpathExpresson();
        List<Element> eleStructMapPhysicalList = xp.evaluateToElements(xpath, null);
        if (eleStructMapPhysicalList.isEmpty()) {
            logger.info("No pages found.");
            return Collections.emptyList();
        }

        logger.info("Image file group selected: {}", useFileGroupGlobal);
        logger.info("Generating {} page documents (count starts at {})...", eleStructMapPhysicalList.size(), pageCountStart);

        Collection<PhysicalElement> pages = Collections.synchronizedList(new ArrayList<PhysicalElement>());
        if (SolrIndexerDaemon.getInstance().getConfiguration().getThreads() > 1) {
            // Generate each page document in its own thread
            ConcurrentHashMap<String, Boolean> usedIddocsMap = new ConcurrentHashMap<>();
            try (ForkJoinPool pool = new ForkJoinPool(SolrIndexerDaemon.getInstance().getConfiguration().getThreads())) {
                pool.submit(() -> eleStructMapPhysicalList.parallelStream().forEach(eleStructMapPhysical -> {
                    try {
                        String iddoc = Indexer.getNextIddoc();
                        if (usedIddocsMap.containsKey(iddoc)) {
                            logger.error("Duplicate IDDOC: {}", iddoc);
                        }
                        PhysicalElement page =
                                generatePageDocument(eleStructMapPhysical, String.valueOf(iddoc), pi, null, dataFolders, dataRepository,
                                        downloadExternalImages);
                        if (page != null) {
                            pages.add(page);
                            // Shapes must be added as regular pages to the WriteStrategy to ensure correct docstrcut mapping
                            for (PhysicalElement shape : page.getShapes()) {
                                pages.add(shape);
                            }
                            page.getShapes().clear();
                        }
                        usedIddocsMap.put(iddoc, true);
                    } catch (FatalIndexerException e) {
                        logger.error("Should be exiting here now...");
                    }
                })).get(GENERATE_PAGE_DOCUMENT_TIMEOUT_HOURS, TimeUnit.HOURS);
            } catch (ExecutionException e) {
                logger.error(e.getMessage(), e);
                SolrIndexerDaemon.getInstance().stop();
            } catch (TimeoutException e) {
                throw new InterruptedException("Generating page documents timed out for object " + pi);
            }
        } else {
            // Generate pages sequentially
            Integer order = pageCountStart;
            for (final Element eleStructMapPhysical : eleStructMapPhysicalList) {
                PhysicalElement page = generatePageDocument(eleStructMapPhysical, String.valueOf(Indexer.getNextIddoc()),
                        pi, order, dataFolders, dataRepository, downloadExternalImages);
                if (page != null) {
                    pages.add(page);
                    // Shapes must be added as regular pages to the WriteStrategy to ensure correct docstrcut mapping
                    for (PhysicalElement shape : page.getShapes()) {
                        pages.add(shape);
                    }
                    page.getShapes().clear();

                    Optional.ofNullable(order).ifPresent(o -> o++);
                }
            }
        }
        logger.info("Generated {} pages.", pages.size());
        return pages;
    }

    public boolean isHasFulltext() {
        return hasFulltext;
    }

    public boolean isHasImages() {
        return hasImages;
    }

    /**
     * 
     * @param eleStructMapPhysical
     * @param iddoc
     * @param pi
     * @param inOrder
     * @param dataFolders
     * @param dataRepository
     * @param downloadExternalImages
     * @return Generated {@link PhysicalElement}
     * @throws FatalIndexerException
     * @should add all basic fields
     * @should add crowdsourcing ALTO field correctly
     * @should add crowdsourcing fulltext field correctly
     * @should add fulltext field correctly
     * @should create ALTO file from ABBYY correctly
     * @should create ALTO file from TEI correctly
     * @should create ALTO file from fileId if none provided in data folders
     * @should add FILENAME_HTML-SANDBOXED field for url paths
     * @should add width and height from techMD correctly
     * @should add width and height from ABBYY correctly
     * @should add width and height from MIX correctly
     * @should add page metadata correctly
     * @should add shape metadata as page documents
     */
    public PhysicalElement generatePageDocument(Element eleStructMapPhysical, String iddoc, String pi, final Integer inOrder,
            final Map<String, Path> dataFolders, final DataRepository dataRepository, boolean downloadExternalImages) throws FatalIndexerException {
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null");
        }
        if (dataRepository == null) {
            throw new IllegalArgumentException("dataRepository may not be null");
        }
        if (useFileGroupGlobal == null) {
            throw new IllegalStateException("useFileGroupGlobal not set");
        }

        String id = eleStructMapPhysical.getAttributeValue("ID");
        Integer order = getOrder(eleStructMapPhysical, inOrder);
        if (order == null) {
            logger.warn("ORDER attribute no found, skipping...");
            return null;
        }

        List<Element> eleStructLinkList = getStructLinks(iddoc, id);
        if (eleStructLinkList.isEmpty()) {
            logger.warn("Page {} (PHYSID: {}) is not mapped to a structure element, skipping...", order, id);
            return null;
        }

        // Create object for this page
        PhysicalElement ret = createPhysicalElement(order, iddoc, id, this.docType);
        ret.getDoc().addField(SolrConstants.PI_TOPSTRUCT, pi);

        List<Element> eleFptrList =
                eleStructMapPhysical.getChildren("fptr", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mets"));

        eleFptrList.forEach(eleFptr -> ret.getShapes().addAll(getShapes(order, eleFptr)));

        FileId useFileID = FileId.getFileId(getFileId(eleFptrList, useFileGroupGlobal), useFileGroupGlobal);
        if (useFileID != null) {
            ret.getDoc().addField(SolrConstants.FILEIDROOT, useFileID.getRoot());
        } else {
            return null;
        }

        setDoublePage(eleStructMapPhysical, ret);
        setOrderLabel(eleStructMapPhysical, ret);
        setImageUrn(eleStructMapPhysical, ret);
        setDmdId(eleStructMapPhysical, ret);

        String altoURL = null;
        // For each mets:fileGroup in the mets:fileSec
        String xpath = "/mets:mets/mets:fileSec/mets:fileGrp";
        List<Element> eleFileGrpList = xp.evaluateToElements(xpath, null);
        for (Element eleFileGrp : eleFileGrpList) {
            String fileGrpUse = eleFileGrp.getAttributeValue("USE");
            String fileGrpId = eleFileGrp.getAttributeValue("ID");
            logger.debug("Found file group: {}", fileGrpUse);
            logger.debug("fileId: {}", useFileID);

            // If fileId is not null, use an XPath expression for the appropriate file element,
            // otherwise get all file elements and get the one with the index of the page order
            String fileIdXPathCondition = getXPathCondition(useFileID, fileGrpId);
            int attrListIndex = useFileID != null ? 0 : order - 1;

            String filePath = getFilepath(useFileID, eleFileGrp, fileGrpUse, fileIdXPathCondition, attrListIndex);
            if (filePath == null) {
                if (useFileGroupGlobal.equals(fileGrpUse)) {
                    logger.warn("Skipping selected file group {} - nothing found at: {}", fileGrpUse, xpath);
                } else {
                    logger.debug("Skipping file group {}", fileGrpUse);
                }
                continue;
            }

            String fileName = getFilename(filePath);

            // Mime type
            xpath = XPATH_FILE + fileIdXPathCondition + "/@MIMETYPE";
            List<Attribute> mimetypeAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
            if (mimetypeAttrList == null || mimetypeAttrList.isEmpty()) {
                logger.error("{}: mime type not found in file group '{}'.", useFileID, fileGrpUse);
                break;
            }

            String mimetype = mimetypeAttrList.get(attrListIndex).getValue();
            if (StringUtils.isEmpty(mimetype)) {
                logger.error("{}: mime type is blank in file group '{}'.", useFileID, fileGrpUse);
                break;
            }

            String[] mimetypeSplit = mimetype.split("/");
            if (mimetypeSplit.length != 2) {
                logger.error("Illegal mime type '{}' in file group '{}'.", mimetype, fileGrpUse);
                break;
            }

            if (fileGrpUse.equals(useFileGroupGlobal)) {
                // The file name from the main file group (usually PRESENTATION or DEFAULT) will be used for thumbnail purposes etc.
                if (filePath.startsWith("http")) {
                    // Should write the full URL into FILENAME because otherwise a PI_TOPSTRUCT+FILENAME combination may no longer be unique
                    if (ret.getDoc().containsKey(SolrConstants.FILENAME)) {
                        logger.error("Page {} already contains FILENAME={}, but attempting to add another value from filegroup {}", iddoc,
                                filePath,
                                fileGrpUse);
                    }

                    String viewerUrl = SolrIndexerDaemon.getInstance().getConfiguration().getViewerUrl();
                    if (downloadExternalImages && dataFolders.get(DataRepository.PARAM_MEDIA) != null && viewerUrl != null
                            && !filePath.startsWith(viewerUrl)) {
                        logger.info("Downloading file: {}", filePath);
                        try {
                            filePath = Path.of(downloadExternalImage(filePath, dataFolders.get(DataRepository.PARAM_MEDIA), fileName))
                                    .getFileName()
                                    .toString();
                        } catch (IOException | URISyntaxException e) {
                            logger.warn("Could not download file: {}", filePath);
                        }
                    }
                    ret.getDoc().addField(SolrConstants.FILENAME, filePath);
                    if (!ret.getShapes().isEmpty()) {
                        for (PhysicalElement shape : ret.getShapes()) {
                            shape.getDoc().addField(SolrConstants.FILENAME, filePath);
                        }
                    }
                    // RosDok IIIF
                    //Don't use if images are downloaded. Then we haven them locally
                    if (!downloadExternalImages && DEFAULT_FILEGROUP.equals(useFileGroupGlobal)
                            && !ret.getDoc().containsKey(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED)) {
                        ret.getDoc().addField(SolrConstants.FILENAME + SolrConstants.SUFFIX_HTML_SANDBOXED, filePath);
                    }
                } else {
                    if (ret.getDoc().containsKey(SolrConstants.FILENAME)) {
                        logger.error("Page {} already contains FILENAME={}, but attempting to add another value from filegroup {}", iddoc, fileName,
                                fileGrpUse);
                    }
                    ret.getDoc().addField(SolrConstants.FILENAME, fileName);
                    if (!ret.getShapes().isEmpty()) {
                        for (PhysicalElement shape : ret.getShapes()) {
                            shape.getDoc().addField(SolrConstants.FILENAME, fileName);
                        }
                    }
                }

                // Add mime type
                ret.getDoc().addField(SolrConstants.MIMETYPE, mimetype);
                if (!ret.getShapes().isEmpty()) {
                    for (PhysicalElement shape : ret.getShapes()) {
                        shape.getDoc().addField(SolrConstants.MIMETYPE, mimetype);
                    }
                }

                // Add file size
                addFileSizeToDoc(ret.getDoc(), dataFolders.get(DataRepository.PARAM_MEDIA), fileName);
            } else if (fileGrpUse.equals(ALTO_FILEGROUP) || fileGrpUse.equals(FULLTEXT_FILEGROUP)) {
                altoURL = filePath;
            } else {
                String fieldName = SolrConstants.FILENAME + "_" + mimetypeSplit[1].toUpperCase();
                if (ret.getDoc().getField(fieldName) == null) {
                    switch (mimetypeSplit[1]) {
                        case "html-sandboxed":
                            // Add full URL
                            ret.getDoc().addField(SolrConstants.FILENAME + "_" + mimetypeSplit[1].toUpperCase(), filePath);
                            break;
                        case "object":
                            ret.getDoc().addField(SolrConstants.FILENAME, fileName);
                            ret.getDoc().addField(SolrConstants.MIMETYPE, mimetypeSplit[1]);
                            break;
                        default:
                            ret.getDoc().addField(SolrConstants.FILENAME + "_" + mimetypeSplit[1].toUpperCase(), fileName);
                    }
                }
            }

            // Width + height (from IIIF)
            if (SolrIndexerDaemon.getInstance().getConfiguration().isReadImageDimensionsFromIIIF()
                    && ret.getDoc().getField(SolrConstants.WIDTH) == null
                    && ret.getDoc().getField(SolrConstants.HEIGHT) == null && !downloadExternalImages && filePath != null
                    && filePath.endsWith("info.json")) {
                int[] dim = getImageDimensionsFromIIIF(filePath);
                if (dim.length == 2) {
                    ret.getDoc().addField(SolrConstants.WIDTH, dim[0]);
                    ret.getDoc().addField(SolrConstants.HEIGHT, dim[1]);
                    logger.debug("Added WIDTH from IIIF: {}", ret.getDoc().getFieldValue(SolrConstants.WIDTH));
                    logger.debug("Added HEIGHT from IIIF: {}", ret.getDoc().getFieldValue(SolrConstants.HEIGHT));
                }
            }

            // Width + height (from techMD)
            if (ret.getDoc().getField(SolrConstants.WIDTH) == null && ret.getDoc().getField(SolrConstants.HEIGHT) == null) {
                // Width + height (from techMD)
                xpath = XPATH_FILE + fileIdXPathCondition + "/@ADMID";
                List<Attribute> amdIdAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
                if (amdIdAttrList != null && !amdIdAttrList.isEmpty() && StringUtils.isNotBlank(amdIdAttrList.get(0).getValue())) {
                    String amdId = amdIdAttrList.get(0).getValue();
                    xpath = "/mets:mets/mets:amdSec/mets:techMD[@ID='" + amdId
                            + "']/mets:mdWrap[@MDTYPE='OTHER']/mets:xmlData/pbcoreInstantiation/formatFrameSize/text()";
                    String frameSize = xp.evaluateToString(xpath, null);
                    if (StringUtils.isNotEmpty(frameSize)) {
                        String[] frameSizeSplit = frameSize.split("x");
                        if (frameSizeSplit.length == 2) {
                            ret.getDoc().addField(SolrConstants.WIDTH, frameSizeSplit[0].trim());
                            ret.getDoc().addField(SolrConstants.HEIGHT, frameSizeSplit[1].trim());
                            logger.info("WIDTH: {}", ret.getDoc().getFieldValue(SolrConstants.WIDTH));
                            logger.info("HEIGHT: {}", ret.getDoc().getFieldValue(SolrConstants.HEIGHT));
                        } else {
                            logger.warn("Invalid formatFrameSize value in mets:techMD[@ID='{}']: '{}'", amdId, frameSize);
                        }
                    }
                }
            }

            // Width + height (invalid)
            if (ret.getDoc().getField(SolrConstants.WIDTH) == null && ret.getDoc().getField(SolrConstants.HEIGHT) == null) {
                xpath = XPATH_FILE + fileIdXPathCondition + "/@WIDTH";
                List<Attribute> widthAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
                Integer width = null;
                Integer height = null;
                if (widthAttrList != null && !widthAttrList.isEmpty() && StringUtils.isNotBlank(widthAttrList.get(0).getValue())) {
                    width = Integer.valueOf(widthAttrList.get(0).getValue());
                    logger.warn("mets:file[@ID='{}'] contains illegal WIDTH attribute. It will still be used, though.", useFileID);
                }
                xpath = XPATH_FILE + fileIdXPathCondition + "/@HEIGHT";
                List<Attribute> heightAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
                if (heightAttrList != null && !heightAttrList.isEmpty() && StringUtils.isNotBlank(heightAttrList.get(0).getValue())) {
                    height = Integer.valueOf(heightAttrList.get(0).getValue());
                    logger.warn("mets:file[@ID='{}'] contains illegal HEIGHT attribute. It will still be used, though.", useFileID);
                }
                if (width != null && height != null) {
                    ret.getDoc().addField(SolrConstants.WIDTH, width);
                    ret.getDoc().addField(SolrConstants.HEIGHT, height);
                }

            }
        }

        // FIELD_IMAGEAVAILABLE indicates whether this page has an image
        if (ret.getDoc().containsKey(SolrConstants.FILENAME) && ret.getDoc().containsKey(SolrConstants.MIMETYPE)
                && ((String) ret.getDoc().getFieldValue(SolrConstants.MIMETYPE)).startsWith("image")) {
            ret.getDoc().addField(FIELD_IMAGEAVAILABLE, true);
            this.hasImages = true;
        } else {
            ret.getDoc().addField(FIELD_IMAGEAVAILABLE, false);
        }

        if (dataFolders != null || altoURL != null) {
            addFullTextToPageDoc(ret.getDoc(), dataFolders, dataRepository, pi, order, altoURL);
        }

        // Page metadata
        String admId = eleStructMapPhysical.getAttributeValue("ADMID");
        if (StringUtils.isNotEmpty(admId)) {
            // Use '//' so faulty duplication in the hierarchy still works
            String techXpath = "/mets:mets/mets:amdSec/mets:techMD[@ID='" + admId + "']//mets:mdWrap[@MDTYPE='OTHER'][mets:xmlData/mix:mix]";
            List<Element> eletechMdList = xp.evaluateToElements(techXpath, null);
            if (!eletechMdList.isEmpty()) {
                IndexObject indexObj = new IndexObject("dummy", pi);
                indexObj.setSourceDocFormat(FileFormat.MIX);
                List<LuceneField> fields = MetadataHelper.retrieveElementMetadata(eletechMdList.get(0), "", indexObj, xp);
                for (LuceneField field : fields) {
                    if (!MetadataHelper.FIELD_HAS_WKT_COORDS.equals(field.getField())) {
                        ret.getDoc().addField(field.getField(), field.getValue());
                        logger.debug("Added simple techMD field: {}", field);
                    }
                }
                ret.getGroupedMetadata().addAll(indexObj.getGroupedMetadataFields());
            }
        }

        return ret;
    }

    protected String getFilename(String filePath) {
        String fileName;
        if (Utils.isFileNameMatchesRegex(filePath, IIIF_IMAGE_FILE_NAMES)) {
            // Extract correct original file name from IIIF
            fileName = Utils.getFileNameFromIiifUrl(filePath);
        } else {
            fileName = FilenameUtils.getName(filePath);
        }
        return fileName;
    }

    protected String getFilepath(FileId useFileID, Element eleFileGrp, String fileGrpUse, String fileIdXPathCondition, int attrListIndex) {
        String xpath;
        // Check whether the fileId_fileGroup pattern applies for this file group, otherwise just use the fileId
        xpath = XPATH_FILE + fileIdXPathCondition + "/mets:FLocat/@xlink:href";
        logger.debug(xpath);
        List<Attribute> filepathAttrList = xp.evaluateToAttributes(xpath, eleFileGrp);
        if (filepathAttrList == null || filepathAttrList.size() <= attrListIndex) {
            return null;
        }

        String filePath = filepathAttrList.get(attrListIndex).getValue();
        logger.trace("filePath: {}", filePath);
        if (StringUtils.isEmpty(filePath)) {
            return null;
        }
        return filePath;
    }

    protected String getXPathCondition(FileId useFileID, String fileGrpId) {
        String fileIdXPathCondition = "";
        if (useFileID != null) {
            if (StringUtils.isNotBlank(fileGrpId)) {
                // File ID may contain the value of ID instead of USE
                fileIdXPathCondition = "[@ID=\"" + useFileID.getFullId() + "\" or @ID=\"" + useFileID.getFullId(fileGrpId) + "\"]";
            } else {
                fileIdXPathCondition = "[@ID=\"" + useFileID.getFullId() + "\"]";
            }
        }
        return fileIdXPathCondition;
    }

    protected void setDmdId(Element eleStructMapPhysical, PhysicalElement ret) throws FatalIndexerException {
        String dmdId = eleStructMapPhysical.getAttributeValue(SolrConstants.DMDID);
        if (StringUtils.isNotEmpty(dmdId)) {
            IndexObject pageObj = new IndexObject("dummy");
            MetadataHelper.writeMetadataToObject(pageObj, xp.getMdWrap(dmdId), "", xp);
            for (LuceneField field : pageObj.getLuceneFields()) {
                ret.getDoc().addField(field.getField(), field.getValue());
            }
        }
    }

    protected void setImageUrn(Element eleStructMapPhysical, PhysicalElement ret) {
        String contentIDs = eleStructMapPhysical.getAttributeValue(ATTRIBUTE_CONTENTIDS);
        if (Utils.isUrn(contentIDs)) {
            ret.getDoc().addField(SolrConstants.IMAGEURN, contentIDs);
        }
    }

    protected void setDoublePage(Element eleStructMapPhysical, PhysicalElement ret) {
        // Double page view
        boolean doubleImage =
                "double page".equals(eleStructMapPhysical.getAttributeValue("label",
                        SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("xlink")));
        if (doubleImage) {
            ret.getDoc().addField(SolrConstants.BOOL_DOUBLE_IMAGE, doubleImage);
        }
    }

    protected void setOrderLabel(Element eleStructMapPhysical, PhysicalElement ret) {
        String orderLabel = eleStructMapPhysical.getAttributeValue("ORDERLABEL");
        if (StringUtils.isNotEmpty(orderLabel)) {
            ret.getDoc().addField(SolrConstants.ORDERLABEL, orderLabel);
        } else {
            if (StringUtils.isNotEmpty(orderLabel)) {
                ret.getDoc().addField(SolrConstants.ORDERLABEL, orderLabel);
            } else {
                ret.getDoc().addField(SolrConstants.ORDERLABEL, SolrIndexerDaemon.getInstance().getConfiguration().getEmptyOrderLabelReplacement());
            }
        }
    }

    /**
     * Get the `FILEID`property containing the string fileGroup
     * 
     * @param eleFptrList The list of all file pointers
     * @param fileGroup The file group to use
     * @return The file id as string. An empty string if no file pointer for the fileGroup is found, and null if fileGroup is null or an empty string
     */

    protected String getFileId(List<Element> eleFptrList, String fileGroup) {
        String useFileID = null;
        for (Element eleFptr : eleFptrList) {
            String fileID = eleFptr.getAttributeValue("FILEID");
            logger.trace("fileID: {}", fileID);
            if (fileID.contains(fileGroup)) {
                useFileID = fileID;
            }
        }
        if (fileGroup != null && StringUtils.isEmpty(useFileID)) {
            logger.warn("FILEID not found for file group {}", fileGroup);
            useFileID = "";
        }
        return useFileID;
    }

    protected List<PhysicalElement> getShapes(Integer order, Element eleFptr) {
        List<PhysicalElement> shapes = new ArrayList<>();
        if (eleFptr.getChild("seq", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mets")) != null) {
            List<Element> eleListArea = eleFptr.getChild("seq", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mets"))
                    .getChildren("area", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mets"));
            if (eleListArea != null && !eleListArea.isEmpty()) {
                int count = 1;
                for (Element eleArea : eleListArea) {
                    String coords = eleArea.getAttributeValue("COORDS");
                    String physId = eleArea.getAttributeValue("ID");
                    String shape = eleArea.getAttributeValue(DocType.SHAPE.name());

                    PhysicalElement shapePage = new PhysicalElement(Utils.generateLongOrderNumber(order, count));
                    shapePage.getDoc().addField(SolrConstants.IDDOC, Indexer.getNextIddoc());
                    shapePage.getDoc().setField(SolrConstants.DOCTYPE, DocType.SHAPE.name());
                    shapePage.getDoc().addField(SolrConstants.ORDER, shapePage.getOrder());
                    shapePage.getDoc().addField(SolrConstants.PHYSID, physId);
                    shapePage.getDoc().addField(FIELD_COORDS, coords);
                    shapePage.getDoc().addField(FIELD_SHAPE, shape);
                    shapePage.getDoc().addField("ORDER_PARENT", order);
                    shapes.add(shapePage);
                    count++;
                    logger.debug("Added SHAPE page document: {}", shapePage.getOrder());
                }
            }
        }
        return shapes;
    }

    protected List<Element> getStructLinks(String iddoc, String id) {
        logger.trace("generatePageDocument: {} (IDDOC {}) processed by thread {}", id, iddoc, Thread.currentThread().threadId());
        // Check whether this physical element is mapped to any logical element, skip if not
        StringBuilder sbXPath = new StringBuilder(70);
        sbXPath.append("/mets:mets/mets:structLink/mets:smLink[@xlink:to=\"").append(id).append("\"]");
        List<Element> eleStructLinkList = xp.evaluateToElements(sbXPath.toString(), null);
        return eleStructLinkList;
    }

    protected Integer getOrder(Element eleStructMapPhysical, final Integer inOrder) {
        Integer order = inOrder;
        if (order == null) {
            String orderValue = eleStructMapPhysical.getAttributeValue("ORDER");
            if (StringUtils.isNotEmpty(orderValue)) {
                order = Integer.parseInt(orderValue);
            } else {
                return null;
            }
        }
        return order;
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
    private String downloadExternalImage(String fileUrl, final Path targetPath, String targetFileName) throws URISyntaxException, IOException {
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
     * @param doc Page Solr input document
     * @param dataFolders Folder paths containing full-text files
     * @param dataRepo
     * @param pi Record identifier
     * @param order Page number
     * @param altoURL Optional URL for ALTO download
     */
    private void addFullTextToPageDoc(SolrInputDocument doc, Map<String, Path> dataFolders, DataRepository dataRepo, String pi, int order,
            String altoURL) {
        if (doc == null || dataFolders == null) {
            return;
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
                doc.addField(SolrConstants.FILENAME_FULLTEXT, dataRepo.getDir(DataRepository.PARAM_FULLTEXTCROWD).getFileName().toString()
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
                doc.addField(SolrConstants.FILENAME_FULLTEXT, dataRepo
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
                if (StringUtils.startsWithIgnoreCase(altoURL, "http")) {
                    // HTTP(S)
                    logger.debug("Downloading ALTO from {}", altoURL);
                    alto = Utils.getWebContentGET(altoURL);
                } else if (StringUtils.startsWithIgnoreCase(altoURL, "file:/")) {
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
                                        Paths.get(dataRepo.getDir(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), pi));
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
            getSize(dataFolders.get(DataRepository.PARAM_MEDIA), (String) doc.getFieldValue(SolrConstants.FILENAME)).ifPresent(dimension -> {
                doc.addField(SolrConstants.WIDTH, dimension.width);
                doc.addField(SolrConstants.HEIGHT, dimension.height);
            });
        }

        // FULLTEXTAVAILABLE indicates whether this page has full-text
        if (doc.getField(SolrConstants.FULLTEXT) != null) {
            doc.addField(SolrConstants.FULLTEXTAVAILABLE, true);
            this.hasFulltext = true;
        } else {
            doc.addField(SolrConstants.FULLTEXTAVAILABLE, false);
        }
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
    private boolean addIndexFieldsFromAltoData(final SolrInputDocument doc, final Map<String, Object> altoData, final Map<String, Path> dataFolders,
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
     * 
     * @param doc
     * @param dataFolder
     * @param fileName
     */
    private static void addFileSizeToDoc(SolrInputDocument doc, Path dataFolder, String fileName) {
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
     * @param url
     * @return int[]
     * @should fetch dimensions correctly
     */
    private static int[] getImageDimensionsFromIIIF(String url) {
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
     * 
     * @param baseFileName
     * @return true if baseFileName is not one of the keywords; false otherwise
     */
    private static boolean isBaseFileNameUsable(String baseFileName) {
        return !("default".equals(baseFileName) || "info".equals(baseFileName) || "native".equals(baseFileName));
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
    private static Optional<Dimension> getSize(Path mediaFolder, String filename) {
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
     * <p>
     * getOpenJpegReader.
     * </p>
     *
     * @return a {@link javax.imageio.ImageReader} object.
     */
    private static ImageReader getOpenJpegReader() {
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
     * Adds named entity fields from the given list to the given SolrInputDocument.
     *
     * @param altoData a {@link java.util.Map} object.
     * @param doc a {@link org.apache.solr.common.SolrInputDocument} object.
     * @should add field
     * @should add untokenized field
     */
    @SuppressWarnings("unchecked")
    private static void addNamedEntitiesFields(Map<String, Object> altoData, SolrInputDocument doc) {
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
    private static String cleanUpNamedEntityValue(String value) {
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
     * <p>
     * getSizeForJp2.
     * </p>
     *
     * @param image a {@link java.nio.file.Path} object.
     * @return a {@link java.awt.Dimension} object.
     * @throws java.io.IOException if any.
     */
    private static Dimension getSizeForJp2(Path image) throws IOException {
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
     * 
     * @param imageFile
     * @return Optional<Dimension>
     */
    private static Optional<Dimension> readDimension(File imageFile) {
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
     * 
     * @param order
     * @param iddoc
     * @param physId
     * @return {@link PhysicalElement}
     */
    private static PhysicalElement createPhysicalElement(int order, String iddoc, String physId, DocType docType) {
        PhysicalElement ret = new PhysicalElement(order);
        ret.getDoc().addField(SolrConstants.IDDOC, iddoc);
        ret.getDoc().addField(SolrConstants.GROUPFIELD, iddoc);
        ret.getDoc().addField(SolrConstants.DOCTYPE, docType.name());
        ret.getDoc().addField(SolrConstants.ORDER, order);
        ret.getDoc().addField(SolrConstants.PHYSID, physId);

        return ret;
    }

    /**
     * Builds XPath expression for physical elements.
     * 
     * @return Constructed expression
     * @should build expression correctly
     */
    private static String buildPagesXpathExpresson() {
        StringBuilder sb = new StringBuilder("/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div[@TYPE=\"page\"");
        List<String> allowedTypes = SolrIndexerDaemon.getInstance().getConfiguration().getMetsAllowedPhysicalTypes();
        for (String type : allowedTypes) {
            sb.append(" or @TYPE=\"").append(type).append('"');
        }
        sb.append(']');

        return sb.toString();
    }

}
