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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Attribute;
import org.jdom2.Element;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.helper.FulltextAugmentor;
import io.goobi.viewer.indexer.helper.HttpConnector;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.PhysicalElement;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.file.FileId;

public class PhysicalDocumentBuilder {

    private static final Logger logger = LogManager.getLogger(PhysicalDocumentBuilder.class);

    private static final int GENERATE_PAGE_DOCUMENT_TIMEOUT_HOURS = 6;

    private static final String FIELD_COORDS = "MD_COORDS";
    private static final String FIELD_IMAGEAVAILABLE = "BOOL_IMAGEAVAILABLE";
    private static final String FIELD_FILESIZE = "MDNUM_FILESIZE";
    private static final String FIELD_SHAPE = "MD_SHAPE";

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

    private final List<String> useFileGroups;
    private final JDomXP xp;
    private final HttpConnector httpConnector;
    private final DataRepository dataRepository;
    private final DocType docType;

    private final Map<String, String> fileIdToFileGrpMap;
    private final List<Element> eleListAllFileGroups;
    private boolean hasImages = false;
    private boolean hasFulltext = false;

    /**
     * Create a builder for pages and other documents based on physical files
     * 
     * @param useFileGroups List of fileGroups containing the files to use
     * @param eleListAllFileGroups
     * @param fileIdToFileGrpMap
     * @param xp an xml parser
     * @param httpConnector for http requests
     * @param dataRepository the repository in which files are to be stored
     * @param docType the doc type to use for this PhysicalElement
     */
    public PhysicalDocumentBuilder(List<String> useFileGroups, List<Element> eleListAllFileGroups, Map<String, String> fileIdToFileGrpMap, JDomXP xp,
            HttpConnector httpConnector,
            DataRepository dataRepository, DocType docType) {
        this.useFileGroups = useFileGroups;
        this.eleListAllFileGroups = eleListAllFileGroups;
        this.fileIdToFileGrpMap = fileIdToFileGrpMap;
        this.xp = xp;
        this.httpConnector = httpConnector;
        this.dataRepository = dataRepository;
        this.docType = docType;
    }

    public boolean isFileGroupExists() {
        return this.useFileGroups.stream().anyMatch(this::isFileGroupExists);
    }

    public boolean isFileGroupExists(String filegroup) {
        String xpath = "/mets:mets/mets:fileSec/mets:fileGrp[@USE='" + filegroup + "']";
        return !xp.evaluateToElements(xpath, null).isEmpty();
    }

    /**
     * Generates a SolrInputDocument for each page that is mapped to a docstruct. Adds all page metadata except those that come from the owning
     * docstruct (such as docstruct iddoc, type, collection, etc.).
     *
     * @param dataFolders a {@link java.util.Map} object.
     * @param pi a {@link java.lang.String} object.
     * @param pageCountStart a int.
     * @param downloadExternalImages
     * @return A list of {@link PhysicalElement} for all pages in the physical structMap with file resources in the given fileGroup
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     */
    public Collection<PhysicalElement> generatePageDocuments(final Map<String, Path> dataFolders, final String pi, Integer pageCountStart,
            boolean downloadExternalImages) throws InterruptedException, FatalIndexerException {
        // Get all physical elements
        String xpath = buildPagesXpathExpresson();
        List<Element> eleStructMapPhysicalList = xp.evaluateToElements(xpath, null);
        if (eleStructMapPhysicalList.isEmpty()) {
            logger.info("No pages found.");
            return Collections.emptyList();
        }

        logger.info("Image file groups selected: {}", useFileGroups);
        logger.info("Generating {} page documents (count starts at {})...", eleStructMapPhysicalList.size(), pageCountStart);

        Collection<PhysicalElement> pages = Collections.synchronizedList(new ArrayList<PhysicalElement>());
        if (SolrIndexerDaemon.getInstance().getConfiguration().getThreads() > 1) {
            // Generate each page document in its own thread
            ConcurrentHashMap<String, Boolean> usedIddocsMap = new ConcurrentHashMap<>();
            try (ForkJoinPool pool = new ForkJoinPool(SolrIndexerDaemon.getInstance().getConfiguration().getThreads())) {
                pool.submit(() -> eleStructMapPhysicalList.parallelStream().forEach(eleStructMapPhysical -> {
                    try {
                        String iddoc = Indexer.getNextIddoc();
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

                    if (order != null) {
                        order++;
                    }
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
        if (useFileGroups == null || useFileGroups.isEmpty()) {
            throw new IllegalStateException("useFileGroups not set");
        }

        String id = eleStructMapPhysical.getAttributeValue("ID");
        Integer order = getOrder(eleStructMapPhysical, inOrder);
        if (order == null) {
            logger.info("ORDER attribute no found, skipping...");
            return null;
        }

        List<Element> eleStructLinkList = getStructLinks(iddoc, id);
        if (eleStructLinkList.isEmpty()) {
            logger.info("Page {} (PHYSID: {}) is not mapped to a structure element, skipping...", order, id);
            return null;
        }

        // Create object for this page
        PhysicalElement ret = createPhysicalElement(order, iddoc, id, this.docType);
        ret.getDoc().addField(SolrConstants.PI_TOPSTRUCT, pi);

        List<Element> eleFptrList =
                eleStructMapPhysical.getChildren("fptr", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("mets"));

        eleFptrList.forEach(eleFptr -> ret.getShapes().addAll(getShapes(order, eleFptr)));

        FileId useFileID = null;
        String useFileGroup = "";
        // for each requested file group
        for (String fileGroup : useFileGroups) {
            // find file id that contains the file group name
            FileId fileID = FileId.getFileId(getFileId(eleFptrList, eleListAllFileGroups, fileGroup), fileGroup);
            if (fileID != null) {
                // find mets:file matching file group and file id
                String xpath = "/mets:mets/mets:fileSec/mets:fileGrp[@USE='%s']/mets:file[@ID='%s']"
                        .formatted(fileGroup, fileID.getFullId()); //NOSONAR XPath, not URI
                List<Element> eleFileGrpList = xp.evaluateToElements(xpath, null);
                if (!eleFileGrpList.isEmpty()) {
                    // first match: use as selected file group and file id
                    useFileID = fileID;
                    ret.getDoc().addField(SolrConstants.FILEIDROOT, useFileID.getRoot());
                    useFileGroup = fileGroup;
                    break;
                }
            }
        }

        if (useFileID == null) {
            logger.error("no useFileID");
            System.out.println("no useFileID");
            return null;
        }

        setDoublePage(eleStructMapPhysical, ret);
        setOrderLabel(eleStructMapPhysical, ret);
        setImageUrn(eleStructMapPhysical, ret);
        setDmdId(eleStructMapPhysical, ret);

        String altoURL = null;
        // For each mets:fileGroup in the mets:fileSec
        for (Element eleFileGrp : eleListAllFileGroups) {
            String fileGrpUse = eleFileGrp.getAttributeValue("USE");
            String fileGrpId = eleFileGrp.getAttributeValue("ID"); // TODO This is probably always null
            logger.debug("Found file group: {}", fileGrpUse);
            logger.debug("fileId: {}", fileGrpId);

            FileId fileID = FileId.getFileId(getFileId(eleFptrList, eleListAllFileGroups, fileGrpUse), fileGrpUse);

            // If fileId is not null, use an XPath expression for the appropriate file element,
            // otherwise get all file elements and get the one with the index of the page order
            String fileIdXPathCondition = getXPathCondition(fileID, fileGrpId);
            int attrListIndex = useFileID != null ? 0 : order - 1;

            String filePath = getFilepath(eleFileGrp, fileIdXPathCondition, attrListIndex);
            if (filePath == null) {
                if (useFileGroup.equals(fileGrpUse)) {
                    logger.warn("Skipping selected file group {} - nothing found.", fileGrpUse);
                } else {
                    logger.debug("Skipping file group {}", fileGrpUse);
                }
                continue;
            }

            String fileName = getFilename(filePath);

            // Mime type
            String xpath = XPATH_FILE + fileIdXPathCondition + "/@MIMETYPE";
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

            if (fileGrpUse.equals(useFileGroup)) {
                // The file name from the main file group (usually PRESENTATION or DEFAULT) will be used for thumbnail purposes etc.
                if (filePath.startsWith("http")) {
                    // Should write the full URL into FILENAME because otherwise a PI_TOPSTRUCT+FILENAME combination may no longer be unique
                    if (ret.getDoc().containsKey(SolrConstants.FILENAME)) {
                        logger.error("Page {} already contains FILENAME={}, but attempting to add another value from filegroup {}", iddoc,
                                filePath, fileGrpUse);
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
                    if (!downloadExternalImages && DEFAULT_FILEGROUP.equals(useFileGroup)
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
                String useFileName = filePath.startsWith("http") ? filePath : fileName;
                if (ret.getDoc().getField(fieldName) == null) {
                    switch (mimetypeSplit[1]) {
                        case "object":
                            ret.getDoc().addField(SolrConstants.FILENAME, useFileName);
                            ret.getDoc().addField(SolrConstants.MIMETYPE, mimetypeSplit[1]);
                            break;
                        default:
                            ret.getDoc().addField(SolrConstants.FILENAME + "_" + mimetypeSplit[1].toUpperCase(), useFileName);
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
                            logger.debug("Added WIDTH from techMD: {}", ret.getDoc().getFieldValue(SolrConstants.WIDTH));
                            logger.debug("Added HEIGHT from techMD: {}", ret.getDoc().getFieldValue(SolrConstants.HEIGHT));
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
            this.hasFulltext = new FulltextAugmentor(dataRepository).addFullTextToPageDoc(ret.getDoc(), dataFolders, pi, order, altoURL);
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

    /**
     * 
     * @param filePath
     * @return Extracted file name
     */
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

    /**
     * 
     * @param eleFileGrp
     * @param fileIdXPathCondition
     * @param attrListIndex
     * @return File path; null if none found
     */
    protected String getFilepath(Element eleFileGrp, String fileIdXPathCondition, int attrListIndex) {
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

    /**
     * 
     * @param useFileID
     * @param fileGrpId
     * @return Generated condition XPath
     */
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

    /**
     * 
     * @param eleStructMapPhysical
     * @param ret
     * @throws FatalIndexerException
     */
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

    /**
     * 
     * @param eleStructMapPhysical
     * @param ret
     */
    protected void setImageUrn(Element eleStructMapPhysical, PhysicalElement ret) {
        String contentIDs = eleStructMapPhysical.getAttributeValue(ATTRIBUTE_CONTENTIDS);
        if (Utils.isUrn(contentIDs)) {
            ret.getDoc().addField(SolrConstants.IMAGEURN, contentIDs);
        }
    }

    /**
     * 
     * @param eleStructMapPhysical
     * @param ret
     */
    protected void setDoublePage(Element eleStructMapPhysical, PhysicalElement ret) {
        // Double page view
        boolean doubleImage =
                "double page".equals(eleStructMapPhysical.getAttributeValue("label",
                        SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("xlink")));
        if (doubleImage) {
            ret.getDoc().addField(SolrConstants.BOOL_DOUBLE_IMAGE, doubleImage);
        }
    }

    /**
     * 
     * @param eleStructMapPhysical
     * @param ret
     */
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
     * @param eleListFileGroups mets:fileGrp elements to check
     * @param fileGroup The file group to use
     * @return The file id as string. An empty string if no file pointer for the fileGroup is found, and null if fileGroup is null or an empty string
     */
    protected String getFileId(List<Element> eleFptrList, List<Element> eleListFileGroups, String fileGroup) {
        if (eleFptrList == null) {
            throw new IllegalArgumentException("eleFptrList may not be null");
        }
        if (eleListFileGroups == null) {
            throw new IllegalArgumentException("eleListFileGroups may not be null");
        }

        for (Element eleFptr : eleFptrList) {
            String fileID = eleFptr.getAttributeValue("FILEID");
            if (fileID == null) {
                continue;
            }
            logger.trace("fileID: {}", fileID);
            if (fileGroup.equals(fileIdToFileGrpMap.get(fileID))) {
                return fileID;
            }
        }

        return null;
    }

    protected String getFileIdViaXpath(List<Element> eleFptrList, String fileGroup) {
        if (eleFptrList == null) {
            return null;
        }

        for (Element eleFptr : eleFptrList) {
            String fileID = eleFptr.getAttributeValue("FILEID");
            if (fileID != null) {
                logger.trace("fileID: {}", fileID);
                String xpath = "/mets:mets/mets:fileSec/mets:fileGrp[@USE='" + fileGroup + "']/mets:file[@ID='" + fileID + "']";
                List<Element> eleListFiles = xp.evaluateToElements(xpath, null);
                if (eleListFiles.isEmpty()) {
                    return fileID;
                }
            }
        }

        return null;
    }

    /**
     * 
     * @param order
     * @param eleFptr
     * @return List<PhysicalElement>
     */
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

    /**
     * 
     * @param iddoc
     * @param id
     * @return List<Element>
     */
    protected List<Element> getStructLinks(String iddoc, String id) {
        logger.trace("generatePageDocument: {} (IDDOC {}) processed by thread {}", id, iddoc, Thread.currentThread().threadId());
        // Check whether this physical element is mapped to any logical element, skip if not
        StringBuilder sbXPath = new StringBuilder(70);
        sbXPath.append("/mets:mets/mets:structLink/mets:smLink[@xlink:to=\"").append(id).append("\"]");
        return xp.evaluateToElements(sbXPath.toString(), null);
    }

    /**
     * 
     * @param eleStructMapPhysical
     * @param inOrder
     * @return Value of inOrder, if present; otherwise value of the ORDER attribute; otherwise null
     */
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
     * @param order
     * @param iddoc
     * @param physId
     * @param docType
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
    static String buildPagesXpathExpresson() {
        StringBuilder sb = new StringBuilder("/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div[@TYPE=\"page\"");
        List<String> allowedTypes = SolrIndexerDaemon.getInstance().getConfiguration().getMetsAllowedPhysicalTypes();
        for (String type : allowedTypes) {
            sb.append(" or @TYPE=\"").append(type).append('"');
        }
        sb.append(']');

        return sb.toString();
    }

}
