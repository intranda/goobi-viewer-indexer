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
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.config.MetadataConfigurationManager;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * <p>
 * DenkXwebIndexer class.
 * </p>
 *
 */
public class DenkXwebIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(DenkXwebIndexer.class);

    /**
     * Whitelist of file names belonging for this particular record (in case the media folder contains files for multiple records). StringBuffer is
     * thread-safe.
     */
    private StringBuilder sbImgFileNames = new StringBuilder();

    /**
     * Constructor.
     * 
     * @param hotfolder a {@link io.goobi.viewer.indexer.helper.Hotfolder} object.
     * @should set attributes correctly
     */
    public DenkXwebIndexer(Hotfolder hotfolder) {
        super();
        this.hotfolder = hotfolder;
    }

    /**
     * Indexes a DenkXweb file.
     * 
     * @param doc a {@link org.jdom2.Document} object.
     * @param dataFolders a {@link java.util.Map} object.
     * @param writeStrategy a {@link io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy} object.
     * @param pageCountStart a int.
     * @param downloadExternalImages
     * @return an array of {@link java.lang.String} objects.
     * @should index record correctly
     * @should update record correctly
     */
    public String[] index(Document doc, Map<String, Path> dataFolders, ISolrWriteStrategy writeStrategy, int pageCountStart,
            boolean downloadExternalImages) {
        String[] ret = { "ERROR", null };
        String pi = null;
        try {
            this.xp = new JDomXP(doc);
            if (this.xp == null) {
                throw new IndexerException("Could not create XML parser.");
            }

            IndexObject indexObj = new IndexObject(getNextIddoc(hotfolder.getSearchIndex()));
            logger.debug("IDDOC: {}", indexObj.getIddoc());
            Element structNode = doc.getRootElement();
            indexObj.setRootStructNode(structNode);

            // set some simple data in den indexObject
            setSimpleData(indexObj);

            // Set PI
            {
                pi = MetadataHelper.getPIFromXML("", xp);
                if (StringUtils.isBlank(pi)) {
                    ret[1] = "PI not found.";
                    throw new IndexerException(ret[1]);
                }

                // Remove prefix
                if (pi.contains(":")) {
                    pi = pi.substring(pi.lastIndexOf(':') + 1);
                }
                if (pi.contains("/")) {
                    pi = pi.substring(pi.lastIndexOf('/') + 1);
                }
                pi = MetadataHelper.applyIdentifierModifications(pi);
                // Do not allow identifiers with illegal characters
                Pattern p = Pattern.compile("[^\\w|-]");
                Matcher m = p.matcher(pi);
                if (m.find()) {
                    ret[1] = "PI contains illegal characters: " + pi;
                    throw new IndexerException(ret[1]);
                }
                indexObj.setPi(pi);
                indexObj.setTopstructPI(pi);
                logger.debug("PI: {}", indexObj.getPi());

                // Determine the data repository to use
                DataRepository[] repositories =
                        hotfolder.getDataRepositoryStrategy()
                                .selectDataRepository(pi, null, dataFolders, hotfolder.getSearchIndex(), hotfolder.getOldSearchIndex());
                dataRepository = repositories[0];
                previousDataRepository = repositories[1];
                if (StringUtils.isNotEmpty(dataRepository.getPath())) {
                    indexObj.setDataRepository(dataRepository.getPath());
                }

                ret[0] = indexObj.getPi();

                // Check and use old data folders, if no new ones found
                checkOldDataFolder(dataFolders, DataRepository.PARAM_MIX, pi);
                checkOldDataFolder(dataFolders, DataRepository.PARAM_UGC, pi);
                checkOldDataFolder(dataFolders, DataRepository.PARAM_CMS, pi);
                checkOldDataFolder(dataFolders, DataRepository.PARAM_TEIMETADATA, pi);
                checkOldDataFolder(dataFolders, DataRepository.PARAM_ANNOTATIONS, pi);
            }

            if (writeStrategy == null) {
                // Request appropriate write strategy
                writeStrategy = AbstractWriteStrategy.create(null, dataFolders, hotfolder);
            } else {
                logger.info("Solr write strategy injected by caller: {}", writeStrategy.getClass().getName());
            }

            // Set source doc format
            indexObj.addToLucene(SolrConstants.SOURCEDOCFORMAT, FileFormat.DENKXWEB.name());

            prepareUpdate(indexObj);

            // Process TEI files
            if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) != null) {
                MetadataHelper.processTEIMetadataFiles(indexObj, dataFolders.get(DataRepository.PARAM_TEIMETADATA));
            }

            indexObj.pushSimpleDataToLuceneArray();
            MetadataHelper.writeMetadataToObject(indexObj, indexObj.getRootStructNode(), "", xp);

            // If this is a volume (= has an anchor) that has already been indexed, copy access conditions from the anchor element
            if (indexObj.isVolume()) {
                String anchorPi = MetadataHelper.getAnchorPi(xp);
                if (anchorPi != null) {
                    SolrDocumentList hits = hotfolder.getSearchIndex()
                            .search(SolrConstants.PI + ":" + anchorPi, Collections.singletonList(SolrConstants.ACCESSCONDITION));
                    if (hits != null && hits.getNumFound() > 0) {
                        Collection<Object> fields = hits.get(0).getFieldValues(SolrConstants.ACCESSCONDITION);
                        for (Object o : fields) {
                            indexObj.getAccessConditions().add(o.toString());
                        }
                    }
                }
            }

            // Add LABEL value
            if (StringUtils.isEmpty(indexObj.getLabel())) {
                LuceneField field = indexObj.getLuceneFieldWithName("MD_TITLE");
                if (field != null) {
                    indexObj.addToLucene(SolrConstants.LABEL, MetadataHelper.applyValueDefaultModifications(field.getValue()));
                }
            }

            // Generate pages
            generatePageDocuments(writeStrategy, dataFolders, pageCountStart, downloadExternalImages);

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // Add THUMBNAIL,THUMBPAGENO,THUMBPAGENOLABEL (must be done AFTER writeDateMondified(), writeAccessConditions() and generatePageDocuments()!)
            List<LuceneField> thumbnailFields = mapPagesToDocstruct(indexObj, writeStrategy);
            if (thumbnailFields != null) {
                indexObj.getLuceneFields().addAll(thumbnailFields);
            }

            // ISWORK only for non-anchors
            indexObj.addToLucene(SolrConstants.ISWORK, "true");
            logger.trace("ISWORK: {}", indexObj.getLuceneFieldWithName(SolrConstants.ISWORK).getValue());

            if (indexObj.getNumPages() > 0) {
                // Write number of pages
                indexObj.addToLucene(SolrConstants.NUMPAGES, String.valueOf(writeStrategy.getPageDocsSize()));

                // Add used-generated content docs
                writeUserGeneratedContents(writeStrategy, dataFolders, indexObj);
            }

            // Write created/updated timestamps
            indexObj.writeDateModified(true);

            // If images have been found for any page, set a boolean in the root doc indicating that the record does have images
            indexObj.addToLucene(FIELD_IMAGEAVAILABLE, String.valueOf(recordHasImages));

            // If full-text has been indexed for any page, set a boolean in the root doc indicating that the record does have full-text
            indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, String.valueOf(recordHasFulltext));

            // Add DEFAULT field
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(indexObj.getDefaultValue()));
                indexObj.setDefaultValue("");
            }

            // Add grouped metadata as separate documents
            addGroupedMetadataDocs(writeStrategy, indexObj, indexObj.getGroupedMetadataFields(), indexObj.getIddoc());

            // Add root doc
            SolrInputDocument rootDoc = SolrSearchIndex.createDocument(indexObj.getLuceneFields());
            writeStrategy.setRootDoc(rootDoc);

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)
            logger.debug("Writing document to index...");
            writeStrategy.writeDocs(Configuration.getInstance().isAggregateRecords());

            // Return image file names
            if (sbImgFileNames.length() > 0 && sbImgFileNames.charAt(0) == ';') {
                sbImgFileNames.deleteCharAt(0);
            }
            ret[1] = sbImgFileNames.toString();
            logger.info("Successfully finished indexing '{}'.", pi);
        } catch (Exception e) {
            if ("No image resource sets found.".equals(e.getMessage())) {
                logger.error("Indexing of '{}' could not be finished due to an error: {}", pi, e.getMessage());
            } else {
                logger.error("Indexing of '{}' could not be finished due to an error.", pi);
                logger.error(e.getMessage(), e);
            }
            ret[0] = "ERROR";
            ret[1] = e.getMessage();
            hotfolder.getSearchIndex().rollback();
        } finally {
            if (writeStrategy != null) {
                writeStrategy.cleanup();
            }
        }

        return ret;
    }

    /**
     * 
     * @param indexObj
     * @param writeStrategy
     * @return
     * @throws FatalIndexerException
     */
    private static List<LuceneField> mapPagesToDocstruct(IndexObject indexObj, ISolrWriteStrategy writeStrategy) throws FatalIndexerException {
        List<String> physIds = new ArrayList<>(writeStrategy.getPageDocsSize());
        for (int i = 1; i <= writeStrategy.getPageDocsSize(); ++i) {
            physIds.add(String.valueOf(i));
        }
        List<SolrInputDocument> pageDocs = writeStrategy.getPageDocsForPhysIdList(physIds);
        if (pageDocs.isEmpty()) {
            logger.warn("No pages found for {}", indexObj.getLogId());
            return Collections.emptyList();
        }

        List<LuceneField> ret = new ArrayList<>(5);

        // If this is a top struct element, look for a representative image
        String filePathBanner = null;
        boolean thumbnailSet = false;
        for (SolrInputDocument pageDoc : pageDocs) {
            String pageFileName = pageDoc.getField(SolrConstants.FILENAME + SolrConstants._HTML_SANDBOXED) != null
                    ? (String) pageDoc.getFieldValue(SolrConstants.FILENAME + SolrConstants._HTML_SANDBOXED)
                    : (String) pageDoc.getFieldValue(SolrConstants.FILENAME);
            String pageFileBaseName = FilenameUtils.getBaseName(pageFileName);

            if (pageDoc.containsKey(SolrConstants.THUMBNAILREPRESENT)) {
                filePathBanner = (String) pageDoc.getFieldValue(SolrConstants.THUMBNAILREPRESENT);
            }

            // Add thumbnail information from the representative page
            if (!thumbnailSet && StringUtils.isNotEmpty(filePathBanner) && pageFileName.equals(filePathBanner)) {
                ret.add(new LuceneField(SolrConstants.THUMBNAIL, pageFileName));
                // THUMBNAILREPRESENT is just used to identify the presence of a custom representation thumbnail to the indexer, it is not used in the viewer
                ret.add(new LuceneField(SolrConstants.THUMBNAILREPRESENT, pageFileName));
                ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(pageDoc.getFieldValue(SolrConstants.ORDER))));
                ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) pageDoc.getFieldValue(SolrConstants.ORDERLABEL)));
                ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) pageDoc.getFieldValue(SolrConstants.MIMETYPE)));
                thumbnailSet = true;
            }

            // Make sure IDDOC_OWNER of a page contains the iddoc of the lowest possible mapped docstruct
            if (pageDoc.getField("MDNUM_OWNERDEPTH") == null || 0 > (Integer) pageDoc.getFieldValue("MDNUM_OWNERDEPTH")) {
                pageDoc.setField(SolrConstants.IDDOC_OWNER, String.valueOf(indexObj.getIddoc()));
                pageDoc.setField("MDNUM_OWNERDEPTH", 0);

                // Add the parent document's structure element to the page
                pageDoc.setField(SolrConstants.DOCSTRCT, indexObj.getType());

                // Add topstruct type to the page
                if (!pageDoc.containsKey(SolrConstants.DOCSTRCT_TOP) && indexObj.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP) != null) {
                    pageDoc.setField(SolrConstants.DOCSTRCT_TOP, indexObj.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP).getValue());
                }

                // Remove SORT_ fields from a previous, higher up docstruct
                Set<String> fieldsToRemove = new HashSet<>();
                for (String fieldName : pageDoc.getFieldNames()) {
                    if (fieldName.startsWith(SolrConstants.SORT_)) {
                        fieldsToRemove.add(fieldName);
                    }
                }
                for (String fieldName : fieldsToRemove) {
                    pageDoc.removeField(fieldName);
                }
                //  Add this docstruct's SORT_* fields to page
                if (indexObj.getIddoc() == Long.valueOf((String) pageDoc.getFieldValue(SolrConstants.IDDOC_OWNER))) {
                    for (LuceneField field : indexObj.getLuceneFields()) {
                        if (field.getField().startsWith(SolrConstants.SORT_)) {
                            pageDoc.addField(field.getField(), field.getValue());
                        }
                    }
                }
            }

            if (pageDoc.getField(SolrConstants.PI_TOPSTRUCT) == null) {
                pageDoc.addField(SolrConstants.PI_TOPSTRUCT, indexObj.getTopstructPI());
            }
            if (pageDoc.getField(SolrConstants.DATAREPOSITORY) == null && indexObj.getDataRepository() != null) {
                pageDoc.addField(SolrConstants.DATAREPOSITORY, indexObj.getDataRepository());
            }
            if (pageDoc.getField(SolrConstants.DATEUPDATED) == null && !indexObj.getDateUpdated().isEmpty()) {
                for (Long date : indexObj.getDateUpdated()) {
                    pageDoc.addField(SolrConstants.DATEUPDATED, date);
                }
            }
            if (pageDoc.getField(SolrConstants.DATEINDEXED) == null && !indexObj.getDateIndexed().isEmpty()) {
                for (Long date : indexObj.getDateIndexed()) {
                    pageDoc.addField(SolrConstants.DATEINDEXED, date);
                }
            }

            // Add of each docstruct access conditions (no duplicates)
            Set<String> existingAccessConditions = new HashSet<>();
            if (pageDoc.getFieldValues(SolrConstants.ACCESSCONDITION) != null) {
                for (Object obj : pageDoc.getFieldValues(SolrConstants.ACCESSCONDITION)) {
                    existingAccessConditions.add((String) obj);
                }
            }
            for (String s : indexObj.getAccessConditions()) {
                if (!existingAccessConditions.contains(s)) {
                    pageDoc.addField(SolrConstants.ACCESSCONDITION, s);
                }
            }
            if (indexObj.getAccessConditions().isEmpty()) {
                logger.warn("{}: {} has no access conditions.", pageFileBaseName, indexObj.getIddoc());
            }

            // Add owner docstruct's metadata (tokenized only!) and SORT_* fields to the page
            Set<String> existingMetadataFieldNames = new HashSet<>();
            Set<String> existingSortFieldNames = new HashSet<>();
            for (String fieldName : pageDoc.getFieldNames()) {
                if (Configuration.getInstance().getMetadataConfigurationManager().getFieldsToAddToPages().contains(fieldName)) {
                    for (Object value : pageDoc.getFieldValues(fieldName)) {
                        existingMetadataFieldNames.add(new StringBuilder(fieldName).append(String.valueOf(value)).toString());
                    }
                } else if (fieldName.startsWith(SolrConstants.SORT_)) {
                    existingSortFieldNames.add(fieldName);
                }
            }
            for (LuceneField field : indexObj.getLuceneFields()) {
                if (Configuration.getInstance().getMetadataConfigurationManager().getFieldsToAddToPages().contains(field.getField())
                        && !existingMetadataFieldNames.contains(new StringBuilder(field.getField()).append(field.getValue()).toString())) {
                    // Avoid duplicates (same field name + value)
                    pageDoc.addField(field.getField(), field.getValue());
                    logger.debug("Added {}:{} to page {}", field.getField(), field.getValue(), pageDoc.getFieldValue(SolrConstants.ORDER));
                } else if (field.getField().startsWith(SolrConstants.SORT_) && !existingSortFieldNames.contains(field.getField())) {
                    // Only one instance of each SORT_ field may exist
                    pageDoc.addField(field.getField(), field.getValue());
                }
            }

            // Update the doc in the write strategy (otherwise some implementations might ignore the changes).
            writeStrategy.updateDoc(pageDoc);
        }

        SolrInputDocument firstPageDoc = pageDocs.get(0);

        // If a representative image is set but not mapped to any docstructs, do not use it
        if (!thumbnailSet && StringUtils.isNotEmpty(filePathBanner) && !pageDocs.isEmpty()) {
            logger.warn("Selected representative image '{}' is not mapped to any structure element - using first mapped image instead.",
                    filePathBanner);
            String pageFileName = firstPageDoc.getField(SolrConstants.FILENAME + SolrConstants._HTML_SANDBOXED) != null
                    ? (String) firstPageDoc.getFieldValue(SolrConstants.FILENAME + SolrConstants._HTML_SANDBOXED)
                    : (String) firstPageDoc.getFieldValue(SolrConstants.FILENAME);
            ret.add(new LuceneField(SolrConstants.THUMBNAIL, pageFileName));
            // THUMBNAILREPRESENT is just used to identify the presence of a custom representation thumbnail to the indexer, it is not used in the viewer
            ret.add(new LuceneField(SolrConstants.THUMBNAILREPRESENT, pageFileName));
            ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPageDoc.getFieldValue(SolrConstants.ORDER))));
            ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) firstPageDoc.getFieldValue(SolrConstants.ORDERLABEL)));
            ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) firstPageDoc.getFieldValue(SolrConstants.MIMETYPE)));
        }

        // Add thumbnail information from the first page
        if (StringUtils.isEmpty(filePathBanner)) {
            String thumbnailFileName = firstPageDoc.getField(SolrConstants.FILENAME + SolrConstants._HTML_SANDBOXED) != null
                    ? (String) firstPageDoc.getFieldValue(SolrConstants.FILENAME + SolrConstants._HTML_SANDBOXED)
                    : (String) firstPageDoc.getFieldValue(SolrConstants.FILENAME);
            ret.add(new LuceneField(SolrConstants.THUMBNAIL, thumbnailFileName));
            ret.add(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(firstPageDoc.getFieldValue(SolrConstants.ORDER))));
            ret.add(new LuceneField(SolrConstants.THUMBPAGENOLABEL, (String) firstPageDoc.getFieldValue(SolrConstants.ORDERLABEL)));
            ret.add(new LuceneField(SolrConstants.MIMETYPE, (String) firstPageDoc.getFieldValue(SolrConstants.MIMETYPE)));
        }

        // Add the number of assigned pages and the labels of the first and last page to this structure element
        indexObj.setNumPages(pageDocs.size());
        if (!pageDocs.isEmpty()) {
            SolrInputDocument lastPagedoc = pageDocs.get(pageDocs.size() - 1);
            String firstPageLabel = (String) firstPageDoc.getFieldValue(SolrConstants.ORDERLABEL);
            String lastPageLabel = (String) lastPagedoc.getFieldValue(SolrConstants.ORDERLABEL);
            if (firstPageLabel != null && !"-".equals(firstPageLabel.trim())) {
                indexObj.setFirstPageLabel(firstPageLabel);
            }
            if (lastPageLabel != null && !"-".equals(lastPageLabel.trim())) {
                indexObj.setLastPageLabel(lastPageLabel);
            }
        }

        return ret;
    }

    /**
     * Sets TYPE and LABEL from the LIDO document.
     * 
     * @param indexObj {@link IndexObject}
     * @throws FatalIndexerException
     */
    private static void setSimpleData(IndexObject indexObj) throws FatalIndexerException {
        Element structNode = indexObj.getRootStructNode();

        // Set type
        {
            String value = structNode.getAttributeValue("type");
            if (StringUtils.isNotEmpty(value)) {
                indexObj.setType(MetadataConfigurationManager.mapDocStrct(value).trim());
            } else {
                indexObj.setType("monument");
            }
            logger.trace("TYPE: {}", indexObj.getType());
        }

        // Set label
        {
            String value = structNode.getAttributeValue("LABEL");
            if (value != null) {
                indexObj.setLabel(value);
            }
        }
        logger.trace("LABEL: {}", indexObj.getLabel());
    }

    /**
     * 
     * @param writeStrategy
     * @param dataFolders
     * @param pageCountStart
     * @param downloadExternalImages
     * @throws FatalIndexerException
     * @should generate pages correctly
     */
    public void generatePageDocuments(ISolrWriteStrategy writeStrategy, Map<String, Path> dataFolders, int pageCountStart,
            boolean downloadExternalImages) throws FatalIndexerException {
        String xpath = "//denkxweb:images/denkxweb:image";
        List<Element> eleImageList = xp.evaluateToElements(xpath, null);
        if (eleImageList == null || eleImageList.isEmpty()) {
            // No pages
            return;
        }

        logger.info("Generating {} page documents (count starts at {})...", eleImageList.size(), pageCountStart);

        // TODO lambda instead of loop (find a way to preserve order first)
        //        eleImageList.parallelStream().forEach(
        //                eleImage -> generatePageDocument(eleImage, String.valueOf(getNextIddoc(hotfolder.getSolrHelper())), null,
        //                        writeStrategy, dataFolders, downloadExternalImages));
        int order = pageCountStart;
        for (Element eleImage : eleImageList) {
            if (generatePageDocument(eleImage, String.valueOf(getNextIddoc(hotfolder.getSearchIndex())), order, writeStrategy, dataFolders,
                    downloadExternalImages)) {
                order++;
            }
        }

        logger.info("Generated {} page documents.", writeStrategy.getPageDocsSize());
    }

    /**
     * 
     * @param eleImage
     * @param iddoc
     * @param order
     * @param writeStrategy
     * @param dataFolders
     * @param downloadExternalImages
     * @return
     * @throws FatalIndexerException
     */
    boolean generatePageDocument(Element eleImage, String iddoc, Integer order, ISolrWriteStrategy writeStrategy, Map<String, Path> dataFolders,
            boolean downloadExternalImages) throws FatalIndexerException {
        if (eleImage == null) {
            throw new IllegalArgumentException("eleImage may not be null");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null");
        }
        if (order == null) {
            // TODO page order within the metadata
        }

        // Create Solr document for this page
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(SolrConstants.IDDOC, iddoc);
        doc.addField(SolrConstants.GROUPFIELD, iddoc);
        doc.addField(SolrConstants.DOCTYPE, DocType.PAGE.name());
        doc.addField(SolrConstants.ORDER, order);
        doc.addField(SolrConstants.PHYSID, String.valueOf(order));

        Element eleStandard = eleImage.getChild("standard", Configuration.getInstance().getNamespaces().get("denkxweb"));
        if (eleStandard == null) {
            logger.warn("No element <standard> found for image {}", order);
            return false;
        }

        String orderLabel = eleStandard.getAttributeValue("ORDERLABEL");
        if (StringUtils.isNotEmpty(orderLabel)) {
            doc.addField(SolrConstants.ORDERLABEL, orderLabel);
        } else {
            doc.addField(SolrConstants.ORDERLABEL, Configuration.getInstance().getEmptyOrderLabelReplacement());
        }

        // Description
        {
            String desc = eleImage.getChildText("description", Configuration.getInstance().getNamespaces().get("denkxweb"));
            if (StringUtils.isNotEmpty(desc)) {
                doc.addField("MD_DESCRIPTION", desc);
            }
        }
        // Copyright
        {
            String copyright = eleStandard.getAttributeValue("right");
            if (StringUtils.isNotEmpty(copyright)) {
                doc.addField("MD_COPYRIGHT", copyright);
            }
        }

        // URL
        String url = eleStandard.getAttributeValue("url");
        String fileName;
        if (StringUtils.isNotEmpty(url) && url.contains("/")) {
            if (Utils.isFileNameMatchesRegex(url, IIIF_IMAGE_FILE_NAMES)) {
                // Extract correct original file name from IIIF
                fileName = Utils.getFileNameFromIiifUrl(url);
            } else {
                fileName = url.substring(url.lastIndexOf("/") + 1);
            }
        } else {
            fileName = url;
        }
        if (StringUtils.isNotEmpty(url)) {
            // External image
            if (url.startsWith("http")) {
                // Download image, if so requested (and not a local resource)
                String viewerUrl = Configuration.getInstance().getViewerUrl();
                if (downloadExternalImages && dataFolders.get(DataRepository.PARAM_MEDIA) != null && viewerUrl != null
                        && !url.startsWith(viewerUrl)) {
                    // Download image and use locally
                    try {
                        File file = new File(downloadExternalImage(url, dataFolders.get(DataRepository.PARAM_MEDIA), fileName));
                        if (file.isFile()) {
                            logger.info("Downloaded {}", file);
                            sbImgFileNames.append(';').append(fileName);
                            doc.addField(SolrConstants.FILENAME, fileName);

                            // Representative image (local)
                            if ("true".equals(eleImage.getAttributeValue("preferred"))) {
                                doc.addField(SolrConstants.THUMBNAILREPRESENT, fileName);
                            }
                        } else {
                            logger.warn("Could not download file: {}", url);
                        }
                    } catch (IOException | URISyntaxException e) {
                        logger.error(e.getMessage());
                    }
                } else {
                    // Add external image URL
                    doc.addField(SolrConstants.FILENAME + SolrConstants._HTML_SANDBOXED, url);

                    // Representative image (external)
                    if ("true".equals(eleImage.getAttributeValue("preferred"))) {
                        doc.addField(SolrConstants.THUMBNAILREPRESENT, url);
                    }
                }
            } else {
                // For non-remote file, add the file name to the list
                sbImgFileNames.append(';').append(fileName);

                // Representative image (local)
                if ("true".equals(eleImage.getAttributeValue("preferred"))) {
                    doc.addField(SolrConstants.THUMBNAILREPRESENT, fileName);
                }
            }

            // Mime type
            parseMimeType(doc, fileName);
        }

        // Add file size
        try {
            Path dataFolder = dataFolders.get(DataRepository.PARAM_MEDIA);
            // TODO other mime types/folders
            if (dataFolder != null) {
                Path path = Paths.get(dataFolder.toAbsolutePath().toString(), fileName);
                if (Files.isRegularFile(path)) {
                    doc.addField("MDNUM_FILESIZE", Files.size(path));
                }
            }
        } catch (IllegalArgumentException | IOException e) {
            logger.warn(e.getMessage());
        }
        if (!doc.containsKey("MDNUM_FILESIZE")) {
            doc.addField("MDNUM_FILESIZE", -1);
        }

        // Add image dimension values from EXIF
        if (!doc.containsKey(SolrConstants.WIDTH) || !doc.containsKey(SolrConstants.HEIGHT)) {
            getSize(dataFolders.get(DataRepository.PARAM_MEDIA), (String) doc.getFieldValue(SolrConstants.FILENAME)).ifPresent(dimension -> {
                doc.addField(SolrConstants.WIDTH, dimension.width);
                doc.addField(SolrConstants.HEIGHT, dimension.height);
            });
        }

        // FIELD_IMAGEAVAILABLE indicates whether this page has an image
        if (doc.containsKey(SolrConstants.FILENAME) && doc.containsKey(SolrConstants.MIMETYPE)
                && ((String) doc.getFieldValue(SolrConstants.MIMETYPE)).startsWith("image")) {
            doc.addField(FIELD_IMAGEAVAILABLE, true);
            recordHasImages = true;
        } else {
            doc.addField(FIELD_IMAGEAVAILABLE, false);
        }

        // FULLTEXTAVAILABLE indicates whether this page has full-text
        if (doc.getField(SolrConstants.FULLTEXT) != null) {
            doc.addField(SolrConstants.FULLTEXTAVAILABLE, true);
            recordHasFulltext = true;
        } else {
            doc.addField(SolrConstants.FULLTEXTAVAILABLE, false);
        }

        writeStrategy.addPageDoc(doc);
        return true;
    }
}
