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
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrHelper;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.FatalIndexerException;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.IndexerException;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.config.MetadataConfigurationManager;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.LazySolrWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.SerializingSolrWriteStrategy;

public class DenkXwebIndexer extends Indexer {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(DenkXwebIndexer.class);

    /**
     * Whitelist of file names belonging for this particular record (in case the media folder contains files for multiple records). StringBuffer is
     * thread-safe.
     */
    private StringBuffer sbImgFileNames = new StringBuffer();

    /**
     * Constructor.
     * 
     * @param hotfolder
     * @should set attributes correctly
     */
    public DenkXwebIndexer(Hotfolder hotfolder) {
        this.hotfolder = hotfolder;
    }

    /**
     * Indexes a DenkXweb file.
     * 
     * @param doc
     * @param dataFolders
     * @param writeStrategy
     * @param pageCountStart
     * @param downloadExternalImages
     * @return
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

            IndexObject indexObj = new IndexObject(getNextIddoc(hotfolder.getSolrHelper()));
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
                        hotfolder.getDataRepositoryStrategy().selectDataRepository(pi, null, dataFolders, hotfolder.getSolrHelper());
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
                boolean useSerializingStrategy = false;
                if (useSerializingStrategy) {
                    writeStrategy = new SerializingSolrWriteStrategy(hotfolder.getSolrHelper(), hotfolder.getTempFolder());

                }
                //                else if (IndexerConfig.getInstance().getBoolean("init.aggregateRecords")) {
                //                    writeStrategy = new HierarchicalLazySolrWriteStrategy(hotfolder.getSolrHelper());
                //                }
                else {
                    writeStrategy = new LazySolrWriteStrategy(hotfolder.getSolrHelper());
                }
            } else {
                logger.info("Solr write strategy injected by caller: {}", writeStrategy.getClass().getName());
            }

            // Set source doc format
            indexObj.addToLucene(SolrConstants.SOURCEDOCFORMAT, SolrConstants._LIDO);

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
                    SolrDocumentList hits = hotfolder.getSolrHelper()
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
            indexObj.writeDateModified(!noTimestampUpdate);

            // If full-text has been indexed for any page, set a boolean in the root doc indicating that the records does have full-text
            indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, String.valueOf(recordHasFulltext));

            // Add DEFAULT field
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(indexObj.getDefaultValue()));
                // indexObj.getSuperDefaultBuilder().append(' ').append(indexObj.getDefaultValue().trim());
                indexObj.setDefaultValue("");
            }

            // Add grouped metadata as separate documents
            addGroupedMetadataDocs(writeStrategy, indexObj);

            // Add root doc
            SolrInputDocument rootDoc = SolrHelper.createDocument(indexObj.getLuceneFields());
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
            hotfolder.getSolrHelper().rollback();
        } finally {
            if (writeStrategy != null) {
                writeStrategy.cleanup();
            }
        }

        return ret;
    }

    /**
     * Prepares the given record for an update. Creation timestamp is preserved. A new update timestamp is added, child docs are removed.
     * 
     * @param indexObj {@link IndexObject}
     * @throws IOException -
     * @throws SolrServerException
     * @throws FatalIndexerException
     */
    private void prepareUpdate(IndexObject indexObj) throws IOException, SolrServerException, FatalIndexerException {
        String pi = indexObj.getPi().trim();
        SolrDocumentList hits = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + pi, null);
        if (hits != null && hits.getNumFound() > 0) {
            logger.debug("This file has already been indexed, initiating an UPDATE instead...");
            indexObj.setUpdate(true);
            SolrDocument doc = hits.get(0);
            // Set creation timestamp, if exists (should never be updated)
            Object dateCreated = doc.getFieldValue(SolrConstants.DATECREATED);
            if (dateCreated != null) {
                // Set creation timestamp, if exists (should never be updated)
                indexObj.setDateCreated((Long) dateCreated);
            }
            // Set update timestamp
            Collection<Object> dateUpdatedValues = doc.getFieldValues(SolrConstants.DATEUPDATED);
            if (dateUpdatedValues != null) {
                for (Object date : dateUpdatedValues) {
                    indexObj.getDateUpdated().add((Long) date);
                }
            }
            // Recursively delete all children
            deleteWithPI(pi, false, hotfolder.getSolrHelper());
        }
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
        String xpath = "//denkxweb:images/denkxweb:image[@preferred='true']";
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
            if (generatePageDocument(eleImage, String.valueOf(getNextIddoc(hotfolder.getSolrHelper())), order, writeStrategy, dataFolders,
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
            if (url.endsWith("default.jpg")) {
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
                String baseFileName = FilenameUtils.getBaseName(fileName);
                String viewerUrl = Configuration.getInstance().getViewerUrl();
                if (downloadExternalImages && dataFolders.get(DataRepository.PARAM_MEDIA) != null && viewerUrl != null
                // Download image and use locally
                        && !url.startsWith(viewerUrl)) {
                    try {
                        File file = new File(dataFolders.get(DataRepository.PARAM_MEDIA).toFile(), fileName);
                        FileUtils.copyURLToFile(new URL(url), file);
                        if (file.isFile()) {
                            logger.info("Downloaded {}", file);
                            sbImgFileNames.append(';').append(fileName);
                            doc.addField(SolrConstants.FILENAME, fileName);
                        } else {
                            logger.warn("Could not download file: {}", url);
                        }
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                } else {
                    // Add external image URL
                    doc.addField(SolrConstants.FILENAME + "_HTML-SANDBOXED", url);
                }
            } else {
                // For non-remote file, add the file name to the list
                sbImgFileNames.append(';').append(fileName);
            }

            // Add full path if this is a local file or download has failed or is disabled
            if (!doc.containsKey(SolrConstants.FILENAME)) {
                doc.addField(SolrConstants.FILENAME, fileName);
            }

            String mimetype = eleImage.getAttributeValue("type");
            String subMimetype = "";
            if (mimetype != null && mimetype.contains("/")) {
                subMimetype = mimetype.substring(mimetype.indexOf("/") + 1);
                mimetype = mimetype.substring(0, mimetype.indexOf("/"));
            } else {
                mimetype = "image";
                if (doc.containsKey(SolrConstants.FILENAME)) {
                    // Determine mime type from file content
                    String filename = (String) doc.getFieldValue(SolrConstants.FILENAME);
                    try {
                        mimetype = Files.probeContentType(Paths.get(filename));
                        if (StringUtils.isBlank(mimetype)) {
                            mimetype = "image";
                        } else if (mimetype.contains("/")) {
                            subMimetype = mimetype.substring(mimetype.indexOf("/") + 1);
                            mimetype = mimetype.substring(0, mimetype.indexOf("/"));
                        }
                    } catch (IOException e) {
                        logger.warn("Cannot guess mime type from " + filename + ". using 'image'");
                    }
                }
            }

            if (StringUtils.isNotBlank(subMimetype)) {
                switch (mimetype.toLowerCase()) {
                    case "video":
                    case "audio":
                    case "html-sandboxed":
                        doc.addField(SolrConstants.MIMETYPE, mimetype);
                        doc.addField(SolrConstants.FILENAME + "_" + subMimetype.toUpperCase(), fileName);
                        break;
                    case "object":
                        doc.addField(SolrConstants.MIMETYPE, subMimetype);
                        break;
                    default:
                        doc.addField(SolrConstants.MIMETYPE, mimetype);
                }
            }
        }

        // Add file size
        if (dataFolders != null) {
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
        }

        String baseFileName = FilenameUtils.getBaseName((String) doc.getFieldValue(SolrConstants.FILENAME));

        // Add image dimension values from EXIF
        if (!doc.containsKey(SolrConstants.WIDTH) || !doc.containsKey(SolrConstants.HEIGHT)) {
            getSize(dataFolders.get(DataRepository.PARAM_MEDIA), (String) doc.getFieldValue(SolrConstants.FILENAME)).ifPresent(dimension -> {
                doc.addField(SolrConstants.WIDTH, dimension.width);
                doc.addField(SolrConstants.HEIGHT, dimension.height);
            });
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

    public static FilenameFilter txt = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".txt");
        }
    };

    public static FilenameFilter xml = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".xml");
        }
    };
}
