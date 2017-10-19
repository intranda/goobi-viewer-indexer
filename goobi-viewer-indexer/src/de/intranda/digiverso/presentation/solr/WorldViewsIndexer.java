/**
 * This file is part of the Goobi Solr Indexer - a content indexing tool for the Goobi Viewer and OAI-PMH/SRU interfaces.
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.helper.Configuration;
import de.intranda.digiverso.presentation.solr.helper.Hotfolder;
import de.intranda.digiverso.presentation.solr.helper.JDomXP;
import de.intranda.digiverso.presentation.solr.helper.MetadataHelper;
import de.intranda.digiverso.presentation.solr.helper.SolrHelper;
import de.intranda.digiverso.presentation.solr.helper.TextHelper;
import de.intranda.digiverso.presentation.solr.helper.language.LanguageHelper;
import de.intranda.digiverso.presentation.solr.model.DataRepository;
import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;
import de.intranda.digiverso.presentation.solr.model.GroupedMetadata;
import de.intranda.digiverso.presentation.solr.model.IndexObject;
import de.intranda.digiverso.presentation.solr.model.IndexerException;
import de.intranda.digiverso.presentation.solr.model.LuceneField;
import de.intranda.digiverso.presentation.solr.model.SolrConstants;
import de.intranda.digiverso.presentation.solr.model.SolrConstants.DocType;
import de.intranda.digiverso.presentation.solr.model.config.MetadataConfigurationManager;
import de.intranda.digiverso.presentation.solr.model.writestrategy.ISolrWriteStrategy;
import de.intranda.digiverso.presentation.solr.model.writestrategy.LazySolrWriteStrategy;
import de.intranda.digiverso.presentation.solr.model.writestrategy.SerializingSolrWriteStrategy;

/**
 * Indexer implementation for WorldViews documents.
 */
public class WorldViewsIndexer extends AbstractIndexer {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(WorldViewsIndexer.class);

    public static final String DEFAULT_FILEGROUP_1 = "PRESENTATION";
    public static final String DEFAULT_FILEGROUP_2 = "DEFAULT";
    public static final String ALTO_FILEGROUP = "FULLTEXT";
    public static final String ANCHOR_UPDATE_EXTENSION = ".UPDATED";
    public static final String DEFAULT_FULLTEXT_CHARSET = "Cp1250";

    public static String fulltextCharset = DEFAULT_FULLTEXT_CHARSET;

    private static List<Path> reindexedChildrenFileList = new ArrayList<>();

    private boolean hasFulltext = false;

    private List<JDomXP> teiDocuments = new ArrayList<>(2);

    /**
     * Constructor.
     * 
     * @param hotfolder
     * @param writeStrategy
     * @should set attributes correctly
     */
    public WorldViewsIndexer(Hotfolder hotfolder) {
        this.hotfolder = hotfolder;
    }

    /**
     * Indexes the given WorldViews file.
     * 
     * @param mainFile {@link Path}
     * @param fromReindexQueue
     * @param dataFolders
     * @param writeStragegy Implementation of {@link ISolrWriteStrategy} (optional). If null, a new one will be created based on METS file and data
     *            folder sizes.
     * @param pageCountStart Order number for the first page.
     * @return
     * @should index record correctly
     */
    public String[] index(Path mainFile, boolean fromReindexQueue, Map<String, Path> dataFolders, ISolrWriteStrategy writeStrategy,
            int pageCountStart) {
        String[] ret = { null, null };

        if (mainFile == null || !Files.exists(mainFile)) {
            throw new IllegalArgumentException("mainFile must point to an existing XML file.");
        }
        if (dataFolders == null) {
            throw new IllegalArgumentException("dataFolders may not be null.");
        }

        try {
            initJDomXP(mainFile);
            IndexObject indexObj = new IndexObject(getNextIddoc(hotfolder.getSolrHelper()));
            logger.debug("IDDOC: {}", indexObj.getIddoc());

            // Set PI
            {
                String pi = xp.evaluateToString("worldviews//identifier/text()", null);
                if (StringUtils.isNotBlank(pi)) {
                    pi = MetadataHelper.applyIdentifierModifications(pi);
                    logger.info("Record PI: {}", pi);

                    // Do not allow identifiers with characters that cannot be used in file names
                    Pattern p = Pattern.compile("[^\\w|-]");
                    Matcher m = p.matcher(pi);
                    if (m.find()) {
                        ret[1] = new StringBuilder("PI contains illegal characters: ").append(pi).toString();
                        throw new IndexerException(ret[1]);
                    }
                    indexObj.setPi(pi);
                    indexObj.setTopstructPI(pi);
                    logger.debug("PI: {}", indexObj.getPi());

                    // Determine the data repository to use
                    hotfolder.selectDataRepository(null, pi);
                    if (StringUtils.isNotEmpty(hotfolder.getSelectedRepository().getName())) {
                        indexObj.setDataRepository(hotfolder.getSelectedRepository().getName());
                    }

                    ret[0] = new StringBuilder(indexObj.getPi()).append(AbstractIndexer.XML_EXTENSION).toString();
                    if (dataFolders.get(DataRepository.PARAM_MEDIA) == null) {
                        // Use the old media folder
                        dataFolders.put(DataRepository.PARAM_MEDIA, Paths.get(hotfolder.getDataRepository().getDir(DataRepository.PARAM_MEDIA)
                                .toAbsolutePath().toString(), pi));
                        if (!Files.isDirectory(dataFolders.get(DataRepository.PARAM_MEDIA))) {
                            dataFolders.put(DataRepository.PARAM_MEDIA, null);
                        } else {
                            logger.info("Using old media folder '{}'.", dataFolders.get(DataRepository.PARAM_MEDIA).toAbsolutePath());
                        }
                    }
                    if (dataFolders.get(DataRepository.PARAM_FULLTEXT) == null) {
                        // Use the old text folder
                        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get(hotfolder.getDataRepository().getDir(DataRepository.PARAM_FULLTEXT)
                                .toAbsolutePath().toString(), pi));
                        if (!Files.isDirectory(dataFolders.get(DataRepository.PARAM_FULLTEXT))) {
                            dataFolders.put(DataRepository.PARAM_FULLTEXT, null);
                        } else {
                            logger.info("Using old text folder '{}'.", dataFolders.get(DataRepository.PARAM_FULLTEXT).toAbsolutePath());
                        }
                    }
                    if (dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) == null) {
                        // Use the old crowdsourcing text folder
                        dataFolders.put(DataRepository.PARAM_FULLTEXTCROWD, Paths.get(hotfolder.getDataRepository().getDir(
                                DataRepository.PARAM_FULLTEXTCROWD).toAbsolutePath().toString(), pi));
                        if (!Files.isDirectory(dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD))) {
                            dataFolders.put(DataRepository.PARAM_FULLTEXTCROWD, null);
                        } else {
                            logger.info("Using old crowdsourcing text folder '{}'.", dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD)
                                    .toAbsolutePath());
                        }
                    }
                    if (dataFolders.get(DataRepository.PARAM_TEIMETADATA) == null) {
                        // Use the old TEI metadata folder
                        dataFolders.put(DataRepository.PARAM_TEIMETADATA, Paths.get(hotfolder.getDataRepository().getDir(
                                DataRepository.PARAM_TEIMETADATA).toAbsolutePath().toString(), pi));
                        if (!Files.isDirectory(dataFolders.get(DataRepository.PARAM_TEIMETADATA))) {
                            dataFolders.put(DataRepository.PARAM_TEIMETADATA, null);
                        } else {
                            logger.info("Using old TEI metadata folder '{}'.", dataFolders.get(DataRepository.PARAM_TEIMETADATA).toAbsolutePath());
                        }
                    }
                    if (dataFolders.get(DataRepository.PARAM_TEIWC) == null) {
                        // Use the old TEI word coordinate folder
                        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get(hotfolder.getDataRepository().getDir(DataRepository.PARAM_TEIWC)
                                .toAbsolutePath().toString(), pi));
                        if (!Files.isDirectory(dataFolders.get(DataRepository.PARAM_TEIWC))) {
                            dataFolders.put(DataRepository.PARAM_TEIWC, null);
                        } else {
                            logger.info("Using old TEI word coordinate folder '{}'.", dataFolders.get(DataRepository.PARAM_TEIWC).toAbsolutePath());
                        }
                    }
                    if (dataFolders.get(DataRepository.PARAM_ALTO) == null) {
                        // Use the old ALTO folder
                        dataFolders.put(DataRepository.PARAM_ALTO, Paths.get(hotfolder.getDataRepository().getDir(DataRepository.PARAM_ALTO)
                                .toAbsolutePath().toString(), pi));
                        if (!Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTO))) {
                            dataFolders.put(DataRepository.PARAM_ALTO, null);
                        } else {
                            logger.info("Using old ALTO folder '{}'.", dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath());
                        }
                    }
                    if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) == null) {
                        // Use the old crowdsourcing ALTO folder
                        dataFolders.put(DataRepository.PARAM_ALTOCROWD, Paths.get(hotfolder.getDataRepository().getDir(DataRepository.PARAM_ALTOCROWD)
                                .toAbsolutePath().toString(), pi));
                        if (!Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTOCROWD))) {
                            dataFolders.put(DataRepository.PARAM_ALTOCROWD, null);
                        } else {
                            logger.info("Using old crowdsourcing ALTO folder '{}'.", dataFolders.get(DataRepository.PARAM_ALTOCROWD)
                                    .toAbsolutePath());
                        }
                    }
                    if (dataFolders.get(DataRepository.PARAM_ABBYY) == null) {
                        // Use the old ABBYY folder
                        dataFolders.put(DataRepository.PARAM_ABBYY, Paths.get(hotfolder.getDataRepository().getDir(DataRepository.PARAM_ABBYY)
                                .toAbsolutePath().toString(), pi));
                        if (!Files.isDirectory(dataFolders.get(DataRepository.PARAM_ABBYY))) {
                            dataFolders.put(DataRepository.PARAM_ABBYY, null);
                        } else {
                            logger.info("Using old ABBYY folder '{}'.", dataFolders.get(DataRepository.PARAM_ABBYY).toAbsolutePath());
                        }
                    }
                    if (dataFolders.get(DataRepository.PARAM_UGC) == null) {
                        // Use the old user generated content folder
                        dataFolders.put(DataRepository.PARAM_UGC, Paths.get(hotfolder.getDataRepository().getDir(DataRepository.PARAM_UGC)
                                .toAbsolutePath().toString(), pi));
                        if (!Files.isDirectory(dataFolders.get(DataRepository.PARAM_UGC))) {
                            dataFolders.put(DataRepository.PARAM_UGC, null);
                        } else {
                            logger.info("Using old user generated content folder '{}'.", dataFolders.get(DataRepository.PARAM_UGC).toAbsolutePath());
                        }
                    }
                    if (dataFolders.get(DataRepository.PARAM_OVERVIEW) == null) {
                        // Use the old overview config folder
                        dataFolders.put(DataRepository.PARAM_OVERVIEW, Paths.get(hotfolder.getDataRepository().getDir(DataRepository.PARAM_OVERVIEW)
                                .toAbsolutePath().toString(), pi));
                        if (!Files.isDirectory(dataFolders.get(DataRepository.PARAM_OVERVIEW))) {
                            dataFolders.put(DataRepository.PARAM_OVERVIEW, null);
                        } else {
                            logger.info("Using old overview config folder '{}'.", dataFolders.get(DataRepository.PARAM_OVERVIEW).toAbsolutePath());
                        }
                    }
                } else {
                    ret[1] = "PI not found.";
                    throw new IndexerException(ret[1]);
                }
            }

            if (writeStrategy == null) {
                boolean useSerializingStrategy = false;
                long size = Files.size(mainFile);
                if (size >= hotfolder.metsFileSizeThreshold) {
                    useSerializingStrategy = true;
                    logger.info("WorldViews file is {} bytes, using a slower Solr write strategy to avoid memory overflows.", size);
                } else {
                    for (String key : dataFolders.keySet()) {
                        switch (key) {
                            case DataRepository.PARAM_ALTO:
                            case DataRepository.PARAM_ALTOCROWD:
                            case DataRepository.PARAM_FULLTEXT:
                            case DataRepository.PARAM_FULLTEXTCROWD:
                            case DataRepository.PARAM_ABBYY:
                            case DataRepository.PARAM_TEIWC:
                            case DataRepository.PARAM_TEIMETADATA:
                                Path dataFolder = dataFolders.get(key);
                                if (dataFolder != null) {
                                    // Files.size() does not work with directories, so use FileUtils
                                    long dataFolderSize = FileUtils.sizeOfDirectory(dataFolder.toFile());
                                    if (dataFolderSize >= hotfolder.dataFolderSizeThreshold) {
                                        useSerializingStrategy = true;
                                        logger.info("Data folder '{}' is {} bytes, using a slower Solr write strategy to avoid memory overflows.",
                                                dataFolder.toAbsolutePath().toString(), dataFolderSize);
                                        break;
                                    }
                                }
                                break;
                            default:
                                // do nothing
                        }
                    }
                }
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
            indexObj.addToLucene(SolrConstants.SOURCEDOCFORMAT, SolrConstants._WORLDVIEWS);

            prepareUpdate(indexObj);

            // Docstruct
            {
                String docstructType = xp.evaluateToString("worldviews//docType/text()", null);
                if (docstructType != null) {
                    indexObj.setType(docstructType);
                } else {
                    indexObj.setType("Monograph");
                    logger.warn("<docType> not found, setting docstruct type to 'Monograph'.");
                }
            }
            // LOGID
            indexObj.setLogId("LOG_0000");
            // Collections
            {
                List<String> collections = xp.evaluateToStringList("worldviews//collection", null);
                if (collections != null && !collections.isEmpty()) {
                    for (String collection : collections) {
                        indexObj.addToLucene(SolrConstants.DC, collection);
                    }
                }
            }
            // MD_WV_*SOURCE
            {
                String sourcePi = xp.evaluateToString("worldviews//relatedItem[@type='primarySource']/identifier/text()", null);
                if (sourcePi != null) {
                    indexObj.addToLucene("MD_WV_PRIMARYSOURCE", sourcePi);
                } else {
                    // For sources use own PI
                    indexObj.addToLucene("MD_WV_PRIMARYSOURCE", indexObj.getPi());
                }
            }
            {
                String sourcePi = xp.evaluateToString("worldviews//relatedItem[@type='secondarySource']/identifier/text()", null);
                if (sourcePi != null) {
                    indexObj.addToLucene("MD_WV_SECONDARYSOURCE", sourcePi);
                }
            }

            int workDepth = 0; // depth of the docstrct that has ISWORK (volume or monograph)

            // Process TEI files
            if (dataFolders.containsKey(DataRepository.PARAM_TEIMETADATA)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataFolders.get(DataRepository.PARAM_TEIMETADATA), "*.{xml}")) {
                    boolean fulltextAdded = false;
                    for (Path path : stream) {
                        logger.info("Found TEI file: {}", path.getFileName().toString());
                        JDomXP tei = new JDomXP(path.toFile());
                        teiDocuments.add(tei);
                        MetadataHelper.writeMetadataToObject(indexObj, tei.getRootElement(), "", tei);

                        // Add text body
                        Element eleText = tei.getRootElement().getChild("text", null);
                        if (eleText != null && eleText.getChild("body", null) != null) {
                            String language = eleText.getAttributeValue("lang", Configuration.getInstance().getNamespaces().get("xml")); // TODO extract language from a different element? - No, this is the correct element (Florian)
                            String fileFieldName = SolrConstants.FILENAME_TEI;
                            if (language != null) {
//                                String isoCode = MetadataConfigurationManager.getLanguageMapping(language);
                                String isoCode = LanguageHelper.getInstance().getLanguage(language).getIsoCodeOld();
                                if (isoCode != null) {
                                    fileFieldName += SolrConstants._LANG_ + isoCode.toUpperCase();
                                }
                                indexObj.getLanguages().add(isoCode);
                            }
                            indexObj.addToLucene(fileFieldName, path.getFileName().toString());

                            // Add searchable version of the text
                            Element eleBody = eleText.getChild("body", null);
                            Element eleNewRoot = new Element("tempRoot");
                            for (Element ele : eleBody.getChildren()) {
                                eleNewRoot.addContent(ele.clone());
                            }
                            if (!fulltextAdded) {
                                String body = TextHelper.getStringFromElement(eleNewRoot, null).replace("<tempRoot>", "").replace("</tempRoot>", "")
                                        .trim();
                                indexObj.addToLucene(SolrConstants.FULLTEXT, Jsoup.parse(body).text());
                                fulltextAdded = true;
                            }
                        } else {
                            logger.warn("No text body found in TEI");
                        }

                    }
                }
            }
            // Add IndexObject member values as Solr fields (after processing the TEI files!)
            indexObj.pushSimpleDataToLuceneArray();

            // Write mapped metadata
            MetadataHelper.writeMetadataToObject(indexObj, xp.getRootElement(), "", xp);

            LuceneField label = indexObj.getLuceneFieldWithName("MD_TITLE");
            if (label != null) {
                indexObj.setLabel(label.getValue());
                indexObj.addToLucene(SolrConstants.LABEL, MetadataHelper.applyValueDefaultModifications(label.getValue()));
            }

            // Add language codes as metadata fields
            indexObj.writeLanguages();

            // Set access conditions
            indexObj.writeAccessConditions(null);

            // Write created/updated timestamps
            indexObj.writeDateModified(!fromReindexQueue && !noTimestampUpdate);

            // Generate docs for all pages and add to the write strategy
            generatePageDocuments(writeStrategy, dataFolders, pageCountStart);
            indexObj.setNumPages(writeStrategy.getPageDocsSize());
            if (indexObj.getNumPages() > 0) {
                indexObj.addToLucene(SolrConstants.NUMPAGES, String.valueOf(indexObj.getNumPages()));
                if (indexObj.getFirstPageLabel() != null) {
                    indexObj.addToLucene(SolrConstants.ORDERLABELFIRST, indexObj.getFirstPageLabel());
                }
                if (indexObj.getLastPageLabel() != null) {
                    indexObj.addToLucene(SolrConstants.ORDERLABELLAST, indexObj.getLastPageLabel());
                }
                if (indexObj.getFirstPageLabel() != null && indexObj.getLastPageLabel() != null) {
                    indexObj.addToLucene("MD_ORDERLABELRANGE", new StringBuilder(indexObj.getFirstPageLabel()).append(" - ").append(indexObj
                            .getLastPageLabel()).toString());
                }
            }

            // If full-text has been indexed for any page, set a boolean in the root doc indicating that the records does have full-text
            if (hasFulltext) {
                indexObj.addToLucene(SolrConstants.FULLTEXTAVAILABLE, "true");
            }

            // Add THUMBNAIL,THUMBPAGENO,THUMBPAGENOLABEL (must be done AFTER writeDateMondified(), writeAccessConditions() and generatePageDocuments()!)
            generateChildDocstructDocuments(indexObj, true, writeStrategy, dataFolders, workDepth);

            // ISWORK only for non-anchors
            indexObj.addToLucene(SolrConstants.ISWORK, "true");

            // Add DEFAULT field
            if (StringUtils.isNotEmpty(indexObj.getDefaultValue())) {
                indexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(indexObj.getDefaultValue()));
                // indexObj.getSuperDefaultBuilder().append(' ').append(indexObj.getDefaultValue().trim());
                indexObj.setDefaultValue("");
            }

            if (dataFolders.get(DataRepository.PARAM_OVERVIEW) != null) {
                Path staticPageFolder = dataFolders.get(DataRepository.PARAM_OVERVIEW);
                if (Files.isDirectory(staticPageFolder)) {
                    // TODO NIO
                    File[] files = staticPageFolder.toFile().listFiles(xml);
                    if (files.length > 0) {
                        for (File file : files) {
                            switch (file.getName()) {
                                case "description.xml": {
                                    String content = TextHelper.readFileToString(file);
                                    indexObj.addToLucene(SolrConstants.OVERVIEWPAGE_DESCRIPTION, Jsoup.parse(content).text());
                                }
                                    break;
                                case "publicationtext.xml": {
                                    String content = TextHelper.readFileToString(file);
                                    indexObj.addToLucene(SolrConstants.OVERVIEWPAGE_PUBLICATIONTEXT, Jsoup.parse(content).text());
                                }
                                    break;
                            }
                        }
                    }
                }
            }

            // Create group documents if this record is part of a group and no doc exists for that group yet
            for (String groupIdField : indexObj.getGroupIds().keySet()) {
                Map<String, String> moreMetadata = new HashMap<>();
                if (indexObj.getLuceneFieldWithName("MD_SHELFMARK") != null) {
                    moreMetadata.put("MD_SHELFMARK", indexObj.getLuceneFieldWithName("MD_SHELFMARK").getValue());
                }
                if (indexObj.getLuceneFieldWithName("MD_SERIESTITLE") != null) {
                    moreMetadata.put("LABEL", indexObj.getLuceneFieldWithName("MD_SERIESTITLE").getValue());
                    moreMetadata.put("MD_TITLE", indexObj.getLuceneFieldWithName("MD_SERIESTITLE").getValue());
                }
                SolrInputDocument doc = hotfolder.getSolrHelper().checkAndCreateGroupDoc(groupIdField, indexObj.getGroupIds().get(groupIdField),
                        moreMetadata, getNextIddoc(hotfolder.getSolrHelper()));
                if (doc != null) {
                    writeStrategy.addDoc(doc);
                    logger.debug("Created group document for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                } else {
                    logger.debug("Group document already exists for {}: {}", groupIdField, indexObj.getGroupIds().get(groupIdField));
                }
            }

            // Add grouped metadata as separate Solr docs (remove duplicates first)
            indexObj.removeDuplicateGroupedMetadata();
            logger.debug("Group fields: {}", indexObj.getGroupedMetadataFields().size());
            for (GroupedMetadata gmd : indexObj.getGroupedMetadataFields()) {
                logger.debug("adding group field: {}", gmd.getMainValue());
                SolrInputDocument doc = SolrHelper.createDocument(gmd.getFields());
                long iddoc = getNextIddoc(hotfolder.getSolrHelper());
                doc.addField(SolrConstants.IDDOC, iddoc);
                if (!doc.getFieldNames().contains(SolrConstants.GROUPFIELD)) {
                    logger.warn("{} not set in grouped metadata doc {}, using IDDOC instead.", SolrConstants.GROUPFIELD, doc.getFieldValue(
                            SolrConstants.LABEL));
                    doc.addField(SolrConstants.GROUPFIELD, iddoc);
                }
                doc.addField(SolrConstants.IDDOC_OWNER, indexObj.getIddoc());
                doc.addField(SolrConstants.DOCTYPE, DocType.METADATA.name());
                doc.addField(SolrConstants.PI_TOPSTRUCT, indexObj.getPi());
                writeStrategy.addDoc(doc);
            }

            boolean indexedChildrenFileList = false;

            logger.debug("reindexedChildrenFileList.size(): {}", WorldViewsIndexer.reindexedChildrenFileList.size());
            if (WorldViewsIndexer.reindexedChildrenFileList.contains(mainFile)) {
                logger.debug("{} in reindexedChildrenFileList, removing...", mainFile.toAbsolutePath());
                WorldViewsIndexer.reindexedChildrenFileList.remove(mainFile);
                indexedChildrenFileList = true;
            }

            // WRITE TO SOLR (POINT OF NO RETURN: any indexObj modifications from here on will not be included in the index!)
            logger.debug("Writing document to index...");
            SolrInputDocument rootDoc = SolrHelper.createDocument(indexObj.getLuceneFields());
            writeStrategy.setRootDoc(rootDoc);
            writeStrategy.writeDocs(Configuration.getInstance().isAggregateRecords());
            if (indexObj.isVolume() && (!indexObj.isUpdate() || indexedChildrenFileList)) {
                logger.info("Re-indexing anchor...");
                copyAndReIndexAnchor(indexObj, hotfolder);
            }
            logger.info("Successfully finished indexing '{}'.", mainFile.getFileName());
        } catch (Exception e) {
            logger.error("Indexing of '{}' could not be finished due to an error.", mainFile.getFileName());
            logger.error(e.getMessage(), e);
            ret[1] = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
            hotfolder.getSolrHelper().rollback();
        } finally {
            if (writeStrategy != null) {
                writeStrategy.cleanup();
            }
        }

        return ret;
    }

    /**
     * Generates thumbnail info fields for the given docstruct. Also generates page docs mapped to this docstruct. <code>IndexObj.topstructPi</code>
     * must be set before calling this method.
     * 
     * @param indexObj {@link IndexObject}
     * @param isWork
     * @param writeStrategy
     * @param dataFolders
     * @param depth Depth of the current docstruct in the docstruct hierarchy.
     * @return {@link LuceneField}
     * @throws IndexerException -
     * @throws IOException
     * @throws FatalIndexerException
     */
    private void generateChildDocstructDocuments(IndexObject rootIndexObj, boolean isWork, ISolrWriteStrategy writeStrategy,
            Map<String, Path> dataFolders, int depth) throws IndexerException, IOException, FatalIndexerException {
        IndexObject currentIndexObj = null;
        String currentDocstructLabel = null;

        String firstPageFile = null;
        String thumbnailFile = null;
        int docstructCount = 0;
        for (int i = 1; i <= writeStrategy.getPageDocsSize(); ++i) {
            SolrInputDocument pageDoc = writeStrategy.getPageDocForOrder(i);

            if (thumbnailFile == null || pageDoc.containsKey(SolrConstants.THUMBNAILREPRESENT) || (currentIndexObj != null && currentIndexObj
                    .getThumbnailRepresent() != null && currentIndexObj.getThumbnailRepresent().equals(pageDoc.getFieldValue(
                            SolrConstants.FILENAME)))) {
                thumbnailFile = (String) pageDoc.getFieldValue(SolrConstants.FILENAME);
                if (pageDoc.containsKey(SolrConstants.THUMBNAILREPRESENT)) {
                    pageDoc.removeField(SolrConstants.THUMBNAILREPRESENT);
                }
            }

            String pageDocstructLabel = (String) pageDoc.getFieldValue(SolrConstants.LABEL);
            String orderLabel = (String) pageDoc.getFieldValue(SolrConstants.ORDERLABEL);
            String pageFileBaseName = FilenameUtils.getBaseName((String) pageDoc.getFieldValue(SolrConstants.FILENAME));

            // New docstruct
            if (pageDocstructLabel != null && !pageDocstructLabel.equals(currentDocstructLabel)) {
                currentDocstructLabel = pageDocstructLabel;
                docstructCount++;

                // Finalize previous docstruct
                if (currentIndexObj != null) {
                    finalizeChildDocstruct(currentIndexObj, writeStrategy);
                }

                // Create new docstruct object
                currentIndexObj = new IndexObject(getNextIddoc(hotfolder.getSolrHelper()));
                currentIndexObj.setParent(rootIndexObj);
                currentIndexObj.setTopstructPI(rootIndexObj.getTopstructPI());
                currentIndexObj.setType(pageDocstructLabel);
                currentIndexObj.setLabel((String) pageDoc.getFieldValue(SolrConstants.LABEL));
                currentIndexObj.getParentLabels().add(rootIndexObj.getLabel());
                currentIndexObj.getParentLabels().addAll(rootIndexObj.getParentLabels());
                if (StringUtils.isNotEmpty(rootIndexObj.getDataRepository())) {
                    currentIndexObj.setDataRepository(rootIndexObj.getDataRepository());
                }
                currentIndexObj.setLogId("LOG_" + MetadataHelper.formatQuadrupleDigit.format(docstructCount));
                currentIndexObj.pushSimpleDataToLuceneArray();

                // This is a new docstruct, so the current page is its first
                currentIndexObj.setFirstPageLabel(orderLabel);

                // Set parent's DATEUPDATED value (needed for OAI)
                for (Long dateUpdated : rootIndexObj.getDateUpdated()) {
                    if (!currentIndexObj.getDateUpdated().contains(dateUpdated)) {
                        currentIndexObj.getDateUpdated().add(dateUpdated);
                        currentIndexObj.addToLucene(SolrConstants.DATEUPDATED, String.valueOf(dateUpdated));
                    }
                }

                // Set thumbnail info
                currentIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBPAGENO, String.valueOf(i)));
                currentIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBPAGENOLABEL, orderLabel));
                currentIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBNAIL, (String) pageDoc.getFieldValue(SolrConstants.FILENAME)));

                // Add parent's metadata and SORT_* fields to this docstruct
                Set<String> existingMetadataFields = new HashSet<>();
                Set<String> existingSortFieldNames = new HashSet<>();
                for (LuceneField field : currentIndexObj.getLuceneFields()) {
                    if (Configuration.getInstance().getMetadataConfigurationManager().getFieldsToAddToPages().contains(field.getField())) {
                        existingMetadataFields.add(new StringBuilder(field.getField()).append(field.getValue()).toString());
                    } else if (field.getField().startsWith(SolrConstants.SORT_)) {
                        existingSortFieldNames.add(field.getField());
                    }
                }
                for (LuceneField field : rootIndexObj.getLuceneFields()) {
                    if (Configuration.getInstance().getMetadataConfigurationManager().getFieldsToAddToChildren().contains(field.getField())
                            && !existingMetadataFields.contains(new StringBuilder(field.getField()).append(field.getValue()).toString())) {
                        // Avoid duplicates (same field name + value)
                        currentIndexObj.addToLucene(field.getField(), field.getValue());
                        logger.debug("Added {}:{} to child element {}", field.getField(), field.getValue(), currentIndexObj.getLogId());
                    } else if (field.getField().startsWith(SolrConstants.SORT_) && !existingSortFieldNames.contains(field.getField())) {
                        // Only one instance of each SORT_ field may exist
                        currentIndexObj.addToLucene(field.getField(), field.getValue());
                    }
                }

                currentIndexObj.writeAccessConditions(rootIndexObj);

                // Add grouped metadata as separate documents
                List<LuceneField> dcFields = currentIndexObj.getLuceneFieldsWithName(SolrConstants.DC);
                for (GroupedMetadata gmd : currentIndexObj.getGroupedMetadataFields()) {
                    SolrInputDocument doc = SolrHelper.createDocument(gmd.getFields());
                    long iddoc = getNextIddoc(hotfolder.getSolrHelper());
                    doc.addField(SolrConstants.IDDOC, iddoc);
                    if (!doc.getFieldNames().contains(SolrConstants.GROUPFIELD)) {
                        logger.warn("{} not set in grouped metadata doc {}, using IDDOC instead.", SolrConstants.GROUPFIELD, doc.getFieldValue(
                                SolrConstants.LABEL));
                        doc.addField(SolrConstants.GROUPFIELD, iddoc);
                    }
                    // IDDOC_OWNER should always contain the IDDOC of the lowest docstruct to which this page is mapped. Since child docstructs are added recursively, this should be the case without further conditions.
                    doc.addField(SolrConstants.IDDOC_OWNER, rootIndexObj.getIddoc());
                    doc.addField(SolrConstants.DOCTYPE, DocType.METADATA.name());
                    doc.addField(SolrConstants.PI_TOPSTRUCT, rootIndexObj.getTopstructPI());

                    // Add DC values to metadata doc
                    if (dcFields != null) {
                        for (LuceneField field : dcFields) {
                            doc.addField(field.getField(), field.getValue());
                        }
                    }

                    writeStrategy.addDoc(doc);
                }

                // Add own and all ancestor LABEL values to the DEFAULT field
                StringBuilder sbDefaultValue = new StringBuilder();
                sbDefaultValue.append(currentIndexObj.getDefaultValue());
                String labelWithSpaces = new StringBuilder(" ").append(currentIndexObj.getLabel()).append(' ').toString();
                if (StringUtils.isNotEmpty(currentIndexObj.getLabel()) && !sbDefaultValue.toString().contains(labelWithSpaces)) {
                    // logger.info("Adding own LABEL to DEFAULT: " + indexObj.getLabel());
                    sbDefaultValue.append(labelWithSpaces);
                }
                if (Configuration.getInstance().isAddLabelToChildren()) {
                    for (String label : currentIndexObj.getParentLabels()) {
                        String parentLabelWithSpaces = new StringBuilder(" ").append(label).append(' ').toString();
                        if (StringUtils.isNotEmpty(label) && !sbDefaultValue.toString().contains(parentLabelWithSpaces)) {
                            // logger.info("Adding ancestor LABEL to DEFAULT: " + label);
                            sbDefaultValue.append(parentLabelWithSpaces);
                        }
                    }
                }

                currentIndexObj.setDefaultValue(sbDefaultValue.toString());

                // Add DEFAULT field
                if (StringUtils.isNotEmpty(currentIndexObj.getDefaultValue())) {
                    currentIndexObj.addToLucene(SolrConstants.DEFAULT, cleanUpDefaultField(currentIndexObj.getDefaultValue()));
                    // Add default value to parent doc
                    // parentIndexObject.getSuperDefaultBuilder().append(' ').append(indexObj.getDefaultValue().trim());
                    currentIndexObj.setDefaultValue("");
                }
            }

            // Add more fields to the page
            currentIndexObj.setNumPages(currentIndexObj.getNumPages() + 1);
            pageDoc.addField(SolrConstants.IDDOC_OWNER, currentIndexObj.getIddoc());
            if (pageDoc.getField(SolrConstants.PI_TOPSTRUCT) == null) {
                pageDoc.addField(SolrConstants.PI_TOPSTRUCT, rootIndexObj.getTopstructPI());
            }
            if (pageDoc.getField(SolrConstants.DATAREPOSITORY) == null && currentIndexObj.getDataRepository() != null) {
                pageDoc.addField(SolrConstants.DATAREPOSITORY, rootIndexObj.getDataRepository());
            }
            if (pageDoc.getField(SolrConstants.DATEUPDATED) == null && !rootIndexObj.getDateUpdated().isEmpty()) {
                for (Long date : rootIndexObj.getDateUpdated()) {
                    pageDoc.addField(SolrConstants.DATEUPDATED, date);
                }
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
            for (LuceneField field : currentIndexObj.getLuceneFields()) {
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

            // Add used-generated content docs
            if (dataFolders.get(DataRepository.PARAM_UGC) != null && pageDoc.getField(SolrConstants.UGCTERMS) == null) {
                writeStrategy.addDocs(generateUserGeneratedContentDocsForPage(pageDoc, dataFolders.get(DataRepository.PARAM_UGC), String.valueOf(
                        rootIndexObj.getParentPI()), (Integer) pageDoc.getFieldValue(SolrConstants.ORDER), pageFileBaseName));
            }

            // Make sure IDDOC_OWNER of a page contains the iddoc of the lowest possible mapped docstruct
            if (pageDoc.getField("MDNUM_OWNERDEPTH") == null || depth > (Integer) pageDoc.getFieldValue("MDNUM_OWNERDEPTH")) {
                pageDoc.setField(SolrConstants.IDDOC_OWNER, String.valueOf(currentIndexObj.getIddoc()));
                pageDoc.setField("MDNUM_OWNERDEPTH", depth);

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
                if (currentIndexObj.getIddoc() == Long.valueOf((String) pageDoc.getFieldValue(SolrConstants.IDDOC_OWNER))) {
                    for (LuceneField field : currentIndexObj.getLuceneFields()) {
                        if (field.getField().startsWith(SolrConstants.SORT_)) {
                            pageDoc.addField(field.getField(), field.getValue());
                        }
                    }
                }
            }

            // Update the doc in the write strategy (otherwise some implementations might ignore the changes).
            writeStrategy.updateDoc(pageDoc);
        }

        // Finalize last docstruct
        if (currentIndexObj != null) {
            finalizeChildDocstruct(currentIndexObj, writeStrategy);
        }

        // Set root doc thumbnail
        rootIndexObj.setThumbnailRepresent(thumbnailFile);
        rootIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBPAGENO, "1"));
        rootIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBPAGENOLABEL, " - "));
        rootIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBNAIL, thumbnailFile));
        rootIndexObj.addToLucene(new LuceneField(SolrConstants.THUMBNAILREPRESENT, thumbnailFile));

    }

    /**
     * 
     * @param indexObj
     * @param writeStrategy
     */
    private static void finalizeChildDocstruct(IndexObject indexObj, ISolrWriteStrategy writeStrategy) {
        // Write number of pages and first/last page labels
        if (indexObj.getNumPages() > 0) {
            indexObj.addToLucene(SolrConstants.NUMPAGES, String.valueOf(indexObj.getNumPages()));
            if (indexObj.getFirstPageLabel() != null) {
                indexObj.addToLucene(SolrConstants.ORDERLABELFIRST, indexObj.getFirstPageLabel());
            }
            if (indexObj.getLastPageLabel() != null) {
                indexObj.addToLucene(SolrConstants.ORDERLABELLAST, indexObj.getLastPageLabel());
            }
            if (indexObj.getFirstPageLabel() != null && indexObj.getLastPageLabel() != null) {
                indexObj.addToLucene("MD_ORDERLABELRANGE", new StringBuilder(indexObj.getFirstPageLabel()).append(" - ").append(indexObj
                        .getLastPageLabel()).toString());
            }
        }

        // Write docstruct doc to Solr
        logger.debug("Writing child document '{}'...", indexObj.getIddoc());
        writeStrategy.addDoc(SolrHelper.createDocument(indexObj.getLuceneFields()));
    }

    /**
     * Generates a SolrInputDocument for each page that is mapped to a docstruct. Adds all page metadata except those that come from the owning
     * docstruct (such as docstruct iddoc, type, collection, etc.).
     * 
     * @param writeStrategy
     * @param dataFolders
     * @param pageCountStart
     * @return
     * @throws FatalIndexerException
     * @should create documents for all mapped pages
     * @should set correct ORDER values
     * @should skip unmapped pages
     * @should switch to DEFAULT file group correctly
     * @should maintain page order after parallel processing
     */
    public void generatePageDocuments(final ISolrWriteStrategy writeStrategy, final Map<String, Path> dataFolders, int pageCountStart)
            throws FatalIndexerException {
        // Get all physical elements
        List<Element> eleListImages = xp.evaluateToElements("worldviews/resource/images/image", null);
        logger.info("Generating {} page documents (count starts at {})...", eleListImages.size(), pageCountStart);

        if (Configuration.getInstance().getThreads() > 1) {
            ExecutorService executor = Executors.newFixedThreadPool(Configuration.getInstance().getThreads());
            for (final Element eleImage : eleListImages) {

                // Generate each page document in its own thread
                Runnable r = new Runnable() {

                    @Override
                    public void run() {
                        try {
                            generatePageDocument(eleImage, String.valueOf(getNextIddoc(hotfolder.getSolrHelper())), null, writeStrategy, dataFolders);
                        } catch (FatalIndexerException e) {
                            logger.error("Should be exiting here now...");
                        } finally {
                        }
                    }
                };
                executor.execute(r);
            }
            executor.shutdown();
            while (!executor.isTerminated()) {
            }

            // TODO lambda instead of loop with Java 8
            //        eleStructMapPhysicalList.parallelStream().forEach(
            //                eleStructMapPhysical -> generatePageDocument(eleStructMapPhysical, String.valueOf(getNextIddoc(hotfolder.getSolrHelper())), null,
            //                        writeStrategy, dataFolders));
        } else {
            int order = pageCountStart;
            for (final Element eleImage : eleListImages) {
                if (generatePageDocument(eleImage, String.valueOf(getNextIddoc(hotfolder.getSolrHelper())), order, writeStrategy, dataFolders)) {
                    order++;
                }
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
     * @return
     * @throws FatalIndexerException
     * @should add all basic fields
     * @should add page metadata correctly
     */
    boolean generatePageDocument(Element eleImage, String iddoc, Integer order, ISolrWriteStrategy writeStrategy, Map<String, Path> dataFolders)
            throws FatalIndexerException {
        String id = eleImage.getAttributeValue("ID");
        if (order == null) {
            order = Integer.parseInt(eleImage.getChildText("sequence"));
        }
        logger.trace("generatePageDocument: {} (IDDOC {}) processed by thread {}", order, iddoc, Thread.currentThread().getId());

        // Create Solr document for this page
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(SolrConstants.IDDOC, iddoc);
        doc.addField(SolrConstants.GROUPFIELD, iddoc);
        doc.addField(SolrConstants.DOCTYPE, DocType.PAGE.name());
        doc.addField(SolrConstants.PHYSID, "PHYS_" + MetadataHelper.formatQuadrupleDigit.format(order));
        doc.addField(SolrConstants.ORDER, order);
        doc.addField(SolrConstants.ORDERLABEL, " - ");

        boolean displayImage = Boolean.valueOf(eleImage.getChildText("displayImage"));
        if (displayImage) {
            // Add file name
            String fileName = eleImage.getChildText("fileName");
            doc.addField(SolrConstants.FILENAME, fileName);

            // Add file size
            if (dataFolders != null) {
                try {
                    Path dataFolder = dataFolders.get(DataRepository.PARAM_MEDIA);
                    // TODO other mime types/folders
                    if (dataFolder != null) {
                        Path path = Paths.get(dataFolder.toAbsolutePath().toString(), fileName);
                        doc.addField("MDNUM_FILESIZE", Files.size(path));
                    } else {
                        doc.addField("MDNUM_FILESIZE", -1);
                    }
                } catch (FileNotFoundException e) {
                    logger.error(e.getMessage());
                    doc.addField("MDNUM_FILESIZE", -1);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    doc.addField("MDNUM_FILESIZE", -1);
                } catch (IllegalArgumentException e) {
                    logger.error(e.getMessage(), e);
                    doc.addField("MDNUM_FILESIZE", -1);
                }
            }

            // Representative image
            boolean representative = Boolean.valueOf(eleImage.getChildText("representative"));
            if (representative) {
                doc.addField(SolrConstants.THUMBNAILREPRESENT, fileName);
            }
        } else {
            // TODO placeholder
            String placeholder = eleImage.getChildText("placeholder");
        }
        doc.addField(SolrConstants.MIMETYPE, "image");

        // Copyright
        String copyright = eleImage.getChildText("copyright");
        if (StringUtils.isNotEmpty(copyright)) {
            doc.addField("MD_COPYRIGHT", copyright);
        }
        // access condition
        String license = eleImage.getChildText("licence");
        if (StringUtils.isNotEmpty(license)) {
            doc.addField(SolrConstants.ACCESSCONDITION, license);
        }

        // Metadata payload for later evaluation
        String structType = eleImage.getChildText("structType");
        if (structType != null) {
            doc.addField(SolrConstants.LABEL, structType);
            doc.addField(SolrConstants.DOCSTRCT, "OtherDocStrct"); // TODO
        }

        if (dataFolders != null) {
            Map<String, Object> altoData = null;
            String baseFileName = FilenameUtils.getBaseName((String) doc.getFieldValue(SolrConstants.FILENAME));

            // Add complete crowdsourcing ALTO document and full-text generated from ALTO, if available
            boolean foundCrowdsourcingData = false;
            if (dataFolders.get(DataRepository.PARAM_ALTOCROWD) != null) {
                try {
                    altoData = TextHelper.readAltoFile(new File(dataFolders.get(DataRepository.PARAM_ALTOCROWD).toAbsolutePath().toString(),
                            baseFileName + XML_EXTENSION));
                } catch (FileNotFoundException e) {
                    // Not all pages will have custom ALTO docs
                } catch (JDOMException | IOException e) {
                    logger.error(e.getMessage(), e);
                }
                if (altoData != null) {
                    foundCrowdsourcingData = true;
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO))) {
                        doc.addField(SolrConstants.ALTO, altoData.get(SolrConstants.ALTO));
                        doc.addField(SolrConstants.FILENAME_ALTO, baseFileName + XML_EXTENSION);
                        logger.debug("Added ALTO from crowdsourcing ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT))) {
                        doc.addField(SolrConstants.FULLTEXT, Jsoup.parse((String) altoData.get(SolrConstants.FULLTEXT)).text());
                        // doc.addField("MD_FULLTEXT", altoData.get(SolrConstants.FULLTEXT));
                        logger.debug("Added FULLTEXT from crowdsourcing ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                        doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                        logger.debug("Added WIDTH from crowdsourcing ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                        doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                        logger.debug("Added WIDTH from crowdsourcing ALTO for page {}", order);
                    }
                    if (altoData.get(SolrConstants.NAMEDENTITIES) != null) {
                        addNamedEntitiesFields(altoData, doc);
                    }
                }
            }

            // Look for plain fulltext from crowdsouring, if the FULLTEXT field is still empty
            if (doc.getField(SolrConstants.FULLTEXT) == null && dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD) != null) {
                String fulltext = TextHelper.generateFulltext(baseFileName + TXT_EXTENSION, dataFolders.get(DataRepository.PARAM_FULLTEXTCROWD),
                        false);
                if (fulltext != null) {
                    foundCrowdsourcingData = true;
                    doc.addField(SolrConstants.FULLTEXT, Jsoup.parse(fulltext).text());
                    // doc.addField("MD_FULLTEXT", fulltext);
                    doc.addField(SolrConstants.FILENAME_FULLTEXT, baseFileName + TXT_EXTENSION);
                    logger.debug("Added FULLTEXT from crowdsourcing plain text for page {}", order);
                }
            }
            // Look for a regular ALTO document for this page and fill ALTO and/or FULLTEXT fields, whichever is still empty
            if (!foundCrowdsourcingData && (doc.getField(SolrConstants.ALTO) == null || doc.getField(SolrConstants.FULLTEXT) == null) && dataFolders
                    .get(DataRepository.PARAM_ALTO) != null) {
                try {
                    altoData = TextHelper.readAltoFile(new File(dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), baseFileName
                            + XML_EXTENSION));
                } catch (JDOMException e) {
                    logger.error(e.getMessage(), e);
                } catch (IOException e) {
                    logger.warn(e.getMessage());
                }
                if (altoData != null) {
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO)) && doc.getField(SolrConstants.ALTO) == null) {
                        doc.addField(SolrConstants.ALTO, altoData.get(SolrConstants.ALTO));
                        doc.addField(SolrConstants.FILENAME_ALTO, baseFileName + XML_EXTENSION);
                        logger.debug("Added ALTO from regular ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT)) && doc.getField(SolrConstants.FULLTEXT) == null) {
                        doc.addField(SolrConstants.FULLTEXT, Jsoup.parse((String) altoData.get(SolrConstants.FULLTEXT)).text());
                        // doc.addField("MD_FULLTEXT", altoData.get(SolrConstants.FULLTEXT));
                        logger.debug("Added FULLTEXT from regular ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                        doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                        logger.debug("Added WIDTH from regular ALTO for page {}", order);
                    }
                    if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                        doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                        logger.debug("Added WIDTH from regular ALTO for page {}", order);
                    }
                    if (altoData.get(SolrConstants.NAMEDENTITIES) != null) {
                        addNamedEntitiesFields(altoData, doc);
                    }
                }
            }

            // If FULLTEXT is still empty, look for a plain full-text
            if (!foundCrowdsourcingData && doc.getField(SolrConstants.FULLTEXT) == null && dataFolders.get(DataRepository.PARAM_FULLTEXT) != null) {
                String fulltext = TextHelper.generateFulltext(baseFileName + TXT_EXTENSION, dataFolders.get(DataRepository.PARAM_FULLTEXT), true);
                if (fulltext != null) {
                    doc.addField(SolrConstants.FULLTEXT, Jsoup.parse(fulltext).text());
                    // doc.addField("MD_FULLTEXT", fulltext);
                    doc.addField(SolrConstants.FILENAME_FULLTEXT, baseFileName + TXT_EXTENSION);
                    logger.debug("Added FULLTEXT from regular plain text for page {}", order);
                }
            }

            // ABBYY XML (converted to ALTO)
            if (!foundCrowdsourcingData && dataFolders.get(DataRepository.PARAM_ABBYY) != null) {
                try {
                    try {
                        altoData = TextHelper.readAbbyyToAlto(new File(dataFolders.get(DataRepository.PARAM_ABBYY).toAbsolutePath().toString(),
                                baseFileName + XML_EXTENSION));
                        if (altoData != null) {
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO)) && doc.getField(SolrConstants.ALTO) == null) {
                                doc.addField(SolrConstants.ALTO, altoData.get(SolrConstants.ALTO));
                                logger.debug("Added ALTO from regular ALTO for page {}", order);
                            }
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT)) && doc.getField(
                                    SolrConstants.FULLTEXT) == null) {
                                doc.addField(SolrConstants.FULLTEXT, Jsoup.parse((String) altoData.get(SolrConstants.FULLTEXT)).text());
                                doc.addField("MD_FULLTEXT", altoData.get(SolrConstants.FULLTEXT));
                                logger.debug("Added FULLTEXT from regular ALTO for page {}", order);
                            }
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                                doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                                logger.debug("Added WIDTH from regular ALTO for page {}", order);
                            }
                            if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                                doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                                logger.debug("Added WIDTH from regular ALTO for page {}", order);
                            }
                            if (altoData.get(SolrConstants.NAMEDENTITIES) != null) {
                                addNamedEntitiesFields(altoData, doc);
                            }
                        }
                    } catch (FileNotFoundException e) {
                        logger.warn(e.getMessage());
                    }
                } catch (XMLStreamException e) {
                    logger.error(e.getMessage(), e);
                } catch (IOException e) {
                    logger.warn(e.getMessage());
                }
            }

            // Read word coords from TEI only if none has been read from ALTO for this page yet
            if (!foundCrowdsourcingData && dataFolders.get(DataRepository.PARAM_TEIWC) != null) {
                try {
                    altoData = TextHelper.readTeiToAlto(new File(dataFolders.get(DataRepository.PARAM_TEIWC).toAbsolutePath().toString(), baseFileName
                            + XML_EXTENSION));
                    if (altoData != null) {
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.ALTO)) && doc.getField(SolrConstants.ALTO) == null) {
                            doc.addField(SolrConstants.ALTO, altoData.get(SolrConstants.ALTO));
                            doc.addField(SolrConstants.FILENAME_ALTO, baseFileName + XML_EXTENSION);
                            logger.debug("Added ALTO from regular ALTO for page {}", order);
                        }
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.FULLTEXT)) && doc.getField(SolrConstants.FULLTEXT) == null) {
                            doc.addField(SolrConstants.FULLTEXT, Jsoup.parse((String) altoData.get(SolrConstants.FULLTEXT)).text());
                            // doc.addField("MD_FULLTEXT", altoData.get(SolrConstants.FULLTEXT));
                            logger.debug("Added FULLTEXT from regular ALTO for page {}", order);

                        }
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.WIDTH)) && doc.getField(SolrConstants.WIDTH) == null) {
                            doc.addField(SolrConstants.WIDTH, altoData.get(SolrConstants.WIDTH));
                            logger.debug("Added WIDTH from regular ALTO for page {}", order);
                        }
                        if (StringUtils.isNotEmpty((String) altoData.get(SolrConstants.HEIGHT)) && doc.getField(SolrConstants.HEIGHT) == null) {
                            doc.addField(SolrConstants.HEIGHT, altoData.get(SolrConstants.HEIGHT));
                            logger.debug("Added WIDTH from regular ALTO for page {}", order);
                        }
                        if (altoData.get(SolrConstants.NAMEDENTITIES) != null) {
                            addNamedEntitiesFields(altoData, doc);
                        }
                    }
                } catch (JDOMException e) {
                    logger.error(e.getMessage(), e);
                } catch (IOException e) {
                    logger.warn(e.getMessage());
                }
            }

            if (dataFolders.get(DataRepository.PARAM_MIX) != null) {
                try {
                    Map<String, String> mixData = TextHelper.readMix(new File(dataFolders.get(DataRepository.PARAM_MIX).toAbsolutePath().toString(),
                            baseFileName + XML_EXTENSION));
                    for (String key : mixData.keySet()) {
                        if (!(key.equals(SolrConstants.WIDTH) && doc.getField(SolrConstants.WIDTH) != null) && !(key.equals(SolrConstants.HEIGHT)
                                && doc.getField(SolrConstants.HEIGHT) != null)) {
                            doc.addField(key, mixData.get(key));
                        }
                    }
                } catch (JDOMException e) {
                    logger.error(e.getMessage(), e);
                } catch (IOException e) {
                    logger.warn(e.getMessage());
                }
            }

            //deskew alto if necessary
            deskewAlto(dataFolders, doc);

            // If the doc has FULLTEXT, indicate it so that the main doc can get a FULLTEXTAVAILABLE field later
            if (doc.getField(SolrConstants.FULLTEXT) != null) {
                doc.addField(SolrConstants.FULLTEXTAVAILABLE, true);
                hasFulltext = true;
            }

        }

        writeStrategy.addPageDoc(doc);
        return true;
    }

    /**
     * Adds the anchor for the given volume object to the re-index queue.
     * 
     * @param indexObj {@link IndexObject}
     * @throws UnsupportedEncodingException
     */
    static void copyAndReIndexAnchor(IndexObject indexObj, Hotfolder hotfolder) throws UnsupportedEncodingException {
        logger.debug("copyAndReIndexAnchor: {}", indexObj.getPi());
        if (indexObj.getParent() != null) {
            String piParent = indexObj.getParent().getPi();
            String indexedAnchorFilePath = new StringBuilder(hotfolder.getDataRepository().getDir(DataRepository.PARAM_INDEXED_METS).toAbsolutePath()
                    .toString()).append("/").append(piParent).append(AbstractIndexer.XML_EXTENSION).toString();
            Path indexedAnchor = Paths.get(indexedAnchorFilePath);
            if (Files.exists(indexedAnchor)) {
                hotfolder.getReindexQueue().add(indexedAnchor);
            }
        } else {
            logger.warn("No anchor file has been indexed for this work yet.");
        }
    }

    /**
     * Prepares the given record for an update. Creation timestamp and representative thumbnail and anchor IDDOC are preserved. A new update timestamp
     * is added, child docs are removed.
     * 
     * @param indexObj {@link IndexObject}
     * @throws IOException
     * @throws SolrServerException
     * @throws FatalIndexerException
     * @should keep creation timestamp
     * @should set update timestamp correctly
     * @should keep representation thumbnail
     * @should keep anchor IDDOC
     * @should delete anchor secondary docs
     */
    protected void prepareUpdate(IndexObject indexObj) throws IOException, SolrServerException, FatalIndexerException {
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
            // Set previous representation thumbnail, if available
            Object thumbnail = doc.getFieldValue(SolrConstants.THUMBNAILREPRESENT);
            if (thumbnail != null) {
                indexObj.setThumbnailRepresent((String) thumbnail);
            }

            // Recursively delete all children, if not an anchor
            deleteWithPI(pi, false, hotfolder.getSolrHelper());
        }
    }
}
