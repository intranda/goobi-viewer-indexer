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
package io.goobi.viewer.indexer.model.writestrategy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;

/**
 * <p>
 * SerializingSolrWriteStrategy class.
 * </p>
 *
 */
public class SerializingSolrWriteStrategy extends AbstractWriteStrategy {

    private static final Logger logger = LoggerFactory.getLogger(SerializingSolrWriteStrategy.class);

    private static final String ENCODING_UTF8 = "UTF8";

    private SolrSearchIndex searchIndex;
    private Path tempFolder;
    private String rootDocIddoc;
    private List<String> docIddocs = new CopyOnWriteArrayList<>();
    private Map<Integer, String> pageDocOrderIddocMap = new ConcurrentHashMap<>();
    private Map<String, String> pageDocFileNameIddocMap = new ConcurrentHashMap<>();
    private Map<String, String> pageDocPhysIdIddocMap = new ConcurrentHashMap<>();
    private AtomicInteger docsCounter = new AtomicInteger();
    private AtomicInteger pageDocsCounter = new AtomicInteger();
    private AtomicInteger tempFileCounter = new AtomicInteger();

    /**
     * <p>
     * Constructor for SerializingSolrWriteStrategy.
     * </p>
     *
     * @param searchIndex a {@link io.goobi.viewer.indexer.helper.SolrSearchIndex} object.
     * @param tempFolder a {@link java.nio.file.Path} object.
     */
    protected SerializingSolrWriteStrategy(SolrSearchIndex searchIndex, Path tempFolder) {
        this.searchIndex = searchIndex;
        this.tempFolder = tempFolder;
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy#setRootDoc(org.apache.solr.common.SolrInputDocument)
     */
    /** {@inheritDoc} */
    @Override
    public void setRootDoc(SolrInputDocument doc) {
        rootDocIddoc = String.valueOf(doc.getFieldValue(SolrConstants.IDDOC));
        if (save(doc, rootDocIddoc)) {
            docIddocs.add(rootDocIddoc);
            docsCounter.incrementAndGet();
        }
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.ISolrWriteStrategy#addDoc(org.apache.solr.common.SolrInputDocument)
     */
    /** {@inheritDoc} */
    @Override
    public void addDoc(SolrInputDocument doc) {
        addDocs(Collections.singletonList(doc));

    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.ISolrWriteStrategy#addDocs(java.util.List)
     */
    /** {@inheritDoc} */
    @Override
    public void addDocs(List<SolrInputDocument> docs) {
        for (SolrInputDocument doc : docs) {
            String iddoc = String.valueOf(doc.getFieldValue(SolrConstants.IDDOC));
            if (save(doc, iddoc)) {
                docIddocs.add(iddoc);
                docsCounter.incrementAndGet();
            }
        }
        logger.debug("Docs added: {}", docsCounter);
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.ISolrWriteStrategy#addPageDoc(java.util.List)
     */
    /** {@inheritDoc} */
    @Override
    public void addPageDoc(SolrInputDocument doc) {
        String iddoc = String.valueOf(doc.getFieldValue(SolrConstants.IDDOC));
        if (doc.getField(SolrConstants.FULLTEXT) != null) {
            String text = (String) doc.getFieldValue(SolrConstants.FULLTEXT);
            try {
                FileUtils.writeStringToFile(new File(tempFolder.toFile(), iddoc + "_" + SolrConstants.FULLTEXT), text, ENCODING_UTF8);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
            doc.removeField(SolrConstants.FULLTEXT);
        }
        if (save(doc, iddoc)) {
            int order = (int) doc.getFieldValue(SolrConstants.ORDER);
            if (pageDocOrderIddocMap.get(order) != null) {
                logger.error("Collision for page order {}", order);
            }
            pageDocOrderIddocMap.put(order, iddoc);
            if (pageDocFileNameIddocMap.get(doc.getFieldValue(SolrConstants.FILENAME)) != null) {
                logger.warn("A doc already exists for file: {}", doc.getFieldValue(SolrConstants.FILENAME));
            }
            pageDocFileNameIddocMap.put((String) doc.getFieldValue(SolrConstants.FILENAME), iddoc);
            pageDocPhysIdIddocMap.put((String) doc.getFieldValue(SolrConstants.PHYSID), iddoc);
            pageDocsCounter.incrementAndGet();
            logger.debug("Page docs added: {}", pageDocsCounter);
        }
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy#updateDoc(org.apache.solr.common.SolrInputDocument)
     */
    /** {@inheritDoc} */
    @Override
    public void updateDoc(SolrInputDocument doc) {
        String iddoc = String.valueOf(doc.getFieldValue(SolrConstants.IDDOC));
        if (save(doc, iddoc)) {
            logger.debug("Page docs updated: {}", pageDocsCounter);
        }
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.ISolrWriteStrategy#getPageDocsSize()
     */
    /** {@inheritDoc} */
    @Override
    public int getPageDocsSize() {
        return pageDocsCounter.get();
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy#getPageOrderNumbers()
     */
    @Override
    public List<Integer> getPageOrderNumbers() {
        List<Integer> ret = new ArrayList<>(pageDocOrderIddocMap.keySet());
        Collections.sort(ret);
        return ret;
    }

    /**
     * {@inheritDoc}
     *
     * Returns the SolrInputDocument for the given order. Fields that are serialized separately (FULLTEXT, ALTO) are not returned!
     * 
     * @see io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy#getPageDocForOrder(int)
     */
    @Override
    public SolrInputDocument getPageDocForOrder(int order) throws FatalIndexerException {
        if (order > 0) {
            String iddoc = pageDocOrderIddocMap.get(order);
            if (iddoc != null) {
                SolrInputDocument doc = load(iddoc);
                return doc;
            }
        }

        return null;
    }

    /**
     * {@inheritDoc}
     *
     * Returns all SolrInputDocuments mapped to the given list of PHYSIDs. Fields that are serialized separately (FULLTEXT, ALTO) are not returned!
     * 
     * @see io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy#getPageDocsForPhysIdList(java.util.List)
     * @should return all docs for the given physIdList
     */
    @Override
    public List<SolrInputDocument> getPageDocsForPhysIdList(List<String> physIdList) throws FatalIndexerException {
        List<SolrInputDocument> ret = new ArrayList<>();

        for (String physId : physIdList) {
            if (pageDocPhysIdIddocMap.get(physId) != null) {
                SolrInputDocument doc = load(pageDocPhysIdIddocMap.get(physId));
                if (doc != null) {
                    ret.add(doc);
                }
            }
        }

        return ret;
    }

    /** {@inheritDoc} */
    @Override
    public void writeDocs(final boolean aggregateRecords) throws IndexerException, FatalIndexerException {
        final SolrInputDocument rootDoc = load(rootDocIddoc);
        if (rootDoc == null) {
            throw new IndexerException("rootDoc may not be null");
        }

        sanitizeDoc(rootDoc);
        logger.info("Writing {} structure/content documents to the index...", docIddocs.size());
        for (String iddoc : docIddocs) {
            SolrInputDocument doc = load(iddoc);
            if (doc != null) {
                // Add the child doc's DEFAULT values to the SUPERDEFAULT value of the root doc
                if (aggregateRecords) {
                    // Add SUPER* fields to root doc
                    if (doc.containsKey(SolrConstants.DEFAULT)) {
                        rootDoc.addField(SolrConstants.SUPERDEFAULT, (doc.getFieldValue(SolrConstants.DEFAULT)));
                    }
                    if (doc.containsKey(SolrConstants.FULLTEXT)) {
                        rootDoc.addField(SolrConstants.SUPERFULLTEXT, (doc.getFieldValue(SolrConstants.FULLTEXT)));
                    }
                    if (doc.containsKey(SolrConstants.UGCTERMS)) {
                        rootDoc.addField(SolrConstants.SUPERUGCTERMS, doc.getFieldValue(SolrConstants.UGCTERMS));
                    }
                }
                sanitizeDoc(doc);
                try {
                    searchIndex.writeToIndex(doc);
                } catch (RemoteSolrException e) {
                    copyFailedFile(Paths.get(tempFolder.toAbsolutePath().toString(), iddoc));
                    logger.error(e.getMessage(), e);
                    throw new IndexerException(e.getMessage());
                }
            } else {
                logger.error("Could not find serialized document for IDDOC: {}", iddoc);
            }
        }

        logger.info("Writing {} page documents to the index...", pageDocOrderIddocMap.size());
        List<Integer> orderList = new ArrayList<>(pageDocOrderIddocMap.keySet());
        Collections.sort(orderList);

        if (Configuration.getInstance().getThreads() > 1) {
            ExecutorService executor = Executors.newFixedThreadPool(Configuration.getInstance().getThreads());
            for (final int order : orderList) {

                // Generate write page document in its own thread
                Runnable r = new Runnable() {

                    @Override
                    public void run() {
                        try {
                            writePageDoc(order, rootDoc, aggregateRecords);
                        } catch (FatalIndexerException e) {
                            logger.error(e.getMessage());
                        } finally {
                        }
                    }
                };
                executor.execute(r);
            }
            executor.shutdown();
            while (!executor.isTerminated()) {
            }
        } else {
            int newOrder = 1;
            for (final int order : orderList) {
                writePageDoc(order, rootDoc, aggregateRecords);
            }
        }

        // Write the root doc
        logger.info("Writing root document to the index...");
        try {
            searchIndex.writeToIndex(rootDoc);
        } catch (RemoteSolrException e) {
            copyFailedFile(Paths.get(tempFolder.toAbsolutePath().toString(), rootDocIddoc));
            logger.error(e.getMessage(), e);
            throw new IndexerException(e.getMessage());
        }

        searchIndex.commit(SolrSearchIndex.optimize);
    }

    /**
     * 
     * @param order
     * @param rootDoc
     * @param aggregateRecords
     * @throws FatalIndexerException
     */
    private void writePageDoc(int order, SolrInputDocument rootDoc, boolean aggregateRecords) throws FatalIndexerException {
        String iddoc = pageDocOrderIddocMap.get(order);
        SolrInputDocument doc = load(iddoc);
        if (doc == null) {
            logger.error("Could not find serialized document for IDDOC: {}", iddoc);
            return;
        }
        // Do not add shape docs
        if (DocType.SHAPE.name().equals(doc.getFieldValue(SolrConstants.DOCTYPE))) {
            return;
        }

        Path xmlFile = Paths.get(tempFolder.toAbsolutePath().toString(),
                new StringBuilder().append(iddoc).append("_").append(SolrConstants.FULLTEXT).toString());
        if (Files.isRegularFile(xmlFile)) {
            try {
                String xml = FileUtils.readFileToString(xmlFile.toFile(), "UTF8");
                doc.addField(SolrConstants.FULLTEXT, xml);

                // Add the child doc's FULLTEXT values to the SUPERFULLTEXT value of the root doc
                if (aggregateRecords) {
                    rootDoc.addField(SolrConstants.SUPERFULLTEXT, (doc.getFieldValue(SolrConstants.FULLTEXT)));
                }
                logger.debug("Found FULLTEXT for: {}", iddoc);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

        checkAndAddAccessCondition(doc);
        if (!searchIndex.writeToIndex(doc)) {
            logger.error(doc.toString());
        }
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.indexer.model.ISolrWriteStrategy#cleanup()
     */
    /** {@inheritDoc} */
    @Override
    public void cleanup() {
        List<String> allIddocs = new ArrayList<>(docIddocs.size() + pageDocPhysIdIddocMap.size());
        allIddocs.addAll(docIddocs);
        for (int order : pageDocOrderIddocMap.keySet()) {
            allIddocs.add(pageDocOrderIddocMap.get(order));
        }

        logger.info("Removing temp files...");
        //        allIddocs.parallelStream().forEach(iddoc -> removeTempFilesForIddoc(iddoc));
        for (String iddoc : allIddocs) {
            removeTempFilesForIddoc(iddoc);
        }
        logger.info("{} temp files removed.", tempFileCounter.intValue());

        rootDocIddoc = null;
        docIddocs.clear();
        pageDocOrderIddocMap.clear();
        docsCounter.set(0);
        pageDocsCounter.set(0);
    }

    /**
     * Removes all temp files for the given IDDOC. Can be executed in parallel.
     * 
     * @param iddoc
     */
    private void removeTempFilesForIddoc(String iddoc) {
        Path tempFile = Paths.get(tempFolder.toAbsolutePath().toString(), iddoc);
        if (Files.isRegularFile(tempFile)) {
            deleteTempFile(tempFile);
            {
                Path tempXmlFile = Paths.get(tempFolder.toAbsolutePath().toString(),
                        new StringBuilder().append(iddoc).append("_").append(SolrConstants.ALTO).toString());
                if (Files.isRegularFile(tempXmlFile)) {
                    deleteTempFile(tempXmlFile);
                }
            }
            {
                Path tempXmlFile = Paths.get(tempFolder.toAbsolutePath().toString(),
                        new StringBuilder().append(iddoc).append("_").append(SolrConstants.FULLTEXT).toString());
                if (Files.isRegularFile(tempXmlFile)) {
                    deleteTempFile(tempXmlFile);
                }
            }
            //            {
            //                Path tempXmlFile = Paths.get(tempFolder.toAbsolutePath().toString(), new StringBuilder().append(iddoc).append("_").append(
            //                        SolrConstants.PAGEURNS).toString());
            //                if (Files.isRegularFile(tempXmlFile)) {
            //                    deleteTempFile(tempXmlFile);
            //                }
            //            }
        }
    }

    /**
     * Deletes the file represented by the given Path and increments <code>tempFileCounter</code>.
     * 
     * @param file
     */
    private void deleteTempFile(Path file) {
        try {
            Files.delete(file);
            tempFileCounter.incrementAndGet();
        } catch (IOException e) {
            logger.warn("Could not remove temp file: {}", file.toAbsolutePath());
        }
    }

    private SolrInputDocument load(String fileName) throws FatalIndexerException {
        logger.debug("Loading '{}'...", fileName);
        Path file = Paths.get(tempFolder.toAbsolutePath().toString(), fileName);
        try (FileInputStream fis = new FileInputStream(file.toFile()); ObjectInputStream ois = new ObjectInputStream(fis)) {
            SolrInputDocument doc = (SolrInputDocument) ois.readObject();
            return doc;
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage(), e);
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            copyFailedFile(file);
        }

        return null;
    }

    private boolean save(SolrInputDocument doc, String fileName) {
        logger.debug("Writing '{}'...", fileName);
        File file = new File(tempFolder.toFile(), fileName);
        try (FileOutputStream fos = new FileOutputStream(file); ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(doc);
            return true;
        } catch (IOException e) {
            logger.error("Could not save file: {}", file.getAbsolutePath());
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    private static void copyFailedFile(Path file) throws FatalIndexerException {
        try {
            Files.copy(file, Paths.get(Configuration.getInstance().getViewerHome(), file.getFileName().toString()),
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
