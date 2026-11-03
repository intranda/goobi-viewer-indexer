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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.model.PhysicalElement;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;

/**
 * Write strategy for large records whose structure documents are final once created (e.g. EAD). Instead of keeping all documents in memory
 * ({@link LazySolrWriteStrategy}) or serializing each of them to a temp file ({@link SerializingSolrWriteStrategy}), documents are sent to Solr in
 * batches while they are being added. Only the root document, the aggregated SUPER* values and the IDDOCs of the documents already sent are kept in
 * memory. Page documents (if any) are kept in memory and written at the end, like in {@link LazySolrWriteStrategy}.
 * <p>
 * Documents reach Solr before {@link #writeDocs(boolean)} is called, but are only committed afterwards. If the record fails before
 * {@link #writeDocs(boolean)} completes, {@link #cleanup()} deletes all documents that were already sent, so that a later commit cannot persist a
 * partial record.
 * <p>
 * Thread-safe: documents may be added concurrently (e.g. by parallel EAD child node indexing). Full batches are sent to Solr by background
 * sender threads (at most {@link #MAX_BATCHES_IN_FLIGHT} at a time), outside of the lock, so adding documents does not wait for Solr; only
 * when that many batches are already being sent does the adding thread wait for one of them.
 */
public class StreamingSolrWriteStrategy extends LazySolrWriteStrategy {

    private static final Logger logger = LogManager.getLogger(StreamingSolrWriteStrategy.class);

    /** Batch size used if none is configured. */
    static final int DEFAULT_BATCH_SIZE = 1000;
    /** Number of IDs per delete request when removing the documents of a failed record. */
    private static final int DELETE_BATCH_SIZE = 10000;
    /** Maximum number of batches that are being sent to Solr at the same time (also bounds the memory used by batches in transit). */
    static final int MAX_BATCHES_IN_FLIGHT = 4;

    private final int batchSize;
    private final boolean accumulateSuperFields;
    /** Guards {@link #batch}, {@link #sentIddocs}, {@link #superFields} and {@link #flushError}. */
    private final Object lock = new Object();
    private final List<SolrInputDocument> batch = new ArrayList<>();
    private final List<String> sentIddocs = new ArrayList<>();
    /** Collects the SUPER* values of all streamed documents until the root document is available. */
    private final SolrInputDocument superFields = new SolrInputDocument();
    /** Error of a failed batch write; rethrown by {@link #writeDocs(boolean)}, since {@link #addDocs(List)} cannot throw checked exceptions. */
    private FatalIndexerException flushError;
    /** True once all documents of the record have been sent; afterwards {@link #cleanup()} no longer deletes them. */
    private volatile boolean complete = false;
    /** One permit per batch that may be in transit; a sending batch holds a permit until its request has finished. */
    private final Semaphore sendPermits = new Semaphore(MAX_BATCHES_IN_FLIGHT);
    /** Background sender threads (created on first use, shut down by {@link #cleanup()}). Guarded by {@link #lock}. */
    private ExecutorService sender;
    /** Set by {@link #cleanup()}; afterwards no further batches are sent. Guarded by {@link #lock}. */
    private boolean closed = false;

    /**
     * Constructor. The batch size is taken from <code>performance.serializingWriteStrategyBatchSize</code>.
     *
     * @param searchIndex a {@link io.goobi.viewer.indexer.helper.SolrSearchIndex} object.
     */
    protected StreamingSolrWriteStrategy(SolrSearchIndex searchIndex) {
        this(searchIndex, SolrIndexerDaemon.getInstance().getConfiguration().getInt("performance.serializingWriteStrategyBatchSize", 0));
    }

    /**
     * Constructor.
     *
     * @param searchIndex a {@link io.goobi.viewer.indexer.helper.SolrSearchIndex} object.
     * @param batchSize Number of documents per Solr add request; {@link #DEFAULT_BATCH_SIZE} if not positive
     */
    StreamingSolrWriteStrategy(SolrSearchIndex searchIndex, int batchSize) {
        super(searchIndex);
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.accumulateSuperFields = SolrIndexerDaemon.getInstance().getConfiguration().isAggregateRecords();
    }

    /**
     * {@inheritDoc}
     *
     * Documents are prepared (FACET_DEFAULT, sanitizing) and buffered; a full batch is sent to Solr immediately.
     */
    @Override
    public void addDocs(List<SolrInputDocument> docs) {
        for (SolrInputDocument doc : docs) {
            // Add URN to set to check for duplicates later
            if (doc.getField(SolrConstants.URN) != null) {
                String urn = (String) doc.getFieldValue(SolrConstants.URN);
                if (StringUtils.isNotEmpty(urn)) {
                    addCollectedValue(SolrConstants.URN, urn);
                }
            }
        }

        List<List<SolrInputDocument>> fullBatches = new ArrayList<>(1);
        synchronized (lock) {
            if (flushError != null || closed) {
                // Indexing has already failed (writeDocs() will report it) or the strategy was cleaned up, so don't buffer any more documents
                return;
            }
            for (SolrInputDocument doc : docs) {
                if (accumulateSuperFields) {
                    addSuperSearchFields(doc, superFields);
                }
                addFacetDefaultField(doc);
                sanitizeDoc(doc);
                batch.add(doc);
                if (batch.size() >= batchSize) {
                    fullBatches.add(takeBatch());
                }
            }
        }
        // Send outside of the lock, so that other threads can keep adding documents meanwhile
        for (List<SolrInputDocument> fullBatch : fullBatches) {
            sendInBackground(fullBatch);
        }
    }

    /**
     * Removes the buffered documents from the batch and records their IDDOCs, so that {@link #cleanup()} also removes the documents of a batch
     * that is only partially written. Must be called while holding {@link #lock}.
     *
     * @return The documents to send; empty if there are none or indexing has already failed
     */
    private List<SolrInputDocument> takeBatch() {
        if (batch.isEmpty() || flushError != null) {
            batch.clear();
            return List.of();
        }
        List<SolrInputDocument> docs = new ArrayList<>(batch);
        batch.clear();
        for (SolrInputDocument doc : docs) {
            Object iddoc = doc.getFieldValue(SolrConstants.IDDOC);
            if (iddoc != null) {
                sentIddocs.add(String.valueOf(iddoc));
            }
        }
        return docs;
    }

    /**
     * Sends the given documents to Solr on a background sender thread. Waits if {@link #MAX_BATCHES_IN_FLIGHT} batches are already being sent.
     * A failure is recorded in {@link #flushError} (and reported by {@link #writeDocs(boolean)}) rather than thrown. Must not be called while
     * holding {@link #lock}.
     *
     * @param docs Documents to send
     */
    private void sendInBackground(List<SolrInputDocument> docs) {
        if (docs.isEmpty()) {
            return;
        }
        sendPermits.acquireUninterruptibly();
        synchronized (lock) {
            // Checked together with the hand-over, so that cleanup() (which sets closed and then waits for all permits) either waits for this
            // batch or this batch is never sent, i.e. no documents can reach Solr after cleanup() has deleted those of a failed record
            if (closed) {
                sendPermits.release();
                return;
            }
            try {
                getSender().execute(() -> {
                    try {
                        send(docs);
                    } finally {
                        sendPermits.release();
                    }
                });
            } catch (RejectedExecutionException e) {
                sendPermits.release();
                logger.error("Could not send {} documents to Solr: {}", docs.size(), e.getMessage());
                if (flushError == null) {
                    flushError = new FatalIndexerException("Could not send documents to Solr: " + e.getMessage());
                }
            }
        }
    }

    /**
     * 
     * @param docs Documents to send
     */
    private void send(List<SolrInputDocument> docs) {
        try {
            searchIndex.writeToIndex(docs);
            logger.debug("Sent {} documents to Solr.", docs.size());
        } catch (FatalIndexerException e) {
            synchronized (lock) {
                if (flushError == null) {
                    flushError = e;
                }
            }
        }
    }

    /**
     * 
     * Must be called while holding {@link #lock}.
     *
     * @return Background sender executor (created on first use)
     */
    private ExecutorService getSender() {
        // Called while holding lock
        if (sender == null) {
            sender = Executors.newFixedThreadPool(MAX_BATCHES_IN_FLIGHT, r -> {
                Thread t = new Thread(r, "solr-batch-sender");
                t.setDaemon(true);
                return t;
            });
        }
        return sender;
    }

    /**
     * Blocks until all batches handed to the background sender threads have been sent (or have failed).
     */
    private void awaitPendingSends() {
        sendPermits.acquireUninterruptibly(MAX_BATCHES_IN_FLIGHT);
        sendPermits.release(MAX_BATCHES_IN_FLIGHT);
    }

    /** {@inheritDoc} */
    @Override
    public void writeDocs(boolean aggregateRecords) throws IndexerException, FatalIndexerException {
        if (rootDoc == null) {
            throw new IndexerException("rootDoc may not be null");
        }
        String pi = (String) rootDoc.getFieldValue(SolrConstants.PI);

        // Send the remaining buffered structure docs and wait until all batches have reached Solr
        List<SolrInputDocument> remaining;
        synchronized (lock) {
            remaining = takeBatch();
        }
        sendInBackground(remaining);
        awaitPendingSends();
        synchronized (lock) {
            if (flushError != null) {
                throw flushError;
            }
        }

        // Check for duplicate URNs (the documents already sent are removed by cleanup() if this fails)
        checkForValueCollisions(SolrConstants.URN, pi);

        // Aggregate in the same order as the other strategies: structure docs, root doc, page docs
        if (aggregateRecords) {
            mergeSuperFields(rootDoc);
            addSuperSearchFields(rootDoc, rootDoc);
        }

        // Page docs
        List<SolrInputDocument> pageDocs = new ArrayList<>(pageOrderMap.size());
        for (Entry<Integer, PhysicalElement> entry : pageOrderMap.entrySet()) {
            SolrInputDocument pageDoc = entry.getValue().getDoc();
            // Do not add shape docs
            if (DocType.SHAPE.name().equals(pageDoc.getFieldValue(SolrConstants.DOCTYPE))) {
                continue;
            }
            checkAndAddAccessCondition(pageDoc);
            if (!pageDoc.containsKey(SolrConstants.PI_TOPSTRUCT) && pi != null) {
                pageDoc.addField(SolrConstants.PI_TOPSTRUCT, pi);
                logger.warn("Page document {} has no PI_TOPSTRUCT fields, adding now...", pageDoc.getFieldValue(SolrConstants.ORDER));
            }
            // Remove ALTO field (easier than removing all the logic involved in adding the ALTO field)
            if (pageDoc.containsKey(SolrConstants.ALTO)) {
                pageDoc.removeField(SolrConstants.ALTO);
            }
            if (aggregateRecords) {
                addSuperSearchFields(pageDoc, rootDoc);
            }
            addFacetDefaultField(pageDoc);
            sanitizeDoc(pageDoc);
            pageDocs.add(pageDoc);
        }
        if (!pageDocs.isEmpty()) {
            logger.info("Writing {} page documents to the index...", pageDocs.size());
            for (int i = 0; i < pageDocs.size(); i += batchSize) {
                writeAndTrack(pageDocs.subList(i, Math.min(i + batchSize, pageDocs.size())));
            }
        }

        // Root doc
        addFacetDefaultField(rootDoc);
        sanitizeDoc(rootDoc);
        logger.info("Writing root document to the index ({} structure documents were streamed before)...", sentIddocs.size());
        writeAndTrack(List.of(rootDoc));

        complete = true;
        searchIndex.commitAfterRecord();
    }

    /**
     * Sends the given documents to Solr and records their IDDOCs for {@link #cleanup()}.
     *
     * @param docs Documents to write
     * @throws FatalIndexerException
     */
    private void writeAndTrack(List<SolrInputDocument> docs) throws FatalIndexerException {
        synchronized (lock) {
            for (SolrInputDocument doc : docs) {
                Object iddoc = doc.getFieldValue(SolrConstants.IDDOC);
                if (iddoc != null) {
                    sentIddocs.add(String.valueOf(iddoc));
                }
            }
        }
        searchIndex.writeToIndex(docs);
    }

    /**
     * Copies the SUPER* values collected from the streamed documents to the given root document.
     *
     * @param target Root document
     */
    private void mergeSuperFields(SolrInputDocument target) {
        synchronized (lock) {
            for (SolrInputField field : superFields) {
                if (SolrConstants.MD_LAYOUTTAG.equals(field.getName())) {
                    // Distinct union, as for regular aggregation
                    addLayoutTagsToRootDoc(superFields, target);
                    continue;
                }
                Collection<Object> values = field.getValues();
                if (values != null) {
                    for (Object value : values) {
                        target.addField(field.getName(), value);
                    }
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * If the record did not complete, all documents already sent to Solr are deleted again.
     */
    @Override
    public void cleanup() {
        synchronized (lock) {
            closed = true;
        }
        // Batches still in transit must have reached Solr before their documents can be deleted again (and the sender threads can go)
        awaitPendingSends();
        synchronized (lock) {
            if (sender != null) {
                sender.shutdown();
                sender = null;
            }
        }
        List<String> toDelete;
        synchronized (lock) {
            toDelete = complete ? List.of() : new ArrayList<>(sentIddocs);
            batch.clear();
            sentIddocs.clear();
            superFields.clear();
        }
        if (!toDelete.isEmpty()) {
            logger.warn("Record was not written completely, removing {} documents already sent to Solr...", toDelete.size());
            for (int i = 0; i < toDelete.size(); i += DELETE_BATCH_SIZE) {
                try {
                    searchIndex.deleteDocuments(toDelete.subList(i, Math.min(i + DELETE_BATCH_SIZE, toDelete.size())));
                } catch (FatalIndexerException e) {
                    logger.error("Could not remove documents of incomplete record: {}", e.getMessage());
                    break;
                }
            }
        }
        super.cleanup();
    }

    /**
     * For unit testing purposes.
     *
     * @return Number of documents handed over for sending to Solr so far (possibly still in transit)
     */
    int getSentDocsCount() {
        synchronized (lock) {
            return sentIddocs.size();
        }
    }
}
