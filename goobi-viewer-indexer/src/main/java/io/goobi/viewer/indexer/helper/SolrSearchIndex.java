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
package io.goobi.viewer.indexer.helper;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.JDOMException;

import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;

/**
 * Solr connection and query methods.
 */
public final class SolrSearchIndex {

    private static final Logger logger = LogManager.getLogger(SolrSearchIndex.class);

    private static final int MAX_HITS = Integer.MAX_VALUE;
    /** Constant <code>TIMEOUT_SO=300000</code> */
    public static final int TIMEOUT_SO = 300000;
    /** Constant <code>TIMEOUT_CONNECTION=300000</code> */
    public static final int TIMEOUT_CONNECTION = 300000;
    private static final int RETRY_ATTEMPTS = 20;

    private static final String ERROR_SOLR_CONNECTION = "Solr connection error";
    private static final String ERROR_UPDATE_STATUS = "Update status: {}";

    private static Map<String, Boolean> usedIddocs = new ConcurrentHashMap<>();

    private boolean optimize = false;

    private SolrClient client;

    /**
     * <p>
     * Constructor for SolrSearchIndex.
     * </p>
     *
     * @param client a {@link org.apache.solr.client.solrj.SolrClient} object.
     * @throws org.apache.commons.configuration2.ex.ConfigurationException
     */
    public SolrSearchIndex(SolrClient client) throws ConfigurationException {
        if (client == null) {
            this.client = getNewSolrClient(SolrIndexerDaemon.getInstance().getConfiguration().getSolrUrl());
        } else {
            this.client = client;
        }
    }

    /**
     * <p>
     * getNewSolrClient.
     * </p>
     *
     * @param solrUrl a {@link java.lang.String} object
     * @return New {@link org.apache.solr.client.solrj.SolrClient}
     * @throws org.apache.commons.configuration2.ex.ConfigurationException
     */
    public static SolrClient getNewSolrClient(String solrUrl) throws ConfigurationException {
        if (StringUtils.isEmpty(solrUrl)) {
            throw new ConfigurationException("No Solr URL configured. Please check <solrUrl/>.");
        }

        return new Http2SolrClient.Builder(solrUrl)
                .withIdleTimeout(TIMEOUT_SO, TimeUnit.MILLISECONDS)
                .withConnectionTimeout(TIMEOUT_CONNECTION, TimeUnit.MILLISECONDS)
                .withFollowRedirects(false)
                .withRequestWriter(new BinaryRequestWriter())
                // .allowCompression(DataManager.getInstance().getConfiguration().isSolrCompressionEnabled())
                .build();
    }

    /**
     * <p>
     * checkIddocAvailability.
     * </p>
     *
     * @param iddoc a String
     * @return a boolean.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException if any.
     */
    public synchronized boolean checkIddocAvailability(String iddoc) throws FatalIndexerException {
        if (usedIddocs.get(iddoc) != null) {
            return false;
        }
        SolrQuery query = new SolrQuery();
        StringBuilder sbQuery = new StringBuilder();
        sbQuery.append(SolrConstants.IDDOC).append(':').append(iddoc);
        query.setQuery(sbQuery.toString());
        query.setRows(1);

        boolean success = false;
        int tries = RETRY_ATTEMPTS;
        while (!success && tries > 0) {
            tries--;
            try {
                QueryResponse resp = client.query(query);
                if (!resp.getResults().isEmpty()) {
                    usedIddocs.put(iddoc, true);
                    return false;
                }
                usedIddocs.put(iddoc, true);
                success = true;
            } catch (SolrServerException | NumberFormatException | IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (!success) {
            logger.error("Could not veryify the next available IDDOC after {} attempts. Check the Solr server connection. Exiting...",
                    RETRY_ATTEMPTS);
            throw new FatalIndexerException(ERROR_SOLR_CONNECTION);
        }

        return true;
    }

    /**
     * <p>
     * getNumHits.
     * </p>
     *
     * @param query a {@link java.lang.String} object.
     * @return a long.
     * @throws org.apache.solr.client.solrj.SolrServerException if any.
     * @throws java.io.IOException
     */
    public long getNumHits(String query) throws SolrServerException, IOException {
        return search(query, null, 0).getNumFound();
    }

    /**
     * <p>
     * search.
     * </p>
     *
     * @param query a {@link java.lang.String} object.
     * @param fields a {@link java.util.List} object.
     * @return a {@link org.apache.solr.common.SolrDocumentList} object.
     * @throws org.apache.solr.client.solrj.SolrServerException if any.
     * @throws java.io.IOException
     */
    public SolrDocumentList search(String query, List<String> fields) throws SolrServerException, IOException {
        return search(query, fields, MAX_HITS);
    }

    /**
     * <p>
     * search.
     * </p>
     *
     * @param query a {@link java.lang.String} object.
     * @param fields a {@link java.util.List} object.
     * @param rows a int.
     * @throws org.apache.solr.client.solrj.SolrServerException
     * @return a {@link org.apache.solr.common.SolrDocumentList} object.
     * @throws java.io.IOException
     */
    public SolrDocumentList search(String query, List<String> fields, int rows) throws SolrServerException, IOException {
        SolrQuery solrQuery = new SolrQuery(query);
        solrQuery.setRows(rows);
        if (fields != null) {
            for (String field : fields) {
                solrQuery.addField(field);
            }
        }

        return client.query(solrQuery).getResults();
    }

    /**
     * Creates a Solr input document from the given list of name:value pairs.
     *
     * @param luceneFields a {@link java.util.List} object.
     * @return {@link org.apache.solr.common.SolrInputDocument}
     * @should skip fields correctly
     */
    public static SolrInputDocument createDocument(List<LuceneField> luceneFields) {
        if (luceneFields == null) {
            return null; //NOSONAR Returning empty map would complicate things
        }

        SolrInputDocument doc = new SolrInputDocument();
        for (LuceneField luceneField : luceneFields) {
            if (luceneField.isSkip() || luceneField.getValue() == null) {
                continue;
            }
            addFieldToDoc(luceneField, doc);
        }

        return doc;

    }

    /**
     * 
     * @param luceneField
     * @param doc
     */
    public static void addFieldToDoc(LuceneField luceneField, SolrInputDocument doc) {
        if (luceneField == null) {
            throw new IllegalArgumentException("luceneField may not be null");
        }
        if (doc == null) {
            throw new IllegalArgumentException("doc may not be null");
        }

        // Explicitly use numeric value in doc, where applicable
        switch (luceneField.getField()) {
            case SolrConstants.CENTURY:
            case SolrConstants.CURRENTNOSORT:
            case SolrConstants.MONTHDAY:
            case SolrConstants.DATECREATED:
            case SolrConstants.DATEINDEXED:
            case SolrConstants.DATEUPDATED:
            case SolrConstants.NUMVOLUMES:
            case SolrConstants.YEAR:
            case SolrConstants.YEARMONTH:
            case SolrConstants.YEARMONTHDAY:
                doc.addField(luceneField.getField(), Long.parseLong(luceneField.getValue()));
                break;
            case SolrConstants.HEIGHT:
            case SolrConstants.NUMPAGES:
            case SolrConstants.ORDER:
            case SolrConstants.THUMBPAGENO:
            case SolrConstants.WIDTH:
                doc.addField(luceneField.getField(), Integer.parseInt(luceneField.getValue()));
                break;
            default:
                if (luceneField.getField().startsWith(SolrConstants.PREFIX_MDNUM) || luceneField.getField().startsWith("SORTNUM_")) {
                    doc.addField(luceneField.getField(), Long.parseLong(luceneField.getValue()));
                } else if (luceneField.getField().startsWith("GROUPORDER_")) {
                    doc.addField(luceneField.getField(), Integer.parseInt(luceneField.getValue()));
                } else {
                    doc.addField(luceneField.getField(), luceneField.getValue());
                }
        }
    }

    /**
     * <p>
     * findCurrentDataRepository.
     * </p>
     *
     * @param pi a {@link java.lang.String} object.
     * @return The name of the data repository currently used for the record with the given PI; "?" if the record is indexed, but not in a repository;
     *         null if the record is not in the index
     * @throws org.apache.solr.client.solrj.SolrServerException
     * @throws java.io.IOException
     * @should find correct data repository for record
     */
    public String findCurrentDataRepository(String pi) throws SolrServerException, IOException {
        SolrQuery solrQuery = new SolrQuery(SolrConstants.PI + ":" + pi);
        solrQuery.setRows(1);
        solrQuery.setFields(SolrConstants.DATAREPOSITORY);
        QueryResponse resp = client.query(solrQuery);

        if (!resp.getResults().isEmpty()) {
            if (resp.getResults().get(0).getFieldValue(SolrConstants.DATAREPOSITORY) != null) {
                return (String) resp.getResults().get(0).getFieldValue(SolrConstants.DATAREPOSITORY);
            }
            return "?";
        }

        return null;
    }

    /**
     * Performs an atomic update of the given solr document. Updates defined in partialUpdates will be applied to the existing document without making
     * any changes to other fields.
     *
     * @param doc a {@link org.apache.solr.common.SolrDocument} object.
     * @param partialUpdates Map of update operations (usage: Map&lt;field, Map&lt;operation, value&gt;&gt;)
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should update doc correctly
     * @should add GROUPFIELD if original doc doesn't have it
     */
    public void updateDoc(SolrDocument doc, Map<String, Map<String, Object>> partialUpdates) throws FatalIndexerException {
        String iddoc = (String) doc.getFieldValue(SolrConstants.IDDOC);
        SolrInputDocument newDoc = new SolrInputDocument();
        newDoc.addField(SolrConstants.IDDOC, iddoc);
        if (!doc.containsKey(SolrConstants.GROUPFIELD)) {
            logger.warn("Document to update {} doesn't contain {} adding now.", iddoc, SolrConstants.GROUPFIELD);
            Map<String, Object> update = new HashMap<>();
            update.put("set", iddoc);
            newDoc.addField(SolrConstants.GROUPFIELD, update);
        }
        for (Entry<String, Map<String, Object>> entry : partialUpdates.entrySet()) {
            newDoc.addField(entry.getKey(), entry.getValue());
        }
        writeToIndex(newDoc);
        commit(false);
    }

    /**
     * <p>
     * writeToIndex.
     * </p>
     *
     * @param doc a {@link org.apache.solr.common.SolrInputDocument} object.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should write doc correctly
     */
    public void writeToIndex(SolrInputDocument doc) throws FatalIndexerException {
        boolean success = false;
        int tries = RETRY_ATTEMPTS;

        while (!success && tries > 0) {
            tries--;
            try {
                UpdateResponse ur = client.add(doc);
                if (ur.getStatus() == 0) {
                    success = true;
                } else {
                    logger.error(ERROR_UPDATE_STATUS, ur.getStatus());
                }
            } catch (SolrServerException | IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

        if (!success) {
            logger.error("Could not write document after {} attempts. Check the Solr server connection. Exiting...", RETRY_ATTEMPTS);
            rollback();
            throw new FatalIndexerException(ERROR_SOLR_CONNECTION);
        }
    }

    /**
     * <p>
     * writeToIndex.
     * </p>
     *
     * @param docs a {@link java.util.List} object.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should write all docs correctly
     */
    public void writeToIndex(List<SolrInputDocument> docs) throws FatalIndexerException {
        boolean success = false;
        int tries = RETRY_ATTEMPTS;

        while (!success && tries > 0) {
            tries--;
            try {
                UpdateResponse ur = client.add(docs);
                if (ur.getStatus() == 0) {
                    success = true;
                } else {
                    logger.error(ERROR_UPDATE_STATUS, ur.getStatus());
                }
            } catch (IOException |  SolrServerException e) {
                logger.error(e.getMessage(), e);
            } catch (RemoteSolrException  e) {
                logger.error(e.getMessage());
            }
        }

        if (!success) {
            logger.error("Could not write {} documents after {} attempts. Check the Solr server connection. Exiting...", docs.size(), RETRY_ATTEMPTS);
            rollback();
            throw new FatalIndexerException(ERROR_SOLR_CONNECTION);
        }
    }

    /**
     * <p>
     * deleteDocument.
     * </p>
     *
     * @param id a {@link java.lang.String} object.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     */
    public void deleteDocument(String id) throws FatalIndexerException {
        boolean success = false;
        int tries = RETRY_ATTEMPTS;

        while (!success && tries > 0) {
            tries--;
            try {
                UpdateResponse ur = client.deleteById(id);
                if (ur.getStatus() == 0) {
                    success = true;
                } else {
                    logger.error(ERROR_UPDATE_STATUS, ur.getStatus());
                }
            } catch (SolrServerException e) {
                logger.error(e.getMessage());
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (!success) {
            logger.error("Could not delete '{}' after {} attempts. Check the Solr server connection. Exiting...", id, RETRY_ATTEMPTS);
            rollback();
            throw new FatalIndexerException(ERROR_SOLR_CONNECTION);
        }
    }

    /**
     * <p>
     * deleteDocuments.
     * </p>
     *
     * @param ids a {@link java.util.List} object.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should return false if id list empty
     * @should delete ids correctly
     * @return a boolean.
     */
    public boolean deleteDocuments(List<String> ids) throws FatalIndexerException {
        if (ids.isEmpty()) {
            logger.warn("Nothing to delete.");
            return false;
        }
        boolean success = false;
        int tries = RETRY_ATTEMPTS;

        while (!success && tries > 0) {
            tries--;
            try {
                UpdateResponse ur = client.deleteById(ids);
                if (ur.getStatus() == 0) {
                    success = true;
                } else {
                    logger.error(ERROR_UPDATE_STATUS, ur.getStatus());
                }
            } catch (SolrServerException e) {
                logger.error(e.getMessage());
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (!success) {
            logger.error("Could not delete {} docs after {} attempts. Check the Solr server connection. Exiting...", ids.size(), RETRY_ATTEMPTS);
            rollback();
            throw new FatalIndexerException(ERROR_SOLR_CONNECTION);
        }

        return success;
    }

    /**
     * Deletes all documents that match the given query. Handle with care!
     *
     * @return true if successful; false otherwise
     * @param query a {@link java.lang.String} object
     */
    public boolean deleteByQuery(String query) {
        if (StringUtils.isEmpty(query)) {
            return false;
        }

        boolean success = false;
        int tries = RETRY_ATTEMPTS;

        while (!success && tries > 0) {
            tries--;
            try {
                UpdateResponse ur = client.deleteByQuery(query);
                if (ur.getStatus() == 0) {
                    success = true;
                } else {
                    logger.error(ERROR_UPDATE_STATUS, ur.getStatus());
                }
            } catch (SolrServerException e) {
                logger.error(e.getMessage());
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (!success) {
            logger.error("Could not delete docs matching query '{}' after {} attempts. Check the Solr server connection. Exiting...", query,
                    RETRY_ATTEMPTS);
            rollback();
        }

        return success;
    }

    /**
     * <p>
     * commit.
     * </p>
     *
     * @param optimize a boolean.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     */
    public void commit(boolean optimize) throws FatalIndexerException {
        boolean success = false;
        int tries = RETRY_ATTEMPTS;

        while (!success && tries > 0) {
            tries--;
            try {
                UpdateResponse ur = client.commit();
                if (ur.getStatus() == 0) {
                    success = true;
                } else {
                    logger.error(ERROR_UPDATE_STATUS, ur.getStatus());
                }
            } catch (SolrServerException e) {
                logger.error(e.getMessage());
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (!success)

        {
            logger.error("Could not commit after {} attempts. Check the Solr server connection. Exiting...", RETRY_ATTEMPTS);
            rollback();
            throw new FatalIndexerException(ERROR_SOLR_CONNECTION);
        }

        if (optimize) {
            logger.debug("Optimizing index...");
            try {
                client.optimize();
                logger.debug("...done.");
            } catch (SolrServerException e) {
                // Optimize is an expensive operation and may cause a socket timeout, which shouldn't cause the entire indexing operation to fail,
                // though.
                logger.warn("Index optimization failed: {}", e.getMessage());
            } catch (IOException e) {
                logger.error("Index optimization failed.", e);
            }
        }
    }

    /**
     * <p>
     * rollback.
     * </p>
     */
    public void rollback() {
        logger.info("Rolling back...");
        try {
            client.rollback();
        } catch (SolrServerException | IOException e) {
            logger.error(e.getMessage(), e);
        } catch (RemoteSolrException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * <p>
     * getSolrSchemaDocument.
     * </p>
     *
     * @param solrUrl a {@link java.lang.String} object
     * @return a {@link org.jdom2.Document} object.
     * @throws java.io.IOException
     * @throws org.jdom2.JDOMException
     * @should return schema document correctly
     */
    public static Document getSolrSchemaDocument(String solrUrl) throws IOException, JDOMException {
        // Set timeout to less than the server default, otherwise it will wait 5 minutes before terminating        
        try {
            String responseBody = Utils.getWebContentGET(
                    solrUrl + "/admin/file/?contentType=text/xml;charset=utf-8&file=schema.xml");
            try (StringReader sr = new StringReader(responseBody)) {
                return XmlTools.getSAXBuilder().build(sr);
            }
        } catch (HTTPException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return null;
    }

    /**
     * <p>
     * removeGrievingAnchors.
     * </p>
     *
     * @return a int.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException if any.
     */
    public int removeGrievingAnchors() throws FatalIndexerException {
        String[] fields = { SolrConstants.IDDOC, SolrConstants.PI };
        try {
            List<String> toDelete = new ArrayList<>();
            SolrDocumentList anchors = search(SolrConstants.ISANCHOR + SolrConstants.SOLR_QUERY_TRUE, Arrays.asList(fields));
            for (SolrDocument anchor : anchors) {
                String iddoc = (String) anchor.getFirstValue(SolrConstants.IDDOC);
                String pi = (String) anchor.getFirstValue(SolrConstants.PI);
                if (search(SolrConstants.PI_PARENT + ":" + pi, Collections.singletonList(SolrConstants.IDDOC)).getNumFound() == 0) {
                    toDelete.add(iddoc);
                    logger.info("{} has no volumes and will be deleted.", pi);
                }
            }
            if (!toDelete.isEmpty()) {
                deleteDocuments(toDelete);
                commit(false);
                return toDelete.size();
            }
        } catch (SolrServerException | IOException e) {
            logger.error(e.getMessage(), e);
        }

        return 0;
    }

    /**
     * Creates a Solr document representing a record group (series/convolute/etc.).
     *
     * @param groupIdField Field name of the group identifier.
     * @param groupId Field value of the group identifier.
     * @param metadata Map with additional metadata fields to add to the group document.
     * @param iddoc IDDOC for the new document.
     * @return Group SolrInputDocument, if created.
     * @should create new document with all values if none exists
     * @should create updated document with all values if one already exists
     * @should add default field
     * @should add access conditions
     */
    public SolrInputDocument checkAndCreateGroupDoc(String groupIdField, String groupId, Map<String, String> metadata, String iddoc) {
        try {
            SolrDocumentList docs = search(SolrConstants.PI + ":" + groupId, null);
            SolrInputDocument doc = new SolrInputDocument();
            long now = System.currentTimeMillis();
            if (docs.isEmpty()) {
                // Document does not exist yet
                doc.setField(SolrConstants.IDDOC, String.valueOf(iddoc));
                doc.setField(SolrConstants.GROUPFIELD, String.valueOf(iddoc));
                doc.setField(SolrConstants.DOCTYPE, DocType.GROUP.name());
                doc.setField(SolrConstants.DATECREATED, now);
            } else {
                // A document already exists for this groupId
                SolrDocument oldDoc = docs.get(0);
                doc.setField(SolrConstants.IDDOC, oldDoc.getFieldValue(SolrConstants.IDDOC));
                if (doc.getField(SolrConstants.GROUPFIELD) == null) {
                    doc.setField(SolrConstants.GROUPFIELD, oldDoc.getFieldValue(SolrConstants.IDDOC));
                }
                doc.setField(SolrConstants.DOCTYPE, DocType.GROUP.name());
                doc.setField(SolrConstants.DATECREATED, Long.parseLong(String.valueOf(oldDoc.getFieldValue(SolrConstants.DATECREATED))));
            }
            doc.setField(SolrConstants.DATEUPDATED, Long.valueOf(now));
            doc.setField(SolrConstants.PI, groupId);
            doc.setField(SolrConstants.PI_TOPSTRUCT, groupId);
            doc.setField(SolrConstants.GROUPTYPE, groupIdField);
            doc.setField(SolrConstants.ACCESSCONDITION, SolrConstants.OPEN_ACCESS_VALUE);
            StringBuilder sbDefault = new StringBuilder();
            sbDefault.append(groupId).append(' ');
            if (metadata != null) {
                for (Entry<String, String> entry : metadata.entrySet()) {
                    doc.setField(entry.getKey(), entry.getValue());
                    sbDefault.append(entry.getValue()).append(' ');
                }
            }
            doc.setField(SolrConstants.DEFAULT, sbDefault.toString().trim());
            return doc;
        } catch (SolrServerException | IOException e) {
            logger.error(e.getMessage(), e);
        }

        return null; //NOSONAR Returning empty map would complicate things
    }

    /**
     * Returns a list with all (string) values for the given field name in the given SolrInputDocument.
     *
     * @param doc a {@link org.apache.solr.common.SolrInputDocument} object.
     * @param fieldName a {@link java.lang.String} object.
     * @return a {@link java.util.List} object.
     * @should return all values for the given field
     */
    public static List<String> getMetadataValues(SolrInputDocument doc, String fieldName) {
        if (doc == null) {
            return Collections.emptyList();
        }

        Collection<Object> values = doc.getFieldValues(fieldName);
        if (values == null) {
            return Collections.emptyList();
        }

        List<String> ret = new ArrayList<>(values.size());
        for (Object value : values) {
            if (value instanceof String s) {
                ret.add(s);
            } else {
                ret.add(String.valueOf(value));
            }
        }

        return ret;
    }

    /**
     * <p>
     * getSingleFieldValue.
     * </p>
     *
     * @param doc {@link org.apache.solr.common.SolrDocument}
     * @param field a {@link java.lang.String} object
     * @return First value found; null otherwise
     */
    public static Object getSingleFieldValue(SolrDocument doc, String field) {
        Collection<Object> valueList = doc.getFieldValues(field);
        if (valueList != null && !valueList.isEmpty()) {
            return valueList.iterator().next();
        }

        return null;
    }

    /**
     * <p>
     * getSingleFieldStringValue.
     * </p>
     *
     * @param doc {@link org.apache.solr.common.SolrDocument}
     * @param field Solr field name
     * @return First value found; null otherwise
     * @should return value as string correctly
     * @should not return null as string if value is null
     */
    public static String getSingleFieldStringValue(SolrDocument doc, String field) {
        Object val = getSingleFieldValue(doc, field);
        return val != null ? String.valueOf(val) : null;
    }

    /**
     * <p>
     * getSingleFieldStringValue.
     * </p>
     *
     * @param doc {@link org.apache.solr.common.SolrInputDocument}
     * @param field Solr field name
     * @return First value found; null otherwise
     */
    public static String getSingleFieldStringValue(SolrInputDocument doc, String field) {
        Object ret = null;

        Collection<Object> valueList = doc.getFieldValues(field);
        if (valueList != null && !valueList.isEmpty()) {
            ret = valueList.iterator().next();
        }
        return ret != null ? String.valueOf(ret) : null;
    }

    /**
     * <p>
     * getSingleFieldIntegerValue.
     * </p>
     *
     * @param doc {@link org.apache.solr.common.SolrDocument}
     * @param field Solr field name
     * @return First value found; null otherwise
     */
    public static Integer getSingleFieldIntegerValue(SolrDocument doc, String field) {
        Object val = getSingleFieldValue(doc, field);
        return getAsInt(val);
    }

    /**
     * <p>
     * getSingleFieldLongValue.
     * </p>
     *
     * @param doc {@link org.apache.solr.common.SolrDocument}
     * @param field Solr field name
     * @return First value found; null otherwise
     */
    public static Long getSingleFieldLongValue(SolrDocument doc, String field) {
        Object val = getSingleFieldValue(doc, field);
        return getAsLong(val);
    }

    /**
     * <p>
     * getAsInt.
     * </p>
     *
     * @param fieldValue a {@link java.lang.Object} object
     * @return {@link java.lang.Integer}
     * @should return null if fieldValue null
     */
    public static Integer getAsInt(Object fieldValue) {
        if (fieldValue == null) {
            return null;
        }
        if (fieldValue instanceof Integer integer) {
            return integer;
        }
        try {
            return Integer.parseInt(fieldValue.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 
     * @param fieldValue
     * @return {@link Long}
     */
    static Long getAsLong(Object fieldValue) {
        if (fieldValue == null) {
            return null;
        }
        if (fieldValue instanceof Long lng) {
            return lng;
        }
        try {
            return Long.parseLong(fieldValue.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * <p>
     * getBooleanFieldName.
     * </p>
     *
     * @param field a {@link java.lang.String} object
     * @return a {@link java.lang.String} object
     * @should boolify field correctly
     */
    public static String getBooleanFieldName(final String field) {
        if (field == null) {
            return null;
        }

        String ret = field;
        if (ret.contains("_")) {
            ret = ret.substring(ret.indexOf("_") + 1);
        }

        return "BOOL_" + ret;
    }

    /**
     * <p>
     * checkDuplicateFieldValues.
     * </p>
     *
     * @param fields Field names to check
     * @param values Values to check
     * @param skipPi Record identifier to skip (typically the currently indexed record)
     * @return Set of PI_TOPSTRUCT values that already possess given field values
     * @throws org.apache.solr.client.solrj.SolrServerException
     * @throws java.io.IOException
     * @should return correct identifiers
     * @should ignore records that match skipPi
     */
    public Set<String> checkDuplicateFieldValues(List<String> fields, List<String> values, String skipPi) throws SolrServerException, IOException {
        if (fields == null || fields.isEmpty() || values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        StringBuilder sbQuery = new StringBuilder("+(");
        for (String field : fields) {
            sbQuery.append(field).append(":(");
            for (String value : values) {
                sbQuery.append('"').append(value).append("\" ");
            }
            sbQuery.append(") ");
        }

        sbQuery.append(')');

        if (StringUtils.isNotEmpty(skipPi)) {
            sbQuery.append(" -").append(SolrConstants.PI_TOPSTRUCT).append(":\"").append(skipPi).append('"');
        }

        SolrDocumentList found = search(sbQuery.toString(), Arrays.asList(SolrConstants.PI, SolrConstants.PI_TOPSTRUCT));
        if (found.isEmpty()) {
            return Collections.emptySet();
        }

        Set<String> ret = HashSet.newHashSet(found.size());
        for (SolrDocument doc : found) {
            if (doc.containsKey(SolrConstants.PI_TOPSTRUCT)) {
                ret.add((String) doc.getFieldValue(SolrConstants.PI_TOPSTRUCT));
            } else if (doc.containsKey(SolrConstants.PI)) {
                ret.add((String) doc.getFieldValue(SolrConstants.PI));
            } else {
                logger.error("Solr document {} contains a duplicate value but no {} field.", doc.getFieldValue(SolrConstants.IDDOC),
                        SolrConstants.PI_TOPSTRUCT);
                logger.error("Query used: {}", sbQuery);
                ret.add("PI NOT FOUND");
            }
        }

        return ret;
    }

    /**
     * <p>
     * isOptimize.
     * </p>
     *
     * @return the optimize
     */
    public boolean isOptimize() {
        return optimize;
    }

    /**
     * <p>
     * Setter for the field <code>optimize</code>.
     * </p>
     *
     * @param optimize the optimize to set
     */
    public void setOptimize(boolean optimize) {
        this.optimize = optimize;
    }
}
