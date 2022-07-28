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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;

/**
 * Solr connection and query methods.
 */
public final class SolrSearchIndex {

    private static final Logger logger = LoggerFactory.getLogger(SolrSearchIndex.class);

    private static final int MAX_HITS = Integer.MAX_VALUE;
    public static final int TIMEOUT_SO = 300000;
    public static final int TIMEOUT_CONNECTION = 300000;
    private static final int RETRY_ATTEMPTS = 20;

    private static final String ERROR_SOLR_CONNECTION = "Solr connection error";
    private static final String ERROR_UPDATE_STATUS = "Update status: {}";

    /** Constant <code>optimize=false</code> */
    public static boolean optimize = false;
    private static Map<Long, Boolean> usedIddocs = new ConcurrentHashMap<>();

    private SolrClient server;

    /**
     * <p>
     * getNewHttpSolrServer.
     * </p>
     *
     * @param solrUrl URL to the Solr server
     * @param timeoutSocket
     * @param timeoutConnection
     * @param allowCompression
     * @return a {@link org.apache.solr.client.solrj.impl.HttpSolrServer} object.
     * @should return null if solrUrl is empty
     */
    public static HttpSolrClient getNewHttpSolrClient(String solrUrl, int timeoutSocket, int timeoutConnection, boolean allowCompression) {
        if (StringUtils.isEmpty(solrUrl)) {
            return null;
        }

        HttpSolrClient server = new HttpSolrClient.Builder()
                .withBaseSolrUrl(solrUrl)
                .withSocketTimeout(timeoutSocket)
                .withConnectionTimeout(timeoutConnection)
                .allowCompression(allowCompression)
                .build();
        //        server.setDefaultMaxConnectionsPerHost(100);
        //        server.setMaxTotalConnections(100);
        server.setFollowRedirects(false); // defaults to false
        //        server.setMaxRetries(1); // defaults to 0. > 1 not recommended.
        server.setRequestWriter(new BinaryRequestWriter());

        return server;
    }

    /**
     * <p>
     * Constructor for SolrSearchIndex.
     * </p>
     *
     * @param server a {@link org.apache.solr.client.solrj.SolrServer} object.
     */
    public SolrSearchIndex(SolrClient server) {
        this.server = server;
    }

    /**
     * <p>
     * checkIddocAvailability.
     * </p>
     *
     * @param iddoc a long.
     * @return a boolean.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException if any.
     */
    public synchronized boolean checkIddocAvailability(long iddoc) throws FatalIndexerException {
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
                QueryResponse resp = server.query(query);
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
     * @throws IOException
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
     * @throws IOException
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
     * @throws IOException
     */
    public SolrDocumentList search(String query, List<String> fields, int rows) throws SolrServerException, IOException {
        SolrQuery solrQuery = new SolrQuery(query);
        solrQuery.setRows(rows);
        if (fields != null) {
            for (String field : fields) {
                solrQuery.addField(field);
            }
        }

        return server.query(solrQuery).getResults();
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
            return null;
        }

        SolrInputDocument doc = new SolrInputDocument();
        for (LuceneField luceneField : luceneFields) {
            if (luceneField.isSkip() || luceneField.getValue() == null) {
                continue;
            }

            // Do not pass a boost value because starting with Solr 3.6, adding an index-time boost to primitive field types will cause the commit to fail
            doc.addField(luceneField.getField(), luceneField.getValue());
        }

        return doc;
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
     * @throws IOException
     * @should find correct data repository for record
     */
    public String findCurrentDataRepository(String pi) throws SolrServerException, IOException {
        SolrQuery solrQuery = new SolrQuery(SolrConstants.PI + ":" + pi);
        solrQuery.setRows(1);
        solrQuery.setFields(SolrConstants.DATAREPOSITORY);
        QueryResponse resp = server.query(solrQuery);

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
     * @param partialUpdates Map of update operations (usage: Map<field, Map<operation, value>>)
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
                UpdateResponse ur = server.add(doc);
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
                UpdateResponse ur = server.add(docs);
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
                UpdateResponse ur = server.deleteById(id);
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
                UpdateResponse ur = server.deleteById(ids);
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
                UpdateResponse ur = server.deleteByQuery(query);
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
                UpdateResponse ur = server.commit();
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
                server.optimize();
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
            server.rollback();
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
     * @param confFilename
     * @return a {@link org.jdom2.Document} object.
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException if any.
     */
    public static Document getSolrSchemaDocument(String solrUrl) throws FatalIndexerException {
        // Set timeout to less than the server default, otherwise it will wait 5 minutes before terminating
        String url = Configuration.getInstance().getConfiguration("solrUrl") + "/admin/file/?contentType=text/xml;charset=utf-8&file=schema.xml";
        try (HttpSolrClient solrClient = getNewHttpSolrClient(solrUrl, 30000, 30000, false)) {
            if (solrClient == null) {
                return null;
            }

            HttpClient client = solrClient.getHttpClient();
            HttpGet httpGet = new HttpGet(url);
            ResponseHandler<String> responseHandler = new BasicResponseHandler();
            String responseBody = client.execute(httpGet, responseHandler);
            try (StringReader sr = new StringReader(responseBody)) {
                return XmlTools.getSAXBuilder().build(sr);
            }
        } catch (ClientProtocolException | JDOMException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage() + "; URL: " + url, e);
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
    public SolrInputDocument checkAndCreateGroupDoc(String groupIdField, String groupId, Map<String, String> metadata, long iddoc) {
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
                doc.setField(SolrConstants.DATECREATED, oldDoc.getFieldValue(SolrConstants.DATECREATED));
            }
            doc.setField(SolrConstants.DATEUPDATED, now);
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

        return null;
    }

    /**
     *
     * @param doc
     * @param field
     * @return
     */
    public static Object getSingleFieldValue(SolrDocument doc, String field) {
        Collection<Object> valueList = doc.getFieldValues(field);
        if (valueList != null && !valueList.isEmpty()) {
            return valueList.iterator().next();
        }

        return null;
    }

    /**
     *
     * @param doc
     * @param field
     * @return
     * @should return value as string correctly
     * @should not return null as string if value is null
     */
    public static String getSingleFieldStringValue(SolrDocument doc, String field) {
        Object val = getSingleFieldValue(doc, field);
        return val != null ? String.valueOf(val) : null;
    }

    /**
     * 
     * @param doc
     * @param field
     * @return
     */
    public static Integer getSingleFieldIntegerValue(SolrDocument doc, String field) {
        Object val = getSingleFieldValue(doc, field);
        return getAsInt(val);
    }

    /**
     * 
     * @param doc
     * @param field
     * @return
     */
    public static Long getSingleFieldLongValue(SolrDocument doc, String field) {
        Object val = getSingleFieldValue(doc, field);
        return getAsLong(val);
    }

    /**
     * 
     * @param fieldValue
     * @return
     */
    static Integer getAsInt(Object fieldValue) {
        if (fieldValue == null) {
            return null;
        }
        if (fieldValue instanceof Integer) {
            return (Integer) fieldValue;
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
     * @return
     */
    static Long getAsLong(Object fieldValue) {
        if (fieldValue == null) {
            return null;
        }
        if (fieldValue instanceof Long) {
            return (Long) fieldValue;
        }
        try {
            return Long.parseLong(fieldValue.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 
     * @param field
     * @return
     * @should boolify field correctly
     */
    public static String getBooleanFieldName(String field) {
        if (field == null) {
            return null;
        }

        if (field.contains("_")) {
            field = field.substring(field.indexOf("_") + 1);
        }

        return "BOOL_" + field;
    }
}
