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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONException;
import org.json.JSONObject;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.statistics.usage.DailyRequestCounts;
import io.goobi.viewer.indexer.model.statistics.usage.DailyUsageStatistics;
import io.goobi.viewer.indexer.model.statistics.usage.RequestType;
import io.goobi.viewer.indexer.model.statistics.usage.StatisticsLuceneFields;
import io.goobi.viewer.indexer.model.writestrategy.AbstractWriteStrategy;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

/**
 * @author florian
 *
 */
public class UsageStatisticsIndexer extends Indexer {

    private static final Logger logger = LogManager.getLogger(UsageStatisticsIndexer.class);

    /**
     * 
     * @param hotfolder
     */
    public UsageStatisticsIndexer(Hotfolder hotfolder) {
        this.hotfolder = hotfolder;
    }

    /**
     * @see io.goobi.viewer.indexer.Indexer#addToIndex(java.nio.file.Path, java.util.Map)
     */
    @Override
    public void addToIndex(Path sourceFile, Map<String, Boolean> reindexSettings) throws IOException, FatalIndexerException {
        if (sourceFile == null) {
            throw new IllegalArgumentException("usage statistics file may not be null");
        } else if (!Files.isRegularFile(sourceFile)) {
            throw new IllegalArgumentException("usage statistics file {} does not exist".replace("{}", sourceFile.toString()));
        }
        try {
            index(sourceFile);
        } catch (SolrServerException e) {
            logger.error("Error indexing file {}. Reason: {}", sourceFile, e.getMessage());
            throw new IOException(e);
        }

    }

    /**
     * @param sourceFile
     * @throws IOException
     * @throws FatalIndexerException
     * @throws SolrServerException
     */
    SolrInputDocument index(Path sourceFile) throws IOException, FatalIndexerException, SolrServerException {
        String solrDateString = getStatisticsDate(sourceFile);
        if (statisticsExists(solrDateString)) {
            logger.info("Don't index usage statistics for {}: Statistics already exist for that date", solrDateString);
            return null; //NOSONAR Returning empty map would complicate things
        }

        String jsonString = Files.readString(sourceFile);
        if (StringUtils.isBlank(jsonString)) {
            throw new IllegalArgumentException("Usage statistics file {} is empty".replace("{}", sourceFile.toString()));
        }
        try {
            JSONObject json = new JSONObject(jsonString);
            DailyUsageStatistics stats = new DailyUsageStatistics(json);
            IndexObject indexObject = createIndexObject(stats);

            logger.debug("Writing document to index...");
            SolrInputDocument rootDoc = SolrSearchIndex.createDocument(indexObject.getLuceneFields());
            ISolrWriteStrategy writeStrategy = AbstractWriteStrategy.create(sourceFile, Collections.emptyMap(), this.hotfolder);
            writeStrategy.setRootDoc(rootDoc);
            writeStrategy.writeDocs(SolrIndexerDaemon.getInstance().getConfiguration().isAggregateRecords());
            logger.info("Written usage statistics from {} to index with IDDOC {}", sourceFile, rootDoc.getFieldValue("IDDOC"));
            return rootDoc;
        } catch (JSONException | IndexerException e) {
            throw new IllegalArgumentException("Usage statistics file {} contains invalid json".replace("{}", sourceFile.toString()));
        }

    }

    /**
     * @param solrDateString
     * @return
     * @throws IOException
     * @throws SolrServerException
     */
    private static boolean statisticsExists(String solrDateString) throws SolrServerException, IOException {
        String query = "+" + StatisticsLuceneFields.DATE + ":\"" + solrDateString + "\" +" + SolrConstants.DOCTYPE + ":"
                + StatisticsLuceneFields.USAGE_STATISTICS_DOCTYPE;
        return SolrIndexerDaemon.getInstance().getSearchIndex().getNumHits(query) > 0;
    }

    /**
     * @param stats
     * @return
     * @throws FatalIndexerException
     */
    private static IndexObject createIndexObject(DailyUsageStatistics stats) throws FatalIndexerException {
        IndexObject indexObj = new IndexObject(getNextIddoc(SolrIndexerDaemon.getInstance().getSearchIndex()));
        indexObj.addToLucene(SolrConstants.IDDOC, indexObj.getIddoc());
        indexObj.addToLucene(SolrConstants.GROUPFIELD, indexObj.getIddoc());
        indexObj.addToLucene(SolrConstants.DOCTYPE, StatisticsLuceneFields.USAGE_STATISTICS_DOCTYPE);
        indexObj.addToLucene(StatisticsLuceneFields.VIEWER_NAME, stats.getViewerName());
        indexObj.addToLucene(StatisticsLuceneFields.DATE, StatisticsLuceneFields.FORMATTER_SOLR_DATE.format(stats.getDate().atStartOfDay()));
        for (Entry<String, DailyRequestCounts> entry : stats.getRequestCounts().entrySet()) {
            String pi = entry.getKey();
            DailyRequestCounts counts = entry.getValue();
            String fieldName = StatisticsLuceneFields.getFieldName(pi);
            for (int i = 0; i < 6; i++) {
                if (i % 2 == 0) {
                    RequestType type = RequestType.getTypeForTotalCountIndex(i);
                    long count = counts.getTotalCount(type);
                    indexObj.addToLucene(fieldName, Long.toString(count));
                } else {
                    RequestType type = RequestType.getTypeForUniqueCountIndex(i);
                    long count = counts.getUniqueCount(type);
                    indexObj.addToLucene(fieldName, Long.toString(count));
                }
            }
        }
        return indexObj;
    }

    /**
     * @param sourceFile
     * @return
     * @throws FatalIndexerException
     */
    public boolean removeFromIndex(Path sourceFile) throws FatalIndexerException {
        String solrDateString = getStatisticsDate(sourceFile);

        try {
            String query = "+" + StatisticsLuceneFields.DATE + ":\"" + solrDateString + "\" +" + SolrConstants.DOCTYPE + ":"
                    + StatisticsLuceneFields.USAGE_STATISTICS_DOCTYPE;
            logger.info("Deleting usage statistics for {}:{}", StatisticsLuceneFields.DATE, solrDateString);
            return SolrIndexerDaemon.getInstance().getSearchIndex().deleteByQuery(query);
        } finally {
            SolrIndexerDaemon.getInstance().getSearchIndex().commit(false);
        }
    }

    /**
     * 
     * @param sourceFile
     * @return
     */
    private static String getStatisticsDate(Path sourceFile) {
        String dateString = sourceFile.getFileName().toString().replaceAll("statistics-usage-([\\d-]+).\\w+", "$1");
        LocalDate date = LocalDate.parse(dateString, DailyUsageStatistics.getDateformatter());
        return StatisticsLuceneFields.FORMATTER_SOLR_DATE.format(date.atStartOfDay());
    }

    @Override
    protected FileFormat getSourceDocFormat() {
        return FileFormat.UNKNOWN;
    }
}
