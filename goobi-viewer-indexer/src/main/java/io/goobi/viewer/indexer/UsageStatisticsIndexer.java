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
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.IndexerException;
import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.Hotfolder;
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

    private static final Logger logger = LoggerFactory.getLogger(UsageStatisticsIndexer.class);

    public UsageStatisticsIndexer(Hotfolder hotfolder) {
        this.hotfolder = hotfolder;
    }

    /**
     * @param sourceFile
     * @throws IOException
     * @throws FatalIndexerException
     * @throws SolrServerException 
     */
    public SolrInputDocument index(Path sourceFile) throws IOException, FatalIndexerException, SolrServerException {
        String solrDateString = getStatisticsDate(sourceFile);
        if(statisticsExists(solrDateString)) {
            logger.info("Don't index usage statistics for " + solrDateString + ": Statistics already exist for that date");
            return null;
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
            writeStrategy.writeDocs(Configuration.getInstance().isAggregateRecords());
            logger.info("Written usage statistics from " + sourceFile.toString() + " to index with IDDOC " + rootDoc.getFieldValue("IDDOC"));
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
    private boolean statisticsExists(String solrDateString) throws SolrServerException, IOException {
        String query = "+" + StatisticsLuceneFields.DATE + ":\"" + solrDateString + "\" +" + SolrConstants.DOCTYPE + ":" + StatisticsLuceneFields.USAGE_STATISTICS_DOCTYPE;
        return hotfolder.getSearchIndex().getNumHits(query) > 0;
    }

    /**
     * @param stats
     * @return
     * @throws FatalIndexerException
     */
    private IndexObject createIndexObject(DailyUsageStatistics stats) throws FatalIndexerException {
        IndexObject indexObj = new IndexObject(getNextIddoc(hotfolder.getSearchIndex()));
        indexObj.addToLucene(SolrConstants.IDDOC, Long.toString(indexObj.getIddoc()));
        indexObj.addToLucene(SolrConstants.GROUPFIELD, Long.toString(indexObj.getIddoc()));
        indexObj.addToLucene(SolrConstants.DOCTYPE, StatisticsLuceneFields.USAGE_STATISTICS_DOCTYPE);
        indexObj.addToLucene(StatisticsLuceneFields.VIEWER_NAME, stats.getViewerName());
        indexObj.addToLucene(StatisticsLuceneFields.DATE, StatisticsLuceneFields.solrDateFormatter.format(stats.getDate().atStartOfDay()));
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
     * @param searchIndex
     * @return
     * @throws FatalIndexerException 
     */
    public boolean removeFromIndex(Path sourceFile) throws FatalIndexerException {
        String solrDateString = getStatisticsDate(sourceFile);
        
        try {            
            String query = "+" + StatisticsLuceneFields.DATE + ":\"" + solrDateString + "\" +" + SolrConstants.DOCTYPE + ":" + StatisticsLuceneFields.USAGE_STATISTICS_DOCTYPE;
            logger.info("Deleting usage statistics for " + StatisticsLuceneFields.DATE + ":" + solrDateString);
            return hotfolder.getSearchIndex().deleteByQuery(query);
        } finally {
            hotfolder.getSearchIndex().commit(false);            
        }

    }

    private String getStatisticsDate(Path sourceFile) {
        String dateString = sourceFile.getFileName().toString().replaceAll("statistics-usage-([\\d-]+).\\w+", "$1");
        LocalDate date = LocalDate.parse(dateString, DailyUsageStatistics.getDateformatter());
        String solrDateString = StatisticsLuceneFields.solrDateFormatter.format(date.atStartOfDay());
        return solrDateString;
    }

}
