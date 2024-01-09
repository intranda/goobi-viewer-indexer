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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.Month;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.model.statistics.usage.StatisticsLuceneFields;

/**
 * @author florian
 *
 */
class UsageStatisticsIndexerTest extends  AbstractSolrEnabledTest {

    private Path statisticsFile1;
    private Path statisticsFile2;
    private Path deleteFile1;

    
    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());

        statisticsFile1 = Paths.get("src/test/resources/usage-statistics/statistics-usage-2022-07-04.json");
        Assertions.assertTrue(Files.isRegularFile(statisticsFile1));
        statisticsFile2 = Paths.get("src/test/resources/usage-statistics/statistics-usage-2022-07-05.json");
        Assertions.assertTrue(Files.isRegularFile(statisticsFile2));
        deleteFile1 = Paths.get("src/test/resources/usage-statistics/statistics-usage-2022-07-04.purge");
        Assertions.assertTrue(Files.isRegularFile(deleteFile1));
    }
    
    @Test
    void test_index() throws IOException, FatalIndexerException, SolrServerException {
        SolrInputDocument doc = new UsageStatisticsIndexer(hotfolder).index(statisticsFile1);
        assertNotNull(doc);
        SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search("DOCTYPE:" + StatisticsLuceneFields.USAGE_STATISTICS_DOCTYPE, null);
        assertEquals(1, docList.size());
    }
    
    @Test
    void test_delete() throws IOException, FatalIndexerException, SolrServerException {
        {
            SolrInputDocument doc = new UsageStatisticsIndexer(hotfolder).index(statisticsFile1);
            assertNotNull(doc);
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search("DOCTYPE:" + StatisticsLuceneFields.USAGE_STATISTICS_DOCTYPE, null);
            assertEquals(1, docList.size());
        }
        {
            SolrInputDocument doc = new UsageStatisticsIndexer(hotfolder).index(statisticsFile2);
            assertNotNull(doc);
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search("DOCTYPE:" + StatisticsLuceneFields.USAGE_STATISTICS_DOCTYPE, null);
            assertEquals(2, docList.size());
        }
        {            
            boolean deleted = new UsageStatisticsIndexer(hotfolder).removeFromIndex(deleteFile1);
            assertTrue(deleted);
            SolrDocumentList docList = SolrIndexerDaemon.getInstance().getSearchIndex().search("DOCTYPE:" + StatisticsLuceneFields.USAGE_STATISTICS_DOCTYPE, null);
            assertEquals(1, docList.size());
        }
    }
    
    @Test
    void test_date() {
        LocalDate date = LocalDate.of(2022, Month.JULY, 19);
        String dateString = StatisticsLuceneFields.solrDateFormatter.format(date.atStartOfDay());
        assertEquals("2022-07-19T00:00:00Z", dateString);
    }

}
