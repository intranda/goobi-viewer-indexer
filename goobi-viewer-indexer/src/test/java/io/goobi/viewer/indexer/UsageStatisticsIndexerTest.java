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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.Month;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.model.statistics.usage.StatisticsUsageLuceneFields;

/**
 * @author florian
 *
 */
public class UsageStatisticsIndexerTest extends  AbstractSolrEnabledTest {

    private Path statisticsFile;
    
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(TEST_CONFIG_PATH, client);

        statisticsFile = Paths.get("src/test/resources/usage-statistics/statistics-usage-2022-07-04.json");
        Assert.assertTrue(Files.isRegularFile(statisticsFile));
    }
    
    @Test
    public void test_index() throws IOException, FatalIndexerException, SolrServerException {
        SolrInputDocument doc = new UsageStatisticsIndexer(hotfolder).index(statisticsFile);
        assertNotNull(doc);
        SolrDocumentList docList = hotfolder.getSearchIndex().search("DOCTYPE:" + StatisticsUsageLuceneFields.USAGE_STATISTICS_DOCTYPE, null);
        assertEquals(1, docList.size());
    }
    
    @Test
    public void test_date() {
        LocalDate date = LocalDate.of(2022, Month.JULY, 19);
        String dateString = UsageStatisticsIndexer.solrDateFormatter.format(date.atStartOfDay());
        assertEquals("2022-07-19T00:00:00Z", dateString);
    }

}
