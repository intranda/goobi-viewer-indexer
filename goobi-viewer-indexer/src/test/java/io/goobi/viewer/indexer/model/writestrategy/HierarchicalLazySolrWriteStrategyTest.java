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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.goobi.viewer.indexer.AbstractSolrEnabledTest;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;

public class HierarchicalLazySolrWriteStrategyTest extends AbstractSolrEnabledTest {

    //    private Path metsFile = Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
    }

    /**
     * @see HierarchicalLazySolrWriteStrategy#HierarchicalLazySolrWriteStrategy(SolrSearchIndex)
     * @verifies set attributes correctly
     */
    @Test
    public void HierarchicalLazySolrWriteStrategy_shouldSetAttributesCorrectly() throws Exception {
        SolrSearchIndex sh = new SolrSearchIndex(client);
        HierarchicalLazySolrWriteStrategy strat = new HierarchicalLazySolrWriteStrategy(sh);
        Assert.assertEquals(sh, strat.searchIndex);
    }

    //    /**
    //     * @see HierarchicalLazySolrWriteStrategy#writeDocs()
    //     * @verifies write all structure docs correctly
    //     */
    //    @Test
    //    public void writeDocs_shouldWriteAllStructureDocsCorrectly() throws Exception {
    //        Map<String, Path> dataFolders = new HashMap<>();
    //        SolrSearchIndex sh = new SolrSearchIndex(server);
    //        HierarchicalLazySolrWriteStrategy strat = new HierarchicalLazySolrWriteStrategy(sh);
    //        MetsIndexer indexer = new MetsIndexer(hotfolder);
    //
    //        indexer.index(metsFile, false, dataFolders, strat, 1);
    //        SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":PPN517154005 AND " + SolrConstants.DOCTYPE + ":"
    //                + DocType.DOCSTRCT.name(), null);
    //        Assert.assertEquals(4, docList.size());
    //    }

    //    /**
    //     * @see HierarchicalLazySolrWriteStrategy#writeDocs(boolean)
    //     * @verifies write all page docs correctly
    //     */
    //    @Test
    //    public void writeDocs_shouldWriteAllPageDocsCorrectly() throws Exception {
    //        Map<String, Path> dataFolders = new HashMap<>();
    //        SolrSearchIndex sh = new SolrSearchIndex(server);
    //        LazySolrWriteStrategy strat = new HierarchicalLazySolrWriteStrategy(sh);
    //        MetsIndexer indexer = new MetsIndexer(hotfolder);
    //
    //        indexer.index(metsFile, false, dataFolders, strat, 1);
    //        SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":PPN517154005 AND " + SolrConstants.DOCTYPE + ":"
    //                + DocType.PAGE.name(), null);
    //        Assert.assertEquals(16, docList.size());
    //    }
}
