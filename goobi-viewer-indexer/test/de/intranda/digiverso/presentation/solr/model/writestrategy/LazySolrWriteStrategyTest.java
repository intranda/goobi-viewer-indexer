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
package de.intranda.digiverso.presentation.solr.model.writestrategy;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.intranda.digiverso.presentation.solr.AbstractSolrEnabledTest;
import de.intranda.digiverso.presentation.solr.MetsIndexer;
import de.intranda.digiverso.presentation.solr.helper.Hotfolder;
import de.intranda.digiverso.presentation.solr.helper.SolrHelper;
import de.intranda.digiverso.presentation.solr.model.DataRepository;
import de.intranda.digiverso.presentation.solr.model.SolrConstants;
import de.intranda.digiverso.presentation.solr.model.SolrConstants.DocType;

public class LazySolrWriteStrategyTest extends AbstractSolrEnabledTest {

    private static Hotfolder hotfolder;

    private Path metsFile = Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder("resources/test/indexerconfig_solr_test.xml", server);
    }

    /**
     * @see LazySolrWriteStrategy#LazySolrWriteStrategy(SolrHelper)
     * @verifies set attributes correctly
     */
    @Test
    public void LazySolrWriteStrategy_shouldSetAttributesCorrectly() throws Exception {
        SolrHelper sh = new SolrHelper(server);
        LazySolrWriteStrategy strat = new LazySolrWriteStrategy(sh);
        Assert.assertEquals(sh, strat.solrHelper);
    }

    /**
     * @see LazySolrWriteStrategy#getPageDocsForPhysIdList(List)
     * @verifies return all docs for the given physIdList
     */
    @Test
    public void getPageDocsForPhysIdList_shouldReturnAllDocsForTheGivenPhysIdList() throws Exception {
        SolrHelper sh = new SolrHelper(server);
        LazySolrWriteStrategy strat = new LazySolrWriteStrategy(sh);
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        indexer.generatePageDocuments(strat, null, 1);
        List<SolrInputDocument> docs = strat.getPageDocsForPhysIdList(Arrays.asList(new String[] { "PHYS_0001", "PHYS_0002", "PHYS_0003" }));
        Assert.assertEquals(3, docs.size());
        Assert.assertEquals("PHYS_0001", docs.get(0).getFieldValue(SolrConstants.PHYSID));
        Assert.assertEquals("PHYS_0002", docs.get(1).getFieldValue(SolrConstants.PHYSID));
        Assert.assertEquals("PHYS_0003", docs.get(2).getFieldValue(SolrConstants.PHYSID));
    }

    /**
     * @see LazySolrWriteStrategy#writeDocs()
     * @verifies write all structure docs correctly
     */
    @Test
    public void writeDocs_shouldWriteAllStructureDocsCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEI, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        dataFolders.put(DataRepository.PARAM_TILEDIMAGES, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_ptif"));
        dataFolders.put(DataRepository.PARAM_OVERVIEW, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_overview"));

        SolrHelper sh = new SolrHelper(server);
        LazySolrWriteStrategy strat = new LazySolrWriteStrategy(sh);
        MetsIndexer indexer = new MetsIndexer(hotfolder);

        indexer.index(metsFile, false, dataFolders, strat, 1);
        SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":PPN517154005 AND " + SolrConstants.DOCTYPE + ":"
                + DocType.DOCSTRCT.name(), null);
        Assert.assertEquals(4, docList.size());
    }

    /**
     * @see LazySolrWriteStrategy#writeDocs()
     * @verifies write all page docs correctly
     */
    @Test
    public void writeDocs_shouldWriteAllPageDocsCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEI, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        dataFolders.put(DataRepository.PARAM_TILEDIMAGES, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_ptif"));
        dataFolders.put(DataRepository.PARAM_OVERVIEW, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_overview"));

        SolrHelper sh = new SolrHelper(server);
        LazySolrWriteStrategy strat = new LazySolrWriteStrategy(sh);
        MetsIndexer indexer = new MetsIndexer(hotfolder);

        indexer.index(metsFile, false, dataFolders, strat, 1);
        SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":PPN517154005 AND " + SolrConstants.DOCTYPE + ":"
                + DocType.PAGE.name(), null);
        Assert.assertEquals(16, docList.size());
    }
}