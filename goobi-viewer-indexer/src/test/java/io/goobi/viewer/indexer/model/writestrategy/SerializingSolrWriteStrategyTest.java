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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractSolrEnabledTest;
import io.goobi.viewer.indexer.MetsIndexer;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.datarepository.strategy.AbstractDataRepositoryStrategy;
import io.goobi.viewer.indexer.model.datarepository.strategy.IDataRepositoryStrategy;

class SerializingSolrWriteStrategyTest extends AbstractSolrEnabledTest {

    private static Path tempFolder = Paths.get("target/temp");

    private Path metsFile = Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());

        Files.createDirectory(tempFolder);
        Assertions.assertTrue(Files.isDirectory(tempFolder));
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();

        Utils.deleteDirectory(tempFolder);
    }

    /**
     * @see SerializingSolrWriteStrategy#getPageDocsForPhysIdList(List)
     * @verifies return all docs for the given physIdList
     */
    @Test
    void getPageDocsForPhysIdList_shouldReturnAllDocsForTheGivenPhysIdList() throws Exception {
        SerializingSolrWriteStrategy strat = new SerializingSolrWriteStrategy(SolrIndexerDaemon.getInstance().getSearchIndex(), tempFolder);
        IDataRepositoryStrategy dataRepositoryStrategy = AbstractDataRepositoryStrategy.create(SolrIndexerDaemon.getInstance().getConfiguration());
        MetsIndexer indexer = new MetsIndexer(hotfolder);
        indexer.initJDomXP(metsFile);
        indexer.generatePageDocuments(strat, null,
                dataRepositoryStrategy.selectDataRepository("PPN517154005", metsFile, null, SolrIndexerDaemon.getInstance().getSearchIndex(),
                        null)[0],
                "PPN517154005", 1, false);
        List<SolrInputDocument> docs = strat.getPageDocsForPhysIdList(Arrays.asList(new String[] { "PHYS_0001", "PHYS_0002", "PHYS_0003" }));
        Assertions.assertEquals(3, docs.size());
        Assertions.assertEquals("PHYS_0001", docs.get(0).getFieldValue(SolrConstants.PHYSID));
        Assertions.assertEquals("PHYS_0002", docs.get(1).getFieldValue(SolrConstants.PHYSID));
        Assertions.assertEquals("PHYS_0003", docs.get(2).getFieldValue(SolrConstants.PHYSID));
    }

    /**
     * @see SerializingSolrWriteStrategy#writeDocs()
     * @verifies write all structure docs correctly
     */
    @Test
    void writeDocs_shouldWriteAllStructureDocsCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        dataFolders.put(DataRepository.PARAM_CMS, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_cms"));

        SerializingSolrWriteStrategy strat = new SerializingSolrWriteStrategy(SolrIndexerDaemon.getInstance().getSearchIndex(), tempFolder);
        MetsIndexer indexer = new MetsIndexer(hotfolder);

        indexer.index(metsFile, false, dataFolders, strat, 1, false);
        SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search(SolrConstants.PI_TOPSTRUCT + ":PPN517154005 AND " + SolrConstants.DOCTYPE + ":" + DocType.DOCSTRCT.name(), null);
        Assertions.assertEquals(4, docList.size());
    }

    /**
     * @see SerializingSolrWriteStrategy#writeDocs()
     * @verifies write all page docs correctly
     */
    @Test
    void writeDocs_shouldWriteAllPageDocsCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEIWC, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        dataFolders.put(DataRepository.PARAM_CMS, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_cms"));

        SerializingSolrWriteStrategy strat = new SerializingSolrWriteStrategy(SolrIndexerDaemon.getInstance().getSearchIndex(), tempFolder);
        MetsIndexer indexer = new MetsIndexer(hotfolder);

        indexer.index(metsFile, false, dataFolders, strat, 1, false);
        SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search(SolrConstants.PI_TOPSTRUCT + ":PPN517154005 AND " + SolrConstants.DOCTYPE + ":" + DocType.PAGE.name(), null);
        Assertions.assertEquals(16, docList.size());
    }
}
