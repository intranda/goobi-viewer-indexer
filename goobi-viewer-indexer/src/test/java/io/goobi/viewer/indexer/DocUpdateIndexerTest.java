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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.datarepository.strategy.AbstractDataRepositoryStrategy;
import io.goobi.viewer.indexer.model.datarepository.strategy.IDataRepositoryStrategy;

public class DocUpdateIndexerTest extends AbstractSolrEnabledTest {

    private static final String PI = "PPN517154005";

    private Path metsFile;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder("src/test/resources/indexerconfig_solr_test.xml", client);

        metsFile = Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        Assert.assertTrue(Files.isRegularFile(metsFile));
    }

    /**
     * @see DocUpdateIndexer#DocUpdateIndexer(Hotfolder)
     * @verifies set attributes correctly
     */
    @Test
    public void DocUpdateIndexer_shouldSetAttributesCorrectly() throws Exception {
        DocUpdateIndexer indexer = new DocUpdateIndexer(hotfolder);
        Assert.assertEquals(hotfolder, indexer.hotfolder);
    }

    /**
     * @see DocUpdateIndexer#index(Path,Map)
     * @verifies update document correctly
     */
    @Test
    public void index_shouldUpdateDocumentCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        String iddoc = null;
        IDataRepositoryStrategy dataRepositoryStrategy = AbstractDataRepositoryStrategy.create(Configuration.getInstance());
        DataRepository dataRepository = dataRepositoryStrategy.selectDataRepository(PI, metsFile, dataFolders, hotfolder.getSearchIndex(), null)[0];

        // Index original doc and make sure all fields that will be updated already exist
        {
            dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
            dataFolders.put(DataRepository.PARAM_ALTO, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_alto"));
            dataFolders.put(DataRepository.PARAM_UGC, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_ugc"));
            String[] ret = new MetsIndexer(hotfolder).index(metsFile, false, dataFolders, null, 1);
            Assert.assertEquals(PI + ".xml", ret[0]);
            Assert.assertNull(ret[1]);
            SolrDocumentList docList = hotfolder.getSearchIndex()
                    .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.ORDER + ":1 AND " + SolrConstants.FULLTEXT + ":*", null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assert.assertNull(doc.getFieldValue(SolrConstants.UGCTERMS));
            // TODO check new UGC docs
        }

        dataFolders.clear();

        // Update doc with new ALTO and UGC (FULLTEXT should be updated via ALTO)
        {
            // New ALTO content
            Path updateCrowdsourcingAltoFolderSourcePath =
                    Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/pageupdate/PPN517154005#1483455145198_altocrowd");
            Path updateCrowdsourcingAltoFolderHotfolderPath =
                    Paths.get(hotfolder.getHotfolder().toAbsolutePath().toString(), "PPN517154005#1483455145198_altocrowd");
            Files.createDirectory(updateCrowdsourcingAltoFolderHotfolderPath);
            Assert.assertEquals(1,
                    Hotfolder.copyDirectory(updateCrowdsourcingAltoFolderSourcePath.toFile(), updateCrowdsourcingAltoFolderHotfolderPath.toFile()));
            dataFolders.put(DataRepository.PARAM_ALTOCROWD, updateCrowdsourcingAltoFolderHotfolderPath);

            // New FULLTEXT (should be ignored because ALTO is present)
            Path updateCrowdsourcingTextFolderSourcePath =
                    Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/pageupdate/PPN517154005#1483455145198_txtcrowd");
            Path updateCrowdsourcingTextFolderHotfolderPath =
                    Paths.get(hotfolder.getHotfolder().toAbsolutePath().toString(), "PPN517154005#1483455145198_txtcrowd");
            Files.createDirectory(updateCrowdsourcingTextFolderHotfolderPath);
            Assert.assertEquals(1,
                    Hotfolder.copyDirectory(updateCrowdsourcingTextFolderSourcePath.toFile(), updateCrowdsourcingTextFolderHotfolderPath.toFile()));
            dataFolders.put(DataRepository.PARAM_FULLTEXTCROWD, updateCrowdsourcingTextFolderHotfolderPath);

            // New UGC
            Path updateCrowdsourcingUgcFolderSourcePath =
                    Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/pageupdate/PPN517154005#1483455145198_ugc");
            Path updateCrowdsourcingUgcFolderHotfolderPath =
                    Paths.get(hotfolder.getHotfolder().toAbsolutePath().toString(), "PPN517154005#1483455145198_ugc");
            Files.createDirectory(updateCrowdsourcingUgcFolderHotfolderPath);
            Assert.assertEquals(1,
                    Hotfolder.copyDirectory(updateCrowdsourcingUgcFolderSourcePath.toFile(), updateCrowdsourcingUgcFolderHotfolderPath.toFile()));
            dataFolders.put(DataRepository.PARAM_UGC, updateCrowdsourcingUgcFolderHotfolderPath);

            // Update doc and check updated values
            Path updateFile = Paths.get(hotfolder.getHotfolder().toAbsolutePath().toString(), PI + "#1" + DocUpdateIndexer.FILE_EXTENSION);
            String[] ret = new DocUpdateIndexer(hotfolder).index(updateFile, dataFolders);
            Assert.assertEquals(ret[0] + ": " + ret[1], PI, ret[0]);
            Assert.assertNull(ret[1]);
            {
                SolrDocumentList docList = hotfolder.getSearchIndex()
                        .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.ORDER + ":1 AND " + SolrConstants.DOCTYPE + ":"
                                + DocType.PAGE.name(), null);
                Assert.assertEquals(1, docList.size());
                SolrDocument doc = docList.get(0);

                // Check for updated ALTO file in file system
                String altoFileName = (String) doc.getFieldValue(SolrConstants.FILENAME_ALTO);
                Assert.assertNotNull(altoFileName);
                Path altoFile = Paths.get(dataRepository.getRootDir().toAbsolutePath().toString(), altoFileName);
                Assert.assertTrue("File not found at " + altoFile.toAbsolutePath().toString(), Files.isRegularFile(altoFile));
                String altoText = FileTools.readFileToString(altoFile.toFile(), null);
                Assert.assertNotNull(altoText);
                Assert.assertTrue(altoText.contains("Bollywood!"));
                Assert.assertNull(doc.getFieldValue(SolrConstants.UGCTERMS));
            }

            //Assert.assertTrue(((String) doc.getFieldValue(SolrConstants.UGCTERMS)).contains("Hütchenspieler"));

            // Check for new UGC docs
            {
                SolrDocumentList docList = hotfolder.getSearchIndex()
                        .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.ORDER + ":1 AND " + SolrConstants.DOCTYPE + ":"
                                + DocType.UGC.name(), null);
                Assert.assertEquals(2, docList.size());
                {
                    SolrDocument doc = docList.get(0);
                    Assert.assertNotNull(doc.getFieldValue(SolrConstants.UGCTERMS));
                    Assert.assertEquals(SolrConstants._UGC_TYPE_PERSON, doc.getFieldValue(SolrConstants.UGCTYPE));
                    Assert.assertEquals("1290.0, 1384.0, 1930.0, 1523.0", doc.getFieldValue(SolrConstants.UGCCOORDS));
                    Assert.assertEquals("Felix", doc.getFirstValue("MD_FIRSTNAME"));
                    Assert.assertEquals("Klein", doc.getFirstValue("MD_LASTNAME"));
                    Assert.assertEquals("Hütchenspieler", doc.getFirstValue("MD_OCCUPATION"));
                }
                {
                    SolrDocument doc = docList.get(1);
                    Assert.assertNotNull(doc.getFieldValue(SolrConstants.UGCTERMS));
                    Assert.assertEquals(SolrConstants._UGC_TYPE_COMMENT, doc.getFieldValue(SolrConstants.UGCTYPE));
                    Assert.assertEquals("new comment text", doc.getFirstValue("MD_TEXT"));
                }
            }
        }

        dataFolders.clear();

        // Update just the FULLTEXT
        {
            Path updateCrowdsourcingTextFolderSourcePath =
                    Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/pageupdate/PPN517154005#1483455145198_txtcrowd");
            Path updateCrowdsourcingTextFolderHotfolderPath =
                    Paths.get(hotfolder.getHotfolder().toAbsolutePath().toString(), "PPN517154005#1483455145198_txtcrowd");
            Hotfolder.copyDirectory(updateCrowdsourcingTextFolderSourcePath.toFile(), updateCrowdsourcingTextFolderHotfolderPath.toFile());
            dataFolders.put(DataRepository.PARAM_FULLTEXTCROWD, updateCrowdsourcingTextFolderHotfolderPath);

            Path updateFile = Paths.get(hotfolder.getHotfolder().toAbsolutePath().toString(), PI + "#1" + DocUpdateIndexer.FILE_EXTENSION);
            String[] ret = new DocUpdateIndexer(hotfolder).index(updateFile, dataFolders);
            Assert.assertEquals(ret[0] + ": " + ret[1], PI, ret[0]);
            Assert.assertNull(ret[1]);
            SolrDocumentList docList = hotfolder.getSearchIndex()
                    .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.ORDER + ":1  AND " + SolrConstants.DOCTYPE + ":"
                            + DocType.PAGE.name(), null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);

            // Check for updated text file in file system
            String textFileName = (String) doc.getFieldValue(SolrConstants.FILENAME_FULLTEXT);
            Assert.assertNotNull(textFileName);
            Path textFile = Paths.get(dataRepository.getRootDir().toAbsolutePath().toString(), textFileName);
            Assert.assertTrue(Files.isRegularFile(textFile));
            String altoText = FileTools.readFileToString(textFile.toFile(), null);
            Assert.assertNotNull(altoText);
            Assert.assertEquals("updated text file", altoText.trim());
        }

    }
}
