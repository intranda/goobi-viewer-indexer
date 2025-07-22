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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.helper.FileTools;
import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.model.IndexingResult;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.datarepository.strategy.AbstractDataRepositoryStrategy;
import io.goobi.viewer.indexer.model.datarepository.strategy.IDataRepositoryStrategy;

class DocUpdateIndexerTest extends AbstractSolrEnabledTest {

    private static final String PI = "PPN517154005";

    private Path metsFile;

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());

        metsFile = Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        Assertions.assertTrue(Files.isRegularFile(metsFile));
    }

    /**
     * @see DocUpdateIndexer#DocUpdateIndexer(Hotfolder)
     * @verifies set attributes correctly
     */
    @Test
    void DocUpdateIndexer_shouldSetAttributesCorrectly() {
        DocUpdateIndexer indexer = new DocUpdateIndexer(hotfolder);
        Assertions.assertEquals(hotfolder, indexer.hotfolder);
    }

    /**
     * @see DocUpdateIndexer#index(Path,Map)
     * @verifies update document correctly
     */
    @Test
    void index_shouldUpdateDocumentCorrectly() throws Exception {
        Map<String, Path> dataFolders = new HashMap<>();
        IDataRepositoryStrategy dataRepositoryStrategy = AbstractDataRepositoryStrategy.create(SolrIndexerDaemon.getInstance().getConfiguration());
        DataRepository dataRepository =
                dataRepositoryStrategy.selectDataRepository(PI, metsFile, dataFolders, SolrIndexerDaemon.getInstance().getSearchIndex(), null)[0];

        // Index original doc and make sure all fields that will be updated already exist
        {
            dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
            dataFolders.put(DataRepository.PARAM_ALTO, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_alto"));
            dataFolders.put(DataRepository.PARAM_UGC, Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_ugc"));
            IndexingResult result = new MetsIndexer(hotfolder).index(metsFile, dataFolders, null, 1, false);
            Assertions.assertEquals(PI + ".xml", result.getRecordFileName());
            Assertions.assertNull(result.getError());
            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.ORDER + ":1 AND " + SolrConstants.FULLTEXT + ":*", null);
            Assertions.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assertions.assertNull(doc.getFieldValue(SolrConstants.UGCTERMS));
            // TODO check new UGC docs
        }

        dataFolders.clear();

        // Update doc with new ALTO and UGC (FULLTEXT should be updated via ALTO)
        {
            // New ALTO content
            Path updateCrowdsourcingAltoFolderSourcePath =
                    Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/pageupdate/PPN517154005#1483455145198_altocrowd");
            Path updateCrowdsourcingAltoFolderHotfolderPath =
                    Paths.get(hotfolder.getHotfolderPath().toAbsolutePath().toString(), "PPN517154005#1483455145198_altocrowd");
            Files.createDirectory(updateCrowdsourcingAltoFolderHotfolderPath);
            Assertions.assertEquals(1,
                    FileTools.copyDirectory(updateCrowdsourcingAltoFolderSourcePath, updateCrowdsourcingAltoFolderHotfolderPath));
            dataFolders.put(DataRepository.PARAM_ALTOCROWD, updateCrowdsourcingAltoFolderHotfolderPath);

            // New FULLTEXT (should be ignored because ALTO is present)
            Path updateCrowdsourcingTextFolderSourcePath =
                    Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/pageupdate/PPN517154005#1483455145198_txtcrowd");
            Path updateCrowdsourcingTextFolderHotfolderPath =
                    Paths.get(hotfolder.getHotfolderPath().toAbsolutePath().toString(), "PPN517154005#1483455145198_txtcrowd");
            Files.createDirectory(updateCrowdsourcingTextFolderHotfolderPath);
            Assertions.assertEquals(1,
                    FileTools.copyDirectory(updateCrowdsourcingTextFolderSourcePath, updateCrowdsourcingTextFolderHotfolderPath));
            dataFolders.put(DataRepository.PARAM_FULLTEXTCROWD, updateCrowdsourcingTextFolderHotfolderPath);

            // New UGC
            Path updateCrowdsourcingUgcFolderSourcePath =
                    Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/pageupdate/PPN517154005#1483455145198_ugc");
            Path updateCrowdsourcingUgcFolderHotfolderPath =
                    Paths.get(hotfolder.getHotfolderPath().toAbsolutePath().toString(), "PPN517154005#1483455145198_ugc");
            Files.createDirectory(updateCrowdsourcingUgcFolderHotfolderPath);
            Assertions.assertEquals(1,
                    FileTools.copyDirectory(updateCrowdsourcingUgcFolderSourcePath, updateCrowdsourcingUgcFolderHotfolderPath));
            dataFolders.put(DataRepository.PARAM_UGC, updateCrowdsourcingUgcFolderHotfolderPath);

            // Update doc and check updated values
            Path updateFile = Paths.get(hotfolder.getHotfolderPath().toAbsolutePath().toString(), PI + "#1" + DocUpdateIndexer.FILE_EXTENSION);
            IndexingResult result = new DocUpdateIndexer(hotfolder).index(updateFile, dataFolders);
            Assertions.assertEquals(PI, result.getPi(), "ERROR: " + result.getError());
            Assertions.assertNull(result.getError());
            {
                SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                        .getSearchIndex()
                        .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.ORDER + ":1 AND " + SolrConstants.DOCTYPE + ":"
                                + DocType.PAGE.name(), null);
                Assertions.assertEquals(1, docList.size());
                SolrDocument doc = docList.get(0);

                // Check for updated ALTO file in file system
                String altoFileName = (String) doc.getFieldValue(SolrConstants.FILENAME_ALTO);
                Assertions.assertNotNull(altoFileName);
                Path altoFile = Paths.get(dataRepository.getRootDir().toAbsolutePath().toString(), altoFileName);
                Assertions.assertTrue(Files.isRegularFile(altoFile), "File not found at " + altoFile.toAbsolutePath().toString());
                String altoText = FileTools.readFileToString(altoFile.toFile(), null);
                Assertions.assertNotNull(altoText);
                Assertions.assertTrue(altoText.contains("Bollywood!"));
                Assertions.assertNull(doc.getFieldValue(SolrConstants.UGCTERMS));
            }

            //Assertions.assertTrue(((String) doc.getFieldValue(SolrConstants.UGCTERMS)).contains("Hütchenspieler"));

            // Check for new UGC docs
            {
                SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                        .getSearchIndex()
                        .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.ORDER + ":1 AND " + SolrConstants.DOCTYPE + ":"
                                + DocType.UGC.name(), null);
                Assertions.assertEquals(2, docList.size());
                {
                    SolrDocument doc = docList.get(0);
                    Assertions.assertNotNull(doc.getFieldValue(SolrConstants.UGCTERMS));
                    Assertions.assertEquals(SolrConstants.UGC_TYPE_PERSON, doc.getFieldValue(SolrConstants.UGCTYPE));
                    Assertions.assertEquals("1290.0, 1384.0, 1930.0, 1523.0", doc.getFieldValue(SolrConstants.UGCCOORDS));
                    Assertions.assertEquals("Felix", doc.getFirstValue("MD_FIRSTNAME"));
                    Assertions.assertEquals("Klein", doc.getFirstValue("MD_LASTNAME"));
                    Assertions.assertEquals("Hütchenspieler", doc.getFirstValue("MD_OCCUPATION"));
                }
                {
                    SolrDocument doc = docList.get(1);
                    Assertions.assertNotNull(doc.getFieldValue(SolrConstants.UGCTERMS));
                    Assertions.assertEquals(SolrConstants.UGC_TYPE_COMMENT, doc.getFieldValue(SolrConstants.UGCTYPE));
                    Assertions.assertEquals("new comment text", doc.getFirstValue("MD_TEXT"));
                }
            }
        }

        dataFolders.clear();

        // Update just the FULLTEXT
        {
            Path updateCrowdsourcingTextFolderSourcePath =
                    Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/pageupdate/PPN517154005#1483455145198_txtcrowd");
            Path updateCrowdsourcingTextFolderHotfolderPath =
                    Paths.get(hotfolder.getHotfolderPath().toAbsolutePath().toString(), "PPN517154005#1483455145198_txtcrowd");
            FileTools.copyDirectory(updateCrowdsourcingTextFolderSourcePath, updateCrowdsourcingTextFolderHotfolderPath);
            dataFolders.put(DataRepository.PARAM_FULLTEXTCROWD, updateCrowdsourcingTextFolderHotfolderPath);

            Path updateFile = Paths.get(hotfolder.getHotfolderPath().toAbsolutePath().toString(), PI + "#1" + DocUpdateIndexer.FILE_EXTENSION);
            IndexingResult result = new DocUpdateIndexer(hotfolder).index(updateFile, dataFolders);
            Assertions.assertEquals(PI, result.getPi(), "ERROR: " + result.getError());
            Assertions.assertNull(result.getError());
            SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                    .getSearchIndex()
                    .search(SolrConstants.PI_TOPSTRUCT + ":" + PI + " AND " + SolrConstants.ORDER + ":1  AND " + SolrConstants.DOCTYPE + ":"
                            + DocType.PAGE.name(), null);
            Assertions.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);

            // Check for updated text file in file system
            String textFileName = (String) doc.getFieldValue(SolrConstants.FILENAME_FULLTEXT);
            Assertions.assertNotNull(textFileName);
            Path textFile = Paths.get(dataRepository.getRootDir().toAbsolutePath().toString(), textFileName);
            Assertions.assertTrue(Files.isRegularFile(textFile));
            String altoText = FileTools.readFileToString(textFile.toFile(), null);
            Assertions.assertNotNull(altoText);
            Assertions.assertEquals("updated text file", altoText.trim());
        }

    }
}
