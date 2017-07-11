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
package de.intranda.digiverso.presentation.solr;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.jdom2.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.intranda.digiverso.presentation.solr.helper.Hotfolder;
import de.intranda.digiverso.presentation.solr.helper.JDomXP;
import de.intranda.digiverso.presentation.solr.model.DataRepository;
import de.intranda.digiverso.presentation.solr.model.SolrConstants;

public class AbstractIndexerTest extends AbstractSolrEnabledTest {

    private static Hotfolder hotfolder;

    private Path metsFile;
    private Path lidoFile;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        hotfolder = new Hotfolder("resources/test/indexerconfig_solr_test.xml", server);
        metsFile = Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        Assert.assertTrue(Files.isRegularFile(metsFile));
        lidoFile = Paths.get("resources/test/LIDO/khm_lido_export.xml");
        Assert.assertTrue(Files.isRegularFile(lidoFile));
    }

    /**
     * @see AbstractIndexer#delete(String,boolean)
     * @verifies delete METS record from index completely
     */
    @Test
    public void delete_shouldDeleteMETSRecordFromIndexCompletely() throws Exception {
        String pi = "PPN517154005";
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_MEDIA, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_tif"));
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEI, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, false, dataFolders, null, 1);
        Assert.assertEquals(pi + ".xml", ret[0]);
        Assert.assertNull(ret[1]);
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + pi, null);
            Assert.assertEquals(22, docList.size());
        }
        Assert.assertTrue(AbstractIndexer.delete(pi, false, hotfolder.getSolrHelper()));
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + pi, null);
            Assert.assertTrue(docList.isEmpty());
        }
    }

    /**
     * @see AbstractIndexer#delete(String,boolean)
     * @verifies delete LIDO record from index completely
     */
    @Test
    public void delete_shouldDeleteLIDORecordFromIndexCompletely() throws Exception {
        String pi = "V0011127";
        Map<String, Path> dataFolders = new HashMap<>();
        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile.toFile());
        String[] ret = new LidoIndexer(hotfolder).index(lidoDocs.get(0), dataFolders, null, 1);
        Assert.assertEquals("ERROR: " + ret[1], pi, ret[0]);
        String iddoc;
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + pi, null);
            Assert.assertEquals(1, docList.size());
            iddoc = (String) docList.get(0).getFieldValue(SolrConstants.IDDOC);
            Assert.assertNotNull(iddoc);
        }
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + pi + " OR " + SolrConstants.IDDOC_OWNER
                    + ":" + iddoc, null);
            Assert.assertEquals(3, docList.size());
        }
        Assert.assertTrue(AbstractIndexer.delete(pi, false, hotfolder.getSolrHelper()));
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + pi + " OR " + SolrConstants.IDDOC_OWNER
                    + ":" + iddoc, null);
            Assert.assertTrue(docList.isEmpty());
        }
    }

    /**
     * @see AbstractIndexer#delete(String,boolean)
     * @verifies leave trace document for METS record if requested
     */
    @Test
    public void delete_shouldLeaveTraceDocumentForMETSRecordIfRequested() throws Exception {
        String pi = "PPN517154005";
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_MEDIA, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_tif"));
        dataFolders.put(DataRepository.PARAM_FULLTEXT, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_txt"));
        dataFolders.put(DataRepository.PARAM_TEI, Paths.get("resources/test/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc"));
        String[] ret = new MetsIndexer(hotfolder).index(metsFile, false, dataFolders, null, 1);
        Assert.assertEquals(pi + ".xml", ret[0]);
        Assert.assertNull(ret[1]);
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + pi, null);
            Assert.assertEquals(22, docList.size());
        }
        Assert.assertTrue(AbstractIndexer.delete(pi, true, hotfolder.getSolrHelper()));
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + pi, null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATEDELETED));
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.GROUPFIELD));
            Assert.assertNotNull(doc.getFieldValues(SolrConstants.IMAGEURN_OAI));
            Assert.assertEquals(16, doc.getFieldValues(SolrConstants.IMAGEURN_OAI).size());
        }
    }

    /**
     * @see AbstractIndexer#delete(String,boolean)
     * @verifies leave trace document for LIDO record if requested
     */
    @Test
    public void delete_shouldLeaveTraceDocumentForLIDORecordIfRequested() throws Exception {
        String pi = "V0011127";
        Map<String, Path> dataFolders = new HashMap<>();
        List<Document> lidoDocs = JDomXP.splitLidoFile(lidoFile.toFile());
        String[] ret = new LidoIndexer(hotfolder).index(lidoDocs.get(0), dataFolders, null, 1);
        Assert.assertEquals(pi, ret[0]);
        String iddoc;
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + pi, null);
            Assert.assertEquals(1, docList.size());
            iddoc = (String) docList.get(0).getFieldValue(SolrConstants.IDDOC);
            Assert.assertNotNull(iddoc);
        }
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI_TOPSTRUCT + ":" + pi + " OR " + SolrConstants.IDDOC_OWNER
                    + ":" + iddoc, null);
            Assert.assertEquals(3, docList.size());
        }
        Assert.assertTrue(AbstractIndexer.delete(pi, true, hotfolder.getSolrHelper()));
        {
            SolrDocumentList docList = hotfolder.getSolrHelper().search(SolrConstants.PI + ":" + pi, null);
            Assert.assertEquals(1, docList.size());
            SolrDocument doc = docList.get(0);
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.DATEDELETED));
            Assert.assertNotNull(doc.getFieldValue(SolrConstants.GROUPFIELD));
        }
    }

    /**
     * @see AbstractIndexer#cleanUpDefaultField(String)
     * @verifies replace irrelevant chars with spaces correctly
     */
    @Test
    public void cleanUpDefaultField_shouldReplaceIrrelevantCharsWithSpacesCorrectly() throws Exception {
        Assert.assertEquals("A B C D", AbstractIndexer.cleanUpDefaultField(" A,B;C:D,  "));
    }

    /**
     * @see AbstractIndexer#cleanUpNamedEntityValue(String)
     * @verifies clean up value correctly
     */
    @Test
    public void cleanUpNamedEntityValue_shouldCleanUpValueCorrectly() throws Exception {
        Assert.assertEquals("abcd", AbstractIndexer.cleanUpNamedEntityValue("\"(abcd,\""));
    }

    /**
     * @see AbstractIndexer#cleanUpNamedEntityValue(String)
     * @verifies throw IllegalArgumentException given null
     */
    @Test(expected = IllegalArgumentException.class)
    public void cleanUpNamedEntityValue_shouldThrowIllegalArgumentExceptionGivenNull() throws Exception {
        AbstractIndexer.cleanUpNamedEntityValue(null);
    }
}