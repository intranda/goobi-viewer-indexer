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
package io.goobi.viewer.indexer.helper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;

class JDomXPTest extends AbstractTest {

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect mets mods files correctly
     */
    @Test
    void determineFileFormat_shouldDetectMetsModsFilesCorrectly() throws Exception {
        File file = new File("src/test/resources/METS/H030001_mets.xml");
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(FileFormat.METS, JDomXP.determineFileFormat(file));
        
        // File containing both MODS and MARC
        file = new File("src/test/resources/METS/BV048249088.xml");
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(FileFormat.METS, JDomXP.determineFileFormat(file));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect mets marc files correctly
     */
    @Test
    void determineFileFormat_shouldDetectMetsMarcFilesCorrectly() throws Exception {
        File file = new File("src/test/resources/METS/VoorbeeldMETS_9940609919905131.xml");
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(FileFormat.METS_MARC, JDomXP.determineFileFormat(file));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect lido files correctly
     */
    @Test
    void determineFileFormat_shouldDetectLidoFilesCorrectly() throws Exception {
        File file = new File("src/test/resources/LIDO/khm_lido_export.xml");
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(FileFormat.LIDO, JDomXP.determineFileFormat(file));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect ead2 files correctly
     */
    @Test
    void determineFileFormat_shouldDetectEad2FilesCorrectly() throws Exception {
        File file = new File("src/test/resources/EAD/Akte_Koch.xml");
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(FileFormat.EAD, JDomXP.determineFileFormat(file));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect ead3 files correctly
     */
    @Test
    void determineFileFormat_shouldDetectEad3FilesCorrectly() throws Exception {
        File file = new File("src/test/resources/EAD/EAD3_example.xml");
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(FileFormat.EAD3, JDomXP.determineFileFormat(file));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect denkxweb files correctly
     */
    @Test
    void determineFileFormat_shouldDetectDenkxwebFilesCorrectly() throws Exception {
        File file = new File("src/test/resources/DenkXweb/denkxweb_30596824_short.xml");
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(FileFormat.DENKXWEB, JDomXP.determineFileFormat(file));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect dublin core files correctly
     */
    @Test
    void determineFileFormat_shouldDetectDublinCoreFilesCorrectly() throws Exception {
        File file = new File("src/test/resources/DC/record.xml");
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(FileFormat.DUBLINCORE, JDomXP.determineFileFormat(file));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect worldviews files correctly
     */
    @Test
    void determineFileFormat_shouldDetectWorldviewsFilesCorrectly() throws Exception {
        File file = new File("src/test/resources/WorldViews/gei_test_sthe_quelle_01.xml");
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(FileFormat.WORLDVIEWS, JDomXP.determineFileFormat(file));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect abbyy files correctly
     */
    @Test
    void determineFileFormat_shouldDetectAbbyyFilesCorrectly() throws Exception {
        File file = new File("src/test/resources/ABBYYXML/00000001.xml");
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(FileFormat.ABBYYXML, JDomXP.determineFileFormat(file));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect tei files correctly
     */
    @Test
    void determineFileFormat_shouldDetectTeiFilesCorrectly() throws Exception {
        File file = new File("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc/00000001.xml");
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(FileFormat.TEI, JDomXP.determineFileFormat(file));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect cms files correctly
     */
    @Test
    void determineFileFormat_shouldDetectCmsFilesCorrectly() throws Exception {
        File file = new File("src/test/resources/indexed_cms/CMS123.xml");
        Assertions.assertTrue(file.isFile());
        Assertions.assertEquals(FileFormat.CMS, JDomXP.determineFileFormat(file));
    }

    /**
     * @see JDomXP#splitLidoFile(File)
     * @verifies split multi record documents correctly
     */
    @Test
    void splitLidoFile_shouldSplitMultiRecordDocumentsCorrectly() throws Exception {
        File file = new File("src/test/resources/LIDO/khm_lido_export.xml");
        Assertions.assertTrue(file.isFile());
        List<Document> docs = JDomXP.splitLidoFile(file);
        Assertions.assertEquals(30, docs.size());
    }

    /**
     * @see JDomXP#splitLidoFile(File)
     * @verifies leave single record documents as is
     */
    @Test
    void splitLidoFile_shouldLeaveSingleRecordDocumentsAsIs() throws Exception {
        File file = new File("src/test/resources/LIDO/V0011127.xml");
        Assertions.assertTrue(file.isFile());
        List<Document> docs = JDomXP.splitLidoFile(file);
        Assertions.assertEquals(1, docs.size());
    }

    /**
     * @see JDomXP#splitLidoFile(File)
     * @verifies return empty list for non lido documents
     */
    @Test
    void splitLidoFile_shouldReturnEmptyListForNonLidoDocuments() throws Exception {
        File file = new File("src/test/resources/METS/H030001_mets.xml");
        Assertions.assertTrue(file.isFile());
        List<Document> docs = JDomXP.splitLidoFile(file);
        Assertions.assertEquals(0, docs.size());
    }

    /**
     * @see JDomXP#splitLidoFile(File)
     * @verifies return empty list for non-existing files
     */
    @Test
    void splitLidoFile_shouldReturnEmptyListForNonexistingFiles() throws Exception {
        File file = new File("nosuchfile.xml");
        Assertions.assertFalse(file.isFile());
        List<Document> docs = JDomXP.splitLidoFile(file);
        Assertions.assertEquals(0, docs.size());
    }

    /**
     * @see JDomXP#splitLidoFile(File)
     * @verifies return empty list given null
     */
    @Test
    void splitLidoFile_shouldReturnEmptyListGivenNull() throws Exception {
        List<Document> docs = JDomXP.splitLidoFile(null);
        Assertions.assertEquals(0, docs.size());
    }

    /**
     * @see JDomXP#splitDenkXwebFile(File)
     * @verifies split multi record documents correctly
     */
    @Test
    void splitDenkXwebFile_shouldSplitMultiRecordDocumentsCorrectly() throws Exception {
        File file = new File("src/test/resources/DenkXweb/denkxweb_30596824_short.xml");
        Assertions.assertTrue(file.isFile());
        List<Document> docs = JDomXP.splitDenkXwebFile(file);
        Assertions.assertEquals(2, docs.size());
    }

    /**
     * @see JDomXP#splitDenkXwebFile(File)
     * @verifies return empty list for non lido documents
     */
    @Test
    void splitDenkXwebFile_shouldReturnEmptyListForNonLidoDocuments() throws Exception {
        File file = new File("src/test/resources/METS/H030001_mets.xml");
        Assertions.assertTrue(file.isFile());
        List<Document> docs = JDomXP.splitDenkXwebFile(file);
        Assertions.assertEquals(0, docs.size());
    }

    /**
     * @see JDomXP#splitDenkXwebFile(File)
     * @verifies return empty list for non-existing files
     */
    @Test
    void splitDenkXwebFile_shouldReturnEmptyListForNonexistingFiles() throws Exception {
        File file = new File("nosuchfile.xml");
        Assertions.assertFalse(file.isFile());
        List<Document> docs = JDomXP.splitDenkXwebFile(file);
        Assertions.assertEquals(0, docs.size());
    }

    /**
     * @see JDomXP#splitDenkXwebFile(File)
     * @verifies return empty list given null
     */
    @Test
    void splitDenkXwebFile_shouldReturnEmptyListGivenNull() throws Exception {
        List<Document> docs = JDomXP.splitDenkXwebFile(null);
        Assertions.assertEquals(0, docs.size());
    }

    /**
     * @see JDomXP#writeXmlFile(Document,String)
     * @verifies write xml file correctly
     */
    @Test
    void writeXmlFile_shouldWriteXmlFileCorrectly() throws Exception {
        File file = new File("src/test/resources/METS/H030001_mets.xml");
        Assertions.assertTrue(file.isFile());

        File newFile = null;
        try (FileInputStream fis = new FileInputStream(file)) {
            SAXBuilder builder = new SAXBuilder();
            Document doc = builder.build(fis);
            Assertions.assertNotNull(doc);
            String path = "target/newmets.xml";
            JDomXP.writeXmlFile(doc, path);
            newFile = new File(path);
            Assertions.assertTrue(newFile.isFile());
            try (FileInputStream fis2 = new FileInputStream(newFile)) {
                Document newDoc = builder.build(fis2);
                Assertions.assertNotNull(newDoc);
                Assertions.assertEquals(doc.getContentSize(), newDoc.getContentSize());
            }
        } finally {
            if (newFile != null) {
                FileUtils.deleteQuietly(newFile);
            }
        }
    }

    /**
     * @see JDomXP#evaluateToAttributeStringValue(String,Object)
     * @verifies return value correctly
     */
    @Test
    void evaluateToAttributeStringValue_shouldReturnValueCorrectly() throws Exception {
        File file = new File("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        Assertions.assertTrue(file.isFile());
        JDomXP xp = new JDomXP(file);
        String xpath = "/mets:mets/mets:amdSec/mets:digiprovMD[@ID='DIGIPROV']/mets:mdWrap/@MDTYPE";
        String value = xp.evaluateToAttributeStringValue(xpath, null);
        Assertions.assertEquals("OTHER", value);
    }

    /**
     * @see JDomXP#evaluateToCdata(String,Object)
     * @verifies return value correctly
     */
    @Test
    void evaluateToCdata_shouldReturnValueCorrectly() throws Exception {
        File file = new File("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        Assertions.assertTrue(file.isFile());
        JDomXP xp = new JDomXP(file);
        String xpath =
                "/mets:mets/mets:amdSec/mets:digiprovMD[@ID='DIGIPROV']/mets:mdWrap[@OTHERMDTYPE='DVLINKS']/mets:xmlData/dv:links/dv:reference/text()";
        String value = xp.evaluateToCdata(xpath, null);
        Assertions.assertEquals("http://opac.sub.uni-goettingen.de/DB=1/PPN?PPN=517154005", value);
    }

    /**
     * @see JDomXP#evaluateToString(String,Object)
     * @verifies return value correctly
     */
    @Test
    void evaluateToString_shouldReturnValueCorrectly() throws Exception {
        File file = new File("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        Assertions.assertTrue(file.isFile());
        JDomXP xp = new JDomXP(file);
        String xpath =
                "/mets:mets/mets:amdSec/mets:digiprovMD[@ID='DIGIPROV']/mets:mdWrap[@OTHERMDTYPE='DVLINKS']/mets:xmlData/dv:links/dv:presentation/text()";
        String value = xp.evaluateToString(xpath, null);
        Assertions.assertEquals("http://resolver.sub.uni-goettingen.de/purl?PPN517154005", value);
    }

    /**
     * @see JDomXP#evaluateToStringList(String,Object)
     * @verifies return all values
     */
    @Test
    void evaluateToStringList_shouldReturnAllValues() throws Exception {
        File file = new File("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        Assertions.assertTrue(file.isFile(), "File not found: " + file.getAbsolutePath());
        JDomXP xp = new JDomXP(file);
        String xpath = "/mets:mets/mets:fileSec/mets:fileGrp[@USE='PRESENTATION']/mets:file";
        List<String> values = xp.evaluateToStringList(xpath, null);
        Assertions.assertEquals(16, values.size());
    }

    /**
     * @see JDomXP#readXmlFile(String)
     * @verifies build document correctly
     */
    @Test
    void readXmlFile_shouldBuildDocumentCorrectly() throws Exception {
        Document doc = JDomXP.readXmlFile(TEST_CONFIG_PATH);
        Assertions.assertNotNull(doc);
        Assertions.assertNotNull(doc.getRootElement());
    }

    /**
     * @see JDomXP#readXmlFile(String)
     * @verifies throw FileNotFoundException if file not found
     */
    @Test
    void readXmlFile_shouldThrowFileNotFoundExceptionIfFileNotFound() throws Exception {
        Assertions.assertThrows(FileNotFoundException.class, () -> JDomXP.readXmlFile("notfound.xml"));
    }

    /**
     * @see JDomXP#getMdWrap(String)
     * @verifies return mdWrap correctly
     */
    @Test
    void getMdWrap_shouldReturnMdWrapCorrectly() throws Exception {
        File file = new File("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml");
        Assertions.assertTrue(file.isFile(), "File not found: " + file.getAbsolutePath());
        JDomXP xp = new JDomXP(file);
        Element eleMdWrap = xp.getMdWrap("DMDLOG_0003");
        Assertions.assertNotNull(eleMdWrap);
    }
}
