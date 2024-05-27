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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.jdom2.Element;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.model.SolrConstants;

class TextHelperTest extends AbstractTest {

    /**
     * @see TextHelper#readAltoFile(String,File)
     * @verifies read ALTO document correctly
     */
    @Test
    void readAltoFile_shouldReadALTODocumentCorrectly() throws Exception {
        File folder = new File("src/test/resources/ALTO");
        Assertions.assertTrue(folder.isDirectory());
        Map<String, Object> result = TextHelper.readAltoFile(new File(folder, "birdsbeneficialt00froh_0031.xml"));
        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.get(SolrConstants.ALTO));
    }

    @SuppressWarnings("unchecked")
    @Test
    void readAltoFile_shouldRetainTags() throws Exception {
        File origFile = new File("src/test/resources/ALTO/altoWithTags.xml");
        Assertions.assertTrue(origFile.isFile());
        Map<String, Object> result = TextHelper.readAltoFile(origFile);
        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.get(SolrConstants.FULLTEXT));
        String altoRead = (String) result.get(SolrConstants.ALTO);
        String altoOrig = FileTools.readFileToString(origFile, null);
        Assertions.assertTrue(altoRead.contains("NamedEntityTag") && altoRead.contains("TAGREFS"));
        Assertions.assertTrue("PERSON_Heinrich".equals(((List<String>) result.get(SolrConstants.NAMEDENTITIES)).get(0)));
        //        Assertions.assertEquals(altoOrig.replaceAll("\\s", "").toLowerCase(), altoRead.replaceAll("\\s", "").toLowerCase());
    }

    /**
     * @see TextHelper#readAltoFile(String,File)
     * @verifies extract fulltext correctly
     */
    @Test
    void readAltoFile_shouldExtractFulltextCorrectly() throws Exception {
        File folder = new File("src/test/resources/ALTO");
        Assertions.assertTrue(folder.isDirectory());
        {
            Map<String, Object> result = TextHelper.readAltoFile(new File(folder, "birdsbeneficialt00froh_0031.xml"));
            Assertions.assertNotNull(result);
            Assertions.assertNotNull(result.get(SolrConstants.FULLTEXT));
            // Plain TextBlock
            Assertions.assertTrue(((String) result.get(SolrConstants.FULLTEXT)).contains("Mus."));
            // TextBlock embedded in a ComposedBlock
            Assertions.assertTrue(((String) result.get(SolrConstants.FULLTEXT)).contains("Stone-Curlew"));
        }
        {
            Map<String, Object> result = TextHelper.readAltoFile(new File(folder, "00000010.xml"));
            Assertions.assertNotNull(result);
            Assertions.assertNotNull(result.get(SolrConstants.FULLTEXT));
            // SUBS_CONTENT
            Assertions.assertTrue(((String) result.get(SolrConstants.FULLTEXT)).contains("Eheschliessungen"));
        }
        {
            Map<String, Object> result = TextHelper.readAltoFile(new File(folder, "00000005a.xml"));
            Assertions.assertNotNull(result);
            Assertions.assertNotNull(result.get(SolrConstants.FULLTEXT));
            // SUBS_CONTENT
            Assertions.assertTrue(((String) result.get(SolrConstants.FULLTEXT)).contains("jeweilig"));
        }
        {
            Map<String, Object> result = TextHelper.readAltoFile(new File(folder, "0230L.xml"));
            Assertions.assertNotNull(result);
            Assertions.assertNotNull(result.get(SolrConstants.FULLTEXT));
            // SUBS_CONTENT
            Assertions.assertTrue(((String) result.get(SolrConstants.FULLTEXT)).contains("Wappen"));
        }
    }

    /**
     * @see TextHelper#readAltoFile(String,File)
     * @verifies extract page dimensions correctly
     */
    @Test
    void readAltoFile_shouldExtractPageDimensionsCorrectly() throws Exception {
        File folder = new File("src/test/resources/ALTO");
        Assertions.assertTrue(folder.isDirectory());
        Map<String, Object> result = TextHelper.readAltoFile(new File(folder, "AC04987957_00000124.xml"));
        Assertions.assertNotNull(result);
        Assertions.assertEquals("4898", result.get(SolrConstants.WIDTH));
        Assertions.assertEquals("6937", result.get(SolrConstants.HEIGHT));
    }

    /**
     * @see TextHelper#readAltoFile(String,File)
     * @verifies throw FileNotFoundException if file not found
     */
    @Test
    void readAltoFile_shouldThrowFileNotFoundExceptionIfFileNotFound() throws Exception {
        File folder = new File("src/test/resources/ALTO");
        Assertions.assertTrue(folder.isDirectory());
        Assertions.assertThrows(FileNotFoundException.class, () -> TextHelper.readAltoFile(new File(folder, "filenotfound")));
    }

    /**
     * @see TextHelper#readMix(File)
     * @verifies throw FileNotFoundException if file not found
     */
    @Test
    void readMix_shouldThrowFileNotFoundExceptionIfFileNotFound() throws Exception {
        Assertions.assertThrows(FileNotFoundException.class, () -> TextHelper.readMix(new File("filenotfound")));
    }

    /**
     * @see TextHelper#readAbbyyToAlto(File)
     * @verifies convert to ALTO correctly
     */
    @Test
    void readAbbyyToAlto_shouldConvertToALTOCorrectly() throws Exception {
        Map<String, Object> result = TextHelper.readAbbyyToAlto(new File("src/test/resources/ABBYYXML/00000001.xml"));
        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.get(SolrConstants.ALTO));
    }

    /**
     * @see TextHelper#readAbbyyToAlto(File)
     * @verifies throw IOException given wrong document format
     */
    @Test
    void readAbbyyToAlto_shouldThrowIllegalArgumentExceptionGivenWrongDocumentFormat() throws Exception {
        Assertions.assertThrows(IOException.class,
                () -> TextHelper.readAbbyyToAlto(new File("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc/00000001.xml")));
    }

    /**
     * @see TextHelper#readTeiToAlto(File)
     * @verifies convert to ALTO correctly
     */
    @Test
    void readTeiToAlto_shouldConvertToALTOCorrectly() throws Exception {
        Map<String, Object> result =
                TextHelper.readTeiToAlto(new File("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc/00000001.xml"));
        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.get(SolrConstants.ALTO));
    }

    /**
     * @see TextHelper#readTeiToAlto(File)
     * @verifies throw IOException given wrong document format
     */
    @Test
    void readTeiToAlto_shouldThrowIllegalArgumentExceptionGivenWrongDocumentFormat() throws Exception {
        Assertions.assertThrows(IOException.class, () -> TextHelper.readTeiToAlto(new File("src/test/resources/ABBYYXML/00000001.xml")));
    }

    /**
     * @see TextHelper#generateFulltext(String,File,boolean)
     * @verifies return text if fulltext file exists
     */
    @Test
    void generateFulltext_shouldReturnTextIfFulltextFileExists() throws Exception {
        Path folder = Paths.get("src/test/resources");
        Assertions.assertTrue(Files.isDirectory(folder));
        String text = TextHelper.generateFulltext("stopwords_de_en.txt", folder, false, false);
        Assertions.assertTrue(StringUtils.isNotEmpty(text));
    }

    /**
     * @see TextHelper#generateFulltext(String,File,boolean)
     * @verifies return null if fulltext folder exists but no file
     */
    @Test
    void generateFulltext_shouldReturnNullIfFulltextFolderExistsButNoFile() throws Exception {
        Path folder = Paths.get("src/test/resources");
        Assertions.assertTrue(Files.isDirectory(folder));
        String text = TextHelper.generateFulltext("filenotfound.txt", folder, false, false);
        Assertions.assertNull(text);
    }

    /**
     * @see TextHelper#generateFulltext(String,File,boolean)
     * @verifies return null of fulltext folder does not exist
     */
    @Test
    void generateFulltext_shouldReturnNullOfFulltextFolderDoesNotExist() throws Exception {
        Path folder = Paths.get("src/test/resources/dirnotfound");
        Assertions.assertFalse(Files.isDirectory(folder));
        String text = TextHelper.generateFulltext("stopwords_de_en.txt", folder, false, false);
        Assertions.assertNull(text);
    }

    /**
     * @see TextHelper#cleanUpHtmlTags(String)
     * @verifies clean up string correctly
     */
    @Test
    void cleanUpHtmlTags_shouldCleanUpStringCorrectly() throws Exception {
        Assertions.assertEquals("foo bar", TextHelper.cleanUpHtmlTags("<p><b>foo</b></p><br/>bar"));
        Assertions.assertEquals("foo bar", TextHelper.cleanUpHtmlTags("foo <bar"));
    }

    /**
     * @see TextHelper#createSimpleNamedEntityTag(Element)
     * @verifies uppercase type
     */
    @Test
    void createSimpleNamedEntityTag_shouldUppercaseType() throws Exception {
        Element eleTag = new Element("NamedEntityTag");
        eleTag.setAttribute("TYPE", "location");
        eleTag.setAttribute("LABEL", "Göttingen");
        eleTag.setAttribute("URI", "https://www.geonames.org/2918632");
        String tag = TextHelper.createSimpleNamedEntityTag(eleTag);
        Assertions.assertEquals("LOCATION###Göttingen###https://www.geonames.org/2918632", tag);
    }
}
