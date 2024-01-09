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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.Indexer;
import io.goobi.viewer.indexer.model.SolrConstants;

public class UtilsTest extends AbstractTest {

    /**
     * @see Utils#getCollisionFreeDataFilePath(String,String,String,String)
     * @verifies construct path correctly and avoid collisions
     */
    @Test
    public void getCollisionFreeDataFilePath_shouldConstructPathCorrectlyAndAvoidCollisions() throws Exception {
        List<Path> paths = new ArrayList<>(2);
        try {
            {
                // filename.xml
                Path path = Utils.getCollisionFreeDataFilePath("target", "filename", "#", ".xml");
                Assertions.assertNotNull(path);
                assertEquals("filename.xml", path.getFileName().toString());
                Files.createFile(path);
                assertTrue(Files.exists(path));
                paths.add(path);
            }
            {
                // filename#1.xml
                Path path = Utils.getCollisionFreeDataFilePath("target", "filename", "#", ".xml");
                Assertions.assertNotNull(path);
                assertEquals("filename#0.xml", path.getFileName().toString());
                Files.createFile(path);
                assertTrue(Files.exists(path));
                paths.add(path);
            }
            {
                // filename#2.xml
                Path path = Utils.getCollisionFreeDataFilePath("target", "filename", "#", ".xml");
                Assertions.assertNotNull(path);
                assertEquals("filename#1.xml", path.getFileName().toString());
            }
        } finally {
            for (Path path : paths) {
                Files.delete(path);
            }
        }

    }

    /**
     * @see Utils#extractPiFromFileName(Path)
     * @verifies extract file name correctly
     */
    @Test
    public void extractPiFromFileName_shouldExtractFileNameCorrectly() throws Exception {
        assertEquals("PPN123", Utils.extractPiFromFileName(Paths.get("PPN123#0.delete")));
        assertEquals("PPN123", Utils.extractPiFromFileName(Paths.get("PPN123#0.purge")));
        assertEquals("PPN123", Utils.extractPiFromFileName(Paths.get("PPN123.UPDATED")));
        assertEquals("PPN123", Utils.extractPiFromFileName(Paths.get("PPN123#0.UPDATED")));
    }

    /**
     * @see Utils#getFileNameFromIiifUrl(String)
     * @verifies extract file name correctly
     */
    @Test
    public void getFileNameFromIiifUrl_shouldExtractFileNameCorrectly() throws Exception {
        assertEquals("00000001.jpg",
                Utils.getFileNameFromIiifUrl("https://localhost:8080/viewer/rest/image/AC05725455/00000001.tif/full/!400,400/0/default.jpg"));
        assertEquals("AFE_1284_1999-17-557-1_a.jpg", Utils
                .getFileNameFromIiifUrl("https://pecunia2.zaw.uni-heidelberg.de:49200/iiif/2/AFE_1284_1999-17-557-1_a.jpg/full/full/0/default.jpg"));
    }

    /**
     * @see Utils#getFileNameFromIiifUrl(String)
     * @verifies extract escaped file name correctly
     */
    @Test
    public void getFileNameFromIiifUrl_shouldExtractEscapedFileNameCorrectly() throws Exception {
        assertEquals("00000001.jpg",
                Utils.getFileNameFromIiifUrl(
                        "https://example.com/api/iiif/image/v2/dbbs_derivate_00041856%2fmax%2F00000001.jpg/full/!256,256/0/color.jpg"));
    }

    /**
     * @see Utils#generateLongOrderNumber(int,int)
     * @verifies construct number correctly
     */
    @Test
    public void generateLongOrderNumber_shouldConstructNumberCorrectly() throws Exception {
        assertEquals(10001, Utils.generateLongOrderNumber(1, 1));
        assertEquals(100001, Utils.generateLongOrderNumber(10, 1));
        assertEquals(110010, Utils.generateLongOrderNumber(11, 10));
        assertEquals(1110100, Utils.generateLongOrderNumber(111, 100));
        assertEquals(11111000, Utils.generateLongOrderNumber(1111, 1000));
        assertEquals(111111000, Utils.generateLongOrderNumber(11111, 1000));
    }

    /**
     * @see Utils#isFileNameMatchesRegex(String,String[])
     * @verifies match correctly
     */
    @Test
    public void isFileNameMatchesRegex_shouldMatchCorrectly() throws Exception {
        assertTrue(Utils.isFileNameMatchesRegex("foo/bar/default.jpg", Indexer.IIIF_IMAGE_FILE_NAMES));
        assertTrue(Utils.isFileNameMatchesRegex("foo/bar/color.png", Indexer.IIIF_IMAGE_FILE_NAMES));
        assertFalse(Utils.isFileNameMatchesRegex("foo/bar/other.jpg", Indexer.IIIF_IMAGE_FILE_NAMES));
    }

    /**
     * @see Utils#adaptField(String,String)
     * @verifies apply prefix correctly
     */
    @Test
    public void adaptField_shouldApplyPrefixCorrectly() throws Exception {
        assertEquals("SORT_DC", Utils.adaptField(SolrConstants.DC, "SORT_"));
        assertEquals("SORT_FOO", Utils.adaptField("MD_FOO", "SORT_"));
        assertEquals("SORT_FOO", Utils.adaptField("MD2_FOO", "SORT_"));
        assertEquals("SORTNUM_FOO", Utils.adaptField("MDNUM_FOO", "SORT_"));
        assertEquals("SORT_FOO", Utils.adaptField("NE_FOO", "SORT_"));
        assertEquals("SORT_FOO", Utils.adaptField("BOOL_FOO", "SORT_"));
    }

    /**
     * @see Utils#adaptField(String,String)
     * @verifies not apply prefix to regular fields if empty
     */
    @Test
    public void adaptField_shouldNotApplyPrefixToRegularFieldsIfEmpty() throws Exception {
        assertEquals("MD_FOO", Utils.adaptField("MD_FOO", ""));
    }

    /**
     * @see Utils#adaptField(String,String)
     * @verifies remove untokenized correctly
     */
    @Test
    public void adaptField_shouldRemoveUntokenizedCorrectly() throws Exception {
        assertEquals("SORT_FOO", Utils.adaptField("MD_FOO_UNTOKENIZED", "SORT_"));
    }

    /**
     * @see Utils#adaptField(String,String)
     * @verifies not apply facet prefix to calendar fields
     */
    @Test
    public void adaptField_shouldNotApplyFacetPrefixToCalendarFields() throws Exception {
        assertEquals(SolrConstants.YEAR, Utils.adaptField(SolrConstants.YEAR, "FACET_"));
        assertEquals(SolrConstants.YEARMONTH, Utils.adaptField(SolrConstants.YEARMONTH, "FACET_"));
        assertEquals(SolrConstants.YEARMONTHDAY, Utils.adaptField(SolrConstants.YEARMONTHDAY, "FACET_"));
        assertEquals(SolrConstants.MONTHDAY, Utils.adaptField(SolrConstants.MONTHDAY, "FACET_"));
    }

    /**
     * @see Utils#sortifyField(String)
     * @verifies sortify correctly
     */
    @Test
    public void sortifyField_shouldSortifyCorrectly() throws Exception {
        assertEquals("SORT_DC", Utils.sortifyField(SolrConstants.DC));
        assertEquals("SORT_DOCSTRCT", Utils.sortifyField(SolrConstants.DOCSTRCT));
        assertEquals("SORT_TITLE", Utils.sortifyField("MD_TITLE_UNTOKENIZED"));
        assertEquals("SORTNUM_YEAR", Utils.sortifyField(SolrConstants.YEAR));
        assertEquals("SORTNUM_FOO", Utils.sortifyField("MDNUM_FOO"));
    }

    /**
     * @see Utils#isValidURL(String)
     * @verifies return true if url starts with http
     */
    @Test
    public void isValidURL_shouldReturnTrueIfUrlStartsWithHttp() throws Exception {
        assertTrue(Utils.isValidURL("http://example.com"));
    }

    /**
     * @see Utils#isValidURL(String)
     * @verifies return true if url starts with https
     */
    @Test
    public void isValidURL_shouldReturnTrueIfUrlStartsWithHttps() throws Exception {
        assertTrue(Utils.isValidURL("https://example.com"));
    }

    /**
     * @see Utils#isValidURL(String)
     * @verifies return true if url starts with file
     */
    @Test
    public void isValidURL_shouldReturnTrueIfUrlStartsWithFile() throws Exception {
        assertTrue(Utils.isValidURL("file://opt/digiverso/indexer/foo.xml"));
    }

    /**
     * @see Utils#isValidURL(String)
     * @verifies return false if not url
     */
    @Test
    public void isValidURL_shouldReturnFalseIfNotUrl() throws Exception {
        assertFalse(Utils.isValidURL("example.com/foo"));
    }
}
