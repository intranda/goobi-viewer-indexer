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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.Indexer;
import io.goobi.viewer.indexer.model.SolrConstants;

class UtilsTest extends AbstractTest {

    /**
     * @see Utils#getCollisionFreeDataFilePath(String,String,String,String)
     * @verifies construct path correctly and avoid collisions
     */
    @Test
    void getCollisionFreeDataFilePath_shouldConstructPathCorrectlyAndAvoidCollisions(@TempDir Path tempDir) throws Exception {
        String destFolderPath = tempDir.toString();
        // The method reserves each path by creating the (empty) file, so no manual Files.createFile is needed.
        {
            // filename.xml
            Path path = Utils.getCollisionFreeDataFilePath(destFolderPath, "filename", "#", ".xml");
            Assertions.assertNotNull(path);
            assertEquals("filename.xml", path.getFileName().toString());
            assertTrue(Files.exists(path));
        }
        {
            // filename#0.xml
            Path path = Utils.getCollisionFreeDataFilePath(destFolderPath, "filename", "#", ".xml");
            Assertions.assertNotNull(path);
            assertEquals("filename#0.xml", path.getFileName().toString());
            assertTrue(Files.exists(path));
        }
        {
            // filename#1.xml
            Path path = Utils.getCollisionFreeDataFilePath(destFolderPath, "filename", "#", ".xml");
            Assertions.assertNotNull(path);
            assertEquals("filename#1.xml", path.getFileName().toString());
            assertTrue(Files.exists(path));
        }
    }

    /**
     * @see Utils#getCollisionFreeDataFilePath(String,String,String,String)
     * @verifies reserve unique paths under concurrent access
     */
    @Test
    void getCollisionFreeDataFilePath_shouldReserveUniquePathsUnderConcurrentAccess(@TempDir Path tempDir) throws Exception {
        int threadCount = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Path>> futures = new ArrayList<>(threadCount);
        try {
            for (int i = 0; i < threadCount; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    start.await(); // release all threads simultaneously
                    return Utils.getCollisionFreeDataFilePath(tempDir.toString(), "filename", "#", ".xml");
                }));
            }
            ready.await();
            start.countDown();
            Set<Path> results = new HashSet<>();
            for (Future<Path> future : futures) {
                results.add(future.get());
            }
            assertEquals(threadCount, results.size(), "All reserved paths must be pairwise distinct");
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * @see Utils#extractPiFromFileName(Path)
     * @verifies extract file name correctly
     */
    @Test
    void extractPiFromFileName_shouldExtractFileNameCorrectly() {
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
    void getFileNameFromIiifUrl_shouldExtractFileNameCorrectly() {
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
    void getFileNameFromIiifUrl_shouldExtractEscapedFileNameCorrectly() {
        assertEquals("00000001.jpg",
                Utils.getFileNameFromIiifUrl(
                        "https://example.com/api/iiif/image/v2/dbbs_derivate_00041856%2fmax%2F00000001.jpg/full/!256,256/0/color.jpg"));
    }

    /**
     * @see Utils#generateLongOrderNumber(int,int)
     * @verifies construct number correctly
     */
    @Test
    void generateLongOrderNumber_shouldConstructNumberCorrectly() {
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
    void isFileNameMatchesRegex_shouldMatchCorrectly() {
        assertTrue(Utils.isFileNameMatchesRegex("foo/bar/default.jpg", Indexer.IIIF_IMAGE_FILE_NAMES));
        assertTrue(Utils.isFileNameMatchesRegex("foo/bar/color.png", Indexer.IIIF_IMAGE_FILE_NAMES));
        assertFalse(Utils.isFileNameMatchesRegex("foo/bar/other.jpg", Indexer.IIIF_IMAGE_FILE_NAMES));
    }

    /**
     * Regression: filenames whose last path segment merely contains an IIIF qualifier ("color", "bitonal", ...) as a
     * substring (e.g. "02_color.tif") were previously misidentified as IIIF URLs because the qualifier in the regex was
     * not anchored to a path separator and the dot before the extension was unescaped.
     *
     * @see Utils#isFileNameMatchesRegex(String,String[])
     * @verifies not match local file paths containing IIIF qualifier as substring
     */
    @Test
    void isFileNameMatchesRegex_shouldNotMatchLocalFilePathsContainingIiifQualifierAsSubstring() {
        assertFalse(Utils.isFileNameMatchesRegex("file:///opt/digiverso/viewer/media/test_digiverso_tif_seb/02_color.tif",
                Indexer.IIIF_IMAGE_FILE_NAMES));
        assertFalse(Utils.isFileNameMatchesRegex("file:///opt/digiverso/viewer/media/foo/02_bitonal.jp2", Indexer.IIIF_IMAGE_FILE_NAMES));
        assertFalse(Utils.isFileNameMatchesRegex("file:///opt/digiverso/viewer/media/foo/page_default.png", Indexer.IIIF_IMAGE_FILE_NAMES));
        assertFalse(Utils.isFileNameMatchesRegex("file:///opt/digiverso/viewer/media/foo/somegray.tif", Indexer.IIIF_IMAGE_FILE_NAMES));
        assertFalse(Utils.isFileNameMatchesRegex("file:///opt/digiverso/viewer/media/foo/native.bak.tif", Indexer.IIIF_IMAGE_FILE_NAMES));
        // Must still recognize valid IIIF URLs whose final path segment is exactly the qualifier + extension
        assertTrue(Utils.isFileNameMatchesRegex(
                "https://example.com/iiif/2/AC05725455%2F00000001.tif/full/!400,400/0/color.tif", Indexer.IIIF_IMAGE_FILE_NAMES));
    }

    /**
     * @see Utils#adaptField(String,String)
     * @verifies apply prefix correctly
     */
    @Test
    void adaptField_shouldApplyPrefixCorrectly() {
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
    void adaptField_shouldNotApplyPrefixToRegularFieldsIfEmpty() {
        assertEquals("MD_FOO", Utils.adaptField("MD_FOO", ""));
    }

    /**
     * @see Utils#adaptField(String,String)
     * @verifies remove untokenized correctly
     */
    @Test
    void adaptField_shouldRemoveUntokenizedCorrectly() {
        assertEquals("SORT_FOO", Utils.adaptField("MD_FOO_UNTOKENIZED", "SORT_"));
    }

    /**
     * @see Utils#adaptField(String,String)
     * @verifies not apply facet prefix to calendar fields
     */
    @Test
    void adaptField_shouldNotApplyFacetPrefixToCalendarFields() {
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
    void sortifyField_shouldSortifyCorrectly() {
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
    void isValidURL_shouldReturnTrueIfUrlStartsWithHttp() {
        assertTrue(Utils.isValidURL("http://example.com"));
    }

    /**
     * @see Utils#isValidURL(String)
     * @verifies return true if url starts with https
     */
    @Test
    void isValidURL_shouldReturnTrueIfUrlStartsWithHttps() {
        assertTrue(Utils.isValidURL("https://example.com"));
    }

    /**
     * @see Utils#isValidURL(String)
     * @verifies return true if url starts with file
     */
    @Test
    void isValidURL_shouldReturnTrueIfUrlStartsWithFile() {
        assertTrue(Utils.isValidURL("file://opt/digiverso/indexer/foo.xml"));
    }

    /**
     * @see Utils#isValidURL(String)
     * @verifies return false if not url
     */
    @Test
    void isValidImageOrIiifURI_shouldReturnFalseIfNotUrl() {
        assertFalse(Utils.isValidURL("example.com/foo"));
    }
    
    /**
     * @see Utils#isValidImageOrIiifURI(String)
     * @verifies return true for image uri
     */
    @Test
    void isValidImageOrIiifURI_shouldReturnTrueForImageUri() {
        assertTrue(Utils.isValidImageOrIiifURI("https://example.com/other/image.jpg"));
        assertTrue(Utils.isValidImageOrIiifURI("https://example.com/other/image.png"));
        assertTrue(Utils.isValidImageOrIiifURI("https://example.com/other/image.tif"));
    }
    
    /**
     * @see Utils#isValidImageOrIiifURI(String)
     * @verifies return true for iiif uri
     */
    @Test
    void isValidImageOrIiifURI_shouldReturnTrueForIiifUri() {
        assertTrue(Utils.isValidImageOrIiifURI("https://iiif.server.org/iiif/identifier/full/full/0/default.jpg"));
    }
    
    /**
     * @see Utils#isValidImageOrIiifURI(String)
     * @verifies return false for other uris
     */
    @Test
    void isValidImageOrIiifURI_shouldReturnFalseForOtherUris() {
        assertFalse(Utils.isValidImageOrIiifURI("https//example.com/other/text.txt"));
    }

    /**
     * @see Utils#encodeIllegalUriChars(String)
     * @verifies encode space as %20
     */
    @Test
    void encodeIllegalUriChars_shouldEncodeSpaceAs20() {
        assertEquals(
                "https://oai.bibnet.lu/request?id=oai:alma.352LUX_BIBNET_NETWORK:99001623666%200107251",
                Utils.encodeIllegalUriChars(
                        "https://oai.bibnet.lu/request?id=oai:alma.352LUX_BIBNET_NETWORK:99001623666 0107251"));
    }

    /**
     * @see Utils#encodeIllegalUriChars(String)
     * @verifies encode non ascii characters
     */
    @Test
    void encodeIllegalUriChars_shouldEncodeNonAsciiCharacters() {
        assertEquals("https://example.com/path?q=%C3%A9clair",
                Utils.encodeIllegalUriChars("https://example.com/path?q=éclair"));
    }

    /**
     * @see Utils#encodeIllegalUriChars(String)
     * @verifies not encode already encoded sequences
     */
    @Test
    void encodeIllegalUriChars_shouldNotEncodeAlreadyEncodedSequences() {
        String url = "https://example.com/path?q=hello%20world";
        assertEquals(url, Utils.encodeIllegalUriChars(url));
    }

    /**
     * @see Utils#encodeIllegalUriChars(String)
     * @verifies not encode valid uri characters
     */
    @Test
    void encodeIllegalUriChars_shouldNotEncodeValidUriCharacters() {
        String url = "https://example.com/path?verb=GetRecord&metadataPrefix=marc21&identifier=oai:alma:123";
        assertEquals(url, Utils.encodeIllegalUriChars(url));
    }
}
