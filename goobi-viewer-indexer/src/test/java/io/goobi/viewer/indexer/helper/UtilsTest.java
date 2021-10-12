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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.Indexer;
import io.goobi.viewer.indexer.helper.Utils;

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
                Assert.assertNotNull(path);
                Assert.assertEquals("filename.xml", path.getFileName().toString());
                Files.createFile(path);
                Assert.assertTrue(Files.exists(path));
                paths.add(path);
            }
            {
                // filename#1.xml
                Path path = Utils.getCollisionFreeDataFilePath("target", "filename", "#", ".xml");
                Assert.assertNotNull(path);
                Assert.assertEquals("filename#0.xml", path.getFileName().toString());
                Files.createFile(path);
                Assert.assertTrue(Files.exists(path));
                paths.add(path);
            }
            {
                // filename#2.xml
                Path path = Utils.getCollisionFreeDataFilePath("target", "filename", "#", ".xml");
                Assert.assertNotNull(path);
                Assert.assertEquals("filename#1.xml", path.getFileName().toString());
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
        Assert.assertEquals("PPN123", Utils.extractPiFromFileName(Paths.get("PPN123#0.delete")));
        Assert.assertEquals("PPN123", Utils.extractPiFromFileName(Paths.get("PPN123#0.purge")));
        Assert.assertEquals("PPN123", Utils.extractPiFromFileName(Paths.get("PPN123.UPDATED")));
        Assert.assertEquals("PPN123", Utils.extractPiFromFileName(Paths.get("PPN123#0.UPDATED")));
    }

    /**
     * @see Utils#getFileNameFromIiifUrl(String)
     * @verifies extract file name correctly
     */
    @Test
    public void getFileNameFromIiifUrl_shouldExtractFileNameCorrectly() throws Exception {
        Assert.assertEquals("00000001.jpg",
                Utils.getFileNameFromIiifUrl("http://localhost:8080/viewer/rest/image/AC05725455/00000001.tif/full/!400,400/0/default.jpg"));
        Assert.assertEquals("AFE_1284_1999-17-557-1_a.jpg", Utils
                .getFileNameFromIiifUrl("http://pecunia2.zaw.uni-heidelberg.de:49200/iiif/2/AFE_1284_1999-17-557-1_a.jpg/full/full/0/default.jpg"));
    }

    /**
     * @see Utils#generateLongOrderNumber(int,int)
     * @verifies construct number correctly
     */
    @Test
    public void generateLongOrderNumber_shouldConstructNumberCorrectly() throws Exception {
        Assert.assertEquals(100000001, Utils.generateLongOrderNumber(1, 1));
        Assert.assertEquals(110000010, Utils.generateLongOrderNumber(11, 10));
        Assert.assertEquals(111000100, Utils.generateLongOrderNumber(111, 100));
        Assert.assertEquals(111101000, Utils.generateLongOrderNumber(1111, 1000));
        Assert.assertEquals(111101000, Utils.generateLongOrderNumber(1111, 1000));
        Assert.assertEquals(111111000, Utils.generateLongOrderNumber(11111, 1000));
    }

    /**
     * @see Utils#isFileNameMatchesRegex(String,String[])
     * @verifies match correctly
     */
    @Test
    public void isFileNameMatchesRegex_shouldMatchCorrectly() throws Exception {
        Assert.assertTrue(Utils.isFileNameMatchesRegex("foo/bar/default.jpg", Indexer.IIIF_IMAGE_FILE_NAMES));
        Assert.assertTrue(Utils.isFileNameMatchesRegex("foo/bar/color.png", Indexer.IIIF_IMAGE_FILE_NAMES));
        Assert.assertFalse(Utils.isFileNameMatchesRegex("foo/bar/other.jpg", Indexer.IIIF_IMAGE_FILE_NAMES));
    }
}
