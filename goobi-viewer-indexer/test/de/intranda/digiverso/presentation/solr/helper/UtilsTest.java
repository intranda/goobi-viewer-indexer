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
package de.intranda.digiverso.presentation.solr.helper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.jdom2.Document;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

    /**
     * @see Utils#getDocumentFromString(String,String)
     * @verifies build document correctly
     */
    @Test
    public void getDocumentFromString_shouldBuildDocumentCorrectly() throws Exception {
        String xml = "<root><child>child1</child><child>child2</child></root>";
        Document doc = Utils.getDocumentFromString(xml, null);
        Assert.assertNotNull(doc);
        Assert.assertNotNull(doc.getRootElement());
        Assert.assertEquals("root", doc.getRootElement().getName());
        Assert.assertNotNull(doc.getRootElement().getChildren("child"));
        Assert.assertEquals(2, doc.getRootElement().getChildren("child").size());
    }

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
                Path path = Utils.getCollisionFreeDataFilePath("build", "filename", "#", ".xml");
                Assert.assertNotNull(path);
                Assert.assertEquals("filename.xml", path.getFileName().toString());
                Files.createFile(path);
                Assert.assertTrue(Files.exists(path));
                paths.add(path);
            }
            {
                // filename#1.xml
                Path path = Utils.getCollisionFreeDataFilePath("build", "filename", "#", ".xml");
                Assert.assertNotNull(path);
                Assert.assertEquals("filename#0.xml", path.getFileName().toString());
                Files.createFile(path);
                Assert.assertTrue(Files.exists(path));
                paths.add(path);
            }
            {
                // filename#2.xml
                Path path = Utils.getCollisionFreeDataFilePath("build", "filename", "#", ".xml");
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
}