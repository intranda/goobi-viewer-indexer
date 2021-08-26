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

import org.junit.Assert;
import org.junit.Test;

import io.goobi.viewer.indexer.AbstractSolrEnabledTest;

public class HotfolderTest extends AbstractSolrEnabledTest {
    
    /**
     * @see Hotfolder#countRecordFiles()
     * @verifies count files correctly
     */
    @Test
    public void countRecordFiles_shouldCountFilesCorrectly() throws Exception {
        hotfolder = new Hotfolder("src/test/resources/indexerconfig_solr_test.xml", client);
        Configuration.getInstance().overrideValue("performance.countHotfolderFiles", true);
        Assert.assertTrue(Configuration.getInstance().isCountHotfolderFiles());
        
        {
            Path path = Paths.get("target/viewer/hotfolder", "1.xml");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "1A.XML");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "2.delete");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "3.purge");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "4.docupdate");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "5.UPDATED");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }

        // Uncounted files
        {
            Path path = Paths.get("target/viewer/hotfolder", "6.exe");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "7");
            Files.createDirectories(path);
            Assert.assertTrue(Files.isDirectory(path));
        }

        Assert.assertEquals(6, hotfolder.countRecordFiles());

    }
}