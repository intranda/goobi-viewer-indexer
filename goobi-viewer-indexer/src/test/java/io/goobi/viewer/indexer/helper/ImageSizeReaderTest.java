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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.awt.Dimension;
import java.io.File;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractSolrEnabledTest;
import io.goobi.viewer.indexer.Indexer;

class ImageSizeReaderTest extends AbstractSolrEnabledTest {

    private static String libraryPath = "";

    @BeforeAll
    public static void setUpBeforeClass() {
        File libraryFile = new File("src/test/resources/lib/libopenjp2.so");
        libraryPath = System.getProperty("java.library.path");
        System.setProperty("java.library.path", libraryPath + ":" + libraryFile.getParentFile().getAbsolutePath());
    }

    @AfterAll
    public static void cleanUpAfterClass() {
        if (StringUtils.isNotBlank(libraryPath)) {
            System.setProperty("java.library.path", libraryPath);
        }
    }

    /**
     * @see Indexer#getSize(Map,SolrInputDocument)
     * @verifies return size correctly
     */
    @Test
    void getSize_shouldReturnSizeCorrectly() throws Exception {

        String[] filenames = { "00000001.tif", "00225231.png", "test1.jp2" };
        Dimension[] imageSizes = { new Dimension(3192, 4790), new Dimension(2794, 3838), new Dimension(3448, 6499) };

        File dataFolder = new File("src/test/resources/image_size").getAbsoluteFile();

        int i = 0;
        File outputFolder = new File(dataFolder, "output").getAbsoluteFile();
        try {
            for (String filename : filenames) {
                if (outputFolder.isDirectory()) {
                    FileUtils.deleteDirectory(outputFolder);
                }
                outputFolder.mkdirs();
                File file = new File(dataFolder, filename);
                assertTrue(file.isFile(), "Test image file " + file + " does not exist");
                Optional<Dimension> dim = ImageSizeReader.getSize(dataFolder.toPath(), filename);
                // jp2 image files cannot be read because of missing jp2 library
                assertTrue(dim.isPresent(), "Failed to retrieve size for " + dataFolder.toPath() + "/" + filename);
                assertEquals(imageSizes[i], dim.get(), "Image size of " + filename + " is " + dim + ", but should be " + imageSizes[i]);
                i++;
            }
        } finally {
            if (outputFolder.isDirectory()) {
                FileUtils.deleteDirectory(outputFolder);
            }
        }
    }

}
