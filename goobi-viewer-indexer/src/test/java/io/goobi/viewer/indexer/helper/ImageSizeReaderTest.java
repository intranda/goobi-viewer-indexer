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
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.Indexer;

class ImageSizeReaderTest {

    /**
     * @see Indexer#getSize(Map,SolrInputDocument)
     * @verifies return size correctly
     */
    @Test
    void getSize_shouldReturnSizeCorrectly() throws Exception {

        String[] filenames = { "00000001.tif", "00225231.png", "test1.jp2" };
        Dimension[] imageSizes = { new Dimension(3192, 4790), new Dimension(2794, 3838), new Dimension(3448, 6499) };

        File dataFolder = new File("src/test/resources/image_size");

        int i = 0;
        File outputFolder = new File(dataFolder, "output");
        try {
            for (String filename : filenames) {
                if (outputFolder.isDirectory()) {
                    FileUtils.deleteDirectory(outputFolder);
                }
                outputFolder.mkdirs();

                Optional<Dimension> dim = ImageSizeReader.getSize(dataFolder.toPath(), filename);
                // jp2 image files cannot be read because of missing jp2 library
                assertTrue(dim.isPresent());
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
