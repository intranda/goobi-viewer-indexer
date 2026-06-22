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
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.jdom2.JDOMException;

import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

/**
 * Augments a page Solr document with image dimension data (WIDTH/HEIGHT) gathered from MIX technical metadata and, as a fallback, from the image
 * file's EXIF data. These dimensions are unrelated to full-text and are extracted independently of full-text indexing.
 */
public final class ImageDimensionsHelper {

    private static final Logger logger = LogManager.getLogger(ImageDimensionsHelper.class);

    private ImageDimensionsHelper() {
        // utility class
    }

    /**
     * Adds image dimension fields (WIDTH/HEIGHT) to the given page document, first from MIX technical metadata, then - if still missing - from the
     * image file's EXIF data.
     *
     * @param doc Page Solr input document
     * @param dataFolders Folder paths containing the page's data files
     * @should add width and height from mix
     * @should add width and height from exif
     */
    public static void addImageDimensions(SolrInputDocument doc, Map<String, Path> dataFolders) {
        if (doc == null || dataFolders == null) {
            return;
        }

        String baseFileName = FilenameUtils.getBaseName((String) doc.getFieldValue(SolrConstants.FILENAME));
        // If main image file is a IIIF URL or anything with no unique file name, look for alternatives
        if (!isBaseFileNameUsable(baseFileName)) {
            if (doc.getFieldValue("FILENAME_JPEG") != null) {
                baseFileName = FilenameUtils.getBaseName((String) doc.getFieldValue("FILENAME_JPEG"));
            } else if (doc.getFieldValue("FILENAME_TIFF") != null) {
                baseFileName = FilenameUtils.getBaseName((String) doc.getFieldValue("FILENAME_TIFF"));
            }
        }

        // WIDTH/HEIGHT from MIX
        if (dataFolders.get(DataRepository.PARAM_MIX) != null && isBaseFileNameUsable(baseFileName)) {
            try {
                Map<String, String> mixData = TextHelper
                        .readMix(new File(dataFolders.get(DataRepository.PARAM_MIX).toAbsolutePath().toString(),
                                baseFileName + FileTools.XML_EXTENSION));
                for (Entry<String, String> entry : mixData.entrySet()) {
                    if (!(entry.getKey().equals(SolrConstants.WIDTH) && doc.getField(SolrConstants.WIDTH) != null)
                            && !(entry.getKey().equals(SolrConstants.HEIGHT) && doc.getField(SolrConstants.HEIGHT) != null)) {
                        doc.addField(entry.getKey(), entry.getValue());
                    }
                }
            } catch (JDOMException e) {
                logger.error(e.getMessage(), e);
            } catch (IOException e) {
                logger.warn(e.getMessage());
            }
        }

        // Add image dimension values from EXIF
        if (!doc.containsKey(SolrConstants.WIDTH) || !doc.containsKey(SolrConstants.HEIGHT)
                || ("0".equals(doc.getFieldValue(SolrConstants.WIDTH)) && "0".equals(doc.getFieldValue(SolrConstants.HEIGHT)))) {
            doc.removeField(SolrConstants.WIDTH);
            doc.removeField(SolrConstants.HEIGHT);
            ImageSizeReader.getSize(dataFolders.get(DataRepository.PARAM_MEDIA), (String) doc.getFieldValue(SolrConstants.FILENAME))
                    .ifPresent(dimension -> {
                        doc.addField(SolrConstants.WIDTH, dimension.width);
                        doc.addField(SolrConstants.HEIGHT, dimension.height);
                    });
        }
    }

    /**
     *
     * @param baseFileName
     * @return true if baseFileName is not one of the keywords; false otherwise
     */
    private static boolean isBaseFileNameUsable(String baseFileName) {
        return !("default".equals(baseFileName) || "info".equals(baseFileName) || "native".equals(baseFileName));
    }
}
