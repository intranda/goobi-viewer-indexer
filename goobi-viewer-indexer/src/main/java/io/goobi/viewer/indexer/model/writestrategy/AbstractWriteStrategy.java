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
package io.goobi.viewer.indexer.model.writestrategy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

/**
 * <p>
 * Abstract AbstractWriteStrategy class.
 * </p>
 *
 */
public abstract class AbstractWriteStrategy implements ISolrWriteStrategy {

    private static final Logger logger = LoggerFactory.getLogger(AbstractWriteStrategy.class);

    /**
     * 
     * @param sourceFile File containing the record(s)
     * @param dataFolders Data folder map
     * @param hotfolder active Hotfolder instance
     * @return An appropriate write strategy instance
     * @throws IOException
     */
    public static ISolrWriteStrategy create(Path sourceFile, Map<String, Path> dataFolders, Hotfolder hotfolder) throws IOException {
        boolean useSerializingStrategy = false;
        long size = sourceFile != null ? Files.size(sourceFile) : 0;
        if (size >= hotfolder.metsFileSizeThreshold) {
            useSerializingStrategy = true;
            logger.info("Source file '{}' is {} bytes, using a slower Solr write strategy to avoid memory overflows.", sourceFile.getFileName(),
                    size);
        } else {
            for (String key : dataFolders.keySet()) {
                switch (key) {
                    case DataRepository.PARAM_ALTO:
                    case DataRepository.PARAM_ALTOCROWD:
                    case DataRepository.PARAM_FULLTEXT:
                    case DataRepository.PARAM_FULLTEXTCROWD:
                    case DataRepository.PARAM_ABBYY:
                    case DataRepository.PARAM_TEIWC:
                        Path dataFolder = dataFolders.get(key);
                        if (dataFolder != null) {
                            // Files.size() does not work with directories, so use FileUtils
                            long dataFolderSize = FileUtils.sizeOfDirectory(dataFolder.toFile());
                            if (dataFolderSize >= hotfolder.dataFolderSizeThreshold) {
                                useSerializingStrategy = true;
                                logger.info("Data folder '{}' is {} bytes, using a slower Solr write strategy to avoid memory overflows.",
                                        dataFolder.toAbsolutePath().toString(), dataFolderSize);
                                break;
                            }
                        }
                        break;
                    default:
                        // do nothing
                }
            }
        }

        if (useSerializingStrategy) {
            return new SerializingSolrWriteStrategy(hotfolder.getSearchIndex(), hotfolder.getTempFolder());
        }

        return new LazySolrWriteStrategy(hotfolder.getSearchIndex());
    }

    /**
     * Adds OPENACCESS access condition field value to the give doc, if it has none.
     * 
     * @param doc
     */
    void checkAndAddAccessCondition(SolrInputDocument doc) {
        if (!doc.containsKey(SolrConstants.ACCESSCONDITION)) {
            doc.addField(SolrConstants.ACCESSCONDITION, SolrConstants.OPEN_ACCESS_VALUE);
        }
    }
}
