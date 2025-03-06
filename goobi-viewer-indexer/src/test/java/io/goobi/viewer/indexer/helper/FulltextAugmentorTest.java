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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.Indexer;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;

class FulltextAugmentorTest {

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add filename for converted alto files
     */
    @Test
    void addIndexFieldsFromAltoData_shouldAddFilenameForConvertedAltoFile() throws Exception {
        DataRepository repository = new DataRepository("build/viewer", true);
        FulltextAugmentor augmentor = new FulltextAugmentor(repository);
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ABBYY, Paths.get("src/test/resources/ABBYYXML"));
        assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ABBYY)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File abbyyfile = new File(dataFolders.get(DataRepository.PARAM_ABBYY).toAbsolutePath().toString(), "00000001.xml");
        Map<String, Object> altoData = TextHelper.readAbbyyToAlto(abbyyfile);

        assertTrue(
                augmentor.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO_CONVERTED, "PPN123", "00000001", 1, true));
        assertEquals("alto/PPN123/00000001.xml", doc.getFieldValue(SolrConstants.FILENAME_ALTO));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies return false if altodata null
     */
    @Test
    void addIndexFieldsFromAltoData_shouldReturnFalseIfAltodataNull() throws Exception {

        FulltextAugmentor augmentor = new FulltextAugmentor(null);
        Assertions.assertFalse(
                augmentor.addIndexFieldsFromAltoData(new SolrInputDocument(new HashMap<>()), null, Collections.emptyMap(),
                        DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies throw IllegalArgumentException if doc null
     */
    @Test
    void addIndexFieldsFromAltoData_shouldThrowIllegalArgumentExceptionIfDocNull() {
        FulltextAugmentor augmentor = new FulltextAugmentor(null);
        Map<String, Object> altoData = Collections.emptyMap();
        Map<String, Path> dataFolders = Collections.emptyMap();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> augmentor
                        .addIndexFieldsFromAltoData(null, altoData, dataFolders, DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies throw IllegalArgumentException if dataFolders null
     */
    @Test
    void addIndexFieldsFromAltoData_shouldThrowIllegalArgumentExceptionIfDataFoldersNull() {
        FulltextAugmentor augmentor = new FulltextAugmentor(null);
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        Map<String, Object> altoData = Collections.emptyMap();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> augmentor.addIndexFieldsFromAltoData(doc, altoData, null, DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies throw IllegalArgumentException if pi null
     */
    @Test
    void addIndexFieldsFromAltoData_shouldThrowIllegalArgumentExceptionIfPiNull() {
        FulltextAugmentor augmentor = new FulltextAugmentor(null);
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        Map<String, Object> altoData = Collections.emptyMap();
        Map<String, Path> dataFolders = Collections.emptyMap();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> augmentor.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, null, "00000010", 10, false));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies throw IllegalArgumentException if baseFileName null
     */
    @Test
    void addIndexFieldsFromAltoData_shouldThrowIllegalArgumentExceptionIfBaseFileNameNull() {
        FulltextAugmentor augmentor = new FulltextAugmentor(null);
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        Map<String, Object> altoData = Collections.emptyMap();
        Map<String, Path> dataFolders = Collections.emptyMap();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> augmentor.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, "PPN123", null, 10, false));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add filename for native alto file
     */
    @Test
    void addIndexFieldsFromAltoData_shouldAddFilenameForNativeAltoFile() throws Exception {
        FulltextAugmentor augmentor = new FulltextAugmentor(new DataRepository("src/test/resources", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ALTO, Paths.get("src/test/resources/ALTO/"));
        assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTO)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File altoFile = new File(dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), "00000010.xml");
        Map<String, Object> altoData = TextHelper.readAltoFile(altoFile);

        assertTrue(augmentor.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
        assertEquals("alto/PPN123/00000010.xml", doc.getFieldValue(SolrConstants.FILENAME_ALTO));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add filename for crowdsourcing alto file
     */
    @Test
    void addIndexFieldsFromAltoData_shouldAddFilenameForCrowdsourcingAltoFile() throws Exception {
        FulltextAugmentor augmentor = new FulltextAugmentor(new DataRepository("src/test/resources", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ALTOCROWD, Paths.get("src/test/resources/ALTO/"));
        assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTOCROWD)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File altoFile = new File(dataFolders.get(DataRepository.PARAM_ALTOCROWD).toAbsolutePath().toString(), "00000010.xml");
        Map<String, Object> altoData = TextHelper.readAltoFile(altoFile);

        assertTrue(augmentor.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTOCROWD, "PPN123", "00000010", 10, false));
        assertEquals("alto_crowd/PPN123/00000010.xml", doc.getFieldValue(SolrConstants.FILENAME_ALTO));
    }

    /**
     * @see Indexer#addIndexFieldsFromAltoData(SolrInputDocument,Map,Map,String,String,String,int,boolean)
     * @verifies add fulltext
     */
    @Test
    void addIndexFieldsFromAltoData_shouldAddFulltext() throws Exception {
        FulltextAugmentor augmentor = new FulltextAugmentor(new DataRepository("src/test/resources", true));
        Map<String, Path> dataFolders = new HashMap<>();
        dataFolders.put(DataRepository.PARAM_ALTO, Paths.get("src/test/resources/ALTO/"));
        assertTrue(Files.isDirectory(dataFolders.get(DataRepository.PARAM_ALTO)));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());
        File altoFile = new File(dataFolders.get(DataRepository.PARAM_ALTO).toAbsolutePath().toString(), "00000010.xml");
        Map<String, Object> altoData = TextHelper.readAltoFile(altoFile);

        assertTrue(augmentor.addIndexFieldsFromAltoData(doc, altoData, dataFolders, DataRepository.PARAM_ALTO, "PPN123", "00000010", 10, false));
        assertNotNull(doc.getFieldValue(SolrConstants.FULLTEXT));
    }

    /**
     * @see Indexer#addNamedEntitiesFields(Map,SolrInputDocument)
     * @verifies add field
     */
    @Test
    void addNamedEntitiesFields_shouldAddField() {
        Map<String, Object> altoData = new HashMap<>(1);
        altoData.put(SolrConstants.NAMEDENTITIES, Collections.singletonList("LOCATION###Göttingen###https://www.geonames.org/2918632"));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());

        FulltextAugmentor.addNamedEntitiesFields(altoData, doc);
        assertEquals("Göttingen", doc.getFieldValue("NE_LOCATION"));
    }

    /**
     * @see Indexer#addNamedEntitiesFields(Map,SolrInputDocument)
     * @verifies add untokenized field
     */
    @Test
    void addNamedEntitiesFields_shouldAddUntokenizedField() {
        Map<String, Object> altoData = new HashMap<>(1);
        altoData.put(SolrConstants.NAMEDENTITIES, Collections.singletonList("LOCATION###Göttingen###https://www.geonames.org/2918632"));
        SolrInputDocument doc = new SolrInputDocument(new HashMap<>());

        FulltextAugmentor.addNamedEntitiesFields(altoData, doc);
        assertEquals("Göttingen", doc.getFieldValue("NE_LOCATION_UNTOKENIZED"));
    }

    /**
     * @see Indexer#cleanUpNamedEntityValue(String)
     * @verifies clean up value correctly
     */
    @Test
    void cleanUpNamedEntityValue_shouldCleanUpValueCorrectly() {
        assertEquals("abcd", FulltextAugmentor.cleanUpNamedEntityValue("\"(abcd,\""));
    }

    /**
     * @see Indexer#cleanUpNamedEntityValue(String)
     * @verifies throw IllegalArgumentException given null
     */
    @Test
    void cleanUpNamedEntityValue_shouldThrowIllegalArgumentExceptionGivenNull() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> FulltextAugmentor.cleanUpNamedEntityValue(null));
    }

}
