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
package io.goobi.viewer.indexer;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.jdom2.Document;
import org.jdom2.Element;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.helper.Hotfolder;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.IndexingResult;
import io.goobi.viewer.indexer.model.IndexingResult.IndexingResultStatus;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.writestrategy.ISolrWriteStrategy;

class EntityXmlIndexerTest extends AbstractSolrEnabledTest {

    /** Record with all person fields */
    private static final String PI_FULL = "p0123456789";
    /** Typical delivery record */
    private static final String PI_TYPICAL = "p0123456790";
    /** Record with a personal name only */
    private static final String PI_PERSONALNAME = "p0123456791";

    private File entityXmlFile;

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());

        entityXmlFile = new File("src/test/resources/entityXML/entityxml_persons.xml");
        Assertions.assertTrue(entityXmlFile.isFile());
    }

    /**
     * @see EntityXmlIndexer#EntityXmlIndexer(Hotfolder)
     * @verifies set attributes correctly
     */
    @Test
    void EntityXmlIndexer_shouldSetAttributesCorrectly() throws Exception {
        EntityXmlIndexer indexer = new EntityXmlIndexer(hotfolder);
        Assertions.assertEquals(hotfolder, indexer.hotfolder);
        Assertions.assertEquals(FileFormat.ENTITYXML, indexer.getSourceDocFormat());
    }

    /**
     * @see EntityXmlIndexer#addToIndex(Path,Map)
     * @verifies throw IllegalArgumentException if entityXmlFile null
     */
    @Test
    void addToIndex_shouldThrowIllegalArgumentExceptionIfEntityXmlFileNull() {
        EntityXmlIndexer indexer = new EntityXmlIndexer(hotfolder);
        Map<String, Boolean> map = Collections.emptyMap();
        Assertions.assertThrows(IllegalArgumentException.class, () -> indexer.addToIndex(null, map));
    }

    /**
     * @see EntityXmlIndexer#normalizeWhitespace(Document)
     * @verifies collapse line breaks and indentation in text values
     */
    @Test
    void normalizeWhitespace_shouldCollapseLineBreaksAndIndentationInTextValues() throws Exception {
        Document doc = JDomXP.splitEntityXmlFile(entityXmlFile).get(1);
        EntityXmlIndexer.normalizeWhitespace(doc);
        Element elePlace = doc.getRootElement()
                .getChild("placeOfActivity", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("gndo"));
        Assertions.assertNotNull(elePlace);
        Assertions.assertEquals("Frankfurt am Main", elePlace.getText());
    }

    /**
     * @see EntityXmlIndexer#index(Document,Map,ISolrWriteStrategy)
     * @verifies index record correctly
     */
    @Test
    void index_shouldIndexRecordCorrectly() throws Exception {
        indexAll();

        // Typical delivery record
        {
            SolrDocument doc = getTopDoc(PI_TYPICAL);
            Assertions.assertEquals(FileFormat.ENTITYXML.name(), doc.getFieldValue(SolrConstants.SOURCEDOCFORMAT));
            Assertions.assertEquals("Person", doc.getFieldValue(SolrConstants.DOCSTRCT));
            Assertions.assertEquals(true, doc.getFieldValue(SolrConstants.ISWORK));
            Assertions.assertEquals("Beispiel, Anna Maria", doc.getFieldValue(SolrConstants.LABEL));
            Assertions.assertEquals("Beispiel, Anna Maria", doc.getFirstValue("MD_TITLE"));
            Assertions.assertEquals("1882-03-20", doc.getFirstValue("MD_DATEOFBIRTH"));
            Assertions.assertEquals("1883-03-20", doc.getFirstValue("MD_DATEOFBIRTH_ALTERNATIVE"));
            Assertions.assertEquals("ZZ", doc.getFirstValue("MD_GEOGRAPHICAREACODE"));
            Assertions.assertNotNull(doc.getFieldValue(SolrConstants.DATECREATED));
            Assertions.assertNotNull(doc.getFieldValues(SolrConstants.ACCESSCONDITION));
            assertFieldValues(doc, "MD_VARIANTNAME", "Beispiel, Mia", "Probe, Anna Maria");
            assertFieldValues(doc, "MD_PLACEOFACTIVITY", "Frankfurt am Main");
            assertFieldValues(doc, "MD_OCCUPATION", "Kaufmann");
            assertFieldValues(doc, "MD_EVENTDATE", "1941-02-26");

            // Grouped metadata
            SolrDocument eventDoc = getGroupedDoc(doc, "MD_EVENTDATE");
            Assertions.assertEquals("1941-02-26", eventDoc.getFirstValue("MD_DATE"));
            Assertions.assertEquals("Ausbürgerung", eventDoc.getFirstValue("MD_NOTE"));
            Assertions.assertEquals("datz", eventDoc.getFirstValue("MD_CODE"));
        }

        // Personal name only
        {
            SolrDocument doc = getTopDoc(PI_PERSONALNAME);
            Assertions.assertEquals("Maria Wilhelmine", doc.getFieldValue(SolrConstants.LABEL));
            Assertions.assertEquals("1901", doc.getFirstValue("MD_DATEOFBIRTH"));
        }

        // Record with all fields
        {
            SolrDocument doc = getTopDoc(PI_FULL);
            Assertions.assertEquals("Mustermann, Max", doc.getFieldValue(SolrConstants.LABEL));
            Assertions.assertEquals("https://d-nb.info/gnd/0000000001", doc.getFirstValue("MD_GNDURI"));
            Assertions.assertEquals("1595-08-06", doc.getFirstValue("MD_DATEOFDEATH"));
            Assertions.assertEquals("1596-08-06", doc.getFirstValue("MD_DATEOFDEATH_ALTERNATIVE"));
            Assertions.assertEquals("XA-DE", doc.getFirstValue("MD_GEOGRAPHICAREACODE"));
            assertFieldValues(doc, "MD_PREFERREDNAME", "Mustermann, Max", "مرزوقي, احمد");
            assertFieldValues(doc, "MD_VARIANTNAME", "Musterjunge, Maximilian von III.", "Mustermann");
            assertFieldValues(doc, "MD_PLACEOFBIRTH", "Muster- dorf an der Leine");
            assertFieldValues(doc, "MD_PERIODOFACTIVITY", "1520-1590");
            assertFieldValues(doc, "MD_PLAYEDINSTRUMENT", "Countertenor");
            assertFieldValues(doc, "MD_PUBLICATION", "Paradoxien der Musterdemokratie", "Werk ohne Autor");
            assertFieldValues(doc, "MD_RELATEDPERSON", "Musterfrau, Erika", "Schmidt, Luise", "Lehrer von: Moritz Musterknabe");
            assertFieldValues(doc, "MD_SOURCE", "Homepage");
            assertFieldValues(doc, "MD_PSEUDONYM", "Moritz Musterknabe");
            assertFieldValues(doc, "MD_HOMEPAGE", "https://example.com/mustermann");
            assertFieldValues(doc, "MD_SAMEAS", "https://example.com/viaf/123");
            assertFieldValues(doc, "MD_SEX", "männlich");
            assertFieldValues(doc, "MD_OCCUPATION", "Hochschullehrer", "Gummibärchentester");
            assertFieldValues(doc, "MD_BIOGRAPHICALINFORMATION", "Max Mustermann ist ein Dummy, um alles zu testen.");
            Assertions.assertEquals("significant", getGroupedDoc(doc, "MD_OCCUPATION", "Hochschullehrer").getFirstValue("MD_TYPE"));
            Assertions.assertNull(doc.getFieldValue("MD_FUNCTIONORROLE"));

            SolrDocument relDoc = getGroupedDoc(doc, "MD_RELATEDPERSON", "Schmidt, Luise");
            Assertions.assertEquals("familialRelationship", relDoc.getFirstValue("MD_RELATIONSHIPTYPE"));
            Assertions.assertEquals("Mutter", relDoc.getFirstValue("MD_NOTE"));

            SolrDocument variantDoc = getGroupedDoc(doc, "MD_VARIANTNAME", "Musterjunge, Maximilian von III.");
            Assertions.assertEquals("Graf von Musterhausen", variantDoc.getFirstValue("MD_NAMEADDITION"));

            SolrDocument sourceDoc = getGroupedDoc(doc, "MD_SOURCE", "Homepage");
            Assertions.assertEquals("https://example.com/source", sourceDoc.getFirstValue("MD_URL"));
        }
    }

    /**
     * @see EntityXmlIndexer#index(Document,Map,ISolrWriteStrategy)
     * @verifies not index internal notes
     */
    @Test
    void index_shouldNotIndexInternalNotes() throws Exception {
        indexAll();

        SolrDocument doc = getTopDoc(PI_FULL);
        assertFieldValues(doc, "MD_NOTE", "Testdatensatz mit allen Feldern.");
        for (Object value : doc.getFieldValues(SolrConstants.DEFAULT)) {
            Assertions.assertFalse(String.valueOf(value).contains("Geheime"));
        }
    }

    /**
     * @see EntityXmlIndexer#index(Document,Map,ISolrWriteStrategy)
     * @verifies update record correctly
     */
    @Test
    void index_shouldUpdateRecordCorrectly() throws Exception {
        indexAll();
        SolrDocument doc = getTopDoc(PI_TYPICAL);
        Object dateCreated = doc.getFieldValue(SolrConstants.DATECREATED);
        Assertions.assertNotNull(dateCreated);
        Assertions.assertEquals(1, doc.getFieldValues(SolrConstants.DATEUPDATED).size());

        indexAll();
        doc = getTopDoc(PI_TYPICAL);
        Assertions.assertEquals(dateCreated, doc.getFieldValue(SolrConstants.DATECREATED));
        Assertions.assertEquals(2, doc.getFieldValues(SolrConstants.DATEUPDATED).size());
        // Grouped docs of the previous instance must have been replaced, not duplicated
        Assertions.assertEquals(2, getGroupedDocs(doc, "MD_VARIANTNAME").size());
    }

    /**
     * Indexes all records from the test file.
     *
     * @throws Exception
     */
    private void indexAll() throws Exception {
        List<Document> recordDocs = JDomXP.splitEntityXmlFile(entityXmlFile);
        Assertions.assertEquals(3, recordDocs.size());
        Map<String, Path> dataFolders = new HashMap<>();
        for (Document recordDoc : recordDocs) {
            IndexingResult result = new EntityXmlIndexer(hotfolder).index(recordDoc, dataFolders, null);
            Assertions.assertEquals(IndexingResultStatus.OK, result.getStatus(), result.getError());
        }
    }

    private static SolrDocument getTopDoc(String pi) throws Exception {
        SolrDocumentList docList = SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search(SolrConstants.PI + ":" + pi + " AND " + SolrConstants.ISWORK + ":true", null);
        Assertions.assertEquals(1, docList.size());
        return docList.get(0);
    }

    private static SolrDocumentList getGroupedDocs(SolrDocument topDoc, String label) throws Exception {
        return SolrIndexerDaemon.getInstance()
                .getSearchIndex()
                .search(SolrConstants.DOCTYPE + ":" + DocType.METADATA.name() + " AND " + SolrConstants.IDDOC_OWNER + ":"
                        + topDoc.getFieldValue(SolrConstants.IDDOC) + " AND " + SolrConstants.LABEL + ":" + label, null);
    }

    private static SolrDocument getGroupedDoc(SolrDocument topDoc, String label) throws Exception {
        SolrDocumentList docList = getGroupedDocs(topDoc, label);
        Assertions.assertEquals(1, docList.size(), label);
        return docList.get(0);
    }

    private static SolrDocument getGroupedDoc(SolrDocument topDoc, String label, String value) throws Exception {
        for (SolrDocument doc : getGroupedDocs(topDoc, label)) {
            if (value.equals(doc.getFirstValue("MD_VALUE"))) {
                return doc;
            }
        }
        Assertions.fail("No grouped " + label + " doc with value: " + value);
        return null;
    }

    private static void assertFieldValues(SolrDocument doc, String field, String... expected) {
        Collection<Object> values = doc.getFieldValues(field);
        Assertions.assertNotNull(values, field);
        for (String exp : expected) {
            Assertions.assertTrue(values.contains(exp), field + " does not contain '" + exp + "': " + values);
        }
        Assertions.assertEquals(expected.length, values.size(), field + ": " + values);
    }
}
