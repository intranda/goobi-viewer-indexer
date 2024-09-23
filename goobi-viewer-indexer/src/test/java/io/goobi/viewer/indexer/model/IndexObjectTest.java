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
package io.goobi.viewer.indexer.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;

class IndexObjectTest extends AbstractTest {

    /**
     * @see IndexObject#IndexObject(long,String)
     * @verifies set attributes correctly
     */
    @Test
    void IndexObject_shouldSetAttributesCorrectly() {
        IndexObject io = new IndexObject("123");
        assertEquals("123", io.getIddoc());
    }

    /**
     * @see IndexObject#getLuceneFieldsWithName(String)
     * @verifies return all fields with given name
     */
    @Test
    void getLuceneFieldsWithName_shouldReturnAllFieldsWithGivenName() {
        IndexObject io = new IndexObject("1");
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE11"));
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE12"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE21"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE22"));

        List<LuceneField> fields = io.getLuceneFieldsWithName("FIELD1");
        assertNotNull(fields);
        assertEquals(2, fields.size());
    }

    /**
     * @see IndexObject#getLuceneFieldsWithName(String)
     * @verifies return empty list if name not found
     */
    @Test
    void getLuceneFieldsWithName_shouldReturnEmptyListIfNameNotFound() {
        IndexObject io = new IndexObject("1");
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE11"));
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE12"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE21"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE22"));

        List<LuceneField> fields = io.getLuceneFieldsWithName("FIELD3");
        assertNotNull(fields);
        assertTrue(fields.isEmpty());
    }

    /**
     * @see IndexObject#getLuceneFieldWithName(String)
     * @verifies return first field with given name
     */
    @Test
    void getLuceneFieldWithName_shouldReturnFirstFieldWithGivenName() {
        IndexObject io = new IndexObject("1");
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE11"));
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE12"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE21"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE22"));

        LuceneField field = io.getLuceneFieldWithName("FIELD2");
        assertNotNull(field);
        assertEquals("VALUE21", field.getValue());
    }

    /**
     * @see IndexObject#getLuceneFieldWithName(String)
     * @verifies return null if name not found
     */
    @Test
    void getLuceneFieldWithName_shouldReturnNullIfNameNotFound() {
        IndexObject io = new IndexObject("1");
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE11"));
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE12"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE21"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE22"));

        LuceneField field = io.getLuceneFieldWithName("FIELD3");
        Assertions.assertNull(field);
    }

    /**
     * @see IndexObject#pushSimpleDataToLuceneArray()
     * @verifies write all required fields
     */
    @Test
    void pushSimpleDataToLuceneArray_shouldWriteAllRequiredFields() {
        IndexObject io = new IndexObject("1");
        io.setType("MusicSupplies");
        io.setPi("PI");
        io.setTopstructPI("TOPSTRUCT_PI");
        io.setParentPI("PARENT_PI");
        io.setLabel("&lt;b&gt;LABEL&lt;/b&gt;");
        io.setDmdid("DMD0000");
        io.setLogId("LOG0000");
        io.setDataRepository("DATA");

        IndexObject io2 = new IndexObject("2");
        io.setParent(io2);
        IndexObject io3 = new IndexObject("3");
        io2.setParent(io3);

        io.pushSimpleDataToLuceneArray();

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.IDDOC));
        assertEquals("1", io.getLuceneFieldWithName(SolrConstants.IDDOC).getValue());
        assertEquals("1", io.getLuceneFieldWithName(SolrConstants.GROUPFIELD).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.DOCTYPE));
        assertEquals(DocType.DOCSTRCT.name(), io.getLuceneFieldWithName(SolrConstants.DOCTYPE).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.PI));
        assertEquals("PI", io.getLuceneFieldWithName(SolrConstants.PI).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.PI_TOPSTRUCT));
        assertEquals("TOPSTRUCT_PI", io.getLuceneFieldWithName(SolrConstants.PI_TOPSTRUCT).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.PI_PARENT));
        assertEquals("PARENT_PI", io.getLuceneFieldWithName(SolrConstants.PI_PARENT).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.PI_ANCHOR));
        assertEquals("PARENT_PI", io.getLuceneFieldWithName(SolrConstants.PI_ANCHOR).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.LABEL));
        assertEquals("<b>LABEL</b>", io.getLuceneFieldWithName(SolrConstants.LABEL).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.DMDID));
        assertEquals("DMD0000", io.getLuceneFieldWithName(SolrConstants.DMDID).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.LOGID));
        assertEquals("LOG0000", io.getLuceneFieldWithName(SolrConstants.LOGID).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.DOCSTRCT));
        assertEquals("MusicSupplies", io.getLuceneFieldWithName(SolrConstants.DOCSTRCT).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.DOCSTRCT_ALT));
        assertEquals("MusicSupplies_ALT", io.getLuceneFieldWithName(SolrConstants.DOCSTRCT_ALT).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP));
        assertEquals("MusicSupplies", io.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.DATAREPOSITORY));
        assertEquals("DATA", io.getLuceneFieldWithName(SolrConstants.DATAREPOSITORY).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.IDDOC_PARENT));
        assertEquals("2", io.getLuceneFieldWithName(SolrConstants.IDDOC_PARENT).getValue());

        assertNotNull(io.getLuceneFieldWithName(SolrConstants.IDDOC_TOPSTRUCT));
        assertEquals("3", io.getLuceneFieldWithName(SolrConstants.IDDOC_TOPSTRUCT).getValue());
    }

    /**
     * @see IndexObject#writeAccessConditions(IndexObject)
     * @verifies inherit access conditions from parent except OPENACCESS
     */
    @Test
    void writeAccessConditions_shouldInheritAccessConditionsFromParentExceptOPENACCESS() {
        IndexObject io = new IndexObject("1");

        IndexObject pio = new IndexObject("2");
        pio.getAccessConditions().add(SolrConstants.OPEN_ACCESS_VALUE);
        pio.getAccessConditions().add("CONDITION3");
        pio.getAccessConditions().add("CONDITION4");

        io.writeAccessConditions(pio);

        List<LuceneField> fields = io.getLuceneFieldsWithName(SolrConstants.ACCESSCONDITION);
        assertNotNull(fields);
        assertEquals(2, fields.size());
        assertEquals("CONDITION3", fields.get(0).getValue());
        assertEquals("CONDITION4", fields.get(1).getValue());
    }

    /**
     * @see IndexObject#writeAccessConditions(IndexObject)
     * @verifies not inherit access conditions from parent if own access conditions exist
     */
    @Test
    void writeAccessConditions_shouldNotInheritAccessConditionsFromParentIfOwnAccessConditionsExist() {
        IndexObject io = new IndexObject("1");
        io.getAccessConditions().add("CONDITION1");
        io.getAccessConditions().add("CONDITION2");

        IndexObject pio = new IndexObject("2");
        pio.getAccessConditions().add("CONDITION3");
        pio.getAccessConditions().add("CONDITION4");

        io.writeAccessConditions(pio);

        List<LuceneField> fields = io.getLuceneFieldsWithName(SolrConstants.ACCESSCONDITION);
        assertNotNull(fields);
        assertEquals(2, fields.size());
        assertEquals("CONDITION1", fields.get(0).getValue());
        assertEquals("CONDITION2", fields.get(1).getValue());
    }

    /**
     * @see IndexObject#writeAccessConditions()
     * @verifies add OPENACCESS if list empty
     */
    @Test
    void writeAccessConditions_shouldAddOPENACCESSIfListEmpty() {
        IndexObject io = new IndexObject("1");

        io.writeAccessConditions(null);

        List<LuceneField> fields = io.getLuceneFieldsWithName(SolrConstants.ACCESSCONDITION);
        assertNotNull(fields);
        assertEquals(1, fields.size());
        assertEquals(SolrConstants.OPEN_ACCESS_VALUE, fields.get(0).getValue());
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies set DATECREATED if not set
     */
    @Test
    void writeDateModified_shouldSetDATECREATEDIfNotSet() throws Exception {
        IndexObject io = new IndexObject("1");

        io.writeDateModified(false);

        assertTrue(io.getDateCreated() > 0);
        LuceneField fieldDateCreated = io.getLuceneFieldWithName(SolrConstants.DATECREATED);
        assertNotNull(fieldDateCreated);
        assertEquals(io.getDateCreated(), Long.parseLong(fieldDateCreated.getValue()));
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies not set DATECREATED if already set
     */
    @Test
    void writeDateModified_shouldNotSetDATECREATEDIfAlreadySet() {
        IndexObject io = new IndexObject("1");
        long now = System.currentTimeMillis();
        io.setDateCreated(now);

        io.writeDateModified(false);

        assertEquals(now, io.getDateCreated());
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies set DATEUPDATED if not set
     */
    @Test
    void writeDateModified_shouldSetDATEUPDATEDIfNotSet() throws Exception {
        IndexObject io = new IndexObject("1");

        io.writeDateModified(false);

        assertTrue(io.getDateCreated() > 0);
        assertEquals(1, io.getDateUpdated().size());
        assertEquals(Long.valueOf(io.getDateCreated()), io.getDateUpdated().get(0));
        List<LuceneField> fieldsDateUpdated = io.getLuceneFieldsWithName(SolrConstants.DATEUPDATED);
        assertNotNull(fieldsDateUpdated);
        assertEquals(1, fieldsDateUpdated.size());
        assertEquals(io.getDateUpdated().get(0), Long.valueOf(fieldsDateUpdated.get(0).getValue()));
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies not set DATEUPDATED if already set
     */
    @Test
    void writeDateModified_shouldNotSetDATEUPDATEDIfAlreadySet() throws Exception {
        IndexObject io = new IndexObject("1");
        long now = System.currentTimeMillis();
        io.setDateCreated(now);
        io.getDateUpdated().add(now);

        Thread.sleep(1);
        io.writeDateModified(false);

        assertEquals(Long.valueOf(now), io.getDateUpdated().get(0));
        LuceneField fieldDateUpdated = io.getLuceneFieldWithName(SolrConstants.DATEUPDATED);
        assertNotNull(fieldDateUpdated);
        assertEquals(now, Long.parseLong(fieldDateUpdated.getValue()));
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies set DATEUPDATED if update requested
     */
    @Test
    void writeDateModified_shouldSetDATEUPDATEDIfUpdateRequested() throws Exception {
        IndexObject io = new IndexObject("1");
        long now = System.currentTimeMillis();
        io.setDateCreated(now);
        io.getDateUpdated().add(now);

        Thread.sleep(1);
        io.writeDateModified(true);

        assertEquals(now, io.getDateUpdated().get(0));
        List<LuceneField> fieldsDateUpdated = io.getLuceneFieldsWithName(SolrConstants.DATEUPDATED);
        assertNotNull(fieldsDateUpdated);
        assertEquals(2, fieldsDateUpdated.size());
        Assertions.assertNotEquals(now, Long.parseLong(fieldsDateUpdated.get(1).getValue()));
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies always set DATEINDEXED
     */
    @Test
    void writeDateModified_shouldAlwaysSetDATEINDEXED() throws Exception {
        IndexObject io = new IndexObject("1");
        long now = System.currentTimeMillis();
        io.setDateCreated(now);
        io.getDateIndexed().add(now);

        Thread.sleep(1);
        io.writeDateModified(true);

        assertEquals(now, io.getDateIndexed().get(0));
        List<LuceneField> fields = io.getLuceneFieldsWithName(SolrConstants.DATEINDEXED);
        assertNotNull(fields);
        assertEquals(2, fields.size());
        Assertions.assertNotEquals(now, Long.parseLong(fields.get(1).getValue()));
    }

    /**
     * @see IndexObject#addToLucene(String,String)
     * @verifies add field to list correctly
     */
    @Test
    void addToLucene_shouldAddFieldToListCorrectly() {
        IndexObject io = new IndexObject("1");
        assertTrue(io.addToLucene("FIELD", "VALUE"));
        LuceneField field = io.getLuceneFieldWithName("FIELD");
        assertNotNull(field);
        assertEquals("VALUE", field.getValue());
    }

    /**
     * @see IndexObject#addAllToLucene(List,boolean)
     * @verifies add fields correctly
     */
    @Test
    void addAllToLucene_shouldAddFieldsCorrectly() {
        IndexObject io = new IndexObject("1");

        List<LuceneField> toAdd = new ArrayList<>(2);
        toAdd.add(new LuceneField("foo", "bar"));
        toAdd.add(new LuceneField("foo2", "bar2"));
        assertEquals(2, io.addAllToLucene(toAdd, true));

        assertEquals(2, io.getLuceneFields().size());
        {
            LuceneField field = io.getLuceneFieldWithName("foo");
            assertNotNull(field);
            assertEquals("bar", field.getValue());
        }
        {
            LuceneField field = io.getLuceneFieldWithName("foo2");
            assertNotNull(field);
            assertEquals("bar2", field.getValue());
        }
    }

    /**
     * @see IndexObject#addAllToLucene(List,boolean)
     * @verifies skip duplicates correctly
     */
    @Test
    void addAllToLucene_shouldSkipDuplicatesCorrectly() {
        IndexObject io = new IndexObject("1");
        io.addToLucene("foo", "bar");
        {
            LuceneField field = io.getLuceneFieldWithName("foo");
            assertNotNull(field);
            assertEquals("bar", field.getValue());
        }

        List<LuceneField> toAdd = new ArrayList<>(2);
        toAdd.add(new LuceneField("foo", "bar"));
        toAdd.add(new LuceneField("foo2", "bar2"));
        assertEquals(1, io.addAllToLucene(toAdd, true));

        assertEquals(2, io.getLuceneFields().size());
        {
            LuceneField field = io.getLuceneFieldWithName("foo");
            assertNotNull(field);
            assertEquals("bar", field.getValue());
        }
        {
            LuceneField field = io.getLuceneFieldWithName("foo2");
            assertNotNull(field);
            assertEquals("bar2", field.getValue());
        }
    }

    /**
     * @see IndexObject#addAllToLucene(List,boolean)
     * @verifies add duplicates correctly
     */
    @Test
    void addAllToLucene_shouldAddDuplicatesCorrectly() {
        IndexObject io = new IndexObject("1");
        io.addToLucene("foo", "bar");
        {
            LuceneField field = io.getLuceneFieldWithName("foo");
            assertNotNull(field);
            assertEquals("bar", field.getValue());
        }

        List<LuceneField> toAdd = new ArrayList<>(2);
        toAdd.add(new LuceneField("foo", "bar"));
        toAdd.add(new LuceneField("foo2", "bar2"));
        assertEquals(2, io.addAllToLucene(toAdd, false));

        assertEquals(3, io.getLuceneFields().size());
        {
            List<LuceneField> fields = io.getLuceneFieldsWithName("foo");
            assertEquals(2, fields.size());
        }
        {
            LuceneField field = io.getLuceneFieldWithName("foo2");
            assertNotNull(field);
            assertEquals("bar2", field.getValue());
        }
    }

    /**
     * @see IndexObject#addToGroupIds(String,String)
     * @verifies collect group id fields correctly
     */
    @Test
    void addToGroupIds_shouldCollectGroupIdFieldsCorrectly() {
        IndexObject io = new IndexObject("1");
        assertTrue(io.getGroupIds().isEmpty());
        io.addToGroupIds(SolrConstants.PREFIX_GROUPID + "FIELD", "VALUE");
        assertEquals("VALUE", io.getGroupIds().get(SolrConstants.PREFIX_GROUPID + "FIELD"));
    }

    /**
     * @see IndexObject#removeDuplicateGroupedMetadata()
     * @verifies remove duplicates correctly
     */
    @Test
    void removeDuplicateGroupedMetadata_shouldRemoveDuplicatesCorrectly() {
        IndexObject indexObj = new IndexObject("1");
        {
            GroupedMetadata gmd = new GroupedMetadata();
            gmd.getFields().add(new LuceneField(SolrConstants.LABEL, "label"));
            gmd.getFields().add(new LuceneField(SolrConstants.MD_VALUE, "value"));
            indexObj.getGroupedMetadataFields().add(gmd);
        }
        {
            GroupedMetadata gmd = new GroupedMetadata();
            gmd.getFields().add(new LuceneField(SolrConstants.LABEL, "label"));
            gmd.getFields().add(new LuceneField(SolrConstants.MD_VALUE, "value"));
            indexObj.getGroupedMetadataFields().add(gmd);
        }
        assertEquals(2, indexObj.getGroupedMetadataFields().size());
        indexObj.removeDuplicateGroupedMetadata();
        assertEquals(1, indexObj.getGroupedMetadataFields().size());
    }

    /**
     * @see IndexObject#removeDuplicateGroupedMetadata()
     * @verifies not remove allowed duplicates
     */
    @Test
    void removeDuplicateGroupedMetadata_shouldNotRemoveAllowedDuplicates() {
        IndexObject indexObj = new IndexObject("1");
        {
            GroupedMetadata gmd = new GroupedMetadata();
            gmd.setAllowDuplicateValues(true);
            gmd.getFields().add(new LuceneField(SolrConstants.LABEL, "label"));
            gmd.getFields().add(new LuceneField("MD_VALUE", "value"));
            indexObj.getGroupedMetadataFields().add(gmd);
        }
        {
            GroupedMetadata gmd = new GroupedMetadata();
            gmd.setAllowDuplicateValues(true);
            gmd.getFields().add(new LuceneField(SolrConstants.LABEL, "label"));
            gmd.getFields().add(new LuceneField("MD_VALUE", "value"));
            indexObj.getGroupedMetadataFields().add(gmd);
        }
        assertEquals(2, indexObj.getGroupedMetadataFields().size());
        indexObj.removeDuplicateGroupedMetadata();
        assertEquals(2, indexObj.getGroupedMetadataFields().size());
    }

    /**
     * @see IndexObject#removeNonMultivaluedFields(String)
     * @verifies remove existing boolean fields
     */
    @Test
    void removeNonMultivaluedFields_shouldRemoveExistingBooleanFields() {
        IndexObject indexObj = new IndexObject("1");
        indexObj.addToLucene("BOOL_TEST", "false");
        assertEquals(1, indexObj.getLuceneFieldsWithName("BOOL_TEST").size());

        indexObj.removeNonMultivaluedFields("BOOL_TEST");
        assertEquals(0, indexObj.getLuceneFieldsWithName("BOOL_TEST").size());
    }

    /**
     * @see IndexObject#removeNonMultivaluedFields(String)
     * @verifies remove existing sorting fields
     */
    @Test
    void removeNonMultivaluedFields_shouldRemoveExistingSortingFields() {
        IndexObject indexObj = new IndexObject("1");
        indexObj.addToLucene("SORT_TEST", "false");
        assertEquals(1, indexObj.getLuceneFieldsWithName("SORT_TEST").size());

        indexObj.removeNonMultivaluedFields("SORT_TEST");
        assertEquals(0, indexObj.getLuceneFieldsWithName("SORT_TEST").size());
    }

    /**
     * @see IndexObject#writeLanguages()
     * @verifies add languages from metadata fields
     */
    @Test
    void writeLanguages_shouldAddLanguagesFromMetadataFields() {
        IndexObject indexObj = new IndexObject("1");
        indexObj.getLuceneFields().add(new LuceneField("MD_TITLE_LANG_EN", "Title"));
        indexObj.getLuceneFields().add(new LuceneField("MD_TITLE_LANG_DE", "Titel"));
        indexObj.writeLanguages();
        assertEquals(2, indexObj.getLanguages().size());
        assertTrue(indexObj.getLanguages().contains("en"));
        assertTrue(indexObj.getLanguages().contains("de"));
    }

    /**
     * @see IndexObject#addChildMetadata(List)
     * @verifies add regular metadata correctly
     */
    @Test
    void addChildMetadata_shouldAddRegularMetadataCorrectly() {
        IndexObject indexObj = new IndexObject("1");
        IndexObject childObj = new IndexObject("2");
        childObj.addToLucene("foo", "bar");
        childObj.getFieldsToInheritToParents().add("foo");

        indexObj.addChildMetadata(Collections.singletonList(childObj));
        assertNotNull(indexObj.getLuceneFieldWithName("foo"));
        assertEquals("bar", indexObj.getLuceneFieldWithName("foo").getValue());
        assertEquals("bar", childObj.getLuceneFieldWithName("foo").getValue());
    }

    /**
     * @see IndexObject#addChildMetadata(List)
     * @verifies add grouped metadata correctly
     */
    @Test
    void addChildMetadata_shouldAddGroupedMetadataCorrectly() {
        IndexObject indexObj = new IndexObject("1");

        IndexObject childObj = new IndexObject("2");
        GroupedMetadata gmd = new GroupedMetadata();
        gmd.setLabel("foo");
        gmd.setMainValue("bar");
        childObj.getGroupedMetadataFields().add(gmd);
        childObj.getFieldsToInheritToParents().add("foo");

        indexObj.addChildMetadata(Collections.singletonList(childObj));
        assertEquals(1, indexObj.getGroupedMetadataFields().size());
        assertEquals(1, childObj.getGroupedMetadataFields().size());
    }

    /**
     * @see IndexObject#addChildMetadata(List)
     * @verifies avoid regular metadata duplicates
     */
    @Test
    void addChildMetadata_shouldAvoidRegularMetadataDuplicates() {
        IndexObject indexObj = new IndexObject("1");
        indexObj.addToLucene("foo", "bar");
        IndexObject childObj = new IndexObject("2");
        childObj.addToLucene("foo", "bar");
        childObj.getFieldsToInheritToParents().add("foo");

        indexObj.addChildMetadata(Collections.singletonList(childObj));
        assertEquals(1, indexObj.getLuceneFieldsWithName("foo").size());
    }

    /**
     * @see IndexObject#addChildMetadata(List)
     * @verifies avoid grouped metadata duplicates
     */
    @Test
    void addChildMetadata_shouldAvoidGroupedMetadataDuplicates() {
        IndexObject indexObj = new IndexObject("1");
        {
            GroupedMetadata gmd = new GroupedMetadata();
            gmd.setLabel("foo");
            gmd.setMainValue("bar");
            indexObj.getGroupedMetadataFields().add(gmd);
        }

        IndexObject childObj = new IndexObject("2");
        {
            GroupedMetadata gmd = new GroupedMetadata();
            gmd.setLabel("foo");
            gmd.setMainValue("bar");
            childObj.getGroupedMetadataFields().add(gmd);
        }
        childObj.getFieldsToInheritToParents().add("foo");

        indexObj.addChildMetadata(Collections.singletonList(childObj));
        assertEquals(1, indexObj.getGroupedMetadataFields().size());
    }

    /**
     * @see IndexObject#applyFinalModifications()
     * @verifies add existence booleans correctly
     */
    @Test
    void applyFinalModifications_shouldAddExistenceBooleansCorrectly() {
        {
            // true
            IndexObject indexObject = new IndexObject("1");
            indexObject.setSourceDocFormat(FileFormat.METS);
            indexObject.addToLucene("MD_TESTFIELD", "foo");
            indexObject.applyFinalModifications();
            assertEquals(2, indexObject.getLuceneFields().size());
            LuceneField boolField = indexObject.getLuceneFields().get(1);
            assertEquals("BOOL_TESTFIELD", boolField.getField());
            assertEquals("true", boolField.getValue());
        }
        {
            // false
            IndexObject indexObject = new IndexObject("1");
            indexObject.setSourceDocFormat(FileFormat.METS);
            indexObject.applyFinalModifications();
            assertEquals(1, indexObject.getLuceneFields().size());
            LuceneField boolField = indexObject.getLuceneFields().get(0);
            assertEquals("BOOL_TESTFIELD", boolField.getField());
            assertEquals("false", boolField.getValue());
        }
    }

    /**
     * @see IndexObject#populateDateCreatedUpdated(ZonedDateTime)
     * @verifies set dateCreated only if not yet set
     */
    @Test
    void populateDateCreatedUpdated_shouldSetDateCreatedOnlyIfNotYetSet() {
        ZonedDateTime now = ZonedDateTime.now();
        IndexObject obj = new IndexObject("1");

        obj.populateDateCreatedUpdated(now);
        assertEquals(now.toInstant().toEpochMilli(), obj.getDateCreated());

        obj.populateDateCreatedUpdated(now.plusDays(1)); // populate again with a later date
        assertEquals(now.toInstant().toEpochMilli(), obj.getDateCreated()); // dateCreated remains unchanged
    }

    /**
     * @see IndexObject#populateDateCreatedUpdated(ZonedDateTime)
     * @verifies add dateUpdated only if later
     */
    @Test
    void populateDateCreatedUpdated_shouldAddDateUpdatedOnlyIfLater() {
        ZonedDateTime now = ZonedDateTime.now();
        IndexObject obj = new IndexObject("1");

        obj.populateDateCreatedUpdated(now);
        assertEquals(1, obj.getDateUpdated().size());
        assertEquals(Long.valueOf(now.toInstant().toEpochMilli()), obj.getDateUpdated().get(0));

        obj.populateDateCreatedUpdated(now); // populate again with the same date
        assertEquals(1, obj.getDateUpdated().size()); // no new value was added

        obj.populateDateCreatedUpdated(now.plusDays(1)); // populate again with a later date
        assertEquals(2, obj.getDateUpdated().size()); // no new value was added
        assertEquals(Long.valueOf(now.toInstant().toEpochMilli()), obj.getDateUpdated().get(0));
        assertEquals(Long.valueOf(now.plusDays(1).toInstant().toEpochMilli()), obj.getDateUpdated().get(1));
    }

    /**
     * @see IndexObject#populateDateCreatedUpdated(ZonedDateTime)
     * @verifies remove dateUpdated values later than given
     */
    @Test
    void populateDateCreatedUpdated_shouldRemoveDateUpdatedValuesLaterThanGiven() {
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime tomorrow = now.plusDays(1);
        IndexObject obj = new IndexObject("1");
        obj.getDateUpdated().add(now.toInstant().toEpochMilli());
        obj.getDateUpdated().add(now.plusDays(2).toInstant().toEpochMilli());
        obj.getDateUpdated().add(now.plusDays(3).toInstant().toEpochMilli());
        assertEquals(3, obj.getDateUpdated().size());

        obj.populateDateCreatedUpdated(tomorrow); // populate with "tomorrow"
        assertEquals(2, obj.getDateUpdated().size()); // only two values remain
        assertEquals(Long.valueOf(now.toInstant().toEpochMilli()), obj.getDateUpdated().get(0)); // now
        assertEquals(Long.valueOf(tomorrow.toInstant().toEpochMilli()), obj.getDateUpdated().get(1)); // tomorrow

    }

    /**
     * @see IndexObject#addToLucene(LuceneField,boolean)
     * @verifies add fields correctly
     */
    @Test
    void addToLucene_shouldAddFieldsCorrectly() {
        IndexObject o = new IndexObject("1");
        o.addToLucene(new LuceneField("title", "once upon..."), false);
        o.addToLucene(new LuceneField("foo", "bar"), false);
        assertEquals(2, o.getLuceneFields().size());
        o.addToLucene(new LuceneField("foo", "bar"), false);
        assertEquals(3, o.getLuceneFields().size());
    }

    /**
     * @see IndexObject#addToLucene(LuceneField,boolean)
     * @verifies skip duplicates correctly
     */
    @Test
    void addToLucene_shouldSkipDuplicatesCorrectly() {
        IndexObject o = new IndexObject("1");
        o.addToLucene(new LuceneField("foo", "bar"), true);
        assertEquals(1, o.getLuceneFields().size());
        o.addToLucene(new LuceneField("foo", "bar"), true);
        assertEquals(1, o.getLuceneFields().size());
    }
}
