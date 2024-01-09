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

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;

public class IndexObjectTest extends AbstractTest {

    /**
     * @see IndexObject#IndexObject(long,String)
     * @verifies set attributes correctly
     */
    @Test
    public void IndexObject_shouldSetAttributesCorrectly() throws Exception {
        IndexObject io = new IndexObject(123L);
        Assertions.assertEquals(123L, io.getIddoc());
    }

    /**
     * @see IndexObject#getLuceneFieldsWithName(String)
     * @verifies return all fields with given name
     */
    @Test
    public void getLuceneFieldsWithName_shouldReturnAllFieldsWithGivenName() throws Exception {
        IndexObject io = new IndexObject(1);
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE11"));
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE12"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE21"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE22"));

        List<LuceneField> fields = io.getLuceneFieldsWithName("FIELD1");
        Assertions.assertNotNull(fields);
        Assertions.assertEquals(2, fields.size());
    }

    /**
     * @see IndexObject#getLuceneFieldsWithName(String)
     * @verifies return empty list if name not found
     */
    @Test
    public void getLuceneFieldsWithName_shouldReturnEmptyListIfNameNotFound() throws Exception {
        IndexObject io = new IndexObject(1);
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE11"));
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE12"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE21"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE22"));

        List<LuceneField> fields = io.getLuceneFieldsWithName("FIELD3");
        Assertions.assertNotNull(fields);
        Assertions.assertTrue(fields.isEmpty());
    }

    /**
     * @see IndexObject#getLuceneFieldWithName(String)
     * @verifies return first field with given name
     */
    @Test
    public void getLuceneFieldWithName_shouldReturnFirstFieldWithGivenName() throws Exception {
        IndexObject io = new IndexObject(1);
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE11"));
        io.getLuceneFields().add(new LuceneField("FIELD1", "VALUE12"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE21"));
        io.getLuceneFields().add(new LuceneField("FIELD2", "VALUE22"));

        LuceneField field = io.getLuceneFieldWithName("FIELD2");
        Assertions.assertNotNull(field);
        Assertions.assertEquals("VALUE21", field.getValue());
    }

    /**
     * @see IndexObject#getLuceneFieldWithName(String)
     * @verifies return null if name not found
     */
    @Test
    public void getLuceneFieldWithName_shouldReturnNullIfNameNotFound() throws Exception {
        IndexObject io = new IndexObject(1);
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
    public void pushSimpleDataToLuceneArray_shouldWriteAllRequiredFields() throws Exception {
        IndexObject io = new IndexObject(1);
        io.setType("MusicSupplies");
        io.setPi("PI");
        io.setTopstructPI("TOPSTRUCT_PI");
        io.setParentPI("PARENT_PI");
        io.setLabel("&lt;b&gt;LABEL&lt;/b&gt;");
        io.setDmdid("DMD0000");
        io.setLogId("LOG0000");
        io.setDataRepository("DATA");

        IndexObject io2 = new IndexObject(2);
        io.setParent(io2);
        IndexObject io3 = new IndexObject(3);
        io2.setParent(io3);

        io.pushSimpleDataToLuceneArray();

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.IDDOC));
        Assertions.assertEquals("1", io.getLuceneFieldWithName(SolrConstants.IDDOC).getValue());
        Assertions.assertEquals("1", io.getLuceneFieldWithName(SolrConstants.GROUPFIELD).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.DOCTYPE));
        Assertions.assertEquals(DocType.DOCSTRCT.name(), io.getLuceneFieldWithName(SolrConstants.DOCTYPE).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.PI));
        Assertions.assertEquals("PI", io.getLuceneFieldWithName(SolrConstants.PI).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.PI_TOPSTRUCT));
        Assertions.assertEquals("TOPSTRUCT_PI", io.getLuceneFieldWithName(SolrConstants.PI_TOPSTRUCT).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.PI_PARENT));
        Assertions.assertEquals("PARENT_PI", io.getLuceneFieldWithName(SolrConstants.PI_PARENT).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.PI_ANCHOR));
        Assertions.assertEquals("PARENT_PI", io.getLuceneFieldWithName(SolrConstants.PI_ANCHOR).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.LABEL));
        Assertions.assertEquals("<b>LABEL</b>", io.getLuceneFieldWithName(SolrConstants.LABEL).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.DMDID));
        Assertions.assertEquals("DMD0000", io.getLuceneFieldWithName(SolrConstants.DMDID).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.LOGID));
        Assertions.assertEquals("LOG0000", io.getLuceneFieldWithName(SolrConstants.LOGID).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.DOCSTRCT));
        Assertions.assertEquals("MusicSupplies", io.getLuceneFieldWithName(SolrConstants.DOCSTRCT).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.DOCSTRCT_ALT));
        Assertions.assertEquals("MusicSupplies_ALT", io.getLuceneFieldWithName(SolrConstants.DOCSTRCT_ALT).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP));
        Assertions.assertEquals("MusicSupplies", io.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.DATAREPOSITORY));
        Assertions.assertEquals("DATA", io.getLuceneFieldWithName(SolrConstants.DATAREPOSITORY).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.IDDOC_PARENT));
        Assertions.assertEquals("2", io.getLuceneFieldWithName(SolrConstants.IDDOC_PARENT).getValue());

        Assertions.assertNotNull(io.getLuceneFieldWithName(SolrConstants.IDDOC_TOPSTRUCT));
        Assertions.assertEquals("3", io.getLuceneFieldWithName(SolrConstants.IDDOC_TOPSTRUCT).getValue());
    }

    /**
     * @see IndexObject#writeAccessConditions(IndexObject)
     * @verifies inherit access conditions from parent except OPENACCESS
     */
    @Test
    public void writeAccessConditions_shouldInheritAccessConditionsFromParentExceptOPENACCESS() throws Exception {
        IndexObject io = new IndexObject(1);

        IndexObject pio = new IndexObject(2);
        pio.getAccessConditions().add(SolrConstants.OPEN_ACCESS_VALUE);
        pio.getAccessConditions().add("CONDITION3");
        pio.getAccessConditions().add("CONDITION4");

        io.writeAccessConditions(pio);

        List<LuceneField> fields = io.getLuceneFieldsWithName(SolrConstants.ACCESSCONDITION);
        Assertions.assertNotNull(fields);
        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("CONDITION3", fields.get(0).getValue());
        Assertions.assertEquals("CONDITION4", fields.get(1).getValue());
    }

    /**
     * @see IndexObject#writeAccessConditions(IndexObject)
     * @verifies not inherit access conditions from parent if own access conditions exist
     */
    @Test
    public void writeAccessConditions_shouldNotInheritAccessConditionsFromParentIfOwnAccessConditionsExist() throws Exception {
        IndexObject io = new IndexObject(1);
        io.getAccessConditions().add("CONDITION1");
        io.getAccessConditions().add("CONDITION2");

        IndexObject pio = new IndexObject(2);
        pio.getAccessConditions().add("CONDITION3");
        pio.getAccessConditions().add("CONDITION4");

        io.writeAccessConditions(pio);

        List<LuceneField> fields = io.getLuceneFieldsWithName(SolrConstants.ACCESSCONDITION);
        Assertions.assertNotNull(fields);
        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("CONDITION1", fields.get(0).getValue());
        Assertions.assertEquals("CONDITION2", fields.get(1).getValue());
    }

    /**
     * @see IndexObject#writeAccessConditions()
     * @verifies add OPENACCESS if list empty
     */
    @Test
    public void writeAccessConditions_shouldAddOPENACCESSIfListEmpty() throws Exception {
        IndexObject io = new IndexObject(1);

        io.writeAccessConditions(null);

        List<LuceneField> fields = io.getLuceneFieldsWithName(SolrConstants.ACCESSCONDITION);
        Assertions.assertNotNull(fields);
        Assertions.assertEquals(1, fields.size());
        Assertions.assertEquals(SolrConstants.OPEN_ACCESS_VALUE, fields.get(0).getValue());
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies set DATECREATED if not set
     */
    @Test
    public void writeDateModified_shouldSetDATECREATEDIfNotSet() throws Exception {
        IndexObject io = new IndexObject(1);

        io.writeDateModified(false);

        Assertions.assertTrue(io.getDateCreated() > 0);
        LuceneField fieldDateCreated = io.getLuceneFieldWithName(SolrConstants.DATECREATED);
        Assertions.assertNotNull(fieldDateCreated);
        Assertions.assertEquals(io.getDateCreated(), Long.parseLong(fieldDateCreated.getValue()));
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies not set DATECREATED if already set
     */
    @Test
    public void writeDateModified_shouldNotSetDATECREATEDIfAlreadySet() throws Exception {
        IndexObject io = new IndexObject(1);
        long now = System.currentTimeMillis();
        io.setDateCreated(now);

        io.writeDateModified(false);

        Assertions.assertEquals(now, io.getDateCreated());
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies set DATEUPDATED if not set
     */
    @Test
    public void writeDateModified_shouldSetDATEUPDATEDIfNotSet() throws Exception {
        IndexObject io = new IndexObject(1);

        io.writeDateModified(false);

        Assertions.assertTrue(io.getDateCreated() > 0);
        Assertions.assertEquals(1, io.getDateUpdated().size());
        Assertions.assertEquals(Long.valueOf(io.getDateCreated()), io.getDateUpdated().get(0));
        List<LuceneField> fieldsDateUpdated = io.getLuceneFieldsWithName(SolrConstants.DATEUPDATED);
        Assertions.assertNotNull(fieldsDateUpdated);
        Assertions.assertEquals(1, fieldsDateUpdated.size());
        Assertions.assertEquals(io.getDateUpdated().get(0), Long.valueOf(fieldsDateUpdated.get(0).getValue()));
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies not set DATEUPDATED if already set
     */
    @Test
    public void writeDateModified_shouldNotSetDATEUPDATEDIfAlreadySet() throws Exception {
        IndexObject io = new IndexObject(1);
        long now = System.currentTimeMillis();
        io.setDateCreated(now);
        io.getDateUpdated().add(now);

        Thread.sleep(1);
        io.writeDateModified(false);

        Assertions.assertEquals(Long.valueOf(now), io.getDateUpdated().get(0));
        LuceneField fieldDateUpdated = io.getLuceneFieldWithName(SolrConstants.DATEUPDATED);
        Assertions.assertNotNull(fieldDateUpdated);
        Assertions.assertEquals(now, Long.parseLong(fieldDateUpdated.getValue()));
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies set DATEUPDATED if update requested
     */
    @Test
    public void writeDateModified_shouldSetDATEUPDATEDIfUpdateRequested() throws Exception {
        IndexObject io = new IndexObject(1);
        long now = System.currentTimeMillis();
        io.setDateCreated(now);
        io.getDateUpdated().add(now);

        Thread.sleep(1);
        io.writeDateModified(true);

        Assertions.assertTrue(now == io.getDateUpdated().get(0));
        List<LuceneField> fieldsDateUpdated = io.getLuceneFieldsWithName(SolrConstants.DATEUPDATED);
        Assertions.assertNotNull(fieldsDateUpdated);
        Assertions.assertEquals(2, fieldsDateUpdated.size());
        Assertions.assertNotEquals(now, Long.parseLong(fieldsDateUpdated.get(1).getValue()));
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies always set DATEINDEXED
     */
    @Test
    public void writeDateModified_shouldAlwaysSetDATEINDEXED() throws Exception {
        IndexObject io = new IndexObject(1);
        long now = System.currentTimeMillis();
        io.setDateCreated(now);
        io.getDateIndexed().add(now);

        Thread.sleep(1);
        io.writeDateModified(true);

        Assertions.assertTrue(now == io.getDateIndexed().get(0));
        List<LuceneField> fields = io.getLuceneFieldsWithName(SolrConstants.DATEINDEXED);
        Assertions.assertNotNull(fields);
        Assertions.assertEquals(2, fields.size());
        Assertions.assertNotEquals(now, Long.parseLong(fields.get(1).getValue()));
    }

    /**
     * @see IndexObject#addToLucene(String,String)
     * @verifies add field to list correctly
     */
    @Test
    public void addToLucene_shouldAddFieldToListCorrectly() throws Exception {
        IndexObject io = new IndexObject(1);
        Assertions.assertTrue(io.addToLucene("FIELD", "VALUE"));
        LuceneField field = io.getLuceneFieldWithName("FIELD");
        Assertions.assertNotNull(field);
        Assertions.assertEquals("VALUE", field.getValue());
    }

    /**
     * @see IndexObject#addAllToLucene(List,boolean)
     * @verifies add fields correctly
     */
    @Test
    public void addAllToLucene_shouldAddFieldsCorrectly() throws Exception {
        IndexObject io = new IndexObject(1);

        List<LuceneField> toAdd = new ArrayList<>(2);
        toAdd.add(new LuceneField("foo", "bar"));
        toAdd.add(new LuceneField("foo2", "bar2"));
        Assertions.assertEquals(2, io.addAllToLucene(toAdd, true));

        Assertions.assertEquals(2, io.getLuceneFields().size());
        {
            LuceneField field = io.getLuceneFieldWithName("foo");
            Assertions.assertNotNull(field);
            Assertions.assertEquals("bar", field.getValue());
        }
        {
            LuceneField field = io.getLuceneFieldWithName("foo2");
            Assertions.assertNotNull(field);
            Assertions.assertEquals("bar2", field.getValue());
        }
    }

    /**
     * @see IndexObject#addAllToLucene(List,boolean)
     * @verifies skip duplicates correctly
     */
    @Test
    public void addAllToLucene_shouldSkipDuplicatesCorrectly() throws Exception {
        IndexObject io = new IndexObject(1);
        io.addToLucene("foo", "bar");
        {
            LuceneField field = io.getLuceneFieldWithName("foo");
            Assertions.assertNotNull(field);
            Assertions.assertEquals("bar", field.getValue());
        }

        List<LuceneField> toAdd = new ArrayList<>(2);
        toAdd.add(new LuceneField("foo", "bar"));
        toAdd.add(new LuceneField("foo2", "bar2"));
        Assertions.assertEquals(1, io.addAllToLucene(toAdd, true));

        Assertions.assertEquals(2, io.getLuceneFields().size());
        {
            LuceneField field = io.getLuceneFieldWithName("foo");
            Assertions.assertNotNull(field);
            Assertions.assertEquals("bar", field.getValue());
        }
        {
            LuceneField field = io.getLuceneFieldWithName("foo2");
            Assertions.assertNotNull(field);
            Assertions.assertEquals("bar2", field.getValue());
        }
    }

    /**
     * @see IndexObject#addAllToLucene(List,boolean)
     * @verifies add duplicates correctly
     */
    @Test
    public void addAllToLucene_shouldAddDuplicatesCorrectly() throws Exception {
        IndexObject io = new IndexObject(1);
        io.addToLucene("foo", "bar");
        {
            LuceneField field = io.getLuceneFieldWithName("foo");
            Assertions.assertNotNull(field);
            Assertions.assertEquals("bar", field.getValue());
        }

        List<LuceneField> toAdd = new ArrayList<>(2);
        toAdd.add(new LuceneField("foo", "bar"));
        toAdd.add(new LuceneField("foo2", "bar2"));
        Assertions.assertEquals(2, io.addAllToLucene(toAdd, false));

        Assertions.assertEquals(3, io.getLuceneFields().size());
        {
            List<LuceneField> fields = io.getLuceneFieldsWithName("foo");
            Assertions.assertEquals(2, fields.size());
        }
        {
            LuceneField field = io.getLuceneFieldWithName("foo2");
            Assertions.assertNotNull(field);
            Assertions.assertEquals("bar2", field.getValue());
        }
    }

    /**
     * @see IndexObject#addToGroupIds(String,String)
     * @verifies collect group id fields correctly
     */
    @Test
    public void addToGroupIds_shouldCollectGroupIdFieldsCorrectly() throws Exception {
        IndexObject io = new IndexObject(1);
        Assertions.assertTrue(io.getGroupIds().isEmpty());
        io.addToGroupIds(SolrConstants.PREFIX_GROUPID + "FIELD", "VALUE");
        Assertions.assertEquals("VALUE", io.getGroupIds().get(SolrConstants.PREFIX_GROUPID + "FIELD"));
    }

    /**
     * @see IndexObject#removeDuplicateGroupedMetadata()
     * @verifies remove duplicates correctly
     */
    @Test
    public void removeDuplicateGroupedMetadata_shouldRemoveDuplicatesCorrectly() throws Exception {
        IndexObject indexObj = new IndexObject(1);
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
        Assertions.assertEquals(2, indexObj.getGroupedMetadataFields().size());
        indexObj.removeDuplicateGroupedMetadata();
        Assertions.assertEquals(1, indexObj.getGroupedMetadataFields().size());
    }

    /**
     * @see IndexObject#removeDuplicateGroupedMetadata()
     * @verifies not remove allowed duplicates
     */
    @Test
    public void removeDuplicateGroupedMetadata_shouldNotRemoveAllowedDuplicates() throws Exception {
        IndexObject indexObj = new IndexObject(1);
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
        Assertions.assertEquals(2, indexObj.getGroupedMetadataFields().size());
        indexObj.removeDuplicateGroupedMetadata();
        Assertions.assertEquals(2, indexObj.getGroupedMetadataFields().size());
    }

    /**
     * @see IndexObject#removeNonMultivaluedFields(String)
     * @verifies remove existing boolean fields
     */
    @Test
    public void removeNonMultivaluedFields_shouldRemoveExistingBooleanFields() throws Exception {
        IndexObject indexObj = new IndexObject(1);
        indexObj.addToLucene("BOOL_TEST", "false");
        Assertions.assertEquals(1, indexObj.getLuceneFieldsWithName("BOOL_TEST").size());

        indexObj.removeNonMultivaluedFields("BOOL_TEST");
        Assertions.assertEquals(0, indexObj.getLuceneFieldsWithName("BOOL_TEST").size());
    }

    /**
     * @see IndexObject#removeNonMultivaluedFields(String)
     * @verifies remove existing sorting fields
     */
    @Test
    public void removeNonMultivaluedFields_shouldRemoveExistingSortingFields() throws Exception {
        IndexObject indexObj = new IndexObject(1);
        indexObj.addToLucene("SORT_TEST", "false");
        Assertions.assertEquals(1, indexObj.getLuceneFieldsWithName("SORT_TEST").size());

        indexObj.removeNonMultivaluedFields("SORT_TEST");
        Assertions.assertEquals(0, indexObj.getLuceneFieldsWithName("SORT_TEST").size());
    }

    /**
     * @see IndexObject#writeLanguages()
     * @verifies add languages from metadata fields
     */
    @Test
    public void writeLanguages_shouldAddLanguagesFromMetadataFields() throws Exception {
        IndexObject indexObj = new IndexObject(1);
        indexObj.getLuceneFields().add(new LuceneField("MD_TITLE_LANG_EN", "Title"));
        indexObj.getLuceneFields().add(new LuceneField("MD_TITLE_LANG_DE", "Titel"));
        indexObj.writeLanguages();
        Assertions.assertEquals(2, indexObj.getLanguages().size());
        Assertions.assertTrue(indexObj.getLanguages().contains("en"));
        Assertions.assertTrue(indexObj.getLanguages().contains("de"));
    }

    /**
     * @see IndexObject#addChildMetadata(List)
     * @verifies add regular metadata correctly
     */
    @Test
    public void addChildMetadata_shouldAddRegularMetadataCorrectly() throws Exception {
        IndexObject indexObj = new IndexObject(1);
        IndexObject childObj = new IndexObject(2);
        childObj.addToLucene("foo", "bar");
        childObj.getFieldsToInheritToParents().add("foo");

        indexObj.addChildMetadata(Collections.singletonList(childObj));
        Assertions.assertNotNull(indexObj.getLuceneFieldWithName("foo"));
        Assertions.assertEquals("bar", indexObj.getLuceneFieldWithName("foo").getValue());
        Assertions.assertEquals("bar", childObj.getLuceneFieldWithName("foo").getValue());
    }

    /**
     * @see IndexObject#addChildMetadata(List)
     * @verifies add grouped metadata correctly
     */
    @Test
    public void addChildMetadata_shouldAddGroupedMetadataCorrectly() throws Exception {
        IndexObject indexObj = new IndexObject(1);

        IndexObject childObj = new IndexObject(2);
        GroupedMetadata gmd = new GroupedMetadata();
        gmd.setLabel("foo");
        gmd.setMainValue("bar");
        childObj.getGroupedMetadataFields().add(gmd);
        childObj.getFieldsToInheritToParents().add("foo");

        indexObj.addChildMetadata(Collections.singletonList(childObj));
        Assertions.assertEquals(1, indexObj.getGroupedMetadataFields().size());
        Assertions.assertEquals(1, childObj.getGroupedMetadataFields().size());
    }

    /**
     * @see IndexObject#addChildMetadata(List)
     * @verifies avoid regular metadata duplicates
     */
    @Test
    public void addChildMetadata_shouldAvoidRegularMetadataDuplicates() throws Exception {
        IndexObject indexObj = new IndexObject(1);
        indexObj.addToLucene("foo", "bar");
        IndexObject childObj = new IndexObject(2);
        childObj.addToLucene("foo", "bar");
        childObj.getFieldsToInheritToParents().add("foo");

        indexObj.addChildMetadata(Collections.singletonList(childObj));
        Assertions.assertEquals(1, indexObj.getLuceneFieldsWithName("foo").size());
    }

    /**
     * @see IndexObject#addChildMetadata(List)
     * @verifies avoid grouped metadata duplicates
     */
    @Test
    public void addChildMetadata_shouldAvoidGroupedMetadataDuplicates() throws Exception {
        IndexObject indexObj = new IndexObject(1);
        {
            GroupedMetadata gmd = new GroupedMetadata();
            gmd.setLabel("foo");
            gmd.setMainValue("bar");
            indexObj.getGroupedMetadataFields().add(gmd);
        }

        IndexObject childObj = new IndexObject(2);
        {
            GroupedMetadata gmd = new GroupedMetadata();
            gmd.setLabel("foo");
            gmd.setMainValue("bar");
            childObj.getGroupedMetadataFields().add(gmd);
        }
        childObj.getFieldsToInheritToParents().add("foo");

        indexObj.addChildMetadata(Collections.singletonList(childObj));
        Assertions.assertEquals(1, indexObj.getGroupedMetadataFields().size());
    }

    /**
     * @see IndexObject#applyFinalModifications()
     * @verifies add existence booleans correctly
     */
    @Test
    public void applyFinalModifications_shouldAddExistenceBooleansCorrectly() throws Exception {
        {
            // true
            IndexObject indexObject = new IndexObject(1L);
            indexObject.addToLucene("MD_TESTFIELD", "foo");
            indexObject.applyFinalModifications();
            Assertions.assertEquals(2, indexObject.getLuceneFields().size());
            LuceneField boolField = indexObject.getLuceneFields().get(1);
            Assertions.assertEquals("BOOL_TESTFIELD", boolField.getField());
            Assertions.assertEquals("true", boolField.getValue());
        }
        {
            // false
            IndexObject indexObject = new IndexObject(1L);
            indexObject.applyFinalModifications();
            Assertions.assertEquals(1, indexObject.getLuceneFields().size());
            LuceneField boolField = indexObject.getLuceneFields().get(0);
            Assertions.assertEquals("BOOL_TESTFIELD", boolField.getField());
            Assertions.assertEquals("false", boolField.getValue());
        }
    }

    /**
     * @see IndexObject#populateDateCreatedUpdated(ZonedDateTime)
     * @verifies set dateCreated only if not yet set
     */
    @Test
    public void populateDateCreatedUpdated_shouldSetDateCreatedOnlyIfNotYetSet() throws Exception {
        ZonedDateTime now = ZonedDateTime.now();
        IndexObject obj = new IndexObject(1);

        obj.populateDateCreatedUpdated(now);
        Assertions.assertEquals(now.toInstant().toEpochMilli(), obj.getDateCreated());

        obj.populateDateCreatedUpdated(now.plusDays(1)); // populate again with a later date
        Assertions.assertEquals(now.toInstant().toEpochMilli(), obj.getDateCreated()); // dateCreated remains unchanged
    }

    /**
     * @see IndexObject#populateDateCreatedUpdated(ZonedDateTime)
     * @verifies add dateUpdated only if later
     */
    @Test
    public void populateDateCreatedUpdated_shouldAddDateUpdatedOnlyIfLater() throws Exception {
        ZonedDateTime now = ZonedDateTime.now();
        IndexObject obj = new IndexObject(1);

        obj.populateDateCreatedUpdated(now);
        Assertions.assertEquals(1, obj.getDateUpdated().size());
        Assertions.assertEquals(Long.valueOf(now.toInstant().toEpochMilli()), obj.getDateUpdated().get(0));

        obj.populateDateCreatedUpdated(now); // populate again with the same date
        Assertions.assertEquals(1, obj.getDateUpdated().size()); // no new value was added

        obj.populateDateCreatedUpdated(now.plusDays(1)); // populate again with a later date
        Assertions.assertEquals(2, obj.getDateUpdated().size()); // no new value was added
        Assertions.assertEquals(Long.valueOf(now.toInstant().toEpochMilli()), obj.getDateUpdated().get(0));
        Assertions.assertEquals(Long.valueOf(now.plusDays(1).toInstant().toEpochMilli()), obj.getDateUpdated().get(1));
    }

    /**
     * @see IndexObject#populateDateCreatedUpdated(ZonedDateTime)
     * @verifies remove dateUpdated values later than given
     */
    @Test
    public void populateDateCreatedUpdated_shouldRemoveDateUpdatedValuesLaterThanGiven() throws Exception {
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime tomorrow = now.plusDays(1);
        IndexObject obj = new IndexObject(1);
        obj.getDateUpdated().add(now.toInstant().toEpochMilli());
        obj.getDateUpdated().add(now.plusDays(2).toInstant().toEpochMilli());
        obj.getDateUpdated().add(now.plusDays(3).toInstant().toEpochMilli());
        Assertions.assertEquals(3, obj.getDateUpdated().size());

        obj.populateDateCreatedUpdated(tomorrow); // populate with "tomorrow"
        Assertions.assertEquals(2, obj.getDateUpdated().size()); // only two values remain
        Assertions.assertEquals(Long.valueOf(now.toInstant().toEpochMilli()), obj.getDateUpdated().get(0)); // now
        Assertions.assertEquals(Long.valueOf(tomorrow.toInstant().toEpochMilli()), obj.getDateUpdated().get(1)); // tomorrow

    }

    /**
     * @see IndexObject#addToLucene(LuceneField,boolean)
     * @verifies add fields correctly
     */
    @Test
    public void addToLucene_shouldAddFieldsCorrectly() throws Exception {
        IndexObject o = new IndexObject(1L);
        o.addToLucene(new LuceneField("title", "once upon..."), false);
        o.addToLucene(new LuceneField("foo", "bar"), false);
        Assertions.assertEquals(2, o.getLuceneFields().size());
        o.addToLucene(new LuceneField("foo", "bar"), false);
        Assertions.assertEquals(3, o.getLuceneFields().size());
    }

    /**
     * @see IndexObject#addToLucene(LuceneField,boolean)
     * @verifies skip duplicates correctly
     */
    @Test
    public void addToLucene_shouldSkipDuplicatesCorrectly() throws Exception {
        IndexObject o = new IndexObject(1L);
        o.addToLucene(new LuceneField("foo", "bar"), true);
        Assertions.assertEquals(1, o.getLuceneFields().size());
        o.addToLucene(new LuceneField("foo", "bar"), true);
        Assertions.assertEquals(1, o.getLuceneFields().size());
    }
}
