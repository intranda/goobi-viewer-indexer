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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.model.GroupedMetadata;
import io.goobi.viewer.indexer.model.IndexObject;
import io.goobi.viewer.indexer.model.LuceneField;
import io.goobi.viewer.indexer.model.SolrConstants;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;

public class IndexObjectTest extends AbstractTest {

    /**
     * @see IndexObject#IndexObject(long,String)
     * @verifies set attributes correctly
     */
    @Test
    public void IndexObject_shouldSetAttributesCorrectly() throws Exception {
        IndexObject io = new IndexObject(123L);
        Assert.assertEquals(123L, io.getIddoc());
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
        Assert.assertNotNull(fields);
        Assert.assertEquals(2, fields.size());
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
        Assert.assertNotNull(fields);
        Assert.assertTrue(fields.isEmpty());
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
        Assert.assertNotNull(field);
        Assert.assertEquals("VALUE21", field.getValue());
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
        Assert.assertNull(field);
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

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.IDDOC));
        Assert.assertEquals("1", io.getLuceneFieldWithName(SolrConstants.IDDOC).getValue());
        Assert.assertEquals("1", io.getLuceneFieldWithName(SolrConstants.GROUPFIELD).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.DOCTYPE));
        Assert.assertEquals(DocType.DOCSTRCT.name(), io.getLuceneFieldWithName(SolrConstants.DOCTYPE).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.PI));
        Assert.assertEquals("PI", io.getLuceneFieldWithName(SolrConstants.PI).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.PI_TOPSTRUCT));
        Assert.assertEquals("TOPSTRUCT_PI", io.getLuceneFieldWithName(SolrConstants.PI_TOPSTRUCT).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.PI_PARENT));
        Assert.assertEquals("PARENT_PI", io.getLuceneFieldWithName(SolrConstants.PI_PARENT).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.PI_ANCHOR));
        Assert.assertEquals("PARENT_PI", io.getLuceneFieldWithName(SolrConstants.PI_ANCHOR).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.LABEL));
        Assert.assertEquals("<b>LABEL</b>", io.getLuceneFieldWithName(SolrConstants.LABEL).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.DMDID));
        Assert.assertEquals("DMD0000", io.getLuceneFieldWithName(SolrConstants.DMDID).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.LOGID));
        Assert.assertEquals("LOG0000", io.getLuceneFieldWithName(SolrConstants.LOGID).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.DOCSTRCT));
        Assert.assertEquals("MusicSupplies", io.getLuceneFieldWithName(SolrConstants.DOCSTRCT).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.DOCSTRCT_ALT));
        Assert.assertEquals("MusicSupplies_ALT", io.getLuceneFieldWithName(SolrConstants.DOCSTRCT_ALT).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP));
        Assert.assertEquals("MusicSupplies", io.getLuceneFieldWithName(SolrConstants.DOCSTRCT_TOP).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.DATAREPOSITORY));
        Assert.assertEquals("DATA", io.getLuceneFieldWithName(SolrConstants.DATAREPOSITORY).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.IDDOC_PARENT));
        Assert.assertEquals("2", io.getLuceneFieldWithName(SolrConstants.IDDOC_PARENT).getValue());

        Assert.assertNotNull(io.getLuceneFieldWithName(SolrConstants.IDDOC_TOPSTRUCT));
        Assert.assertEquals("3", io.getLuceneFieldWithName(SolrConstants.IDDOC_TOPSTRUCT).getValue());
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
        Assert.assertNotNull(fields);
        Assert.assertEquals(2, fields.size());
        Assert.assertEquals("CONDITION3", fields.get(0).getValue());
        Assert.assertEquals("CONDITION4", fields.get(1).getValue());
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
        Assert.assertNotNull(fields);
        Assert.assertEquals(2, fields.size());
        Assert.assertEquals("CONDITION1", fields.get(0).getValue());
        Assert.assertEquals("CONDITION2", fields.get(1).getValue());
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
        Assert.assertNotNull(fields);
        Assert.assertEquals(1, fields.size());
        Assert.assertEquals(SolrConstants.OPEN_ACCESS_VALUE, fields.get(0).getValue());
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies set DATECREATED if not set
     */
    @Test
    public void writeDateModified_shouldSetDATECREATEDIfNotSet() throws Exception {
        IndexObject io = new IndexObject(1);

        io.writeDateModified(false);

        Assert.assertTrue(io.getDateCreated() > 0);
        LuceneField fieldDateCreated = io.getLuceneFieldWithName(SolrConstants.DATECREATED);
        Assert.assertNotNull(fieldDateCreated);
        Assert.assertEquals(io.getDateCreated(), Long.parseLong(fieldDateCreated.getValue()));
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

        Assert.assertEquals(now, io.getDateCreated());
    }

    /**
     * @see IndexObject#writeDateModified(boolean)
     * @verifies set DATEUPDATED if not set
     */
    @Test
    public void writeDateModified_shouldSetDATEUPDATEDIfNotSet() throws Exception {
        IndexObject io = new IndexObject(1);

        io.writeDateModified(false);

        Assert.assertTrue(io.getDateCreated() > 0);
        Assert.assertEquals(1, io.getDateUpdated().size());
        Assert.assertEquals(Long.valueOf(io.getDateCreated()), io.getDateUpdated().get(0));
        List<LuceneField> fieldsDateUpdated = io.getLuceneFieldsWithName(SolrConstants.DATEUPDATED);
        Assert.assertNotNull(fieldsDateUpdated);
        Assert.assertEquals(1, fieldsDateUpdated.size());
        Assert.assertEquals(io.getDateUpdated().get(0), Long.valueOf(fieldsDateUpdated.get(0).getValue()));
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

        Assert.assertEquals(Long.valueOf(now), io.getDateUpdated().get(0));
        LuceneField fieldDateUpdated = io.getLuceneFieldWithName(SolrConstants.DATEUPDATED);
        Assert.assertNotNull(fieldDateUpdated);
        Assert.assertEquals(now, Long.parseLong(fieldDateUpdated.getValue()));
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

        Assert.assertNotEquals(now, io.getDateUpdated());
        List<LuceneField> fieldsDateUpdated = io.getLuceneFieldsWithName(SolrConstants.DATEUPDATED);
        Assert.assertNotNull(fieldsDateUpdated);
        Assert.assertEquals(2, fieldsDateUpdated.size());
        Assert.assertNotEquals(now, Long.parseLong(fieldsDateUpdated.get(1).getValue()));
    }

    /**
     * @see IndexObject#addToLucene(String,String)
     * @verifies add field to list correctly
     */
    @Test
    public void addToLucene_shouldAddFieldToListCorrectly() throws Exception {
        IndexObject io = new IndexObject(1);
        Assert.assertTrue(io.addToLucene("FIELD", "VALUE"));
        LuceneField field = io.getLuceneFieldWithName("FIELD");
        Assert.assertNotNull(field);
        Assert.assertEquals("VALUE", field.getValue());
    }

    /**
     * @see IndexObject#addToGroupIds(String,String)
     * @verifies collect group id fields correctly
     */
    @Test
    public void addToGroupIds_shouldCollectGroupIdFieldsCorrectly() throws Exception {
        IndexObject io = new IndexObject(1);
        Assert.assertTrue(io.getGroupIds().isEmpty());
        io.addToGroupIds(SolrConstants.GROUPID_ + "FIELD", "VALUE");
        Assert.assertEquals("VALUE", io.getGroupIds().get(SolrConstants.GROUPID_ + "FIELD"));
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
            gmd.getFields().add(new LuceneField("MD_VALUE", "value"));
            indexObj.getGroupedMetadataFields().add(gmd);
        }
        {
            GroupedMetadata gmd = new GroupedMetadata();
            gmd.getFields().add(new LuceneField(SolrConstants.LABEL, "label"));
            gmd.getFields().add(new LuceneField("MD_VALUE", "value"));
            indexObj.getGroupedMetadataFields().add(gmd);
        }
        Assert.assertEquals(2, indexObj.getGroupedMetadataFields().size());
        indexObj.removeDuplicateGroupedMetadata();
        Assert.assertEquals(1, indexObj.getGroupedMetadataFields().size());
    }

    /**
     * @see IndexObject#removeExistingFields(String)
     * @verifies remove existing boolean fields
     */
    @Test
    public void removeExistingFields_shouldRemoveExistingBooleanFields() throws Exception {
        IndexObject indexObj = new IndexObject(1);
        indexObj.addToLucene("BOOL_TEST", "false");
        Assert.assertEquals(1, indexObj.getLuceneFieldsWithName("BOOL_TEST").size());

        indexObj.removeExistingFields("BOOL_TEST");
        Assert.assertEquals(0, indexObj.getLuceneFieldsWithName("BOOL_TEST").size());
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
        Assert.assertEquals(2, indexObj.getLanguages().size());
        Assert.assertTrue(indexObj.getLanguages().contains("en"));
        Assert.assertTrue(indexObj.getLanguages().contains("de"));
    }
}
