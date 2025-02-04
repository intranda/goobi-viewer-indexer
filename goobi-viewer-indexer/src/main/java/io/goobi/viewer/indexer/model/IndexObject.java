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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdom2.Element;

import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.SolrSearchIndex;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;
import io.goobi.viewer.indexer.model.config.FieldConfig;

/**
 * Object representing a docstruct.
 */
public class IndexObject {

    private static final Logger logger = LogManager.getLogger(IndexObject.class);

    private String iddoc;
    private String pi;
    private IndexObject parent = null;
    private boolean update = false;
    /** Timestamp of initial import. Never altered once written. */
    private long dateCreated = -1;
    /** Timestamps collected from the source document (e.g. METS). */
    private final List<Long> dateUpdated = new ArrayList<>();
    /** Timestamps of each indexing of this record. */
    private final List<Long> dateIndexed = new ArrayList<>();

    private DocType docType = DocType.DOCSTRCT;
    private FileFormat sourceDocFormat;
    private String dmdId;
    private String logId;
    private String type;
    private String label;
    /** Collection of labels from the element hierarchy above this element. */
    private final List<String> parentLabels = new ArrayList<>();
    private Element rootStructNode;
    private boolean anchor = false;
    private boolean volume;
    private String defaultValue = "";
    private String parentPI = "";
    private String topstructPI = "";
    private String anchorPI = "";
    private final Map<String, String> groupIds = new HashMap<>();
    private String urn;
    private List<String> imageUrns;
    private final Set<String> accessConditions = new HashSet<>();
    private String thumbnailRepresent = null;
    private String dataRepository;
    private final List<LuceneField> luceneFields = new ArrayList<>();
    private List<GroupedMetadata> groupedMetadataFields = new ArrayList<>();
    private final Set<String> fieldsToInheritToParents = new HashSet<>();
    private int numPages = 0;
    private String firstPageLabel;
    private String lastPageLabel;
    /** Available language versions for this record. */
    private final Set<String> languages = new HashSet<>();

    /**
     * <p>
     * Constructor for IndexObject.
     * </p>
     *
     * @param iddoc a long.
     */
    public IndexObject(String iddoc) {
        this.iddoc = iddoc;
    }

    /**
     * <p>
     * Constructor for IndexObject.
     * </p>
     *
     * @param iddoc a long.
     * @param pi a {@link java.lang.String} object.
     * @should set attributes correctly
     */
    public IndexObject(String iddoc, String pi) {
        this.iddoc = iddoc;
        this.pi = pi;
    }

    /**
     * Generates Solr fields for essential metadata.
     * 
     * @should write all required fields
     */
    public void pushSimpleDataToLuceneArray() {
        String iddocString = String.valueOf(iddoc);
        addToLucene(SolrConstants.IDDOC, iddocString);
        addToLucene(SolrConstants.GROUPFIELD, iddocString);
        addToLucene(SolrConstants.DOCTYPE, docType.name());
        addToLucene(SolrConstants.PI, pi);
        addToLucene(SolrConstants.PI_TOPSTRUCT, topstructPI);
        if (StringUtils.isNotEmpty(parentPI)) {
            addToLucene(SolrConstants.PI_PARENT, parentPI);
            addToLucene(SolrConstants.PI_ANCHOR, parentPI);
        }
        if (label != null) {
            // Apply default modifications and replace rules to LABEL
            String moddedLabel = MetadataHelper.applyValueDefaultModifications(label);
            List<FieldConfig> configurationItemList =
                    SolrIndexerDaemon.getInstance()
                            .getConfiguration()
                            .getMetadataConfigurationManager()
                            .getConfigurationListForField(SolrConstants.LABEL);
            if (configurationItemList != null && !configurationItemList.isEmpty()) {
                moddedLabel = MetadataHelper.applyReplaceRules(moddedLabel, configurationItemList.get(0).getReplaceRules());
            }
            addToLucene(SolrConstants.LABEL, moddedLabel);
        }
        addToLucene(SolrConstants.DMDID, dmdId);
        addToLucene(SolrConstants.LOGID, logId);

        if (getParent() != null) {
            addToLucene(SolrConstants.IDDOC_PARENT, String.valueOf(getParent().getIddoc()));

            // Add IDDOC of the top element of this work as 'IDDOC_TOPSTRUCT' (required for bookshelves)
            IndexObject top = this;
            while (top.getParent() != null && !top.getParent().isAnchor()) {
                top = top.getParent();
            }
            addToLucene(SolrConstants.IDDOC_TOPSTRUCT, String.valueOf(top.getIddoc()));
        }

        addToLucene(SolrConstants.DOCSTRCT, getType());
        addToLucene(SolrConstants.DOCSTRCT_ALT, getType() + "_ALT");
        if (this.parent != null && pi == null) {
            // Add own type value as DOCSTRCT_SUB to true subelements (no volumes)
            addToLucene(SolrConstants.DOCSTRCT_SUB, getType());
            // Add topstruct type value as DOCSTRCT_TOP 
            IndexObject p = this.parent;
            while (p.getParent() != null) {
                p = p.getParent();
            }
            addToLucene(SolrConstants.DOCSTRCT_TOP, p.getType());
        } else {
            // Add topstruct type value as DOCSTRSCT_TOP as well
            addToLucene(SolrConstants.DOCSTRCT_TOP, getType());
        }
        addToLucene(SolrConstants.DATAREPOSITORY, getDataRepository());
        if (sourceDocFormat != null) {
            addToLucene(SolrConstants.SOURCEDOCFORMAT, sourceDocFormat.name());
        }
    }

    /**
     * Writes ACCESSCONDITION fields for every string in the <code>accessConditions</code> list. Should be done in the end so that only docs with no
     * access condition get the open access value.
     *
     * @should inherit access conditions from parent except OPENACCESS
     * @should not inherit access conditions from parent if own access conditions exist
     * @should add OPENACCESS if list empty
     * @param parentIndexObject a {@link io.goobi.viewer.indexer.model.IndexObject} object.
     */
    public void writeAccessConditions(IndexObject parentIndexObject) {
        // If the docstruct has no own access conditions, add those from the parent (except for openaccess)
        if (accessConditions.isEmpty() && parentIndexObject != null) {
            for (String accessCondition : parentIndexObject.getAccessConditions()) {
                if (!SolrConstants.OPEN_ACCESS_VALUE.equals(accessCondition)) {
                    accessConditions.add(accessCondition);
                }
            }
        }
        // Add OPENACCESS if no conditions found
        if (accessConditions.isEmpty()) {
            accessConditions.add(SolrConstants.OPEN_ACCESS_VALUE);
        }

        // Add index fields
        for (String s : getAccessConditions()) {
            addToLucene(SolrConstants.ACCESSCONDITION, s);
        }
    }

    /**
     * Writes created/updated/indexed timestamps into Solr fields.
     *
     * @param updateDateUpdated DATEINDEXED will be set to the current timestamp if true; old value will be used if false.
     * @should set DATECREATED if not set
     * @should not set DATECREATED if already set
     * @should set DATEUPDATED if not set
     * @should not set DATEUPDATED if already set
     * @should set DATEUPDATED if update requested
     * @should always set DATEINDEXED
     */
    public void writeDateModified(boolean updateDateUpdated) {
        long now = System.currentTimeMillis();

        // DATECREATED
        if (dateCreated == -1) {
            setDateCreated(now);
        }
        addToLucene(SolrConstants.DATECREATED, String.valueOf(getDateCreated()));

        // DATEUPDATED
        if (updateDateUpdated || dateUpdated.isEmpty()) {
            dateUpdated.add(now);
        }
        long latestDateUpdated = 0;
        for (Long date : dateUpdated) {
            addToLucene(SolrConstants.DATEUPDATED, String.valueOf(date));
            if (date > latestDateUpdated) {
                latestDateUpdated = date;
            }
        }
        // Add latest DATEUPDATED value as SORT_DATEUPDATED
        if (latestDateUpdated > 0) {
            addToLucene(SolrConstants.PREFIX_SORT + SolrConstants.DATEUPDATED, String.valueOf(latestDateUpdated));
        }

        // DATEINDEXED
        dateIndexed.add(now);
        for (Long date : dateIndexed) {
            addToLucene(SolrConstants.DATEINDEXED, String.valueOf(date));
        }
    }

    /**
     * <p>
     * writeLanguages.
     * </p>
     *
     * @should add languages from metadata fields
     */
    public void writeLanguages() {
        if (languages.isEmpty()) {
            Set<String> languageSet = new HashSet<>();
            for (LuceneField field : luceneFields) {
                if (field.getField().contains(SolrConstants.MIXFIX_LANG)) {
                    String lang = field.getField()
                            .substring(field.getField().lastIndexOf(SolrConstants.MIXFIX_LANG) + SolrConstants.MIXFIX_LANG.length())
                            .toLowerCase();
                    languageSet.add(lang);
                }
            }
            if (!languageSet.isEmpty()) {
                languages.addAll(languageSet);
            }
        }
        if (!languages.isEmpty()) {
            for (String language : languages) {
                addToLucene(SolrConstants.LANGUAGE, language);
            }
        }
    }

    /**
     * Returns the first {@link io.goobi.viewer.indexer.model.LuceneField} with the given name.
     *
     * @param name String
     * @return {@link io.goobi.viewer.indexer.model.LuceneField} or null
     * @should return first field with given name
     * @should return null if name not found
     */
    public LuceneField getLuceneFieldWithName(String name) {
        for (LuceneField luceneField : luceneFields) {
            if (luceneField.getField().equals(name)) {
                return luceneField;
            }
        }
        return null;
    }

    /**
     * Returns all {@link io.goobi.viewer.indexer.model.LuceneField}s with the given name.
     *
     * @param name a {@link java.lang.String} object.
     * @should return all fields with given name
     * @should return empty list if name not found
     * @return a {@link java.util.List} object.
     */
    public List<LuceneField> getLuceneFieldsWithName(String name) {
        List<LuceneField> ret = new ArrayList<>();

        for (LuceneField luceneField : luceneFields) {
            if (luceneField.getField().equals(name)) {
                ret.add(luceneField);
            }
        }

        return ret;
    }

    /**
     * Adds a new field with the given name and value to the field list.
     *
     * @param field {@link java.lang.String}
     * @param value {@link java.lang.String}
     * @return {@link java.lang.Boolean} true if sucessful
     * @should add field to list correctly
     */
    public boolean addToLucene(String field, String value) {
        addToGroupIds(field, value);
        removeNonMultivaluedFields(field);

        return luceneFields.add(new LuceneField(field, value));
    }

    /**
     * Adds the given LuceneField to the field list.
     *
     * @param luceneField a {@link io.goobi.viewer.indexer.model.LuceneField} object.
     * @param skipDuplicates if true; fields with the same name and value as existing fields will not be added
     * @return true if field was added; false otherwise
     * @should add fields correctly
     * @should skip duplicates correctly
     */
    public boolean addToLucene(LuceneField luceneField, boolean skipDuplicates) {
        if (luceneField == null) {
            throw new IllegalArgumentException("luceneField may not be null");
        }

        if (skipDuplicates && this.luceneFields.contains(luceneField)) {
            return false;
        }

        addToGroupIds(luceneField.getField(), luceneField.getValue());
        removeNonMultivaluedFields(luceneField.getField());

        return luceneFields.add(luceneField);
    }

    /**
     * Adds all of the given fields to this object's <code>luceneFields</code> (minus duplicates, if so requested).
     * 
     * @param luceneFields
     * @param skipDuplicates
     * @return Number of added fields
     * @should add fields correctly
     * @should skip duplicates correctly
     * @should add duplicates correctly
     */
    public int addAllToLucene(List<LuceneField> luceneFields, boolean skipDuplicates) {
        if (luceneFields == null) {
            throw new IllegalArgumentException("luceneField may not be null");
        }
        if (luceneFields.isEmpty()) {
            return 0;
        }

        int ret = 0;
        for (LuceneField luceneField : luceneFields) {
            if (addToLucene(luceneField, skipDuplicates)) {
                ret++;
            }
        }

        return ret;
    }

    /**
     * Removes existing instances of boolean or sorting fields with the given name from collected Solr fields.
     * 
     * @param field
     * @should remove existing boolean fields
     * @should remove existing sorting fields
     */
    void removeNonMultivaluedFields(String field) {
        if (field == null) {
            throw new IllegalArgumentException("field may not be null");
        }

        if (field.startsWith("BOOL_") || field.startsWith("SORT_")) {
            List<LuceneField> existing = getLuceneFieldsWithName(field);
            if (!existing.isEmpty()) {
                luceneFields.removeAll(existing);
            }
        }
    }

    /**
     * Adds the given field and value to the groupIds map, if field name starts with <code>GROUPID_</code>.
     *
     * @param field {@link java.lang.String}
     * @param value {@link java.lang.String}
     * @should collect group id fields correctly
     */
    protected void addToGroupIds(String field, String value) {
        if (field == null || value == null || !field.startsWith(SolrConstants.PREFIX_GROUPID)) {
            return;
        }
        if (groupIds.get(field) == null) {
            groupIds.put(field, value);
        } else {
            logger.warn("Multiple values for group field '{}'.", field);
        }
    }

    /**
     * <p>
     * removeDuplicateGroupedMetadata.
     * </p>
     *
     * @should remove duplicates correctly
     * @should not remove allowed duplicates
     */
    public void removeDuplicateGroupedMetadata() {
        Set<GroupedMetadata> existing = new HashSet<>();
        List<GroupedMetadata> metadataToRemove = new ArrayList<>();
        for (GroupedMetadata gmd : getGroupedMetadataFields()) {
            if (existing.contains(gmd) && !gmd.isAllowDuplicateValues()) {
                metadataToRemove.add(gmd);
            } else {
                existing.add(gmd);
            }
        }
        if (!metadataToRemove.isEmpty()) {
            for (GroupedMetadata gmd : metadataToRemove) {
                // Do not use removeAll because it will remove all objects equal to the ones in the list
                getGroupedMetadataFields().remove(gmd);
            }
            logger.info("Removed {} duplicate grouped metadata documents.", metadataToRemove.size());
        }
    }

    /**
     * Adds regular and grouped metadata fields from the given list of <code>IndexObject</code>s to this object.
     * 
     * @param childObjectList
     * @should add regular metadata correctly
     * @should add grouped metadata correctly
     * @should avoid regular metadata duplicates
     * @should avoid grouped metadata duplicates
     */
    public void addChildMetadata(List<IndexObject> childObjectList) {
        if (childObjectList == null || childObjectList.isEmpty()) {
            return;
        }

        // Add recursively collected child metadata fields that are configured to be inherited up
        for (IndexObject childObj : childObjectList) {
            logger.debug("Inheriting metadata from child {} to parent {}...", childObj.getLogId(), getLogId());
            if (childObj.getFieldsToInheritToParents().isEmpty()) {
                continue;
            }

            // Add child element's regular metadata fields
            for (LuceneField field : childObj.getLuceneFields()) {
                if (!childObj.getFieldsToInheritToParents().contains(field.getField())) {
                    continue;
                }
                addToLucene(new LuceneField(field), true);
                // Pass the info about inheritance up the hierarchy
                fieldsToInheritToParents.add(field.getField());
                logger.debug("Added field: {}", field);
            }

            // Add child element's grouped metadata fields
            int count = 0;
            for (GroupedMetadata field : childObj.getGroupedMetadataFields()) {
                if (!childObj.getFieldsToInheritToParents().contains(field.getLabel())) {
                    continue;
                }
                // Only add new instance if not a duplicate
                if (!groupedMetadataFields.contains(field)) {
                    groupedMetadataFields.add(new GroupedMetadata(field));
                    count++;
                }
                // Pass the info about inheritance up the hierarchy
                fieldsToInheritToParents.add(field.getLabel());
            }
            if (count > 0) {
                logger.debug("Added {} grouped field(s)", count);
            }
        }
    }

    /**
     * Applies modifications that should happen that the very end, after all values have been collected.
     * 
     * @should add existence booleans correctly
     */
    public void applyFinalModifications() {
        Set<String> existingFields = HashSet.newHashSet(luceneFields.size());
        Set<String> alreadyFinishedFields = new HashSet<>();

        // Collect existing metadata fields
        for (LuceneField field : luceneFields) {
            existingFields.add(field.getField());
        }
        // Check each field configuration where additional modifications might be configured
        List<String> fieldNamesList =
                SolrIndexerDaemon.getInstance().getConfiguration().getMetadataConfigurationManager().getListWithAllFieldNames(sourceDocFormat);
        for (String fieldName : fieldNamesList) {
            List<FieldConfig> fieldConfigList =
                    SolrIndexerDaemon.getInstance().getConfiguration().getMetadataConfigurationManager().getConfigurationListForField(fieldName);
            if (fieldConfigList == null || fieldConfigList.isEmpty()) {
                continue;
            }
            for (FieldConfig fieldConfig : fieldConfigList) {
                if (alreadyFinishedFields.contains(fieldName)) {
                    continue;
                }
                // Existence boolean field
                if (fieldConfig.isAddExistenceBoolean()) {
                    luceneFields.add(new LuceneField(SolrSearchIndex.getBooleanFieldName(fieldName),
                            existingFields.contains(fieldName) ? "true" : "false"));
                    alreadyFinishedFields.add(fieldName);
                }
            }
        }
    }

    /**
     * Sets <code>dateCreated</code> and <code>dateUpdated</code> values appropriately. Use this when source document provides an export date/time. If
     * not used, automatic values will be added later in the indexing process.
     * 
     * @param date Date to set
     * @should set dateCreated only if not yet set
     * @should add dateUpdated only if later
     * @should remove dateUpdated values later than given
     */
    public void populateDateCreatedUpdated(ZonedDateTime date) {
        if (date == null) {
            return;
        }

        Long millis = date.toInstant().toEpochMilli();

        // Set DATECREATED, if new record
        if (dateCreated == -1) {
            setDateCreated(millis);
            logger.info("Using creation timestamp from METS: {}", getDateCreated());
        }

        // Add new DATEUPDATED value
        if (!dateUpdated.contains(millis)) {
            dateUpdated.add(millis);
        }

        // Remove any DATEUPDATED values that come after the one from METS
        Collections.sort(dateUpdated);
        List<Long> toRemove = new ArrayList<>();
        for (long timestamp : dateUpdated) {
            if (timestamp > millis) {
                toRemove.add(timestamp);
            }
        }
        for (long timestamp : toRemove) {
            dateUpdated.remove(timestamp);
            logger.info("Removed false DATEUPDATED value: {}", timestamp);
        }
    }

    /**
     * <p>
     * Setter for the field <code>iddoc</code>.
     * </p>
     *
     * @param iddoc a long.
     */
    public void setIddoc(String iddoc) {
        this.iddoc = iddoc;
    }

    /**
     * <p>
     * Getter for the field <code>iddoc</code>.
     * </p>
     *
     * @return a long.
     */
    public String getIddoc() {
        return iddoc;
    }

    /**
     * <p>
     * Setter for the field <code>volume</code>.
     * </p>
     *
     * @param volume a boolean.
     */
    public void setVolume(boolean volume) {
        this.volume = volume;
    }

    /**
     * <p>
     * isVolume.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isVolume() {
        return volume;
    }

    /**
     * <p>
     * Setter for the field <code>pi</code>.
     * </p>
     *
     * @param pi a {@link java.lang.String} object.
     */
    public void setPi(String pi) {
        this.pi = pi;
    }

    /**
     * <p>
     * Getter for the field <code>pi</code>.
     * </p>
     *
     * @return a {@link java.lang.String} object.
     */
    public String getPi() {
        return pi;
    }

    /**
     * <p>
     * Setter for the field <code>parent</code>.
     * </p>
     *
     * @param parent a {@link io.goobi.viewer.indexer.model.IndexObject} object.
     */
    public void setParent(IndexObject parent) {
        this.parent = parent;
    }

    /**
     * <p>
     * Getter for the field <code>parent</code>.
     * </p>
     *
     * @return a {@link io.goobi.viewer.indexer.model.IndexObject} object.
     */
    public IndexObject getParent() {
        return parent;
    }

    /**
     * @return the docType
     */
    public DocType getDocType() {
        return docType;
    }

    /**
     * @param docType the docType to set
     */
    public void setDocType(DocType docType) {
        this.docType = docType;
    }

    /**
     * @return the sourceDocFormat
     */
    public FileFormat getSourceDocFormat() {
        return sourceDocFormat;
    }

    /**
     * @param sourceDocFormat the sourceDocFormat to set
     */
    public void setSourceDocFormat(FileFormat sourceDocFormat) {
        this.sourceDocFormat = sourceDocFormat;
    }

    /**
     * <p>
     * setDmdid.
     * </p>
     *
     * @param dmdid a {@link java.lang.String} object.
     */
    public void setDmdid(String dmdid) {
        this.dmdId = dmdid;
    }

    /**
     * <p>
     * getDmdid.
     * </p>
     *
     * @return a {@link java.lang.String} object.
     */
    public String getDmdid() {
        return dmdId;
    }

    /**
     * <p>
     * Setter for the field <code>logId</code>.
     * </p>
     *
     * @param logId a {@link java.lang.String} object.
     */
    public void setLogId(String logId) {
        this.logId = logId;
    }

    /**
     * <p>
     * Getter for the field <code>logId</code>.
     * </p>
     *
     * @return a {@link java.lang.String} object.
     */
    public String getLogId() {
        return logId;
    }

    /**
     * <p>
     * Setter for the field <code>type</code>.
     * </p>
     *
     * @param type a {@link java.lang.String} object.
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * <p>
     * Getter for the field <code>type</code>.
     * </p>
     *
     * @return a {@link java.lang.String} object.
     */
    public String getType() {
        return type;
    }

    /**
     * <p>
     * Setter for the field <code>label</code>.
     * </p>
     *
     * @param label a {@link java.lang.String} object.
     */
    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * <p>
     * Getter for the field <code>label</code>.
     * </p>
     *
     * @return a {@link java.lang.String} object.
     */
    public String getLabel() {
        return label;
    }

    /**
     * <p>
     * Getter for the field <code>parentLabels</code>.
     * </p>
     *
     * @return the parentLabels
     */
    public List<String> getParentLabels() {
        return parentLabels;
    }

    /**
     * <p>
     * Setter for the field <code>rootStructNode</code>.
     * </p>
     *
     * @param rootStructNode a {@link org.jdom2.Element} object.
     */
    public void setRootStructNode(Element rootStructNode) {
        this.rootStructNode = rootStructNode;
    }

    /**
     * <p>
     * Getter for the field <code>rootStructNode</code>.
     * </p>
     *
     * @return a {@link org.jdom2.Element} object.
     */
    public Element getRootStructNode() {
        return rootStructNode;
    }

    /**
     * <p>
     * Setter for the field <code>anchor</code>.
     * </p>
     *
     * @param anchor a boolean.
     */
    public void setAnchor(boolean anchor) {
        this.anchor = anchor;
    }

    /**
     * <p>
     * isAnchor.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isAnchor() {
        return anchor;
    }

    /**
     * <p>
     * Setter for the field <code>update</code>.
     * </p>
     *
     * @param update a boolean.
     */
    public void setUpdate(boolean update) {
        this.update = update;
    }

    /**
     * <p>
     * isUpdate.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isUpdate() {
        return update;
    }

    /**
     * <p>
     * Setter for the field <code>defaultValue</code>.
     * </p>
     *
     * @param defaultValue the defaultValue to set
     */
    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    /**
     * <p>
     * Getter for the field <code>defaultValue</code>.
     * </p>
     *
     * @return the defaultValue
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * <p>
     * Getter for the field <code>dateCreated</code>.
     * </p>
     *
     * @return the dateCreated
     */
    public long getDateCreated() {
        return dateCreated;
    }

    /**
     * <p>
     * Setter for the field <code>dateCreated</code>.
     * </p>
     *
     * @param dateCreated the dateCreated to set
     */
    public void setDateCreated(long dateCreated) {
        this.dateCreated = dateCreated;
    }

    /**
     * <p>
     * Getter for the field <code>dateUpdated</code>.
     * </p>
     *
     * @return the dateUpdated
     */
    public List<Long> getDateUpdated() {
        return dateUpdated;
    }

    /**
     * @return the dateIndexed
     */
    public List<Long> getDateIndexed() {
        return dateIndexed;
    }

    /**
     * <p>
     * Getter for the field <code>urn</code>.
     * </p>
     *
     * @return the urn
     */
    public String getUrn() {
        return urn;
    }

    /**
     * <p>
     * Setter for the field <code>urn</code>.
     * </p>
     *
     * @param urn the urn to set
     */
    public void setUrn(String urn) {
        this.urn = urn;
    }

    /**
     * <p>
     * Getter for the field <code>imageUrns</code>.
     * </p>
     *
     * @return the imageUrns
     */
    public List<String> getImageUrns() {
        return imageUrns;
    }

    /**
     * <p>
     * Setter for the field <code>imageUrns</code>.
     * </p>
     *
     * @param imageUrns the imageUrns to set
     */
    public void setImageUrns(List<String> imageUrns) {
        this.imageUrns = imageUrns;
    }

    /**
     * <p>
     * Setter for the field <code>parentPI</code>.
     * </p>
     *
     * @param parentPI the parentPI to set
     */
    public void setParentPI(String parentPI) {
        this.parentPI = parentPI;
    }

    /**
     * <p>
     * Getter for the field <code>parentPI</code>.
     * </p>
     *
     * @return the parentPI
     */
    public String getParentPI() {
        return parentPI;
    }

    /**
     * <p>
     * Getter for the field <code>topstructPI</code>.
     * </p>
     *
     * @return the topstructPI
     */
    public String getTopstructPI() {
        return topstructPI;
    }

    /**
     * <p>
     * Setter for the field <code>topstructPI</code>.
     * </p>
     *
     * @param topstructPI the topstructPI to set
     */
    public void setTopstructPI(String topstructPI) {
        this.topstructPI = topstructPI;
    }

    /**
     * <p>
     * Getter for the field <code>anchorPI</code>.
     * </p>
     *
     * @return the anchorPI
     */
    public String getAnchorPI() {
        return anchorPI;
    }

    /**
     * <p>
     * Setter for the field <code>anchorPI</code>.
     * </p>
     *
     * @param anchorPI the anchorPI to set
     */
    public void setAnchorPI(String anchorPI) {
        this.anchorPI = anchorPI;
    }

    /**
     * <p>
     * Getter for the field <code>groupIds</code>.
     * </p>
     *
     * @return the groupIds
     */
    public Map<String, String> getGroupIds() {
        return groupIds;
    }

    /**
     * <p>
     * Getter for the field <code>accessConditions</code>.
     * </p>
     *
     * @return a {@link java.util.Set} object.
     */
    public Set<String> getAccessConditions() {
        return accessConditions;
    }

    /**
     * <p>
     * Getter for the field <code>thumbnailRepresent</code>.
     * </p>
     *
     * @return the thumbnailRepresent
     */
    public String getThumbnailRepresent() {
        return thumbnailRepresent;
    }

    /**
     * <p>
     * Setter for the field <code>thumbnailRepresent</code>.
     * </p>
     *
     * @param thumbnailRepresent the thumbnailRepresent to set
     */
    public void setThumbnailRepresent(String thumbnailRepresent) {
        this.thumbnailRepresent = thumbnailRepresent;
    }

    /**
     * <p>
     * Getter for the field <code>dataRepository</code>.
     * </p>
     *
     * @return the dataRepository
     */
    public String getDataRepository() {
        return dataRepository;
    }

    /**
     * <p>
     * Setter for the field <code>dataRepository</code>.
     * </p>
     *
     * @param dataRepository the dataRepository to set
     */
    public void setDataRepository(String dataRepository) {
        this.dataRepository = dataRepository;
    }

    /**
     * <p>
     * Getter for the field <code>luceneFields</code>.
     * </p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<LuceneField> getLuceneFields() {
        return luceneFields;
    }

    /**
     * <p>
     * Getter for the field <code>groupedMetadataFields</code>.
     * </p>
     *
     * @return the groupedMetadataFields
     */
    public List<GroupedMetadata> getGroupedMetadataFields() {
        return groupedMetadataFields;
    }

    /**
     * @return the fieldsToInheritToParents
     */
    public Set<String> getFieldsToInheritToParents() {
        return fieldsToInheritToParents;
    }

    /**
     * <p>
     * Setter for the field <code>groupedMetadataFields</code>.
     * </p>
     *
     * @param groupedMetadataFields the groupedMetadataFields to set
     */
    public void setGroupedMetadataFields(List<GroupedMetadata> groupedMetadataFields) {
        this.groupedMetadataFields = groupedMetadataFields;
    }

    /**
     * <p>
     * Getter for the field <code>numPages</code>.
     * </p>
     *
     * @return the numPages
     */
    public int getNumPages() {
        return numPages;
    }

    /**
     * <p>
     * Setter for the field <code>numPages</code>.
     * </p>
     *
     * @param numPages the numPages to set
     */
    public void setNumPages(int numPages) {
        this.numPages = numPages;
    }

    /**
     * <p>
     * Getter for the field <code>firstPageLabel</code>.
     * </p>
     *
     * @return the firstPageLabel
     */
    public String getFirstPageLabel() {
        return firstPageLabel;
    }

    /**
     * <p>
     * Setter for the field <code>firstPageLabel</code>.
     * </p>
     *
     * @param firstPageLabel the firstPageLabel to set
     */
    public void setFirstPageLabel(String firstPageLabel) {
        this.firstPageLabel = firstPageLabel;
    }

    /**
     * <p>
     * Getter for the field <code>lastPageLabel</code>.
     * </p>
     *
     * @return the lastPageLabel
     */
    public String getLastPageLabel() {
        return lastPageLabel;
    }

    /**
     * <p>
     * Setter for the field <code>lastPageLabel</code>.
     * </p>
     *
     * @param lastPageLabel the lastPageLabel to set
     */
    public void setLastPageLabel(String lastPageLabel) {
        this.lastPageLabel = lastPageLabel;
    }

    /**
     * <p>
     * Getter for the field <code>languages</code>.
     * </p>
     *
     * @return the languages
     */
    public Set<String> getLanguages() {
        return languages;
    }
}
