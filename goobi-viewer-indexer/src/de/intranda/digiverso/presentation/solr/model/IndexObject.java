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
package de.intranda.digiverso.presentation.solr.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jdom2.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.helper.MetadataHelper;
import de.intranda.digiverso.presentation.solr.model.SolrConstants.DocType;

/**
 * Object representing a docstruct.
 */
public class IndexObject {

    private static final Logger logger = LoggerFactory.getLogger(IndexObject.class);

    private long iddoc;
    private String pi;
    private IndexObject parent = null;
    private boolean update = false;
    private long dateCreated = -1;
    private final List<Long> dateUpdated = new ArrayList<>();

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
    private final Map<String, String> groupIds = new HashMap<>();
    private String urn;
    private List<String> imageUrns;
    private final Set<String> accessConditions = new HashSet<>();
    private String thumbnailRepresent = null;
    private String dataRepository;
    private final List<LuceneField> luceneFields = new ArrayList<>();
    private List<GroupedMetadata> groupedMetadataFields = new ArrayList<>();
    private int numPages = 0;
    private String firstPageLabel;
    private String lastPageLabel;
    /** Available language versions for this record. */
    private final Set<String> languages = new HashSet<>();

    /**
     * @param iddoc
     */
    public IndexObject(long iddoc) {
        this.iddoc = iddoc;
    }

    /**
     * 
     * @param iddoc
     * @param pi
     * @should set attributes correctly
     */
    public IndexObject(long iddoc, String pi) {
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
        addToLucene(SolrConstants.DOCTYPE, DocType.DOCSTRCT.name());
        addToLucene(SolrConstants.PI, pi);
        addToLucene(SolrConstants.PI_TOPSTRUCT, topstructPI);
        if (StringUtils.isNotEmpty(parentPI)) {
            addToLucene(SolrConstants.PI_PARENT, parentPI);
            addToLucene(SolrConstants.PI_ANCHOR, parentPI);
        }
        addToLucene(SolrConstants.LABEL, MetadataHelper.applyValueDefaultModifications(getLabel()));
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
        // Add DOCSTRCT_SUB to true subelements (no volumes)
        if (parent != null && pi == null) {
            addToLucene(SolrConstants.DOCSTRCT_SUB, getType());
        }
        addToLucene(SolrConstants.DATAREPOSITORY, getDataRepository());
    }

    /**
     * Writes ACCESSCONDITION fields for every string in the <code>accessConditions</code> list. Should be done in the end so that only docs with no
     * access condition get the open access value.
     * 
     * @should inherit access conditions from parent except OPENACCESS
     * @should not inherit access conditions from parent if own access conditions exist
     * @should add OPENACCESS if list empty
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
            // logger.debug("Writing access condition '" + s + "' to " + getIddoc());
            addToLucene(SolrConstants.ACCESSCONDITION, s);
        }
    }

    /**
     * Writes created/updated timestamps.
     * 
     * @param updateDateUpdated DATEUPDATED will be set to the current timestamp if true; old value will be used if false.
     * @should set DATECREATED if not set
     * @should not set DATECREATED if already set
     * @should set DATEUPDATED if not set
     * @should not set DATEUPDATED if already set
     * @should set DATEUPDATED if update requested
     */
    public void writeDateModified(boolean updateDateUpdated) {
        long now = System.currentTimeMillis();
        if (getDateCreated() == -1) {
            setDateCreated(now);
        }
        if (updateDateUpdated || dateUpdated.isEmpty()) {
            dateUpdated.add(now);
        }

        addToLucene(SolrConstants.DATECREATED, String.valueOf(getDateCreated()));
        long latest = 0;
        for (Long date : getDateUpdated()) {
            addToLucene(SolrConstants.DATEUPDATED, String.valueOf(date));
            if (date > latest) {
                date = latest;
            }
        }
        // Add latest DATEUPDATED value as SORT_DATEUPDATED
        if (latest > 0) {
            addToLucene(SolrConstants.SORT_ + SolrConstants.DATEUPDATED, String.valueOf(latest));
        }
    }

    /**
     * @should add languages from metadata fields
     */
    public void writeLanguages() {
        if (languages.isEmpty()) {
            Set<String> languageSet = new HashSet<>();
            for (LuceneField field : luceneFields) {
                if (field.getField().contains(SolrConstants._LANG_)) {
                    String lang = field.getField()
                            .substring(field.getField().lastIndexOf(SolrConstants._LANG_) + SolrConstants._LANG_.length())
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
     * Returns the first {@link LuceneField} with the given name.
     * 
     * @param name String
     * @return {@link LuceneField} or null
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
     * Returns all {@link LuceneField}s with the given name.
     * 
     * @param name
     * @return
     * @should return all fields with given name
     * @should return empty list if name not found
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
     * @param field {@link String}
     * @param value {@link String}
     * @return {@link Boolean} true if sucessful
     * @should add field to list correctly
     */
    public boolean addToLucene(String field, String value) {
        addToGroupIds(field, value);
        removeExistingFields(field);

        return luceneFields.add(new LuceneField(field, value));
    }

    /**
     * Adds the given LuceneField to the field list.
     * 
     * @param luceneField
     * @return
     */
    public boolean addToLucene(LuceneField luceneField) {
        if (luceneField == null) {
            throw new IllegalArgumentException("luceneField may not be null");
        }

        addToGroupIds(luceneField.getField(), luceneField.getValue());
        removeExistingFields(luceneField.getField());

        return luceneFields.add(luceneField);
    }

    /**
     * Removes existing instances of boolean fields with the given name from collected lucene fields.
     * 
     * @param field
     * @should remove existing boolean fields
     */
    void removeExistingFields(String field) {
        if (field == null) {
            throw new IllegalArgumentException("field may not be null");
        }

        if (field.startsWith("BOOL_")) {
            List<LuceneField> existing = getLuceneFieldsWithName(field);
            if (!existing.isEmpty()) {
                luceneFields.removeAll(existing);
            }
        }
    }

    /**
     * Adds the given field and value to the groupIds map, if field name starts with <code>GROUPID_</code>.
     * 
     * @param field {@link String}
     * @param value {@link String}
     * @should collect group id fields correctly
     */
    protected void addToGroupIds(String field, String value) {
        if (field != null && value != null && field.startsWith(SolrConstants.GROUPID_)) {
            if (groupIds.get(field) == null) {
                groupIds.put(field, value);
            } else {
                logger.warn("Multiple values for group field '{}'.", field);
            }
        }
    }

    /**
     * @should remove duplicates correctly
     */
    public void removeDuplicateGroupedMetadata() {
        Set<GroupedMetadata> existing = new HashSet<>();
        List<GroupedMetadata> metadataToRemove = new ArrayList<>();
        for (GroupedMetadata gmd : getGroupedMetadataFields()) {
            if (existing.contains(gmd)) {
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

    public void setIddoc(long iddoc) {
        this.iddoc = iddoc;
    }

    public long getIddoc() {
        return iddoc;
    }

    public void setVolume(boolean volume) {
        this.volume = volume;
    }

    public boolean isVolume() {
        return volume;
    }

    public void setPi(String pi) {
        this.pi = pi;
    }

    public String getPi() {
        return pi;
    }

    public void setParent(IndexObject parent) {
        this.parent = parent;
    }

    public IndexObject getParent() {
        return parent;
    }

    public void setDmdid(String dmdid) {
        this.dmdId = dmdid;
    }

    public String getDmdid() {
        return dmdId;
    }

    public void setLogId(String logId) {
        this.logId = logId;
    }

    public String getLogId() {
        return logId;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    /**
     * @return the parentLabels
     */
    public List<String> getParentLabels() {
        return parentLabels;
    }

    public void setRootStructNode(Element rootStructNode) {
        this.rootStructNode = rootStructNode;
    }

    public Element getRootStructNode() {
        return rootStructNode;
    }

    public void setAnchor(boolean anchor) {
        this.anchor = anchor;
    }

    public boolean isAnchor() {
        return anchor;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public boolean isUpdate() {
        return update;
    }

    /**
     * @param defaultValue the defaultValue to set
     */
    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    /**
     * @return the defaultValue
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * @return the dateCreated
     */
    public long getDateCreated() {
        return dateCreated;
    }

    /**
     * @param dateCreated the dateCreated to set
     */
    public void setDateCreated(long dateCreated) {
        this.dateCreated = dateCreated;
    }

    /**
     * @return the dateUpdated
     */
    public List<Long> getDateUpdated() {
        return dateUpdated;
    }

    /**
     * @return the urn
     */
    public String getUrn() {
        return urn;
    }

    /**
     * @param urn the urn to set
     */
    public void setUrn(String urn) {
        this.urn = urn;
    }

    /**
     * @return the imageUrns
     */
    public List<String> getImageUrns() {
        return imageUrns;
    }

    /**
     * @param imageUrns the imageUrns to set
     */
    public void setImageUrns(List<String> imageUrns) {
        this.imageUrns = imageUrns;
    }

    /**
     * @param parentPI the parentPI to set
     */
    public void setParentPI(String parentPI) {
        this.parentPI = parentPI;
    }

    /**
     * @return the parentPI
     */
    public String getParentPI() {
        return parentPI;
    }

    /**
     * @return the topstructPI
     */
    public String getTopstructPI() {
        return topstructPI;
    }

    /**
     * @param topstructPI the topstructPI to set
     */
    public void setTopstructPI(String topstructPI) {
        this.topstructPI = topstructPI;
    }

    /**
     * @return the groupIds
     */
    public Map<String, String> getGroupIds() {
        return groupIds;
    }

    public Set<String> getAccessConditions() {
        return accessConditions;
    }

    /**
     * @return the thumbnailRepresent
     */
    public String getThumbnailRepresent() {
        return thumbnailRepresent;
    }

    /**
     * @param thumbnailRepresent the thumbnailRepresent to set
     */
    public void setThumbnailRepresent(String thumbnailRepresent) {
        this.thumbnailRepresent = thumbnailRepresent;
    }

    /**
     * @return the dataRepository
     */
    public String getDataRepository() {
        return dataRepository;
    }

    /**
     * @param dataRepository the dataRepository to set
     */
    public void setDataRepository(String dataRepository) {
        this.dataRepository = dataRepository;
    }

    public List<LuceneField> getLuceneFields() {
        return luceneFields;
    }

    /**
     * @return the groupedMetadataFields
     */
    public List<GroupedMetadata> getGroupedMetadataFields() {
        return groupedMetadataFields;
    }

    /**
     * @param groupedMetadataFields the groupedMetadataFields to set
     */
    public void setGroupedMetadataFields(List<GroupedMetadata> groupedMetadataFields) {
        this.groupedMetadataFields = groupedMetadataFields;
    }

    /**
     * @return the numPages
     */
    public int getNumPages() {
        return numPages;
    }

    /**
     * @param numPages the numPages to set
     */
    public void setNumPages(int numPages) {
        this.numPages = numPages;
    }

    /**
     * @return the firstPageLabel
     */
    public String getFirstPageLabel() {
        return firstPageLabel;
    }

    /**
     * @param firstPageLabel the firstPageLabel to set
     */
    public void setFirstPageLabel(String firstPageLabel) {
        this.firstPageLabel = firstPageLabel;
    }

    /**
     * @return the lastPageLabel
     */
    public String getLastPageLabel() {
        return lastPageLabel;
    }

    /**
     * @param lastPageLabel the lastPageLabel to set
     */
    public void setLastPageLabel(String lastPageLabel) {
        this.lastPageLabel = lastPageLabel;
    }

    /**
     * @return the languages
     */
    public Set<String> getLanguages() {
        return languages;
    }
}
