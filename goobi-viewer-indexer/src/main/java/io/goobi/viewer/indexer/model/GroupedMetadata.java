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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdom2.Element;
import org.jdom2.JDOMException;

import de.intranda.digiverso.normdataimporter.NormDataImporter;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.MetadataHelper;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.model.config.FieldConfig;
import io.goobi.viewer.indexer.model.config.GroupEntity;
import io.goobi.viewer.indexer.model.config.SubfieldConfig;

/**
 * <p>
 * GroupedMetadata class.
 * </p>
 *
 */
public class GroupedMetadata {

    private static final Logger logger = LogManager.getLogger(GroupedMetadata.class);

    private String label;
    private String mainValue;
    private String authorityURI;
    private final List<LuceneField> fields = new ArrayList<>();
    private final List<LuceneField> authorityDataFields = new ArrayList<>();
    private boolean addAuthorityDataToDocstruct = false;
    private boolean addCoordsToDocstruct = false;
    /** If true, this field won't be added to he index. */
    private boolean skip = false;
    private boolean allowDuplicateValues = false;
    private List<GroupedMetadata> children = new ArrayList<>();

    /**
     * Empty constructor.
     */
    public GroupedMetadata() {
    }

    /**
     * Cloning constructor.
     * 
     * @param orig {@link GroupedMetadata} to clone
     * @should clone child metadata
     */
    public GroupedMetadata(GroupedMetadata orig) {
        if (orig == null) {
            throw new IllegalArgumentException("orig may not be null");
        }

        setLabel(orig.getLabel());
        setMainValue(orig.getMainValue());
        setAuthorityURI(orig.getAuthorityURI());
        for (LuceneField field : orig.getFields()) {
            fields.add(new LuceneField(field));
        }
        for (GroupedMetadata gmd : orig.getChildren()) {
            getChildren().add(new GroupedMetadata(gmd));
        }
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (fields.hashCode());
        result = prime * result + ((label == null) ? 0 : label.hashCode());
        result = prime * result + ((mainValue == null) ? 0 : mainValue.hashCode());
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GroupedMetadata other = (GroupedMetadata) obj;
        if (fields.size() != other.fields.size()) {
            return false;
        }
        if (label == null) {
            if (other.label != null) {
                return false;
            }
        } else if (!label.equals(other.label)) {
            return false;
        }
        if (mainValue == null) {
            if (other.mainValue != null) {
                return false;
            }
        } else if (!mainValue.equals(other.mainValue)) {
            return false;
        }
        return true;
    }

    /**
     * 
     * @param collectedValues Collected field values for special purposes
     * @param groupEntityFields
     * @param ele Root of the XML (sub)tree
     * @param authorityDataEnabled
     * @param xpathReplacements
     * @param configurationItem Master field configuration
     */
    public void collectGroupMetadataValues(Map<String, List<String>> collectedValues, Map<String, SubfieldConfig> groupEntityFields, Element ele,
            boolean authorityDataEnabled, Map<String, String> xpathReplacements, FieldConfig configurationItem) {
        if (ele == null) {
            throw new IllegalArgumentException("element may not be null");
        }

        logger.debug("element: {}", ele.getName());
        for (Entry<String, SubfieldConfig> entry : groupEntityFields.entrySet()) {
            if (entry.getKey() == null || "type".equals(entry.getKey()) || "url".equals(entry
                    .getKey())
                    || groupEntityFields.get(entry.getKey()) == null) {
                continue;
            }

            SubfieldConfig subfield = entry.getValue();
            for (final String xp : subfield.getXpaths()) {
                String xpath = xp;
                if (xpathReplacements != null && !xpathReplacements.isEmpty()) {
                    boolean replacementKeyFound = false;
                    for (Entry<String, String> xpathReplacementsEntry : xpathReplacements.entrySet()) {
                        xpath = xpath.replace(xpathReplacementsEntry.getKey(), xpathReplacementsEntry.getValue());
                        if (!xpath.equals(xp)) {
                            replacementKeyFound = true;
                        }
                    }
                    if (!replacementKeyFound) {
                        // Skip XPath expressions that don't contain placeholders
                        continue;
                    }
                }
                logger.trace("XPath: {} (relative to {})", xpath, ele.getName());
                List<String> values = JDomXP.evaluateToStringListStatic(xpath, ele);
                if (values == null || values.isEmpty()) {
                    // Use default value, if available
                    if (subfield.getDefaultValues().get(xpath) != null) {
                        values = Collections.singletonList(subfield.getDefaultValues().get(xpath));
                    }
                    if (values == null || values.isEmpty()) {
                        continue;
                    }
                }
                // Trim down to the first value if subfield is not multivalued
                if (!subfield.isMultivalued() && values.size() > 1) {
                    logger.debug("{} is not multivalued", subfield.getFieldname());
                    values = values.subList(0, 1);
                }
                for (Object val : values) {
                    String fieldValue = JDomXP.objectToString(val);
                    fieldValue = MetadataHelper.applyAllModifications(configurationItem, fieldValue.trim());
                    if (StringUtils.isBlank(fieldValue)) {
                        continue;
                    }

                    if ("MD_PERSON_PRINTER".equals(label) && "MD_VALUE".equals(subfield.getFieldname())) {
                        logger.info("found: {}:{}", subfield.getFieldname(), fieldValue);
                    }
                    logger.debug("found: {}:{}", subfield.getFieldname(), fieldValue);

                    if (authorityDataEnabled && subfield.getFieldname().startsWith(NormDataImporter.FIELD_URI) && fieldValue.length() > 1) {
                        // Skip values that probably aren't real identifiers or URIs
                        if (NormDataImporter.FIELD_URI.equals(subfield.getFieldname())) {
                            setAuthorityURI(fieldValue);
                        }
                        // Add GND URL part, if the value is not a URL
                        if (!fieldValue.startsWith("http")) {
                            fieldValue = "https://d-nb.info/gnd/" + fieldValue;
                        }
                    }

                    // Add value to this object
                    fields.add(new LuceneField(subfield.getFieldname(), fieldValue));

                    // Add sorting field
                    if (subfield.isAddSortField() && !subfield.getFieldname().startsWith(SolrConstants.PREFIX_SORT) && values.indexOf(val) == 0) {
                        fields.add(new LuceneField(Utils.sortifyField(subfield.getFieldname()), fieldValue));
                    }

                    if (!collectedValues.containsKey(fieldValue)) {
                        collectedValues.put(subfield.getFieldname(), new ArrayList<>(values.size()));
                    }
                    collectedValues.get(subfield.getFieldname()).add(fieldValue);
                }
            }
        }
    }

    /**
     * 
     * @param groupEntity
     * @param collectedValues
     * @param configurationItem
     */
    public void harvestCitationMetadataFromUrl(GroupEntity groupEntity, Map<String, List<String>> collectedValues, FieldConfig configurationItem) {
        if (groupEntity == null) {
            throw new IllegalArgumentException("groupEntity may not be null");
        }

        if (collectedValues == null) {
            throw new IllegalArgumentException("collectedValues may not be null");
        }
        if (StringUtils.isEmpty(groupEntity.getUrl())) {
            logger.warn("Citation metadata field {} is missing a URL.", getLabel());
            return;
        }

        try {
            CitationXmlDocument xmlDoc = new CitationXmlDocument(groupEntity.getUrl())
                    .prepareURL(collectedValues)
                    .fetch()
                    .build();
            collectGroupMetadataValues(collectedValues, groupEntity.getSubfields(),
                    xmlDoc.getXp().getRootElement(), MetadataHelper.isAuthorityDataEnabled(), null, configurationItem);
        } catch (HTTPException | JDOMException | IOException | IllegalStateException e) {
            logger.error(e.getMessage(), e);
        }

    }

    /**
     * <p>
     * Getter for the field <code>label</code>.
     * </p>
     *
     * @return the label
     */
    public String getLabel() {
        return label;
    }

    /**
     * <p>
     * Setter for the field <code>label</code>.
     * </p>
     *
     * @param label the label to set
     */
    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * <p>
     * Getter for the field <code>mainValue</code>.
     * </p>
     *
     * @return the mainValue
     */
    public String getMainValue() {
        return mainValue;
    }

    /**
     * <p>
     * Setter for the field <code>mainValue</code>.
     * </p>
     *
     * @param mainValue the mainValue to set
     */
    public void setMainValue(String mainValue) {
        this.mainValue = mainValue;
    }

    /**
     * <p>
     * Getter for the field <code>authorityURI</code>.
     * </p>
     *
     * @return the authorityURI
     */
    public String getAuthorityURI() {
        return authorityURI;
    }

    /**
     * <p>
     * Setter for the field <code>authorityURI</code>.
     * </p>
     *
     * @param authorityURI the authorityURI to set
     */
    public void setAuthorityURI(String authorityURI) {
        this.authorityURI = authorityURI;
    }

    /**
     * <p>
     * Getter for the field <code>fields</code>.
     * </p>
     *
     * @return the fields
     */
    public List<LuceneField> getFields() {
        return fields;
    }

    /**
     * @return the authorityDataFields
     */
    public List<LuceneField> getAuthorityDataFields() {
        return authorityDataFields;
    }

    /**
     * @return the addAuthorityDataToDocstruct
     */
    public boolean isAddAuthorityDataToDocstruct() {
        return addAuthorityDataToDocstruct;
    }

    /**
     * @param addAuthorityDataToDocstruct the addAuthorityDataToDocstruct to set
     */
    public void setAddAuthorityDataToDocstruct(boolean addAuthorityDataToDocstruct) {
        this.addAuthorityDataToDocstruct = addAuthorityDataToDocstruct;
    }

    /**
     * @return the addCoordsToDocstruct
     */
    public boolean isAddCoordsToDocstruct() {
        return addCoordsToDocstruct;
    }

    /**
     * @param addCoordsToDocstruct the addCoordsToDocstruct to set
     */
    public void setAddCoordsToDocstruct(boolean addCoordsToDocstruct) {
        this.addCoordsToDocstruct = addCoordsToDocstruct;
    }

    /**
     * @return the skip
     */
    public boolean isSkip() {
        return skip;
    }

    /**
     * @param skip the skip to set
     */
    public void setSkip(boolean skip) {
        this.skip = skip;
    }

    /**
     * @return the allowDuplicateValues
     */
    public boolean isAllowDuplicateValues() {
        return allowDuplicateValues;
    }

    /**
     * @param allowDuplicateValues the allowDuplicateValues to set
     */
    public void setAllowDuplicateValues(boolean allowDuplicateValues) {
        this.allowDuplicateValues = allowDuplicateValues;
    }

    /**
     * @return the children
     */
    public List<GroupedMetadata> getChildren() {
        return children;
    }

    /**
     * @param children the children to set
     */
    public void setChildren(List<GroupedMetadata> children) {
        this.children = children;
    }
}
