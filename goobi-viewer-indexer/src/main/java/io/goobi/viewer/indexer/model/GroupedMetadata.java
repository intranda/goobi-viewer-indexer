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

import java.util.ArrayList;
import java.util.List;

/**
 * <p>GroupedMetadata class.</p>
 *
 */
public class GroupedMetadata {

    private String label;
    private String mainValue;
    private String normUri;
    private final List<LuceneField> fields;

    /**
     * <p>Constructor for GroupedMetadata.</p>
     */
    public GroupedMetadata() {
        fields = new ArrayList<>();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fields == null) ? 0 : fields.hashCode());
        result = prime * result + ((label == null) ? 0 : label.hashCode());
        result = prime * result + ((mainValue == null) ? 0 : mainValue.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        GroupedMetadata other = (GroupedMetadata) obj;
        if (fields == null) {
            if (other.fields != null)
                return false;
        } else if (!fields.equals(other.fields))
            return false;
        if (label == null) {
            if (other.label != null)
                return false;
        } else if (!label.equals(other.label))
            return false;
        if (mainValue == null) {
            if (other.mainValue != null)
                return false;
        } else if (!mainValue.equals(other.mainValue))
            return false;
        return true;
    }

    /**
     * <p>Getter for the field <code>label</code>.</p>
     *
     * @return the label
     */
    public String getLabel() {
        return label;
    }

    /**
     * <p>Setter for the field <code>label</code>.</p>
     *
     * @param label the label to set
     */
    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * <p>Getter for the field <code>mainValue</code>.</p>
     *
     * @return the mainValue
     */
    public String getMainValue() {
        return mainValue;
    }

    /**
     * <p>Setter for the field <code>mainValue</code>.</p>
     *
     * @param mainValue the mainValue to set
     */
    public void setMainValue(String mainValue) {
        this.mainValue = mainValue;
    }

    /**
     * <p>Getter for the field <code>normUri</code>.</p>
     *
     * @return the normUri
     */
    public String getNormUri() {
        return normUri;
    }

    /**
     * <p>Setter for the field <code>normUri</code>.</p>
     *
     * @param normUri the normUri to set
     */
    public void setNormUri(String normUri) {
        this.normUri = normUri;
    }

    /**
     * <p>Getter for the field <code>fields</code>.</p>
     *
     * @return the fields
     */
    public List<LuceneField> getFields() {
        return fields;
    }

}
