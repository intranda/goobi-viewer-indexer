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
package io.goobi.viewer.indexer.model.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration object for a single grouped metadata subfield configuration.
 */
public class SubfieldConfig {

    private final String fieldname;
    private final boolean multivalued;
    private final List<String> xpaths = new ArrayList<>();
    private final Map<String, String> defaultValues = new HashMap<>();

    /**
     * <p>
     * Getter for the field <code>xpaths</code>.
     * </p>
     *
     * @return the xpaths
     */
    public SubfieldConfig(String fieldname, boolean multivalued) {
        this.fieldname = fieldname;
        this.multivalued = multivalued;
    }

    /**
     * <p>
     * Getter for the field <code>fieldname</code>.
     * </p>
     *
     * @return the fieldname
     */
    public String getFieldname() {
        return fieldname;
    }

    /**
     * <p>
     * isMultivalued.
     * </p>
     *
     * @return the multivalued
     */
    public boolean isMultivalued() {
        return multivalued;
    }

    /**
     * @return the xpaths
     */
    public List<String> getXpaths() {
        return xpaths;
    }

    /**
     * @return the defaultValues
     */
    public Map<String, String> getDefaultValues() {
        return defaultValues;
    }
}
