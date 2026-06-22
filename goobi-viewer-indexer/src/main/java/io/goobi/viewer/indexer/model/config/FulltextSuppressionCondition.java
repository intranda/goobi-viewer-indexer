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

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import io.goobi.viewer.indexer.helper.JDomXP;

/**
 * <p>
 * Holds a single full-text suppression condition: an XPath expression plus an optional expected value and a match mode. If the condition matches the
 * record's source XML, full-text indexing is disabled for that record.
 * </p>
 * <p>
 * The XPath expression should select string values (i.e. end in {@code /text()} or {@code /@attribute}), as it is evaluated via
 * {@link JDomXP#evaluateToStringList(String, Object)}.
 * </p>
 */
public class FulltextSuppressionCondition {

    /**
     * Determines how the value found at the XPath is compared to the configured value.
     */
    public enum MatchMode {
        /** The condition matches if any non-blank value exists at the XPath. The configured value is ignored. */
        EXISTS,
        /** The condition matches if any value at the XPath equals (after trimming) the configured value. */
        EQUALS,
        /** The condition matches if any value at the XPath contains the configured value as a substring. */
        CONTAINS;
    }

    private final String xpath;
    private final String value;
    private final MatchMode mode;

    /**
     * Constructor.
     *
     * @param xpath XPath expression selecting a string value; may not be blank
     * @param value expected value; may be null/empty only for {@link MatchMode#EXISTS}
     * @param mode {@link MatchMode}; may not be null
     * @should throw IllegalArgumentException if xpath blank
     * @should throw IllegalArgumentException if mode null
     * @should throw IllegalArgumentException if value null and mode not EXISTS
     */
    public FulltextSuppressionCondition(String xpath, String value, MatchMode mode) {
        if (StringUtils.isBlank(xpath)) {
            throw new IllegalArgumentException("xpath may not be blank");
        }
        if (mode == null) {
            throw new IllegalArgumentException("mode may not be null");
        }
        if (mode != MatchMode.EXISTS && StringUtils.isEmpty(value)) {
            throw new IllegalArgumentException("value may not be empty for match mode " + mode);
        }
        this.xpath = xpath;
        this.value = value;
        this.mode = mode;
    }

    /**
     * Evaluates this condition against the given record XML.
     *
     * @param xp {@link JDomXP} wrapping the record's source document
     * @return true if the condition matches; false otherwise
     * @should return false if no values found
     * @should match if value exists
     * @should match if value equals
     * @should match if value contains
     * @should not match if value differs
     */
    public boolean matches(JDomXP xp) {
        if (xp == null) {
            return false;
        }
        List<String> results = xp.evaluateToStringList(xpath, null);
        if (results == null || results.isEmpty()) {
            return false;
        }
        for (String result : results) {
            if (result == null) {
                continue;
            }
            switch (mode) {
                case EXISTS:
                    if (StringUtils.isNotBlank(result)) {
                        return true;
                    }
                    break;
                case EQUALS:
                    if (result.trim().equals(value)) {
                        return true;
                    }
                    break;
                case CONTAINS:
                    if (result.contains(value)) {
                        return true;
                    }
                    break;
                default:
                    break;
            }
        }
        return false;
    }

    /**
     * @return the xpath
     */
    public String getXpath() {
        return xpath;
    }

    /**
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * @return the mode
     */
    public MatchMode getMode() {
        return mode;
    }
}
