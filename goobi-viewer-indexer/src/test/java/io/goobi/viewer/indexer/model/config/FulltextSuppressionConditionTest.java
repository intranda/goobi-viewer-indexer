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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.jdom2.Document;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.XmlTools;
import io.goobi.viewer.indexer.model.config.FulltextSuppressionCondition.MatchMode;

class FulltextSuppressionConditionTest extends AbstractTest {

    private static final String XML = "<record><access>no-fulltext allowed</access><status>restricted</status></record>";

    private static JDomXP buildXp() throws Exception {
        Document doc = XmlTools.getDocumentFromString(XML, null);
        return new JDomXP(doc);
    }

    /**
     * @see FulltextSuppressionCondition#FulltextSuppressionCondition(String,String,MatchMode)
     * @verifies throw IllegalArgumentException if xpath blank
     */
    @Test
    void constructor_shouldThrowIfXpathBlank() {
        assertThrows(IllegalArgumentException.class, () -> new FulltextSuppressionCondition(" ", "x", MatchMode.EXISTS));
    }

    /**
     * @see FulltextSuppressionCondition#FulltextSuppressionCondition(String,String,MatchMode)
     * @verifies throw IllegalArgumentException if mode null
     */
    @Test
    void constructor_shouldThrowIfModeNull() {
        assertThrows(IllegalArgumentException.class, () -> new FulltextSuppressionCondition("/record/access", "x", null));
    }

    /**
     * @see FulltextSuppressionCondition#FulltextSuppressionCondition(String,String,MatchMode)
     * @verifies throw IllegalArgumentException if value empty and mode not EXISTS
     */
    @Test
    void constructor_shouldThrowIfValueEmptyAndModeNotExists() {
        assertThrows(IllegalArgumentException.class, () -> new FulltextSuppressionCondition("/record/access", "", MatchMode.EQUALS));
        assertThrows(IllegalArgumentException.class, () -> new FulltextSuppressionCondition("/record/access", null, MatchMode.CONTAINS));
    }

    /**
     * @see FulltextSuppressionCondition#matches(JDomXP)
     * @verifies return false if no values found
     */
    @Test
    void matches_shouldReturnFalseIfNoValuesFound() throws Exception {
        FulltextSuppressionCondition condition = new FulltextSuppressionCondition("/record/missing/text()", null, MatchMode.EXISTS);
        assertFalse(condition.matches(buildXp()));
    }

    /**
     * @see FulltextSuppressionCondition#matches(JDomXP)
     * @verifies match if value exists
     */
    @Test
    void matches_shouldMatchIfValueExists() throws Exception {
        FulltextSuppressionCondition condition = new FulltextSuppressionCondition("/record/access/text()", null, MatchMode.EXISTS);
        assertTrue(condition.matches(buildXp()));
    }

    /**
     * @see FulltextSuppressionCondition#matches(JDomXP)
     * @verifies match if value equals
     */
    @Test
    void matches_shouldMatchIfValueEquals() throws Exception {
        FulltextSuppressionCondition condition = new FulltextSuppressionCondition("/record/status/text()", "restricted", MatchMode.EQUALS);
        assertTrue(condition.matches(buildXp()));
        FulltextSuppressionCondition noMatch = new FulltextSuppressionCondition("/record/status/text()", "open", MatchMode.EQUALS);
        assertFalse(noMatch.matches(buildXp()));
    }

    /**
     * @see FulltextSuppressionCondition#matches(JDomXP)
     * @verifies match if value contains
     */
    @Test
    void matches_shouldMatchIfValueContains() throws Exception {
        FulltextSuppressionCondition condition = new FulltextSuppressionCondition("/record/access/text()", "no-fulltext", MatchMode.CONTAINS);
        assertTrue(condition.matches(buildXp()));
        FulltextSuppressionCondition noMatch = new FulltextSuppressionCondition("/record/access/text()", "missing", MatchMode.CONTAINS);
        assertFalse(noMatch.matches(buildXp()));
    }
}
