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
package io.goobi.viewer.indexer.helper.logging;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LoggingToolsTest {

    @BeforeEach
    void setUp() {
        // The packaged logging configuration attaches the console appender to the root logger
        LoggingTools.setConsoleAppenderEnabled(true);
        assertTrue(LoggingTools.isConsoleAppenderEnabled());
    }

    @AfterEach
    void tearDown() {
        LoggingTools.setConsoleAppenderEnabled(true);
    }

    /**
     * @see LoggingTools#setConsoleAppenderEnabled(boolean)
     * @verifies detach and reattach console appender
     */
    @Test
    void setConsoleAppenderEnabled_shouldDetachAndReattachConsoleAppender() {
        assertTrue(LoggingTools.setConsoleAppenderEnabled(false));
        assertFalse(LoggingTools.isConsoleAppenderEnabled());

        assertTrue(LoggingTools.setConsoleAppenderEnabled(true));
        assertTrue(LoggingTools.isConsoleAppenderEnabled());
    }

    /**
     * @see LoggingTools#setConsoleAppenderEnabled(boolean)
     * @verifies not change state if already in requested state
     */
    @Test
    void setConsoleAppenderEnabled_shouldNotChangeStateIfAlreadyInRequestedState() {
        // Already attached: no change
        assertFalse(LoggingTools.setConsoleAppenderEnabled(true));
        assertTrue(LoggingTools.isConsoleAppenderEnabled());
        // Detaching once is enough
        assertTrue(LoggingTools.setConsoleAppenderEnabled(false));
        assertFalse(LoggingTools.setConsoleAppenderEnabled(false));
        assertFalse(LoggingTools.isConsoleAppenderEnabled());
    }
}
