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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * Runtime adjustments of the logging configuration packaged with the indexer.
 */
public final class LoggingTools {

    private static final Logger logger = LogManager.getLogger(LoggingTools.class);

    /** Name of the console appender in log4j2.xml. */
    public static final String CONSOLE_APPENDER_NAME = "stdout";

    private LoggingTools() {
    }

    /**
     * Attaches the console appender to the root logger or detaches it. Does nothing if the logging configuration has no console appender or
     * the appender is already in the requested state.
     *
     * @param enabled true to log to the console; false otherwise
     * @return true if the state was changed; false otherwise
     * @should detach and reattach console appender
     * @should not change state if already in requested state
     */
    public static synchronized boolean setConsoleAppenderEnabled(boolean enabled) {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration config = context.getConfiguration();
        Appender appender = config.getAppender(CONSOLE_APPENDER_NAME);
        if (appender == null) {
            return false;
        }

        LoggerConfig rootConfig = config.getRootLogger();
        boolean attached = rootConfig.getAppenders().containsKey(CONSOLE_APPENDER_NAME);
        if (enabled == attached) {
            return false;
        }

        if (enabled) {
            rootConfig.addAppender(appender, null, null);
        } else {
            rootConfig.removeAppender(CONSOLE_APPENDER_NAME);
        }
        context.updateLoggers();
        logger.info("Console log output {}.", enabled ? "enabled" : "disabled");
        return true;
    }

    /**
     * @return true if the console appender is attached to the root logger; false otherwise
     */
    public static boolean isConsoleAppenderEnabled() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        return context.getConfiguration().getRootLogger().getAppenders().containsKey(CONSOLE_APPENDER_NAME);
    }
}
