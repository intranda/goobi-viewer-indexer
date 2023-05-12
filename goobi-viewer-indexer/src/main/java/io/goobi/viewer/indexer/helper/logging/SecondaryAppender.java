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

import java.io.Serializable;
import java.io.StringWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * Appender that logs the indexing of a single records. The contents can be then e-mailed to configured recipients in case of errors.
 */
@Plugin(name = "SecondaryAppender", category = "Core", elementType = "appender", printObject = false)
public class SecondaryAppender extends AbstractAppender {

    private static final Logger logger = LogManager.getLogger(SecondaryAppender.class);

    private static final StringWriter writer = new StringWriter();

    /**
     * Constructor.
     * 
     * @param name
     * @param filter
     * @param layout
     * @param ignoreExceptions
     */
    protected SecondaryAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions) {
        super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent logEvent) {
        logger.trace("APPENDING ({} / {})", System.identityHashCode(this), System.identityHashCode(writer));
        writer.append(getLayout().toSerializable(logEvent).toString());
    }

    @PluginFactory
    public static SecondaryAppender createAppender(@PluginAttribute("name") String name,
            @PluginElement("Layout") Layout<? extends Serializable> layout, @PluginElement("Filter") final Filter filter) {
        if (name == null) {
            logger.error("No name provided for SecondaryAppender");
            return null;
        }
        if (layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }

        return new SecondaryAppender(name, filter, layout, true);
    }

    public String getLog() {
        logger.trace("getLog ({} / {})", System.identityHashCode(this), System.identityHashCode(writer));
        return writer.toString();
    }

    /**
     * @should reset writer correctly
     */
    public void reset() {
        logger.info("resetting writer");
        writer.getBuffer().setLength(0);
    }
}