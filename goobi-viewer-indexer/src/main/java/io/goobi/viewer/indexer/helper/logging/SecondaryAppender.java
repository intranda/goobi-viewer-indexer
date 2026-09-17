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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
 * Appender that captures the log of a single record's indexing so it can be e-mailed to configured recipients on error.
 * <p>
 * With record-level parallelism ({@code performance.recordThreads} &gt; 1) several records are indexed on different
 * threads at once, so a single shared buffer would interleave and clobber. Instead this appender keeps one buffer per
 * record, keyed by the {@link org.apache.logging.log4j.ThreadContext} (MDC) value {@value #MDC_KEY}, which the indexer
 * sets to the record's file name at the start of processing and propagates to the intra-record worker pools. Log events
 * without that key (framework/startup logging) collect under an empty-string key and are ignored by callers.
 */
@Plugin(name = "SecondaryAppender", category = "Core", elementType = "appender", printObject = false)
public class SecondaryAppender extends AbstractAppender {

    private static final Logger logger = LogManager.getLogger(SecondaryAppender.class);

    /**
     * MDC/{@link org.apache.logging.log4j.ThreadContext} key for the <b>displayed</b> record tag (referenced as
     * {@code %X{rec}} in the log pattern). It starts as the record's file name and is upgraded to the PI once the record
     * is parsed, so log lines carry the most meaningful identifier available at the time they were written.
     */
    public static final String MDC_KEY = "rec";

    /**
     * MDC key for the <b>stable</b> record identifier used to route events into this appender's per-record buffer. Unlike
     * {@link #MDC_KEY} it never changes during a record's processing, so all of a record's lines (before and after the PI
     * becomes known) land in the same buffer and the error e-mail contains the complete log.
     */
    public static final String MDC_KEY_ID = "recId";

    private static final String NO_RECORD_KEY = "";

    /** One buffer per record identifier. {@link StringBuffer} is used because a record's worker pool threads append concurrently. */
    private static final Map<String, StringBuffer> BUFFERS = new ConcurrentHashMap<>();

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
        BUFFERS.computeIfAbsent(resolveKey(logEvent), k -> new StringBuffer()).append(getLayout().toSerializable(logEvent).toString());
    }

    /**
     * Resolves the record key for the given event from its context data, falling back to {@link #NO_RECORD_KEY}.
     *
     * @param logEvent event to inspect
     * @return record key, never null
     */
    private static String resolveKey(LogEvent logEvent) {
        String key = logEvent.getContextData() != null ? logEvent.getContextData().getValue(MDC_KEY_ID) : null;
        return key != null ? key : NO_RECORD_KEY;
    }

    @PluginFactory
    public static SecondaryAppender createAppender(@PluginAttribute("name") String name,
            final @PluginElement("Layout") Layout<? extends Serializable> layout, @PluginElement("Filter") final Filter filter) {
        if (name == null) {
            logger.error("No name provided for SecondaryAppender");
            return null;
        }

        return new SecondaryAppender(name, filter, layout != null ? layout : PatternLayout.createDefaultLayout(), true);
    }

    /**
     * Returns the accumulated log for the given record identifier.
     *
     * @param record record identifier (the MDC {@value #MDC_KEY} value); null is treated as the no-record key
     * @return the captured log, or an empty string if nothing was captured for that record
     */
    public String getLog(String record) {
        StringBuffer buffer = BUFFERS.get(record != null ? record : NO_RECORD_KEY);
        return buffer != null ? buffer.toString() : "";
    }

    /**
     * Discards the buffer for the given record identifier (both to start fresh and to release memory when done).
     *
     * @param record record identifier (the MDC {@value #MDC_KEY} value); null is treated as the no-record key
     * @should reset writer correctly
     */
    public void reset(String record) {
        BUFFERS.remove(record != null ? record : NO_RECORD_KEY);
    }
}