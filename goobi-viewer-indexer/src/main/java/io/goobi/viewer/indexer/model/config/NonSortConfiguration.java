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

/**
 * <p>NonSortConfiguration class.</p>
 *
 */
public class NonSortConfiguration {

    private final String prefix;
    private final String suffix;

    /**
     * Constructor.
     *
     * @param prefix a {@link java.lang.String} object.
     * @param suffix a {@link java.lang.String} object.
     * @should set attributes correctly
     */
    public NonSortConfiguration(String prefix, String suffix) {
        this.prefix = prefix;
        this.suffix = suffix;
    }

    /**
     * <p>apply.</p>
     *
     * @param s a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     */
    public String apply(String s) {
        // remove non-sort characters and anything between them
        StringBuilder regex = new StringBuilder();
        if (prefix != null) {
            regex.append(prefix);
        }
        regex.append("[\\w|\\W]*");
        if (suffix != null) {
            regex.append(suffix);
        }
        s = s.replaceAll(regex.toString(), "").trim();
        // logger.info("Non-sort regex: {}", regex);
        // logger.info("s: {}", s);

        return s;
    }

    /**
     * <p>Getter for the field <code>prefix</code>.</p>
     *
     * @return the prefix
     */
    public String getPrefix() {
        return prefix;
    }

    /**
     * <p>Getter for the field <code>suffix</code>.</p>
     *
     * @return the suffix
     */
    public String getSuffix() {
        return suffix;
    }

}
