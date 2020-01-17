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
 * <p>XPathConfig class.</p>
 *
 */
public class XPathConfig {

    private final String xPath;
    private final String prefix;
    private final String suffix;

    /**
     * Constructor.
     *
     * @param xPath a {@link java.lang.String} object.
     * @param prefix a {@link java.lang.String} object.
     * @param suffix a {@link java.lang.String} object.
     * @should set members correctly
     */
    public XPathConfig(String xPath, String prefix, String suffix) {
        this.xPath = xPath;
        this.prefix = prefix;
        this.suffix = suffix;
    }

    /**
     * <p>Getter for the field <code>xPath</code>.</p>
     *
     * @return the xPath
     */
    public String getxPath() {
        return xPath;
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
