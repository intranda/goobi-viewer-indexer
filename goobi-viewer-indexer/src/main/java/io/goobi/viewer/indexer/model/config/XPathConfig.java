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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.SolrConstants;

/**
 * <p>XPathConfig class.</p>
 *
 */
public class XPathConfig {

    private final String xPath;
    private final String prefix;
    private final String suffix;
    private final Set<FileFormat> supportedFormats;

    /**
     * Constructor.
     *
     * @param xPath a {@link java.lang.String} object.
     * @param prefix a {@link java.lang.String} object.
     * @param suffix a {@link java.lang.String} object.
     * @param fieldname Name of the field this expression belongs to; used to determine the supported file formats.
     * @should set members correctly
     */
    public XPathConfig(String xPath, String prefix, String suffix, String fieldname) {
        this.xPath = xPath;
        this.prefix = prefix;
        this.suffix = suffix;
        this.supportedFormats = Collections.unmodifiableSet(determineSupportedFormats(xPath, fieldname));
    }

    /**
     * Determines the source document formats an XPath expression can produce values for, based on the namespace prefixes it uses.
     *
     * <p>An empty result means the format could not be determined; such an expression is treated as applicable to every format.
     *
     * @param xpath XPath expression to examine
     * @param fieldname Name of the field the expression belongs to
     * @return {@link java.util.Set} of supported {@link io.goobi.viewer.indexer.helper.JDomXP.FileFormat}s; empty if not determinable
     * @should detect format from xpath prefix
     * @should detect ead for otherlevel attribute
     * @should detect ead for EAD_NODE_ID field name
     * @should return empty set when format not determinable
     */
    static Set<FileFormat> determineSupportedFormats(String xpath, String fieldname) {
        Set<FileFormat> ret = EnumSet.noneOf(FileFormat.class);
        if (xpath == null) {
            return ret;
        }

        if (xpath.contains("mets:xmlData") || xpath.startsWith("@OBJID")) {
            ret.add(FileFormat.METS);
            ret.add(FileFormat.METS_MARC);
            if (xpath.contains("mix:")) {
                ret.add(FileFormat.MIX);
            }
        } else if (SolrConstants.EAD_NODE_ID.equals(fieldname) || xpath.contains("ead:") || xpath.equals("@otherlevel")) {
            ret.add(FileFormat.EAD);
        } else if (xpath.contains("lido:")) {
            ret.add(FileFormat.LIDO);
        } else if (xpath.contains("dc:")) {
            ret.add(FileFormat.DUBLINCORE);
        } else if (xpath.contains("denkxweb:")) {
            ret.add(FileFormat.DENKXWEB);
        }

        return ret;
    }

    /**
     * Checks whether this expression should be evaluated for a document of the given format.
     *
     * <p>Expressions whose format could not be determined apply to every format, so that expressions without a recognizable namespace prefix keep
     * producing values for all document types.
     *
     * @param format {@link io.goobi.viewer.indexer.helper.JDomXP.FileFormat} of the document being indexed; may be null
     * @return true if the expression applies to the given format; false otherwise
     * @should return true when format supported
     * @should return false when xpath belongs to other format
     * @should return true when format not determinable
     * @should return true when given format null
     */
    public boolean appliesTo(FileFormat format) {
        return format == null || supportedFormats.isEmpty() || supportedFormats.contains(format);
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

    /**
     * <p>Getter for the field <code>supportedFormats</code>.</p>
     *
     * @return the supportedFormats
     */
    public Set<FileFormat> getSupportedFormats() {
        return supportedFormats;
    }
}
