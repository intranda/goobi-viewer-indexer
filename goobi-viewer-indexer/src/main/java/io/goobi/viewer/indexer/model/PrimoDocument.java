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
package io.goobi.viewer.indexer.model;

import java.io.IOException;

import javax.xml.ws.http.HTTPException;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.jdom2.Document;
import org.jdom2.JDOMException;

import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.helper.TextHelper;
import io.goobi.viewer.indexer.helper.Utils;
import io.goobi.viewer.indexer.helper.XmlTools;

public class PrimoDocument {

    private String url;
    private String xml;
    private JDomXP xp;

    /**
     * Empty constructor.
     */
    public PrimoDocument() {
    }

    /**
     * 
     * @param url URL to resolve
     */
    public PrimoDocument(String url) {
        this.url = url;
    }

    public PrimoDocument fetch() throws HTTPException, ClientProtocolException, IOException {
        if (StringUtils.isEmpty(url)) {
            throw new IllegalArgumentException("url may not be null or empty");
        }

        xml = Utils.getWebContentGET(url);

        return this;
    }

    /**
     * 
     * @return this
     * @throws JDOMException
     * @throws IOException
     * @should build document correctly
     */
    public PrimoDocument build() throws JDOMException, IOException {
        if (xml == null) {
            throw new IllegalStateException("Document not fetched");
        }

        Document doc = XmlTools.getDocumentFromString(xml, TextHelper.DEFAULT_CHARSET);
        this.xp = new JDomXP(doc);

        return this;
    }

    public PrimoDocument parse() {
        if (xp == null) {
            throw new IllegalStateException("Document not built");
        }

        return this;
    }

    /**
     * @return the url
     */
    public String getUrl() {
        return url;
    }

    /**
     * @param url the url to set
     * @return this
     */
    public PrimoDocument setUrl(String url) {
        this.url = url;
        return this;
    }

    /**
     * @return the xml
     */
    public String getXml() {
        return xml;
    }

    /**
     * @param xml the xml to set
     * @return this
     */
    public PrimoDocument setXml(String xml) {
        this.xml = xml;
        return this;
    }

    /**
     * @return the xp
     */
    public JDomXP getXp() {
        return xp;
    }
}
