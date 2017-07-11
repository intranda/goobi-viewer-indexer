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
package de.intranda.digiverso.presentation.solr.model.config;

import org.junit.Assert;
import org.junit.Test;

public class XPathConfigTest {

    /**
     * @see XPathConfig#XPathConfig(String,String,String)
     * @verifies set members correctly
     */
    @Test
    public void XPathConfig_shouldSetMembersCorrectly() throws Exception {
        XPathConfig xpc = new XPathConfig("path", "pre", "suf");
        Assert.assertEquals("path", xpc.getxPath());
        Assert.assertEquals("pre", xpc.getPrefix());
        Assert.assertEquals("suf", xpc.getSuffix());
    }
}