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

import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.SolrConstants;

class XPathConfigTest extends AbstractTest {

    /**
     * @see XPathConfig#XPathConfig(String,String,String,String)
     * @verifies set members correctly
     */
    @Test
    void XPathConfig_shouldSetMembersCorrectly() {
        XPathConfig xpc = new XPathConfig("path", "pre", "suf", "field_name");
        Assertions.assertEquals("path", xpc.getxPath());
        Assertions.assertEquals("pre", xpc.getPrefix());
        Assertions.assertEquals("suf", xpc.getSuffix());
    }

    /**
     * @see XPathConfig#determineSupportedFormats(String,String)
     * @verifies detect format from xpath prefix
     */
    @Test
    void determineSupportedFormats_shouldDetectFormatFromXpathPrefix() {
        Set<FileFormat> mets = XPathConfig.determineSupportedFormats("mets:xmlData/mods:mods/mods:titleInfo/mods:title", "MD_TITLE");
        Assertions.assertTrue(mets.contains(FileFormat.METS));
        Assertions.assertTrue(mets.contains(FileFormat.METS_MARC));

        Assertions.assertTrue(XPathConfig.determineSupportedFormats("mets:xmlData/mix:mix/mix:byteOrder", "MD_X").contains(FileFormat.MIX));
        Assertions.assertTrue(XPathConfig.determineSupportedFormats("ead:did/ead:unittitle", "MD_TITLE").contains(FileFormat.EAD));
        Assertions.assertTrue(XPathConfig.determineSupportedFormats("lido:descriptiveMetadata/lido:titleWrap", "MD_TITLE")
                .contains(FileFormat.LIDO));
        Assertions.assertTrue(XPathConfig.determineSupportedFormats("dc:title", "MD_TITLE").contains(FileFormat.DUBLINCORE));
        Assertions.assertTrue(XPathConfig.determineSupportedFormats("//denkxweb:name", "MD_TITLE").contains(FileFormat.DENKXWEB));
    }

    /**
     * @see XPathConfig#determineSupportedFormats(String,String)
     * @verifies detect ead for otherlevel attribute
     */
    @Test
    void determineSupportedFormats_shouldDetectEadForOtherlevelAttribute() {
        Assertions.assertEquals(Set.of(FileFormat.EAD), XPathConfig.determineSupportedFormats("@otherlevel", "MD_ARCHIVE_ENTRY_OTHERLEVEL"));
    }

    /**
     * @see XPathConfig#determineSupportedFormats(String,String)
     * @verifies detect ead for EAD_NODE_ID field name
     */
    @Test
    void determineSupportedFormats_shouldDetectEadForEadNodeIdFieldName() {
        Assertions.assertEquals(Set.of(FileFormat.EAD), XPathConfig.determineSupportedFormats("@id", SolrConstants.EAD_NODE_ID));
    }

    /**
     * @see XPathConfig#determineSupportedFormats(String,String)
     * @verifies return empty set when format not determinable
     */
    @Test
    void determineSupportedFormats_shouldReturnEmptySetWhenFormatNotDeterminable() {
        Assertions.assertTrue(XPathConfig.determineSupportedFormats("@level", "MD_ARCHIVE_ENTRY_LEVEL").isEmpty());
        Assertions.assertTrue(XPathConfig.determineSupportedFormats("mets:structMap[@type='LOGICAL']/mets:div/@type", "MD_X").isEmpty());
    }

    /**
     * @see XPathConfig#appliesTo(FileFormat)
     * @verifies return true when format supported
     */
    @Test
    void appliesTo_shouldReturnTrueWhenFormatSupported() {
        Assertions.assertTrue(new XPathConfig("ead:did/ead:unittitle", null, null, "MD_TITLE").appliesTo(FileFormat.EAD));
    }

    /**
     * @see XPathConfig#appliesTo(FileFormat)
     * @verifies return false when xpath belongs to other format
     */
    @Test
    void appliesTo_shouldReturnFalseWhenXpathBelongsToOtherFormat() {
        Assertions.assertFalse(new XPathConfig("//denkxweb:name", null, null, "MD_TITLE").appliesTo(FileFormat.EAD));
        Assertions.assertFalse(new XPathConfig("ead:did/ead:unittitle", null, null, "MD_TITLE").appliesTo(FileFormat.LIDO));
    }

    /**
     * @see XPathConfig#appliesTo(FileFormat)
     * @verifies return true when format not determinable
     */
    @Test
    void appliesTo_shouldReturnTrueWhenFormatNotDeterminable() {
        Assertions.assertTrue(new XPathConfig("@level", null, null, "MD_ARCHIVE_ENTRY_LEVEL").appliesTo(FileFormat.EAD));
        Assertions.assertTrue(new XPathConfig("@level", null, null, "MD_ARCHIVE_ENTRY_LEVEL").appliesTo(FileFormat.LIDO));
    }

    /**
     * @see XPathConfig#appliesTo(FileFormat)
     * @verifies return true when given format null
     */
    @Test
    void appliesTo_shouldReturnTrueWhenGivenFormatNull() {
        Assertions.assertTrue(new XPathConfig("//denkxweb:name", null, null, "MD_TITLE").appliesTo(null));
    }
}
