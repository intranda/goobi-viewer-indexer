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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;

class FieldConfigTest extends AbstractTest {

    /**
     * @see FieldConfig#ConfigurationItem(String)
     * @verifies set attributes correctly
     */
    @Test
    void FieldConfig_shouldSetAttributesCorrectly() throws Exception {
        FieldConfig ci = new FieldConfig("field_name");
        Assertions.assertEquals("field_name", ci.getFieldname());
    }

    /**
     * @see FieldConfig#checkXpathSupportedFormats(String)
     * @verifies @should add FileFormats correctly
     */
    @Test
    void checkXpathSupportedFormats_should_add_FileFormats_correctly() throws Exception {
        FieldConfig ci = new FieldConfig("field_name");
        ci.checkXpathSupportedFormats("mets:xmlData/mods:mods/mods:titleInfo[not(@*)]/mods:title[not(@lang)]");
        Assertions.assertTrue(ci.getSupportedFormats().contains(FileFormat.METS));
        ci.checkXpathSupportedFormats("mets:xmlData/bib/record/datafield[@tag='245']/subfield[@code='a']");
        Assertions.assertTrue(ci.getSupportedFormats().contains(FileFormat.METS_MARC));
        ci.checkXpathSupportedFormats("lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:titleWrap/lido:titleSet/lido:appellationValue");
        Assertions.assertTrue(ci.getSupportedFormats().contains(FileFormat.LIDO));
        ci.checkXpathSupportedFormats("ead:did/ead:unittitle");
        Assertions.assertTrue(ci.getSupportedFormats().contains(FileFormat.EAD));
        ci.checkXpathSupportedFormats("dc:titlee");
        Assertions.assertTrue(ci.getSupportedFormats().contains(FileFormat.DUBLINCORE));
        ci.checkXpathSupportedFormats("denkxweb:description");
        Assertions.assertTrue(ci.getSupportedFormats().contains(FileFormat.DENKXWEB));
    }
}