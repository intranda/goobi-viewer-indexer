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

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.helper.FileTools;

public class PrimoDocumentTest {

    /**
     * @see PrimoDocument#build()
     * @verifies build document correctly
     */
    @Test
    public void build_shouldBuildDocumentCorrectly() throws Exception {
        File file = new File("src/test/resources/Primo/000110550.xml");
        Assertions.assertTrue(file.isFile());
        String xml = FileTools.readFileToString(file, null);
        Assertions.assertTrue(StringUtils.isNotEmpty(xml));
        PrimoDocument pd = new PrimoDocument().setXml(xml).build();
        Assertions.assertNotNull(pd.getXp());
    }

    /**
     * @see PrimoDocument#prepareURL(Map)
     * @verifies find and replace identifier correctly
     */
    @Test
    public void prepare_shouldFindAndReplaceIdentifierCorrectly() throws Exception {
        Map<String, List<String>> values = new HashMap<>(1);
        values.put("MD_FOO", Collections.singletonList("123"));
        PrimoDocument pd = new PrimoDocument("https://example.com?id=${MD_FOO}&format=xml");
        pd.prepareURL(values);
        Assertions.assertEquals("https://example.com?id=123&format=xml", pd.getUrl());
    }
}