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
package io.goobi.viewer.indexer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.model.SolrConstants.DocType;

class PhysicalDocumentBuilderTest extends AbstractSolrEnabledTest {

    /** Logger for this class. */
    //    private static final Logger logger = LogManager.getLogger(PhysicalDocumentBuilderTest.class);

    /**
     * @see MetsIndexer#buildPagesXpathExpresson()
     * @verifies build expression correctly
     */
    @Test
    void buildPagesXpathExpresson_shouldBuildExpressionCorrectly() {
        assertEquals(
                "/mets:mets/mets:structMap[@TYPE=\"PHYSICAL\"]/mets:div/mets:div"
                        + "[@TYPE=\"page\" or @TYPE=\"object\" or @TYPE=\"audio\" or @TYPE=\"video\"]",
                PhysicalDocumentBuilder.buildPagesXpathExpresson());
    }

    @Test
    void index_shouldFindDownloadResourceFilegroup() throws Exception {
        Path metPath = Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_download_resources.xml").toAbsolutePath();
        JDomXP xp = new JDomXP(JDomXP.readXmlFile(metPath.toAbsolutePath().toString()));
        PhysicalDocumentBuilder builder = new PhysicalDocumentBuilder(List.of("DOWNLOAD_RESOURCE"), Collections.emptyList(), Collections.emptyMap(),
                xp, null, null, DocType.DOWNLOAD_RESOURCE);
        Assertions.assertTrue(builder.isFileGroupExists());
    }

    @Test
    void index_shouldFindNoDownloadResourceFilegroup() throws Exception {
        Path metPath = Paths.get("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005.xml").toAbsolutePath();
        JDomXP xp = new JDomXP(JDomXP.readXmlFile(metPath.toAbsolutePath().toString()));
        PhysicalDocumentBuilder builder = new PhysicalDocumentBuilder(List.of("DOWNLOAD_RESOURCE"), Collections.emptyList(), Collections.emptyMap(),
                xp, null, null, DocType.DOWNLOAD_RESOURCE);
        Assertions.assertFalse(builder.isFileGroupExists());
    }

}