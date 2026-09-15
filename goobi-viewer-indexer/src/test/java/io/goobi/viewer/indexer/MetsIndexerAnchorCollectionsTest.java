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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jdom2.Element;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.helper.Configuration;
import io.goobi.viewer.indexer.helper.JDomXP;
import io.goobi.viewer.indexer.model.IndexObject;

/**
 * Tests for the volume-collections-to-anchor merge in {@link MetsIndexer}: field-driven collection element creation ({@code buildCollectionElement}),
 * merging into an anchor that already has a matching collection element (template path), and bootstrapping into an anchor that has none. These tests
 * do not require a Solr connection.
 */
class MetsIndexerAnchorCollectionsTest extends AbstractTest {

    private static final String ANCHOR_FILE = "src/test/resources/METS/baltst_559838239/baltst_559838239_NF_75_anchor.xml";
    private static final String ANCHOR_DMDID = "DMDLOG_0000";
    private static final String CLASSIFICATION_XPATH =
            "/mets:mets/mets:dmdSec[@ID='" + ANCHOR_DMDID + "']/mets:mdWrap/mets:xmlData/mods:mods/mods:classification";

    @Test
    void getAddVolumeCollectionsToAnchorFields_shouldReturnEmptyListWhenDisabled() {
        // The default test config has enabled="false" but still lists <field>DC</field>: the enabled attribute must gate the feature off.
        List<String> fields = SolrIndexerDaemon.getInstance().getConfiguration().getAddVolumeCollectionsToAnchorFields();
        assertNotNull(fields);
        assertTrue(fields.isEmpty());
    }

    @Test
    void getAddVolumeCollectionsToAnchorFields_shouldReturnConfiguredFieldsWhenEnabled() {
        Configuration config = new Configuration(new File("src/test/resources/config_indexer.test_addvolumecollections.xml").getAbsolutePath());
        assertEquals(List.of("DC", "MD_FOO"), config.getAddVolumeCollectionsToAnchorFields());
    }

    @Test
    void buildCollectionElement_shouldCreateBareElementForNoAttributeXPath() {
        MetsIndexer indexer = new MetsIndexer((io.goobi.viewer.indexer.helper.Hotfolder) null);
        Element element = indexer.buildCollectionElement("mets:xmlData/mods:mods/mods:classification[not(@*)]");
        assertNotNull(element);
        assertEquals("classification", element.getName());
        assertEquals("http://www.loc.gov/mods/v3", element.getNamespaceURI());
        assertTrue(element.getAttributes().isEmpty());
    }

    @Test
    void buildCollectionElement_shouldApplyLiteralAttributePredicate() {
        MetsIndexer indexer = new MetsIndexer((io.goobi.viewer.indexer.helper.Hotfolder) null);
        // Single-quoted, or-joined predicate: the first literal value must be applied so the created element is re-matched by the XPath
        Element element = indexer.buildCollectionElement("mets:xmlData/mods:mods/mods:classification[@authority='GDZ' or @authority='CAU']");
        assertNotNull(element);
        assertEquals("classification", element.getName());
        assertEquals("GDZ", element.getAttributeValue("authority"));
    }

    @Test
    void buildCollectionElement_shouldReturnNullForAttributeOrNonModsXPath() {
        MetsIndexer indexer = new MetsIndexer((io.goobi.viewer.indexer.helper.Hotfolder) null);
        // Attribute-selecting last step cannot be represented as an element
        assertNull(indexer.buildCollectionElement("mets:xmlData/mods:mods/mods:relatedItem[@type='host']/@ID"));
        // Not under mets:xmlData/mods:mods
        assertNull(indexer.buildCollectionElement("lido:administrativeMetadata/lido:recordWrap/lido:recordSource"));
        assertNull(indexer.buildCollectionElement(""));
    }

    @Test
    void addVolumeCollectionsToAnchor_shouldMergeIntoExistingCollectionElement() throws Exception {
        MetsIndexer indexer = new MetsIndexer((io.goobi.viewer.indexer.helper.Hotfolder) null);
        indexer.xp = new JDomXP(new File(ANCHOR_FILE));
        IndexObject indexObj = new IndexObject("1");
        indexObj.setDmdid(ANCHOR_DMDID);

        Map<String, List<String>> collectionsByField = new LinkedHashMap<>();
        collectionsByField.put("DC", new ArrayList<>(List.of("aaa#bbb")));

        boolean added = indexer.addVolumeCollectionsToAnchor(indexObj, collectionsByField);
        assertTrue(added);

        List<Element> classifications = indexer.xp.evaluateToElements(CLASSIFICATION_XPATH, null);
        // The anchor's own GDZ collection plus the new volume collection
        assertEquals(2, classifications.size());
        assertTrue(classifications.stream().anyMatch(e -> "aaa#bbb".equals(e.getTextTrim())));
        // The new element was cloned from the existing GDZ template, so it keeps the authority attribute (and is re-matched on reindex)
        assertTrue(classifications.stream().allMatch(e -> "GDZ".equals(e.getAttributeValue("authority"))));
    }

    @Test
    void addVolumeCollectionsToAnchor_shouldBootstrapWhenAnchorHasNoCollection() throws Exception {
        MetsIndexer indexer = new MetsIndexer((io.goobi.viewer.indexer.helper.Hotfolder) null);
        indexer.xp = new JDomXP(new File(ANCHOR_FILE));
        IndexObject indexObj = new IndexObject("1");
        indexObj.setDmdid(ANCHOR_DMDID);

        // Strip the anchor's own collection element so the bootstrap path is exercised
        for (Element existing : indexer.xp.evaluateToElements(CLASSIFICATION_XPATH, null)) {
            existing.detach();
        }
        assertTrue(indexer.xp.evaluateToElements(CLASSIFICATION_XPATH, null).isEmpty());

        Map<String, List<String>> collectionsByField = new LinkedHashMap<>();
        collectionsByField.put("DC", new ArrayList<>(List.of("varia#foo")));

        boolean added = indexer.addVolumeCollectionsToAnchor(indexObj, collectionsByField);
        assertTrue(added);

        List<Element> classifications = indexer.xp.evaluateToElements(CLASSIFICATION_XPATH, null);
        assertEquals(1, classifications.size());
        Element created = classifications.get(0);
        assertEquals("varia#foo", created.getTextTrim());
        // Bootstrapped from the first DC XPath (mods:classification[not(@*)]): a bare element that XPath 1 re-matches on reindex
        assertTrue(created.getAttributes().isEmpty());
        assertFalse(indexer.xp.evaluateToElements(
                "/mets:mets/mets:dmdSec[@ID='" + ANCHOR_DMDID + "']/mets:mdWrap/mets:xmlData/mods:mods/mods:classification[not(@*)]", null)
                .isEmpty());
    }
}
