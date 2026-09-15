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
package io.goobi.viewer.indexer.helper;

import java.io.File;

import org.jdom2.Namespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.Ead3Indexer;
import io.goobi.viewer.indexer.EadIndexer;
import io.goobi.viewer.indexer.SolrIndexerDaemon;

class JDomXPTest extends AbstractTest {

    private static final String EAD2_FILE = "src/test/resources/EAD/Akte_Koch.xml";
    private static final String EAD3_FILE = "src/test/resources/EAD/EAD3_example.xml";
    private static final String EAD_C_XPATH = "ead:ead/ead:archdesc/ead:dsc/ead:c";

    /**
     * @see JDomXP#addNamespace(Namespace)
     * @verifies override global namespace for same prefix
     */
    @Test
    void addNamespace_shouldOverrideGlobalNamespaceForSamePrefix() throws Exception {
        // EAD3 document: its 'ead:' elements live in the EAD3 namespace.
        // Without an instance binding the 'ead' prefix resolves to the global default (EAD2) and matches nothing.
        JDomXP ead3 = new JDomXP(new File(EAD3_FILE));
        Assertions.assertTrue(ead3.evaluateToElements(EAD_C_XPATH, null).isEmpty());
        // With an EAD3 instance binding the same expression resolves correctly.
        ead3.addNamespace(Ead3Indexer.NAMESPACE_EAD3);
        Assertions.assertFalse(ead3.evaluateToElements(EAD_C_XPATH, null).isEmpty());

        // Cross-namespace guard: the element name 'archdesc' exists in both the EAD2 and EAD3 namespaces.
        // The EAD2 document's elements must only match when the prefix is bound to the EAD2 namespace,
        // never when it is bound to EAD3 (the previous namespace-blind behaviour would have matched either).
        JDomXP ead2 = new JDomXP(new File(EAD2_FILE));
        ead2.addNamespace(EadIndexer.NAMESPACE_EAD2);
        Assertions.assertFalse(ead2.evaluateToElements("ead:ead/ead:archdesc", null).isEmpty());

        JDomXP ead2BoundToEad3 = new JDomXP(new File(EAD2_FILE));
        ead2BoundToEad3.addNamespace(Ead3Indexer.NAMESPACE_EAD3);
        Assertions.assertTrue(ead2BoundToEad3.evaluateToElements("ead:ead/ead:archdesc", null).isEmpty());
    }

    /**
     * @see JDomXP#addNamespace(Namespace)
     * @verifies not affect global configuration
     */
    @Test
    void addNamespace_shouldNotAffectGlobalConfiguration() throws Exception {
        Namespace globalBefore = SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("ead");

        JDomXP ead3 = new JDomXP(new File(EAD3_FILE));
        ead3.addNamespace(Ead3Indexer.NAMESPACE_EAD3);
        ead3.evaluateToElements(EAD_C_XPATH, null);

        Namespace globalAfter = SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("ead");
        Assertions.assertEquals(globalBefore, globalAfter);
        // The global 'ead' prefix must remain bound to EAD2 - the instance binding must not leak into shared state.
        Assertions.assertEquals(EadIndexer.NAMESPACE_EAD2, globalAfter);
    }
}
