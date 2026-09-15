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
package io.goobi.viewer.indexer.model.writestrategy;

import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.model.PhysicalElement;
import io.goobi.viewer.indexer.model.SolrConstants;

/**
 * Concurrency regression test for the URN collection in the write strategies.
 *
 * WorldViewsIndexer calls {@link LazySolrWriteStrategy#addPage(PhysicalElement)} from a parallelStream when configured with threads &gt; 1. The URN
 * values are collected into {@link AbstractWriteStrategy#collectedValues} for later duplicate checking. Before the fix, the inner list was a plain
 * ArrayList, so concurrent adds could lose entries or corrupt the list; the backing list is now a synchronized list.
 *
 * This is a pure in-memory test: addPage does not touch the Solr search index, so it deliberately does NOT extend AbstractSolrEnabledTest.
 */
class WriteStrategyConcurrencyTest {

    /**
     * @see LazySolrWriteStrategy#addPage(PhysicalElement)
     * @verifies collect all urns under concurrent access
     */
    @Test
    void addPage_shouldCollectAllUrnsUnderConcurrentAccess() {
        int n = 2000;
        LazySolrWriteStrategy strat = new LazySolrWriteStrategy(null);

        IntStream.range(0, n).parallel().forEach(i -> {
            PhysicalElement page = new PhysicalElement(i + 1);
            page.getDoc().addField(SolrConstants.PHYSID, "PHYS_" + (i + 1));
            page.getDoc().addField(SolrConstants.IMAGEURN, "urn:test:" + i);
            strat.addPage(page);
        });

        List<String> urns = strat.collectedValues.get(SolrConstants.URN);
        Assertions.assertNotNull(urns);
        Assertions.assertEquals(n, urns.size());
        Assertions.assertEquals(n, new HashSet<>(urns).size());
    }
}
