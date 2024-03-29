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
package io.goobi.viewer.indexer.model.datarepository.strategy;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.model.datarepository.DataRepository;
import io.goobi.viewer.indexer.model.datarepository.DummyDataRepository;

class RemainingSpaceStrategyTest extends AbstractTest {

    /**
     * @see RemainingSpaceStrategy#selectRepository(SortedMap,long)
     * @verifies select repository with the smallest sufficient space
     */
    @Test
    void selectRepository_shouldSelectRepositoryWithTheSmallestSufficientSpace() throws Exception {
        SortedMap<Long, DataRepository> repositorySpaceMap = new TreeMap<>();
        repositorySpaceMap.put(30L, new DataRepository("/opt/digiverso/viewer/data/3"));
        repositorySpaceMap.put(20L, new DataRepository("/opt/digiverso/viewer/data/2"));
        repositorySpaceMap.put(10L, new DataRepository("/opt/digiverso/viewer/data/1"));
        Assertions.assertEquals(repositorySpaceMap.get(20L), RemainingSpaceStrategy.selectRepository(repositorySpaceMap, 11L));
        Assertions.assertEquals(repositorySpaceMap.get(30L), RemainingSpaceStrategy.selectRepository(repositorySpaceMap, 22L));
        Assertions.assertEquals(null, RemainingSpaceStrategy.selectRepository(repositorySpaceMap, 33L));
    }

    /**
     * @see RemainingSpaceStrategy#selectRepository(SortedMap,long)
     * @verifies return null if recordSize is larger than any available repository space
     */
    @Test
    void selectRepository_shouldReturnNullIfRecordSizeIsLargerThanAnyAvailableRepositorySpace() throws Exception {
        SortedMap<Long, DataRepository> repositorySpaceMap = new TreeMap<>();
        repositorySpaceMap.put(30L, new DataRepository("/opt/digiverso/viewer/data/3"));
        repositorySpaceMap.put(20L, new DataRepository("/opt/digiverso/viewer/data/2"));
        repositorySpaceMap.put(10L, new DataRepository("/opt/digiverso/viewer/data/1"));
        Assertions.assertEquals(null, RemainingSpaceStrategy.selectRepository(repositorySpaceMap, 33L));
    }

    /**
     * @see RemainingSpaceStrategy#generateRepositorySpaceMap()
     * @verifies subtract the buffer size from available space
     */
    @Test
    void generateRepositorySpaceMap_shouldSubtractTheBufferSizeFromAvailableSpace() throws Exception {
        DummyDataRepository repo = new DummyDataRepository("/opt/digiverso/viewer/data/1", 30L, 3L);

        SortedMap<Long, DataRepository> repositorySpaceMap = RemainingSpaceStrategy.generateRepositorySpaceMap(Collections.singletonList(repo));
        Assertions.assertEquals(1, repositorySpaceMap.size());
        Assertions.assertEquals(repo, repositorySpaceMap.get(27L));
    }
}