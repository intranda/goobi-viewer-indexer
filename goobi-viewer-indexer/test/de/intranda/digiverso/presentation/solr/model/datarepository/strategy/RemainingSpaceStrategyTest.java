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
package de.intranda.digiverso.presentation.solr.model.datarepository.strategy;

import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import de.intranda.digiverso.presentation.solr.AbstractTest;
import de.intranda.digiverso.presentation.solr.model.datarepository.DataRepository;

public class RemainingSpaceStrategyTest extends AbstractTest {

    /**
     * @see RemainingSpaceStrategy#selectRepository(SortedMap,long)
     * @verifies select repository with the smallest sufficient space
     */
    @Test
    public void selectRepository_shouldSelectRepositoryWithTheSmallestSufficientSpace() throws Exception {
        SortedMap<Long, DataRepository> repositorySpaceMap = new TreeMap<>();
        repositorySpaceMap.put(30L, new DataRepository("/opt/digiverso/viewer/data/3"));
        repositorySpaceMap.put(20L, new DataRepository("/opt/digiverso/viewer/data/2"));
        repositorySpaceMap.put(10L, new DataRepository("/opt/digiverso/viewer/data/1"));
        Assert.assertEquals(repositorySpaceMap.get(20L), RemainingSpaceStrategy.selectRepository(repositorySpaceMap, 11L));
        Assert.assertEquals(repositorySpaceMap.get(30L), RemainingSpaceStrategy.selectRepository(repositorySpaceMap, 22L));
        Assert.assertEquals(null, RemainingSpaceStrategy.selectRepository(repositorySpaceMap, 33L));
    }

    /**
     * @see RemainingSpaceStrategy#selectRepository(SortedMap,long)
     * @verifies return null if recordSize is larger than any available repository space
     */
    @Test
    public void selectRepository_shouldReturnNullIfRecordSizeIsLargerThanAnyAvailableRepositorySpace() throws Exception {
        SortedMap<Long, DataRepository> repositorySpaceMap = new TreeMap<>();
        repositorySpaceMap.put(30L, new DataRepository("/opt/digiverso/viewer/data/3"));
        repositorySpaceMap.put(20L, new DataRepository("/opt/digiverso/viewer/data/2"));
        repositorySpaceMap.put(10L, new DataRepository("/opt/digiverso/viewer/data/1"));
        Assert.assertEquals(null, RemainingSpaceStrategy.selectRepository(repositorySpaceMap, 33L));
    }
}