package de.intranda.digiverso.presentation.solr.model.datarepository.strategy;

import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import de.intranda.digiverso.presentation.solr.model.datarepository.DataRepository;

public class RemainingSpaceStrategyTest {

    /**
     * @see RemainingSpaceStrategy#selectRepository(SortedMap,long)
     * @verifies select repository with the smallest sufficient space
     */
    @Test
    public void selectRepository_shouldSelectRepositoryWithTheSmallestSufficientSpace() throws Exception {
        SortedMap<Long, DataRepository> repositorySpaceMap = new TreeMap<>();
        repositorySpaceMap.put(30L, new DataRepository("/opt/digiverso/viewer/data/3", true));
        repositorySpaceMap.put(20L, new DataRepository("/opt/digiverso/viewer/data/2", true));
        repositorySpaceMap.put(10L, new DataRepository("/opt/digiverso/viewer/data/1", true));
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
        repositorySpaceMap.put(30L, new DataRepository("/opt/digiverso/viewer/data/3", true));
        repositorySpaceMap.put(20L, new DataRepository("/opt/digiverso/viewer/data/2", true));
        repositorySpaceMap.put(10L, new DataRepository("/opt/digiverso/viewer/data/1", true));
        Assert.assertEquals(null, RemainingSpaceStrategy.selectRepository(repositorySpaceMap, 33L));
    }
}