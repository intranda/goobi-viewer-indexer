package io.goobi.viewer.indexer.model.statistics.usage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.json.JSONArray;
import org.json.JSONException;

import io.goobi.viewer.indexer.model.statistics.usage.RequestType;

public class DailyRequestCounts {

    private final Map<RequestType, Long> totalCounts = new HashMap<>();
    private final Map<RequestType, Long> uniqueCounts = new HashMap<>();
    
    public DailyRequestCounts() {
        
    }
    
    public DailyRequestCounts(String json) {
        this(new JSONArray(json));
    }
    
    public DailyRequestCounts(JSONArray array) {
        for (RequestType type : RequestType.values()) {
            if(type.getTotalCountIndex() < array.length()) {                
                Long totalCount = array.getLong(type.getTotalCountIndex());
                totalCounts.put(type, totalCount);
            }
            if(type.getUniqueCountIndex() < array.length()) {                
                Long uniqueCount = array.getLong(type.getUniqueCountIndex());
                uniqueCounts.put(type, uniqueCount);
            }
        }
    }
    
    public String toJsonArray() {
        JSONArray array = new JSONArray(6);
        for (int i = 0; i < 6; i+=2) {
            RequestType type = RequestType.getTypeForTotalCountIndex(i);
            Long totalCount = Optional.ofNullable(totalCounts.get(type)).orElse(0l);
            array.put(totalCount);
            Long uniqueCount = Optional.ofNullable(uniqueCounts.get(type)).orElse(0l);
            array.put(uniqueCount);
        }
        return array.toString();
    }
    
    public long getTotalCount(RequestType type) {
        return Optional.ofNullable(totalCounts.get(type)).orElse(0l);
    }
    
    public long getUniqueCount(RequestType type) {
        return Optional.ofNullable(uniqueCounts.get(type)).orElse(0l);
    }
    

}
