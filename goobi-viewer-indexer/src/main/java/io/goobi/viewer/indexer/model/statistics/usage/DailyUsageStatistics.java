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
package io.goobi.viewer.indexer.model.statistics.usage;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import io.goobi.viewer.indexer.helper.DateTools;

/**
 * @author florian
 *
 */
public class DailyUsageStatistics {

    private final LocalDate date;
    private final String viewerName;
    private final Map<String, DailyRequestCounts> requestCounts;

    /**
     * @param date
     * @param viewerName
     * @param requestCounts
     */
    public DailyUsageStatistics(LocalDate date, String viewerName, Map<String, DailyRequestCounts> requestCounts) {
        super();
        this.date = date;
        this.viewerName = viewerName;
        this.requestCounts = requestCounts;
    }

    public DailyUsageStatistics(JSONObject json) {
        try {
            String dateString = json.getString("date");
            this.date = LocalDate.parse(dateString, DateTools.FORMATTER_ISO8601_DATE);
            this.viewerName = json.getString("viewer-name");
            JSONArray records = json.getJSONArray("records");
            requestCounts = new HashMap<>();
            for (int i = 0; i < records.length(); i++) {
                JSONObject jsonRecord = records.getJSONObject(i);
                String pi = jsonRecord.getString("pi");
                JSONArray counts = jsonRecord.getJSONArray("counts");
                requestCounts.put(pi, new DailyRequestCounts(counts));
            }
        } catch (JSONException | DateTimeParseException e) {
            throw new IllegalArgumentException("Error parsing json as DailyUsageStatistics: " + e.toString());
        }
    }

    /**
     * @return the dateformatter
     */
    public static DateTimeFormatter getDateformatter() {
        return DateTools.FORMATTER_ISO8601_DATE;
    }

    /**
     * @return the date
     */
    public LocalDate getDate() {
        return date;
    }

    /**
     * @return the viewerName
     */
    public String getViewerName() {
        return viewerName;
    }

    /**
     * @return the requestCounts
     */
    public Map<String, DailyRequestCounts> getRequestCounts() {
        return requestCounts;
    }

}
