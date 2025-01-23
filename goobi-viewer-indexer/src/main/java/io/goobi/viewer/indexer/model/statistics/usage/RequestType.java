/**
 * This file is part of the Goobi viewer - a content presentation and management application for digitized objects.
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

import java.util.Arrays;

/**
 * @author florian
 *
 *         A list of types of request to count independently for usage statistics. When recording a request in
 *         {@see io.goobi.viewer.model.statistics.usage.UsageStatisticsRecorder} you need to pass an appropriate type. Statistics for each type are
 *         recorded independently
 */
public enum RequestType {

    /**
     * Call of a viewer html page belonging to a record
     */
    RECORD_VIEW(0, 1),
    /**
     * Download of a file (pdf, epub) belonging to a record
     */
    FILE_DOWNLOAD(2, 3),
    /**
     * REST-call to an image or other media resource of a record
     */
    MEDIA_RESOURCE(4, 5);

    private final int totalCountIndex;
    private final int uniqueCountIndex;

    private RequestType(int totalCountIndex, int uniqueCountIndex) {
        this.totalCountIndex = totalCountIndex;
        this.uniqueCountIndex = uniqueCountIndex;
    }

    /**
     * Index of the total request count within the array of values of the SOLR-field recording requests for a record
     * 
     * @return the totalCountIndex
     */
    public int getTotalCountIndex() {
        return totalCountIndex;
    }

    /**
     * Index of the count of requests by a unique http session within the array of values of the SOLR-field recording requests for a record
     * 
     * @return the uniqueCountIndex
     */
    public int getUniqueCountIndex() {
        return uniqueCountIndex;
    }

    /**
     * Index of the count for this type in {@link RequestType} within {@see io.goobi.viewer.model.statistics.usage.SessionUsageStatistics}
     * 
     * @return the ordinal of the instance
     */
    public int getSessionCountIndex() {
        return this.ordinal();
    }

    /**
     * Get the RequestType for the given index of the count array in {@link RequestType} within
     * {@see io.goobi.viewer.model.statistics.usage.SessionUsageStatistics}
     * 
     * @param index
     * @return the type
     */
    public static RequestType getTypeForSessionCountIndex(int index) {
        RequestType[] types = RequestType.values();
        return Arrays.stream(types).filter(t -> t.getSessionCountIndex() == index).findAny().orElse(null);
    }

    /**
     * Get the RequestType for the given index of the count array for total count in the SOLR field for the counts of a record identifier
     * 
     * @param index
     * @return the type
     */
    public static RequestType getTypeForTotalCountIndex(int index) {
        RequestType[] types = RequestType.values();
        return Arrays.stream(types).filter(t -> t.getTotalCountIndex() == index).findAny().orElse(null);
    }

    /**
     * Get the RequestType for the given index of the count array for unique count in the SOLR field for the counts of a record identifier
     * 
     * @param index
     * @return the type
     */
    public static RequestType getTypeForUniqueCountIndex(int index) {
        RequestType[] types = RequestType.values();
        return Arrays.stream(types).filter(t -> t.getUniqueCountIndex() == index).findAny().orElse(null);
    }
}
