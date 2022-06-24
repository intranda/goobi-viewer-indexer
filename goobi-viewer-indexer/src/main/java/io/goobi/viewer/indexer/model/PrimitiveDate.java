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
package io.goobi.viewer.indexer.model;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class PrimitiveDate {

    private Integer year;
    private Integer month;
    private Integer day;

    /** Individual values constructor. */
    public PrimitiveDate(Integer year, Integer month, Integer day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    /**
     * Date constructor.
     * 
     * @param date
     * @should set date correctly
     */
    public PrimitiveDate(LocalDate date) {
        this.year = date.getYear();
        this.month = date.getMonthValue();
        this.day = date.getDayOfMonth();
    }

    /**
     * 
     * @return LocalDateTime based off of this date
     */
    public LocalDateTime toLocalDateTime() {
        return LocalDateTime.of(year != null ? year : 1970, month != null ? month : 1, day != null ? day : 1, 0, 0);
    }

    /**
     * @return the year
     */
    public Integer getYear() {
        return year;
    }

    /**
     * @return the month
     */
    public Integer getMonth() {
        return month;
    }

    /**
     * @return the day
     */
    public Integer getDay() {
        return day;
    }

    @Override
    public String toString() {
        return year + "-" + month + "-" + day;
    }
}