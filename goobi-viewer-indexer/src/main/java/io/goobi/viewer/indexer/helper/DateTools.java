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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.model.PrimitiveDate;

/**
 * 
 */
public class DateTools {

    private static final Logger logger = LoggerFactory.getLogger(DateTools.class);

    /** Constant <code>formatterISO8601Full</code> */
    public static DateTimeFormatter formatterISO8601Full = DateTimeFormatter.ISO_LOCAL_DATE_TIME; // yyyy-MM-dd'T'HH:mm:ss
    /** Constant <code>formatterISO8601DateTimeInstant</code> */
    public static DateTimeFormatter formatterISO8601DateTimeInstant = DateTimeFormatter.ISO_INSTANT; // yyyy-MM-dd'T'HH:mm:ssZ
    /** Constant <code>formatterISO8601DateTimeWithOffset</code> */
    public static DateTimeFormatter formatterISO8601DateTimeWithOffset = DateTimeFormatter.ISO_OFFSET_DATE_TIME; // yyyy-MM-dd'T'HH:mm:ss+01:00
    /** Constant <code>formatterISO8601Date</code> */
    public static DateTimeFormatter formatterISO8601Date = DateTimeFormatter.ISO_LOCAL_DATE; // yyyy-MM-dd
    /** Constant <code>formatterISO8601DateTimeNoSeconds</code> */
    public static DateTimeFormatter formatterISO8601DateTimeNoSeconds = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    /** Constant <code>formatterISO8601YearMonth</code> */
    public static DateTimeFormatter formatterISO8601YearMonth = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM")
            .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
            .toFormatter();
    /** Constant <code>formatterDEDate</code> */
    public static DateTimeFormatter formatterDEDate = DateTimeFormatter.ofPattern("dd.MM.yyyy");
    /** Constant <code>formatterUSDate</code> */
    public static DateTimeFormatter formatterUSDate = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    /** Constant <code>formatterCNDate</code> */
    public static DateTimeFormatter formatterCNDate = DateTimeFormatter.ofPattern("yyyy.MM.dd");
    /** Constant <code>formatterJPDate</code> */
    public static DateTimeFormatter formatterJPDate = DateTimeFormatter.ofPattern("yyyy/MM/dd");;
    /** Constant <code>formatterBasicDateTime</code> */
    public static DateTimeFormatter formatterBasicDateTime = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    /**
     * Extract dates from the given string and returns them as a list.
     * 
     * @param dateString
     * @param normalizeYearMinDigits
     * @return List of parsed PrimitiveDates.
     * @should parse german date formats correctly
     * @should parse rfc date formats correctly
     * @should parse american date formats correctly
     * @should parse chinese date formats correctly
     * @should parse japanese date formats correctly
     * @should parse year ranges correctly
     * @should parse single years correctly
     * @should throw IllegalArgumentException if normalizeYearMinDigits less than 1
     */
    static List<PrimitiveDate> normalizeDate(String dateString, int normalizeYearMinDigits) {
        if (normalizeYearMinDigits < 1) {
            throw new IllegalArgumentException("normalizeYearMinDigits must be at least 1");
        }
        logger.trace("normalizeDate: {} (min digits: {})", dateString, normalizeYearMinDigits);

        List<PrimitiveDate> ret = new ArrayList<>();

        // Try known date formats first
        try {
            LocalDate date = LocalDate.parse(dateString, formatterISO8601Full);
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (DateTimeParseException e) {
        }
        try {
            LocalDate date = LocalDate.parse(dateString, formatterDEDate);
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (DateTimeParseException e) {
        }
        try {
            LocalDate date = LocalDate.parse(dateString, formatterISO8601Date);
            ret.add(new PrimitiveDate(date));
            logger.trace("parsed date: {} (using format yyyy-MM-dd)", date.toString());
            return ret;
        } catch (DateTimeParseException e) {
        }
        try {
            LocalDate date = LocalDate.parse(dateString, formatterISO8601YearMonth);
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (DateTimeParseException e) {
        }
        try {
            LocalDate date = LocalDate.parse(dateString, formatterUSDate);
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (DateTimeParseException e) {
        }
        try {
            LocalDate date = LocalDate.parse(dateString, formatterCNDate);
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (DateTimeParseException e) {
        }
        try {
            LocalDate date = LocalDate.parse(dateString, formatterJPDate);
            ret.add(new PrimitiveDate(date));
            return ret;
        } catch (DateTimeParseException e) {
        }

        // Try parsing year ranges
        if (dateString.contains("-") && dateString.charAt(0) != '-') {
            Pattern p = Pattern.compile("[\\d+]\\d+");
            Matcher m = p.matcher(dateString);
            while (m.find()) {
                try {
                    String sub = dateString.substring(m.start(), m.end());
                    if (sub.length() >= normalizeYearMinDigits) {
                        int year = Integer.valueOf(sub);
                        ret.add(new PrimitiveDate(year, null, null));
                    }
                } catch (NumberFormatException e) {
                    logger.error(e.getMessage());
                }
            }
            return ret;
        }
        // Try parsing remaining numbers
        Pattern p = Pattern.compile("[-]{0,1}\\d+");
        Matcher m = p.matcher(dateString);
        while (m.find()) {
            try {
                String sub = dateString.substring(m.start(), m.end());
                if (sub.length() >= (sub.charAt(0) == '-' ? (normalizeYearMinDigits + 1) : normalizeYearMinDigits)) {
                    int year = Integer.valueOf(sub);
                    ret.add(new PrimitiveDate(year, null, null));
                }
            } catch (NumberFormatException e) {
                logger.error(e.getMessage());
            }
        }

        return ret;
    }

    /**
     * 
     * @param value
     * @param useUTC If true, UTC time zone will be used; default time zone otherwise
     * @return Converted datetime string
     * @should convert date correctly
     */
    public static String convertDateStringForSolrField(String value, boolean useUTC) {
        List<PrimitiveDate> dates = normalizeDate(value, 4);
        if (!dates.isEmpty()) {
            PrimitiveDate date = dates.get(0);
            if (date.getYear() != null) {
                ZonedDateTime ld =
                        LocalDateTime.of(date.getYear(), date.getMonth() != null ? date.getMonth() : 1, date.getDay() != null ? date.getDay() : 1, 0,
                                0, 0, 0).atZone(useUTC ? ZoneOffset.UTC : ZoneOffset.systemDefault());
                return ld.format(formatterISO8601DateTimeInstant);
            }
        }

        logger.warn("Could not parse date from value: {}", value);
        return null;
    }
}
