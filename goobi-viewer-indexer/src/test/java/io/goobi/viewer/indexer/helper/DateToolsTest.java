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

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.model.PrimitiveDate;

class DateToolsTest {

    /**
     * @see DateTools#convertDateStringForSolrField(String,boolean)
     * @verifies convert date correctly
     */
    @Test
    void convertDateStringForSolrField_shouldConvertDateCorrectly() throws Exception {
        Assertions.assertEquals("2016-11-02T00:00:00Z", DateTools.convertDateStringForSolrField("2016-11-02", true));
        Assertions.assertEquals("2016-11-01T00:00:00Z", DateTools.convertDateStringForSolrField("2016-11", true));
        Assertions.assertEquals("2016-01-01T00:00:00Z", DateTools.convertDateStringForSolrField("2016", true));
    }

    /**
     * @see DateTools#normalizeDate(String,int)
     * @verifies parse german date formats correctly
     */
    @Test
    void normalizeDate_shouldParseGermanDateFormatsCorrectly() throws Exception {
        List<PrimitiveDate> ret = DateTools.normalizeDate("05.08.2014", 3);
        Assertions.assertEquals(1, ret.size());
        Assertions.assertEquals(Integer.valueOf(2014), ret.get(0).getYear());
        Assertions.assertEquals(Integer.valueOf(8), ret.get(0).getMonth());
        Assertions.assertEquals(Integer.valueOf(5), ret.get(0).getDay());
    }

    /**
     * @see DateTools#normalizeDate(String,int)
     * @verifies parse rfc date formats correctly
     */
    @Test
    void normalizeDate_shouldParseRfcDateFormatsCorrectly() throws Exception {
        List<PrimitiveDate> ret = DateTools.normalizeDate("2014-08-05", 3);
        Assertions.assertEquals(1, ret.size());
        Assertions.assertEquals(Integer.valueOf(2014), ret.get(0).getYear());
        Assertions.assertEquals(Integer.valueOf(8), ret.get(0).getMonth());
        Assertions.assertEquals(Integer.valueOf(5), ret.get(0).getDay());

        ret = DateTools.normalizeDate("0002-05", 3);
        Assertions.assertEquals(1, ret.size());
        Assertions.assertEquals(Integer.valueOf(2), ret.get(0).getYear());
        Assertions.assertEquals(Integer.valueOf(5), ret.get(0).getMonth());
    }

    /**
     * @see DateTools#normalizeDate(String,int)
     * @verifies parse american date formats correctly
     */
    @Test
    void normalizeDate_shouldParseAmericanDateFormatsCorrectly() throws Exception {
        List<PrimitiveDate> ret = DateTools.normalizeDate("08/05/2014", 3);
        Assertions.assertEquals(1, ret.size());
        Assertions.assertEquals(Integer.valueOf(2014), ret.get(0).getYear());
        Assertions.assertEquals(Integer.valueOf(8), ret.get(0).getMonth());
        Assertions.assertEquals(Integer.valueOf(5), ret.get(0).getDay());
    }

    /**
     * @see DateTools#normalizeDate(String,int)
     * @verifies parse chinese date formats correctly
     */
    @Test
    void normalizeDate_shouldParseChineseDateFormatsCorrectly() throws Exception {
        List<PrimitiveDate> ret = DateTools.normalizeDate("2014.08.05", 3);
        Assertions.assertEquals(1, ret.size());
        Assertions.assertEquals(Integer.valueOf(2014), ret.get(0).getYear());
        Assertions.assertEquals(Integer.valueOf(8), ret.get(0).getMonth());
        Assertions.assertEquals(Integer.valueOf(5), ret.get(0).getDay());
    }

    /**
     * @see DateTools#normalizeDate(String,int)
     * @verifies parse japanese date formats correctly
     */
    @Test
    void normalizeDate_shouldParseJapaneseDateFormatsCorrectly() throws Exception {
        List<PrimitiveDate> ret = DateTools.normalizeDate("2014/08/05", 3);
        Assertions.assertEquals(1, ret.size());
        Assertions.assertEquals(Integer.valueOf(2014), ret.get(0).getYear());
        Assertions.assertEquals(Integer.valueOf(8), ret.get(0).getMonth());
        Assertions.assertEquals(Integer.valueOf(5), ret.get(0).getDay());
    }

    /**
     * @see DateTools#normalizeDate(String,int)
     * @verifies parse year ranges correctly
     */
    @Test
    void normalizeDate_shouldParseYearRangesCorrectly() throws Exception {
        List<PrimitiveDate> ret = DateTools.normalizeDate("2010-2014", 3);
        Assertions.assertEquals(2, ret.size());
        Assertions.assertEquals(Integer.valueOf(2010), ret.get(0).getYear());
        Assertions.assertEquals(Integer.valueOf(2014), ret.get(1).getYear());

        ret = DateTools.normalizeDate("10-20", 2);
        Assertions.assertEquals(2, ret.size());
        Assertions.assertEquals(Integer.valueOf(10), ret.get(0).getYear());
        Assertions.assertEquals(Integer.valueOf(20), ret.get(1).getYear());
    }

    /**
     * @see DateTools#normalizeDate(String,int)
     * @verifies parse single years correctly
     */
    @Test
    void normalizeDate_shouldParseSingleYearsCorrectly() throws Exception {
        List<PrimitiveDate> ret = DateTools.normalizeDate("-300 -30 -3", 2);
        Assertions.assertEquals(2, ret.size());
        Assertions.assertEquals(Integer.valueOf(-300), ret.get(0).getYear());
        Assertions.assertEquals(Integer.valueOf(-30), ret.get(1).getYear());

        ret = DateTools.normalizeDate("300 30 3", 3);
        Assertions.assertEquals(1, ret.size());
        Assertions.assertEquals(Integer.valueOf(300), ret.get(0).getYear());

        ret = DateTools.normalizeDate("300 30 3", 1);
        Assertions.assertEquals(3, ret.size());
        Assertions.assertEquals(Integer.valueOf(300), ret.get(0).getYear());
        Assertions.assertEquals(Integer.valueOf(30), ret.get(1).getYear());
        Assertions.assertEquals(Integer.valueOf(3), ret.get(2).getYear());
    }

    /**
     * @see DateTools#normalizeDate(String,int)
     * @verifies throw IllegalArgumentException if normalizeYearMinDigits less than 1
     */
    @Test
    void normalizeDate_shouldThrowIllegalArgumentExceptionIfNormalizeYearMinDigitsLessThan1() throws Exception {
        Assertions.assertThrows(IllegalArgumentException.class, () -> DateTools.normalizeDate("05.08.2014", 0));
    }

    /**
     * @see DateTools#normalizeDateFieldValue(String)
     * @verifies format years correctly
     */
    @Test
    void normalizeDateFieldValue_shouldFormatYearsCorrectly() throws Exception {
        Assertions.assertEquals("2022-01-01T00:00:00Z", DateTools.normalizeDateFieldValue("2022"));
    }

    /**
     * @see DateTools#normalizeDateFieldValue(String)
     * @verifies format iso instant correctly
     */
    @Test
    void normalizeDateFieldValue_shouldFormatIsoInstantCorrectly() throws Exception {
        Assertions.assertEquals("2022-01-01T00:01:02Z", DateTools.normalizeDateFieldValue("2022-01-01T00:01:02Z"));
    }

    /**
     * @see DateTools#normalizeDateFieldValue(String)
     * @verifies format iso local time correctly
     */
    @Test
    void normalizeDateFieldValue_shouldFormatIsoLocalTimeCorrectly() throws Exception {
        Assertions.assertEquals("2022-01-01T00:02:03Z", DateTools.normalizeDateFieldValue("2022-01-01T00:02:03"));
    }
}