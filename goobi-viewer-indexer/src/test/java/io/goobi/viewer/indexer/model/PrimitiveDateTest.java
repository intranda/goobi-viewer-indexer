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

import java.sql.Date;
import java.time.LocalDate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.helper.DateTools;

class PrimitiveDateTest {

    /**
     * @see PrimitiveDate#PrimitiveDate(Date)
     * @verifies set date correctly
     */
    @Test
    void PrimitiveDate_shouldSetDateCorrectly() throws Exception {
        LocalDate date = LocalDate.parse("2020-05-29", DateTools.FORMATTER_ISO8601_DATE);
        PrimitiveDate pd = new PrimitiveDate(date);
        Assertions.assertEquals(Integer.valueOf(2020), pd.getYear());
        Assertions.assertEquals(Integer.valueOf(5), pd.getMonth());
        Assertions.assertEquals(Integer.valueOf(29), pd.getDay());
    }
}