package io.goobi.viewer.indexer.model;

import java.time.LocalDate;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import io.goobi.viewer.indexer.helper.MetadataHelper;

public class PrimitiveDateTest {

    /**
     * @see PrimitiveDate#PrimitiveDate(Date)
     * @verifies set date correctly
     */
    @Test
    public void PrimitiveDate_shouldSetDateCorrectly() throws Exception {
        LocalDate date = LocalDate.parse("2020-05-29", MetadataHelper.formatterISO8601Date);
        PrimitiveDate pd = new PrimitiveDate(date);
        Assert.assertEquals(Integer.valueOf(2020), pd.getYear());
        Assert.assertEquals(Integer.valueOf(5), pd.getMonth());
        Assert.assertEquals(Integer.valueOf(29), pd.getDay());
    }
}