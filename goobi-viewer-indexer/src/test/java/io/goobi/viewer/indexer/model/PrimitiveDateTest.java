package io.goobi.viewer.indexer.model;

import java.util.Calendar;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

public class PrimitiveDateTest {

    /**
     * @see PrimitiveDate#PrimitiveDate(Date)
     * @verifies set date correctly
     */
    @Test
    public void PrimitiveDate_shouldSetDateCorrectly() throws Exception {

        Date date = new Date();
        PrimitiveDate pd = new PrimitiveDate(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        Assert.assertTrue(cal.get(Calendar.DAY_OF_MONTH) + " (" + pd.getDay() + ")", cal.get(Calendar.DAY_OF_MONTH) == pd.getDay());
    }
}