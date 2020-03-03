package io.goobi.viewer.indexer.helper;

import org.junit.Assert;
import org.junit.Test;

public class GeoJSONToolsTest {
    
    /**
     * @see GeoJSONTools#convertGMLToGeoJSON(String,String)
     * @verifies convert polygon to geoJSON correctly
     */
    @Test
    public void convertGMLToGeoJSON_shouldConvertPolygonToGeoJSONCorrectly() throws Exception {
        // TODO proper content comparison
        Assert.assertTrue(GeoJSONTools.convertGMLToGeoJSON(
                "51.8164115931853 9.86927764300289 51.8164107865348 9.86927733742467 51.81640215001 9.86936345721908 51.8162882108512 9.86933544862899 51.8163013830216 9.86921939760192 51.8164140490608 9.86924941164474 51.8164115931853 9.86927764300289",
                "gml:Polygon").contains("Polygon"));
    }
}