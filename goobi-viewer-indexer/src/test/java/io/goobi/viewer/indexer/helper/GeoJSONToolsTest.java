package io.goobi.viewer.indexer.helper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.geojson.LngLatAlt;
import org.junit.Assert;
import org.junit.Test;

public class GeoJSONToolsTest {

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONString(String,String,String)
     * @verifies convert GML point correctly
     */
    @Test
    public void convertCoordinatesToGeoJSONString_shouldConvertGMLPointCorrectly() throws Exception {
        String geoJson = GeoJSONTools.convertCoordinatesToGeoJSONString(
                "51.8164115931853 9.86927764300289 ",
                "gml:Point", " ");
        Assert.assertTrue(geoJson, geoJson.contains("{\"type\":\"Point\",\"coordinates\":[51.8164115931853,9.86927764300289]}}]}"));
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONString(String,String,String)
     * @verifies convert GML polygon correctly
     */
    @Test
    public void convertCoordinatesToGeoJSONString_shouldConvertGMLPolygonCorrectly() throws Exception {
        // TODO proper content comparison
        Assert.assertTrue(GeoJSONTools.convertCoordinatesToGeoJSONString(
                "51.8164115931853 9.86927764300289 51.8164107865348 9.86927733742467 51.81640215001 9.86936345721908 51.8162882108512 9.86933544862899 51.8163013830216 9.86921939760192 51.8164140490608 9.86924941164474 51.8164115931853 9.86927764300289",
                "gml:Polygon", " ").contains("Polygon"));
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONString(String,String,String)
     * @verifies convert MODS point correctly
     */
    @Test
    public void convertCoordinatesToGeoJSONString_shouldConvertMODSPointCorrectly() throws Exception {
        {
            // 2D
            String geoJson = GeoJSONTools.convertCoordinatesToGeoJSONString("51.8164115931853 / 9.86927764300289", "mods:coordinates/point", " / ");
            Assert.assertTrue(geoJson, geoJson.contains("{\"type\":\"Point\",\"coordinates\":[51.8164115931853,9.86927764300289]}}]}"));
        }
        {
            // 3D
            String geoJson = GeoJSONTools.convertCoordinatesToGeoJSONString(
                    "51.8164115931853 / 9.86927764300289 / 123", "mods:coordinates/point", " / ");
            Assert.assertTrue(geoJson, geoJson.contains("{\"type\":\"Point\",\"coordinates\":[51.8164115931853,9.86927764300289,123.0]}}]}"));
        }
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONString(String,String,String)
     * @verifies convert deg min sec polygon correctly
     */
    @Test
    public void convertCoordinatesToGeoJSONString_shouldConvertDegMinSecPolygonCorrectly() throws Exception {
        System.out.println(GeoJSONTools.convertCoordinatesToGeoJSONString(
                "E0080756 E0083024 N0465228 N0465228",
                "sexagesimal:polygon", " "));
        Assert.assertTrue(GeoJSONTools.convertCoordinatesToGeoJSONString(
                "E0080756 E0083024 N0465228 N0465228",
                "sexagesimal:polygon", " ").contains("Polygon"));
    }

    /**
     * @see GeoJSONTools#convertToWKT(List)
     * @verifies convert points correctly
     */
    @Test
    public void convertToWKT_shouldConvertPointsCorrectly() throws Exception {
        LngLatAlt point = new LngLatAlt(1, 2);
        Assert.assertEquals("1.0 2.0", GeoJSONTools.convertToWKT(Collections.singletonList(point)));
    }

    /**
     * @see GeoJSONTools#convertToWKT(List)
     * @verifies convert polygons correctly
     */
    @Test
    public void convertToWKT_shouldConvertPolygonsCorrectly() throws Exception {
        List<LngLatAlt> points = new ArrayList<>(4);
        points.add(new LngLatAlt(0, 2));
        points.add(new LngLatAlt(2, 2));
        points.add(new LngLatAlt(2, 0));
        points.add(new LngLatAlt(0, 0));
        points.add(new LngLatAlt(0, 2));

        Assert.assertEquals("POLYGON((0.0 2.0, 2.0 2.0, 2.0 0.0, 0.0 0.0, 0.0 2.0))", GeoJSONTools.convertToWKT(points));
    }

    /**
     * @see GeoJSONTools#convertSexagesimalToDecimalPoints(String,String)
     * @verifies convert polygons correctly
     */
    @Test
    public void convertSexagesimalToDecimalPoints_shouldConvertPolygonsCorrectly() throws Exception {

        List<LngLatAlt> result = GeoJSONTools.convertSexagesimalToDecimalPoints("E0080756 E0083024 N0465228 N0465228", " ");
        Assert.assertEquals(5, result.size());
    }

    /**
     * @see GeoJSONTools#convertSexagesimalToDecimalPoints(String,String)
     * @verifies convert points correctly
     */
    @Test
    public void convertSexagesimalToDecimalPoints_shouldConvertPointsCorrectly() throws Exception {
        List<LngLatAlt> result = GeoJSONTools.convertSexagesimalToDecimalPoints("E0080756 N0465228", " ");
        Assert.assertEquals(1, result.size());
    }

    /**
     * @see GeoJSONTools#convertSexagesimalCoordinateToDecimal(String)
     * @verifies convert coordinate correctly
     */
    @Test
    public void convertSexagesimalCoordinateToDecimal_shouldConvertCoordinateCorrectly() throws Exception {
        Assert.assertEquals(8.13222222, GeoJSONTools.convertSexagesimalCoordinateToDecimal("E0080756"), 1e-8);
    }
}