package io.goobi.viewer.indexer.helper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import mil.nga.sf.geojson.FeatureCollection;
import mil.nga.sf.geojson.Geometry;
import mil.nga.sf.geojson.Point;
import mil.nga.sf.geojson.Polygon;
import mil.nga.sf.geojson.Position;

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
        Assert.assertTrue(geoJson, geoJson.contains("{\"type\":\"Point\",\"coordinates\":[51.8164115931853,9.86927764300289]}"));
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
            Assert.assertTrue(geoJson, geoJson.contains("{\"type\":\"Point\",\"coordinates\":[51.8164115931853,9.86927764300289]}"));
        }
        {
            // 3D
            String geoJson = GeoJSONTools.convertCoordinatesToGeoJSONString(
                    "51.8164115931853 / 9.86927764300289 / 123", "mods:coordinates/point", " / ");
            Assert.assertTrue(geoJson, geoJson.contains("{\"type\":\"Point\",\"coordinates\":[51.8164115931853,9.86927764300289,123.0]}"));
        }
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONString(String,String,String)
     * @verifies convert deg min sec polygon correctly
     */
    @Test
    public void convertCoordinatesToGeoJSONString_shouldConvertDegMinSecPolygonCorrectly() throws Exception {
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
        Position position = new Position(1.0, 2.0);
        Assert.assertEquals("1.0 2.0", GeoJSONTools.convertToWKT(Collections.singletonList(position)));
    }

    /**
     * @see GeoJSONTools#convertToWKT(List)
     * @verifies convert polygons correctly
     */
    @Test
    public void convertToWKT_shouldConvertPolygonsCorrectly() throws Exception {
        List<Position> positions = new ArrayList<>(4);
        positions.add(new Position(0.0, 2.0));
        positions.add(new Position(2.0, 2.0));
        positions.add(new Position(2.0, 0.0));
        positions.add(new Position(0.0, 0.0));
        positions.add(new Position(0.0, 2.0));

        Assert.assertEquals("POLYGON((0.0 2.0, 2.0 2.0, 2.0 0.0, 0.0 0.0, 0.0 2.0))", GeoJSONTools.convertToWKT(positions));
    }

    /**
     * @see GeoJSONTools#convertSexagesimalToDecimalPoints(String,String)
     * @verifies convert polygons correctly
     */
    @Test
    public void convertSexagesimalToDecimalPoints_shouldConvertPolygonsCorrectly() throws Exception {
        List<Position> result = GeoJSONTools.convertSexagesimalToDecimalPoints("E0080756 E0083024 N0465228 N0465228", " ");
        Assert.assertEquals(5, result.size());
    }

    /**
     * @see GeoJSONTools#convertSexagesimalToDecimalPoints(String,String)
     * @verifies convert points correctly
     */
    @Test
    public void convertSexagesimalToDecimalPoints_shouldConvertPointsCorrectly() throws Exception {
        List<Position> result = GeoJSONTools.convertSexagesimalToDecimalPoints("E0080756 N0465228", " ");
        Assert.assertEquals(1, result.size());
    }

    /**
     * @see GeoJSONTools#convertSexagesimalToDecimalPoints(String,String)
     * @verifies return single point if coordinates duplicate
     */
    @Test
    public void convertSexagesimalToDecimalPoints_shouldReturnSinglePointIfCoordinatesDuplicate() throws Exception {
        List<Position> result = GeoJSONTools.convertSexagesimalToDecimalPoints("E0080756 E0080756 N0465228 N0465228", " ");
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(Double.valueOf(8.13), Double.valueOf(Math.floor(result.get(0).getX() * 100) / 100));
        Assert.assertEquals(Double.valueOf(46.87), Double.valueOf(Math.floor(result.get(0).getY() * 100) / 100));
    }

    /**
     * @see GeoJSONTools#convertSexagesimalCoordinateToDecimal(String)
     * @verifies convert coordinate correctly
     */
    @Test
    public void convertSexagesimalCoordinateToDecimal_shouldConvertCoordinateCorrectly() throws Exception {
        Assert.assertEquals(8.13222222, GeoJSONTools.convertSexagesimalCoordinateToDecimal("E0080756"), 1e-8);
    }

    @Test
    public void convertSexagesimalCoordinateToDecimal_shouldConvertWesternCoordinateCorrectly() throws Exception {
        Assert.assertEquals(-8.13222222, GeoJSONTools.convertSexagesimalCoordinateToDecimal("W0080756"), 1e-8);
    }

    @Test
    public void convertSexagesimalCoordinateToDecimal_shouldConvertSouthernCoordinateCorrectly() throws Exception {
        Assert.assertEquals(-8.13222222, GeoJSONTools.convertSexagesimalCoordinateToDecimal("S0080756"), 1e-8);
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert gml point correctly
     */
    @Test
    public void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertGmlPointCorrectly() throws Exception {
        //TODO auto-generated
        FeatureCollection result = GeoJSONTools.convertCoordinatesToGeoJSONFeatureCollection("51.8164115931853 9.86927764300289 ", "gml:point", " ");
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.getFeatures().size());
        Geometry geometry = result.getFeatures().get(0).getGeometry();
        Assert.assertTrue(geometry.getClass().getName(), geometry.getClass().equals(Point.class));
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert gml point 4326 correctly
     */
    @Test
    public void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertGmlPoint4326Correctly() throws Exception {
        //TODO auto-generated
        Assert.fail("Not yet implemented");
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert gml polygon correctly
     */
    @Test
    public void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertGmlPolygonCorrectly() throws Exception {
        //TODO auto-generated
        FeatureCollection result = GeoJSONTools.convertCoordinatesToGeoJSONFeatureCollection(
                "51.8164115931853 9.86927764300289 51.8164107865348 9.86927733742467 51.81640215001 9.86936345721908 51.8162882108512 9.86933544862899 51.8163013830216 9.86921939760192 51.8164140490608 9.86924941164474 51.8164115931853 9.86927764300289",
                "gml:polygon", " ");
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.getFeatures().size());
        Geometry geometry = result.getFeatures().get(0).getGeometry();
        Assert.assertTrue(geometry.getClass().getName(), geometry.getClass().equals(Polygon.class));
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert gml polygon 4326 correctly
     */
    @Test
    public void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertGmlPolygon4326Correctly() throws Exception {
        //TODO auto-generated
        Assert.fail("Not yet implemented");
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert mods point correctly
     */
    @Test
    public void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertModsPointCorrectly() throws Exception {
        //TODO auto-generated
        Assert.fail("Not yet implemented");
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert sexagesimal point correctly
     */
    @Test
    public void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertSexagesimalPointCorrectly() throws Exception {
        //TODO auto-generated
        FeatureCollection result =
                GeoJSONTools.convertCoordinatesToGeoJSONFeatureCollection("E0080756 E0083024 N0465228 N0465228", "sexagesimal:polygon", " ");
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.getFeatures().size());
        Geometry geometry = result.getFeatures().get(0).getGeometry();
        Assert.assertTrue(geometry.getClass().getName(), geometry.getClass().equals(Polygon.class));
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert sexagesimal polygon correctly
     */
    @Test
    public void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertSexagesimalPolygonCorrectly() throws Exception {
        //TODO auto-generated
        FeatureCollection result = GeoJSONTools.convertCoordinatesToGeoJSONFeatureCollection("E0080756 N0465228", "sexagesimal:point", " ");
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.getFeatures().size());
        Geometry geometry = result.getFeatures().get(0).getGeometry();
        Assert.assertTrue(geometry.getClass().getName(), geometry.getClass().equals(Point.class));

    }
}