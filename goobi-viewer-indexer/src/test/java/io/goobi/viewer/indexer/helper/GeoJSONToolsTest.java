package io.goobi.viewer.indexer.helper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import mil.nga.sf.geojson.FeatureCollection;
import mil.nga.sf.geojson.Geometry;
import mil.nga.sf.geojson.Point;
import mil.nga.sf.geojson.Polygon;
import mil.nga.sf.geojson.Position;

class GeoJSONToolsTest {

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONString(String,String,String)
     * @verifies convert GML point correctly
     */
    @Test
    void convertCoordinatesToGeoJSONString_shouldConvertGMLPointCorrectly() throws Exception {
        String geoJson = GeoJSONTools.convertCoordinatesToGeoJSONString(
                "51.8164115931853 9.86927764300289 ",
                "gml:Point", " ");
        Assertions.assertTrue(geoJson.contains("{\"type\":\"Point\",\"coordinates\":[51.8164115931853,9.86927764300289]}"), geoJson);
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONString(String,String,String)
     * @verifies convert GML polygon correctly
     */
    @Test
    void convertCoordinatesToGeoJSONString_shouldConvertGMLPolygonCorrectly() throws Exception {
        // TODO proper content comparison
        Assertions.assertTrue(GeoJSONTools.convertCoordinatesToGeoJSONString(
                "51.8164115931853 9.86927764300289 51.8164107865348 9.86927733742467 51.81640215001 9.86936345721908 51.8162882108512 9.86933544862899 51.8163013830216 9.86921939760192 51.8164140490608 9.86924941164474 51.8164115931853 9.86927764300289",
                "gml:Polygon", " ").contains("Polygon"));
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONString(String,String,String)
     * @verifies convert MODS point correctly
     */
    @Test
    void convertCoordinatesToGeoJSONString_shouldConvertMODSPointCorrectly() throws Exception {
        {
            // 2D
            String geoJson = GeoJSONTools.convertCoordinatesToGeoJSONString("51.8164115931853 / 9.86927764300289", "mods:coordinates/point", " / ");
            Assertions.assertTrue(geoJson.contains("{\"type\":\"Point\",\"coordinates\":[51.8164115931853,9.86927764300289]}"), geoJson);
        }
        {
            // 3D
            String geoJson = GeoJSONTools.convertCoordinatesToGeoJSONString(
                    "51.8164115931853 / 9.86927764300289 / 123", "mods:coordinates/point", " / ");
            Assertions.assertTrue(geoJson.contains("{\"type\":\"Point\",\"coordinates\":[51.8164115931853,9.86927764300289,123.0]}"), geoJson);
        }
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONString(String,String,String)
     * @verifies convert deg min sec polygon correctly
     */
    @Test
    void convertCoordinatesToGeoJSONString_shouldConvertDegMinSecPolygonCorrectly() throws Exception {
        Assertions.assertTrue(GeoJSONTools.convertCoordinatesToGeoJSONString(
                "E0080756 E0083024 N0465228 N0465228",
                "sexagesimal:polygon", " ").contains("Polygon"));
    }

    /**
     * @see GeoJSONTools#convertToWKT(List)
     * @verifies convert points correctly
     */
    @Test
    void convertToWKT_shouldConvertPointsCorrectly() throws Exception {
        Position position = new Position(1.0, 2.0);
        Assertions.assertEquals("1.0 2.0", GeoJSONTools.convertToWKT(Collections.singletonList(position)));
    }

    /**
     * @see GeoJSONTools#convertToWKT(List)
     * @verifies convert polygons correctly
     */
    @Test
    void convertToWKT_shouldConvertPolygonsCorrectly() throws Exception {
        List<Position> positions = new ArrayList<>(4);
        positions.add(new Position(0.0, 2.0));
        positions.add(new Position(2.0, 2.0));
        positions.add(new Position(2.0, 0.0));
        positions.add(new Position(0.0, 0.0));
        positions.add(new Position(0.0, 2.0));

        Assertions.assertEquals("POLYGON((0.0 2.0, 2.0 2.0, 2.0 0.0, 0.0 0.0, 0.0 2.0))", GeoJSONTools.convertToWKT(positions));
    }

    /**
     * @see GeoJSONTools#convertSexagesimalToDecimalPoints(String,String)
     * @verifies convert polygons correctly
     */
    @Test
    void convertSexagesimalToDecimalPoints_shouldConvertPolygonsCorrectly() throws Exception {
        List<Position> result = GeoJSONTools.convertSexagesimalToDecimalPoints("E0080756 E0083024 N0465228 N0465228", " ");
        Assertions.assertEquals(5, result.size());
    }

    /**
     * @see GeoJSONTools#convertSexagesimalToDecimalPoints(String,String)
     * @verifies convert points correctly
     */
    @Test
    void convertSexagesimalToDecimalPoints_shouldConvertPointsCorrectly() throws Exception {
        Assertions.assertEquals(1, GeoJSONTools.convertSexagesimalToDecimalPoints("E0080756 N0465228", " ").size());
        Assertions.assertEquals(1, GeoJSONTools.convertSexagesimalToDecimalPoints("W0024143 N0544530", " ").size());
    }

    /**
     * @see GeoJSONTools#convertSexagesimalToDecimalPoints(String,String)
     * @verifies return single point if coordinates duplicate
     */
    @Test
    void convertSexagesimalToDecimalPoints_shouldReturnSinglePointIfCoordinatesDuplicate() throws Exception {
        List<Position> result = GeoJSONTools.convertSexagesimalToDecimalPoints("E0080756 E0080756 N0465228 N0465228", " ");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(Double.valueOf(8.13), Double.valueOf(Math.floor(result.get(0).getX() * 100) / 100));
        Assertions.assertEquals(Double.valueOf(46.87), Double.valueOf(Math.floor(result.get(0).getY() * 100) / 100));

        result = GeoJSONTools.convertSexagesimalToDecimalPoints("W0024143 W0024143 N0544530 N0544530", " ");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(Double.valueOf(-2.7), Double.valueOf(Math.floor(result.get(0).getX() * 100) / 100));
        Assertions.assertEquals(Double.valueOf(54.75), Double.valueOf(Math.floor(result.get(0).getY() * 100) / 100));
    }

    /**
     * @see GeoJSONTools#convertSexagesimalCoordinateToDecimal(String)
     * @verifies convert coordinate correctly
     */
    @Test
    void convertSexagesimalCoordinateToDecimal_shouldConvertCoordinateCorrectly() throws Exception {
        Assertions.assertEquals(8.13222222, GeoJSONTools.convertSexagesimalCoordinateToDecimal("E0080756"), 1e-8);
    }

    @Test
    void convertSexagesimalCoordinateToDecimal_shouldConvertWesternCoordinateCorrectly() throws Exception {
        Assertions.assertEquals(-8.13222222, GeoJSONTools.convertSexagesimalCoordinateToDecimal("W0080756"), 1e-8);
    }

    @Test
    void convertSexagesimalCoordinateToDecimal_shouldConvertSouthernCoordinateCorrectly() throws Exception {
        Assertions.assertEquals(-8.13222222, GeoJSONTools.convertSexagesimalCoordinateToDecimal("S0080756"), 1e-8);
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert gml point correctly
     */
    @Test
    void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertGmlPointCorrectly() throws Exception {
        FeatureCollection result = GeoJSONTools.convertCoordinatesToGeoJSONFeatureCollection("51.8164115931853 9.86927764300289 ", "gml:Point", " ");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getFeatures().size());
        Geometry geometry = result.getFeatures().get(0).getGeometry();
        Assertions.assertEquals(Point.class, geometry.getClass(), geometry.getClass().getName());
        Assertions.assertEquals(Double.valueOf(51.8164115931853), ((Point) geometry).getPosition().getX());
        Assertions.assertEquals(Double.valueOf(9.86927764300289), ((Point) geometry).getPosition().getY());
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert gml point 4326 correctly
     */
    @Test
    void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertGmlPoint4326Correctly() throws Exception {
        FeatureCollection result =
                GeoJSONTools.convertCoordinatesToGeoJSONFeatureCollection("9.86927764300289 51.8164115931853", "gml:Point:4326", " ");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getFeatures().size());
        Geometry geometry = result.getFeatures().get(0).getGeometry();
        Assertions.assertEquals(Point.class, geometry.getClass(), geometry.getClass().getName());
        Assertions.assertEquals(Double.valueOf(51.8164115931853), ((Point) geometry).getPosition().getX());
        Assertions.assertEquals(Double.valueOf(9.86927764300289), ((Point) geometry).getPosition().getY());
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert gml polygon correctly
     */
    @Test
    void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertGmlPolygonCorrectly() throws Exception {
        FeatureCollection result = GeoJSONTools.convertCoordinatesToGeoJSONFeatureCollection(
                "51.8164115931853 9.86927764300289 51.8164107865348 9.86927733742467 51.81640215001 9.86936345721908 51.8162882108512 9.86933544862899 51.8163013830216 9.86921939760192 51.8164140490608 9.86924941164474 51.8164115931853 9.86927764300289",
                "gml:polygon", " ");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getFeatures().size());
        Geometry geometry = result.getFeatures().get(0).getGeometry();
        Assertions.assertEquals(Polygon.class, geometry.getClass(), geometry.getClass().getName());
        Assertions.assertEquals(Double.valueOf(51.8164115931853), ((Polygon) geometry).getCoordinates().get(0).get(0).getX());
        Assertions.assertEquals(Double.valueOf(9.86927764300289), ((Polygon) geometry).getCoordinates().get(0).get(0).getY());
        Assertions.assertEquals(Double.valueOf(51.8164107865348), ((Polygon) geometry).getCoordinates().get(0).get(1).getX());
        Assertions.assertEquals(Double.valueOf(9.86927733742467), ((Polygon) geometry).getCoordinates().get(0).get(1).getY());
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert gml polygon 4326 correctly
     */
    @Test
    void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertGmlPolygon4326Correctly() throws Exception {
        FeatureCollection result = GeoJSONTools.convertCoordinatesToGeoJSONFeatureCollection(
                "51.8164115931853 9.86927764300289 51.8164107865348 9.86927733742467 51.81640215001 9.86936345721908 51.8162882108512 9.86933544862899 51.8163013830216 9.86921939760192 51.8164140490608 9.86924941164474 51.8164115931853 9.86927764300289",
                "gml:polygon:4326", " ");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getFeatures().size());
        Geometry geometry = result.getFeatures().get(0).getGeometry();
        Assertions.assertEquals(Polygon.class, geometry.getClass(), geometry.getClass().getName());
        Assertions.assertEquals(Double.valueOf(51.8164115931853), ((Polygon) geometry).getCoordinates().get(0).get(0).getY());
        Assertions.assertEquals(Double.valueOf(9.86927764300289), ((Polygon) geometry).getCoordinates().get(0).get(0).getX());
        Assertions.assertEquals(Double.valueOf(51.8164107865348), ((Polygon) geometry).getCoordinates().get(0).get(1).getY());
        Assertions.assertEquals(Double.valueOf(9.86927733742467), ((Polygon) geometry).getCoordinates().get(0).get(1).getX());
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert mods point correctly
     */
    @Test
    void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertModsPointCorrectly() throws Exception {
        {
            // 2D
            FeatureCollection result =
                    GeoJSONTools.convertCoordinatesToGeoJSONFeatureCollection("51.8164115931853 / 9.86927764300289", "mods:coordinates/point", " / ");
            Assertions.assertEquals(1, result.getFeatures().size());
            Geometry geometry = result.getFeatures().get(0).getGeometry();
            Assertions.assertEquals(Point.class, geometry.getClass(), geometry.getClass().getName());
            Assertions.assertEquals(Double.valueOf(51.8164115931853), ((Point) geometry).getPosition().getX());
            Assertions.assertEquals(Double.valueOf(9.86927764300289), ((Point) geometry).getPosition().getY());
        }
        {
            // 3D
            FeatureCollection result = GeoJSONTools.convertCoordinatesToGeoJSONFeatureCollection(
                    "51.8164115931853 / 9.86927764300289 / 123", "mods:coordinates/point", " / ");
            Assertions.assertEquals(1, result.getFeatures().size());
            Geometry geometry = result.getFeatures().get(0).getGeometry();
            Assertions.assertEquals(Point.class, geometry.getClass(), geometry.getClass().getName());
            Assertions.assertEquals(Double.valueOf(51.8164115931853), ((Point) geometry).getPosition().getX());
            Assertions.assertEquals(Double.valueOf(9.86927764300289), ((Point) geometry).getPosition().getY());
            Assertions.assertEquals(Double.valueOf(123), ((Point) geometry).getPosition().getZ());
        }
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert sexagesimal point correctly
     */
    @Test
    void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertSexagesimalPointCorrectly() throws Exception {
        FeatureCollection result =
                GeoJSONTools.convertCoordinatesToGeoJSONFeatureCollection("E0080756 E0083024 N0465228 N0465228", "sexagesimal:polygon", " ");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getFeatures().size());
        Geometry geometry = result.getFeatures().get(0).getGeometry();
        Assertions.assertEquals(Polygon.class, geometry.getClass(), geometry.getClass().getName());
        Assertions.assertEquals(Double.valueOf(8.132222222222223), ((Polygon) geometry).getCoordinates().get(0).get(0).getX());
        Assertions.assertEquals(Double.valueOf(46.87444444444444), ((Polygon) geometry).getCoordinates().get(0).get(0).getY());
        Assertions.assertEquals(Double.valueOf(8.506666666666666), ((Polygon) geometry).getCoordinates().get(0).get(1).getX());
        Assertions.assertEquals(Double.valueOf(46.87444444444444), ((Polygon) geometry).getCoordinates().get(0).get(1).getY());
    }

    /**
     * @see GeoJSONTools#convertCoordinatesToGeoJSONFeatureCollection(String,String,String)
     * @verifies convert sexagesimal polygon correctly
     */
    @Test
    void convertCoordinatesToGeoJSONFeatureCollection_shouldConvertSexagesimalPolygonCorrectly() throws Exception {
        FeatureCollection result = GeoJSONTools.convertCoordinatesToGeoJSONFeatureCollection("E0080756 N0465228", "sexagesimal:point", " ");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getFeatures().size());
        Geometry geometry = result.getFeatures().get(0).getGeometry();
        Assertions.assertEquals(Point.class, geometry.getClass(), geometry.getClass().getName());
        Assertions.assertEquals(Double.valueOf(8.132222222222223), ((Point) geometry).getPosition().getX());
        Assertions.assertEquals(Double.valueOf(46.87444444444444), ((Point) geometry).getPosition().getY());

    }

    /**
     * @see GeoJSONTools#getCoordinatesType(String)
     * @verifies detect sexagesimal points correctly
     */
    @Test
    void getCoordinatesType_shouldDetectSexagesimalPointsCorrectly() throws Exception {
        Assertions.assertEquals("sexagesimal:point", GeoJSONTools.getCoordinatesType("W0024143 N0465228"));
    }

    /**
     * @see GeoJSONTools#getCoordinatesType(String)
     * @verifies detect sexagesimal polygons correctly
     */
    @Test
    void getCoordinatesType_shouldDetectSexagesimalPolygonsCorrectly() throws Exception {
        Assertions.assertEquals("sexagesimal:polygon", GeoJSONTools.getCoordinatesType("E0080756 E0083024 N0465228 N0465228"));
    }
}