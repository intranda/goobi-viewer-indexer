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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.goobi.viewer.indexer.model.GeoCoords;
import mil.nga.sf.geojson.Feature;
import mil.nga.sf.geojson.FeatureCollection;
import mil.nga.sf.geojson.GeoJsonObject;
import mil.nga.sf.geojson.Geometry;
import mil.nga.sf.geojson.Point;
import mil.nga.sf.geojson.Polygon;
import mil.nga.sf.geojson.Position;

public class GeoJSONTools {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(GeoJSONTools.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new JavaTimeModule());
    }

    public static GeoCoords convert(String coords, String type, String separator) {
        FeatureCollection featureCollection = convertCoordinatesToGeoJSONFeatureCollection(coords, type, separator);

        String geoJSON = null;
        try {
            geoJSON = new ObjectMapper().writeValueAsString(featureCollection);
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
        }

        String wkt = convertToWKT(featureCollection);

        return new GeoCoords(geoJSON, wkt);
    }

    /**
     * Extracts points from the given <code>FeatureCollection</code> and converts them to their WKT representation (via converting them to geoJSON
     * first).
     * 
     * @param featureCollection
     * @return WKT representation of the given coordinates
     */
    static String convertToWKT(FeatureCollection featureCollection) {
        if (featureCollection == null || featureCollection.getFeatures().isEmpty() || featureCollection.getFeatures().get(0).getGeometry() == null) {
            logger.error("geoJSON object invalid or null.");
            return null;
        }

        GeoJsonObject geometry = featureCollection.getFeatures().get(0).getGeometry();
        if (geometry instanceof Point) {
            // Point
            Point point = (Point) geometry;
            return convertToWKT(Collections.singletonList(point.getPosition()));
        }
        if (geometry instanceof Polygon) {
            // Polygon
            Polygon polygon = (Polygon) geometry;
            if (polygon.getCoordinates().isEmpty()) {
                return null;
            }

            return convertToWKT(polygon.getCoordinates().get(0));
        }

        return null;

    }

    /**
     * Converts the given coordinates to their WKT representation (via converting them to geoJSON first).
     * 
     * @param positions List of <code>LngLatAlt</code>
     * @return WKT representation of the given coordinates
     * @should convert points correctly
     * @should convert polygons correctly
     */
    static String convertToWKT(List<Position> positions) {
        if (positions == null || positions.isEmpty()) {
            return null;
        }

        if (positions.size() == 1) {
            // Point
            return positions.get(0).getX() + " " + positions.get(0).getY();
        }

        // Polygon
        StringBuilder sb = new StringBuilder("POLYGON((");
        int count = 0;
        for (Position position : positions) {
            if (count > 0) {
                sb.append(", ");
            }
            sb.append(position.getX()).append(' ').append(position.getY());
            count++;
        }
        sb.append("))");

        return sb.toString();
    }

    /**
     * 
     * @param coords
     * @return geoJSON string
     * @should convert GML point correctly
     * @should convert GML polygon correctly
     * @should convert MODS point correctly
     * @should convert deg min sec polygon correctly
     */
    public static String convertCoordinatesToGeoJSONString(String coords, String type, String separator) {
        FeatureCollection featureCollection = convertCoordinatesToGeoJSONFeatureCollection(coords, type, separator);
        try {
            return mapper.writeValueAsString(featureCollection);
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
        }

        return null;
    }

    /**
     * 
     * @param coords
     * @param type
     * @param separator
     * @return
     * @should convert gml point correctly
     * @should convert gml point 4326 correctly
     * @should convert gml polygon correctly
     * @should convert gml polygon 4326 correctly
     * @should convert mods point correctly
     * @should convert sexagesimal point correctly
     * @should convert sexagesimal polygon correctly
     */
    public static FeatureCollection convertCoordinatesToGeoJSONFeatureCollection(String coords, String type, String separator) {
        if (coords == null) {
            return null;
        }
        if (type == null) {
            throw new IllegalArgumentException("type may not be null");
        }
        logger.trace("convertCoordinatesToGeoJSONFeatureCollection: {} / {}", coords, type);

        FeatureCollection featureCollection = new FeatureCollection();
        Feature feature = new Feature();
        featureCollection.addFeature(feature);

        Geometry geometry = null;
        switch (type.toLowerCase()) {
            case "gml:point": {
                List<Position> polygon = convertPoints(coords, separator, 2, false);
                if (!polygon.isEmpty()) {
                    geometry = Point.fromCoordinates(polygon.get(0));
                }
            }
                break;
            case "gml:point:4326": {
                List<Position> polygon = convertPoints(coords, separator, 2, true);
                if (!polygon.isEmpty()) {
                    geometry = Point.fromCoordinates(polygon.get(0));
                }
            }
                break;
            case "gml:polygon": {
                List<Position> polygon = convertPoints(coords, separator, 2, false);
                if (!polygon.isEmpty()) {
                    geometry = Polygon.fromCoordinates(Collections.singletonList(polygon));
                }
            }
                break;
            case "gml:polygon:4326": {
                List<Position> polygon = convertPoints(coords, separator, 2, true);
                if (!polygon.isEmpty()) {
                    geometry = Polygon.fromCoordinates(Collections.singletonList(polygon));
                }
            }
                break;
            case "mods:coordinates/point": {
                List<Position> polygon = convertPoints(coords, separator, coords.split(separator).length, false);
                if (!polygon.isEmpty()) {
                    geometry = Point.fromCoordinates(polygon.get(0));
                }
            }
                break;
            case "sexagesimal:point": {
                List<Position> polygon = convertSexagesimalToDecimalPoints(coords, separator);
                if (!polygon.isEmpty()) {
                    geometry = Point.fromCoordinates(polygon.get(0));
                }
            }
                break;
            case "sexagesimal:polygon": {
                List<Position> polygon = convertSexagesimalToDecimalPoints(coords, separator);
                if (!polygon.isEmpty()) {
                    if (polygon.size() == 1) {
                        geometry = Point.fromCoordinates(polygon.get(0));
                    } else {
                        geometry = Polygon.fromCoordinates(Collections.singletonList(polygon));
                    }
                }
            }
                break;
            default:
                logger.error("Unknown type: {}", type);
                return null;
        }

        if (geometry != null) {
            feature.setGeometry(geometry);
        }

        return featureCollection;
    }

    /**
     * 
     * @param coords Coordinates
     * @param separator Optional separator between axes
     * @param dimensions Number of dimensions (usually 2 or 3)
     * @param revert If true, it will be assumed the format is lat-long instead of long-lat
     * @return List of LngLatAlt points
     */
    static List<Position> convertPoints(String coords, String separator, int dimensions, boolean revert) {
        if (StringUtils.isEmpty(coords)) {
            return Collections.emptyList();
        }
        if (separator == null) {
            separator = " ";
        }

        String[] coordsSplit = coords.split(separator);
        List<Position> ret = new ArrayList<>(coordsSplit.length / 2);
        double[] point = { -1, -1, -1 };
        int count = 0;
        for (String coord : coordsSplit) {
            point[count] = Double.valueOf(coord);
            count++;
            if (count == dimensions) {
                if (point[2] != -1) {
                    if (revert) {
                        // Not sure this can ever be the case
                        ret.add(new Position(point[1], point[0], point[2]));
                    } else {
                        ret.add(new Position(point[0], point[1], point[2]));
                    }
                } else {
                    if (revert) {
                        ret.add(new Position(point[1], point[0]));
                    } else {
                        ret.add(new Position(point[0], point[1]));
                    }
                }
                count = 0;
            }
        }

        return ret;
    }

    /**
     * Converts a string containing two or four sexagesimal coordinates into <code>LngLatAlt</code> objects.
     * 
     * @param coords Two or four sexagesimal coordinates in the order "lon lat" (point) or "west east north south" (rectangle)
     * @param separator Optional custom separator between coordinates
     * @return List of LngLatAlt points
     * @should convert points correctly
     * @should convert polygons correctly
     * @should return single point if coordinates duplicate
     */
    static List<Position> convertSexagesimalToDecimalPoints(String coords, String separator) {
        if (StringUtils.isEmpty(coords)) {
            return Collections.emptyList();
        }
        if (separator == null) {
            separator = " ";
        }

        String[] coordsSplit = coords.split(separator);
        List<Position> ret = new ArrayList<>();
        double[] decimalValues = new double[coordsSplit.length];
        for (int i = 0; i < coordsSplit.length; ++i) {
            decimalValues[i] = convertSexagesimalCoordinateToDecimal(coordsSplit[i]);
        }
        switch (decimalValues.length) {
            case 2:
                ret.add(new Position(decimalValues[0], decimalValues[1]));
                break;
            case 4:
                if (decimalValues[0] == decimalValues[1] && decimalValues[2] == decimalValues[3]) {
                    // A single point in four duplicate coords
                    ret.add(new Position(decimalValues[0], decimalValues[2]));
                } else {
                    // Proper polygon
                    ret.add(new Position(decimalValues[0], decimalValues[2]));
                    ret.add(new Position(decimalValues[1], decimalValues[2]));
                    ret.add(new Position(decimalValues[1], decimalValues[3]));
                    ret.add(new Position(decimalValues[0], decimalValues[3]));
                    ret.add(new Position(decimalValues[0], decimalValues[2]));
                }
                break;
            default:
                logger.warn("Incompatible coordinates: {}", coords);
                break;
        }

        return ret;

    }

    /**
     * Converts a single sexagesimal (XX° YY' UU") coordinate in degrees, minutes and seconds to a decimal coordinate.
     * 
     * @param coordinate 8-character coordinate string (e.g. E0080756)
     * @return
     * @should convert coordinate correctly
     */
    static double convertSexagesimalCoordinateToDecimal(String coordinate) {
        if (coordinate == null) {
            throw new IllegalArgumentException("coordinate may not be null");
        }
        if (coordinate.length() != 8) {
            throw new IllegalArgumentException("coordinate length must be 8: " + coordinate);
        }

        String direction = coordinate.substring(0, 1); // W, E, N, S
        int directionFactor = direction.matches("(?i)W|S") ? -1 : 1;
        int deg = Integer.valueOf(coordinate.substring(1, 4));
        double min = Integer.valueOf(coordinate.substring(4, 6));
        double sec = Integer.valueOf(coordinate.substring(6, 8));
        double ret = deg + (min / 60) + (sec / 3600);

        return ret * directionFactor;
        // return Double.valueOf(MetadataHelper.FORMAT_EIGHT_DECIMAL_PLACES.get().format(ret));
    }

    /**
     * 
     * @param coords
     * @return
     * @should detect sexagesimal points correctly
     * @should detect sexagesimal polygons correctly
     */
    static String getCoordinatesType(String coords) {
        String type = "mods:coordinates/point";

        String[] coordsSplit = coords.split(" ");
        if (coords.startsWith("E") || coords.startsWith("W")) {
            switch (coordsSplit.length) {
                case 2:
                    type = "sexagesimal:point";
                    break;
                case 4:
                    type = "sexagesimal:polygon";
                    break;
                default:
                    break;
            }
        }

        return type;
    }
}
