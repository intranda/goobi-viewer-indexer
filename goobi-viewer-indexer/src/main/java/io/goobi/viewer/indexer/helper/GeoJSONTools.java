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
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.GeoJsonObject;
import org.geojson.LngLatAlt;
import org.geojson.Point;
import org.geojson.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GeoJSONTools {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(GeoJSONTools.class);

    /**
     * Converts the given coordinates to their WKT representation (via converting them to geoJSON first).
     * 
     * @param points One or more points
     * @return WKT representation of the given coordinates
     * @should convert points correctly
     * @should convert polygons correctly
     */
    static String convertoToWKT(String coords, String type, String separator) {
        FeatureCollection featureCollection = convertCoordinatesToGeoJSONFeatureCollection(coords, type, separator);
        if (featureCollection.getFeatures().isEmpty()) {
            return null;
        }

        GeoJsonObject geometry = featureCollection.getFeatures().get(0).getGeometry();

        if (geometry instanceof Point) {
            // Point
            Point point = (Point) geometry;
            return point.getCoordinates().getLongitude() + " " + point.getCoordinates().getLatitude();
        }

        if (geometry instanceof Polygon) {
            // Polygon
            Polygon polygon = (Polygon) geometry;
            StringBuilder sb = new StringBuilder("POLYGON((");
            int count = 0;
            if (polygon.getCoordinates().isEmpty()) {
                return null;
            }

            for (LngLatAlt point : polygon.getCoordinates().get(0)) {
                if (count > 0) {
                    sb.append(", ");
                }
                sb.append(point.getLongitude()).append(' ').append(point.getLatitude());
                count++;
            }
            sb.append("))");

            return sb.toString();
        }

        return null;

    }

    /**
     * 
     * @param coords
     * @return geoJSON string
     * @should convert GML point correctly
     * @should convert GML polygon correctly
     * @should convert MODS point correctly
     */
    public static String convertCoordinatesToGeoJSONString(String coords, String type, String separator) {
        FeatureCollection featureCollection = convertCoordinatesToGeoJSONFeatureCollection(coords, type, separator);
        try {
            return new ObjectMapper().writeValueAsString(featureCollection);
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
     */
    public static FeatureCollection convertCoordinatesToGeoJSONFeatureCollection(String coords, String type, String separator) {
        if (coords == null) {
            return null;
        }
        if (type == null) {
            throw new IllegalArgumentException("type may not be null");
        }

        FeatureCollection featureCollection = new FeatureCollection();
        Feature feature = new Feature();
        featureCollection.add(feature);

        GeoJsonObject geometry = null;
        switch (type.toLowerCase()) {
            case "gml:point": {
                List<LngLatAlt> polygon = convertPoints(coords, separator, 2, false);
                if (!polygon.isEmpty()) {
                    geometry = new Point(polygon.get(0));
                }
            }
                break;
            case "gml:point:4326": {
                List<LngLatAlt> polygon = convertPoints(coords, separator, 2, true);
                if (!polygon.isEmpty()) {
                    geometry = new Point(polygon.get(0));
                }
            }
                break;
            case "gml:polygon": {
                List<LngLatAlt> polygon = convertPoints(coords, separator, 2, false);
                if (!polygon.isEmpty()) {
                    geometry = new Polygon(polygon);
                }
            }
                break;
            case "gml:polygon:4326": {
                List<LngLatAlt> polygon = convertPoints(coords, separator, 2, true);
                if (!polygon.isEmpty()) {
                    geometry = new Polygon(polygon);
                }
            }
                break;
            case "mods:coordinates/point": {
                List<LngLatAlt> polygon = convertPoints(coords, separator, coords.split(separator).length, false);
                if (!polygon.isEmpty()) {
                    geometry = new Point(polygon.get(0));
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
    static List<LngLatAlt> convertPoints(String coords, String separator, int dimensions, boolean revert) {
        if (StringUtils.isEmpty(coords)) {
            return Collections.emptyList();
        }
        if (separator == null) {
            separator = " ";
        }

        String[] coordsSplit = coords.split(separator);
        List<LngLatAlt> ret = new ArrayList<>(coordsSplit.length / 2);
        double[] point = { -1, -1, -1 };
        int count = 0;
        for (String coord : coordsSplit) {
            point[count] = Double.valueOf(coord);
            count++;
            if (count == dimensions) {
                if (point[2] != -1) {
                    if (revert) {
                        // Not sure this can ever be the case
                        ret.add(new LngLatAlt(point[1], point[0], point[2]));
                    } else {
                        ret.add(new LngLatAlt(point[0], point[1], point[2]));
                    }
                } else {
                    if (revert) {
                        ret.add(new LngLatAlt(point[1], point[0]));
                    } else {
                        ret.add(new LngLatAlt(point[0], point[1]));
                    }
                }
                count = 0;
            }
        }

        return ret;
    }
}
