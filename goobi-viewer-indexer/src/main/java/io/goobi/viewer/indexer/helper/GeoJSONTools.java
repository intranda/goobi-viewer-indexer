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
     * 
     * @param coords
     * @return geoJSON string
     * @should convert GML point correctly
     * @should convert GML polygon correctly
     * @should convert MODS point correctly
     */
    public static String convertCoordinatesToGeoJSON(String coords, String type, String separator) {
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
                List<LngLatAlt> polygon = convertPoints(coords, separator, 2);
                if (!polygon.isEmpty()) {
                    geometry = new Point(polygon.get(0));
                }
            }
                break;
            case "gml:polygon": {
                List<LngLatAlt> polygon = convertPoints(coords, separator, 2);
                if (!polygon.isEmpty()) {
                    geometry = new Polygon(polygon);
                }
            }
                break;
            case "mods:coordinates/point": {
                List<LngLatAlt> polygon = convertPoints(coords, separator, coords.split(separator).length);
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

        try {
            return new ObjectMapper().writeValueAsString(featureCollection);
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * 
     * @param gml GML coordinates
     * @param separator
     * @return List of LngLatAlt points
     */
    static List<LngLatAlt> convertPoints(String gml, String separator, int dimensions) {
        if (StringUtils.isEmpty(gml)) {
            return Collections.emptyList();
        }
        if (separator == null) {
            separator = " ";
        }

        String[] gmlSplit = gml.split(separator);
        List<LngLatAlt> ret = new ArrayList<>(gmlSplit.length / 2);
        double[] point = { -1, -1, -1 };
        int count = 0;
        for (String coord : gmlSplit) {
            point[count] = Double.valueOf(coord);
            count++;
            if (count == dimensions) {
                if (point[2] != -1) {
                    ret.add(new LngLatAlt(point[0], point[1], point[2]));
                } else {
                    ret.add(new LngLatAlt(point[0], point[1]));
                }
                count = 0;
            }
        }

        return ret;
    }
}
