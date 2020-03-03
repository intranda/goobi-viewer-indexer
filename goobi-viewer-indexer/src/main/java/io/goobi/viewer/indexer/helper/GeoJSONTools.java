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
     * @param gml
     * @return
     * @should convert point correctly
     * @should convert polygon correctly
     */
    public static String convertGMLToGeoJSON(String gml, String type) {
        if (gml == null) {
            return null;
        }
        if (type == null) {
            throw new IllegalArgumentException("type may not be null");
        }

        FeatureCollection featureCollection = new FeatureCollection();
        Feature feature = new Feature();
        featureCollection.add(feature);

        List<LngLatAlt> polygon = convertPoints(gml);
        if (!polygon.isEmpty()) {
            GeoJsonObject geometry = null;
            switch (type.toLowerCase()) {
                case "gml:point":
                    geometry = new Point(polygon.get(0));
                    break;
                case "gml:polygon":
                    geometry = new Polygon(polygon);
                    break;
                default:
                    logger.error("Unknown type: {}", type);
                    return null;
            }
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
     * @return List of LngLatAlt points
     */
    static List<LngLatAlt> convertPoints(String gml) {
        if (StringUtils.isEmpty(gml)) {
            return Collections.emptyList();
        }

        String[] gmlSplit = gml.split(" ");
        List<LngLatAlt> ret = new ArrayList<>(gmlSplit.length / 2);
        boolean newPoint = true;
        double[] point = { -1, -1 };
        for (String coord : gmlSplit) {
            if (newPoint) {
                point[0] = Double.valueOf(coord);
            } else {
                point[1] = Double.valueOf(coord);
                if (point[0] != -1 && point[1] != -1) {
                    ret.add(new LngLatAlt(point[0], point[1]));
                    point[0] = -1;
                    point[1] = -1;
                }
            }
            newPoint = !newPoint;
        }

        return ret;
    }
}
