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

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * <p>
 * WebAnnotationTools class.
 * </p>
 *
 */
public class WebAnnotationTools {

    private static final String TARGET_REGEX = ".*/iiif/manifests/(.+?)/(?:canvas|manifest)?(?:/(\\d+))?/?$";
    private static final String TARGET_REGEX_CANVAS = ".*/records/(.+?)/pages/(\\d+)/canvas/?$";
    private static final String TARGET_REGEX_MANIFEST = ".*/records/(.+?)/manifest/?$";
    private static final String TARGET_REGEX_SECTION = ".*/records/(.+?)/sections/(.+?)/range/?$";

    /**
     * Extract the page order from a canvas url. If the url points to a manifest, return null
     *
     * @param uri a {@link java.net.URI} object.
     * @return a {@link java.lang.Integer} object.
     */
    public static Integer parsePageOrder(URI uri) {

        Matcher matcher = getMatchingMatcher(uri);
        if (matcher.groupCount() > 1) {
            String pageNo = matcher.group(2);
            if (StringUtils.isNotBlank(pageNo) && StringUtils.isNumeric(pageNo)) {
                return Integer.parseInt(pageNo);
            }
        }
        return null;

    }

    public static String parsePI(URI uri) {
        Matcher matcher = getMatchingMatcher(uri);
        if (matcher.groupCount() > 0) {
            String pi = matcher.group(1);
            if (StringUtils.isNotBlank(pi)) {
                return pi;
            }
        }
        return null;
    }

    public static String parseDivId(URI uri) {
        Matcher matcher = Pattern.compile(TARGET_REGEX_SECTION).matcher(uri.toString());

        if (matcher.find()) {
            String divId = matcher.group(2);
            if (StringUtils.isNotBlank(divId)) {
                return divId;
            }
        }
        return null;
    }

    /**
     * @param uri
     * @return
     */
    public static Matcher getMatchingMatcher(URI uri) {
        Matcher matcher = null;
        Matcher matcherOld = Pattern.compile(TARGET_REGEX).matcher(uri.toString());
        Matcher matcherCanvas = Pattern.compile(TARGET_REGEX_CANVAS).matcher(uri.toString());
        Matcher matcherManifest = Pattern.compile(TARGET_REGEX_MANIFEST).matcher(uri.toString());
        Matcher matcherSection = Pattern.compile(TARGET_REGEX_SECTION).matcher(uri.toString());

        if (matcherOld.matches()) {
            matcher = matcherOld;
        } else if (matcherCanvas.matches()) {
            matcher = matcherCanvas;
        } else if (matcherManifest.matches()) {
            matcher = matcherManifest;
        } else if (matcherSection.matches()) {
            matcher = matcherSection;
        }
        return matcher;
    }
}
