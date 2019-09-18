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

import org.apache.commons.lang.StringUtils;

public class WebAnnotationTools {

    private static final String TARGET_REGEX = ".*/iiif/manifests/(.+?)/(?:canvas|manifest)?(?:/(\\d+))?/?$";

    /**
     * Extract the page order from a canvas url. If the url points to a manifest, return null
     * 
     * @param uri
     * @return
     */
    public static Integer parsePageOrder(URI uri) {
        Matcher matcher = Pattern.compile(TARGET_REGEX).matcher(uri.toString());
        if (matcher.find()) {
            String pageNo = matcher.group(2);
            if (StringUtils.isNotBlank(pageNo)) {
                return Integer.parseInt(pageNo);
            }
            return null;
        }
        return null;
    }
}
