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
package io.goobi.viewer.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.helper.MetadataHelper;

/**
 * <p>
 * Version class.
 * </p>
 *
 */
public class Version {

    private static final Logger logger = LoggerFactory.getLogger(Version.class);

    /** Constant <code>APPLICATION_NAME</code> */
    public static final String APPLICATION_NAME;
    /** Constant <code>VERSION</code> */
    public final static String VERSION;
    /** Constant <code>BUILDVERSION</code> */
    public final static String BUILDVERSION;
    /** Constant <code>BUILDDATE</code> */
    public final static String BUILDDATE;

    static {
        String manifest = getManifestStringFromJar();
        if (StringUtils.isNotBlank(manifest)) {
            APPLICATION_NAME = getInfo("ApplicationName", manifest);
            VERSION = getInfo("version", manifest);
            BUILDDATE = getInfo("Implementation-Build-Date", manifest);
            BUILDVERSION = getInfo("Implementation-Version", manifest);
        } else {
            APPLICATION_NAME = "goobi-viewer-indexer";
            VERSION = "unknown";
            BUILDDATE = LocalDateTime.now().format(MetadataHelper.formatterISO8601DateTimeNoSeconds);
            BUILDVERSION = "unknown";
        }
    }

    private static String getManifestStringFromJar() {
        Class clazz = Version.class;
        String className = clazz.getSimpleName() + ".class";
        String classPath = clazz.getResource(className).toString();
        logger.trace("classpath: {}", classPath);
        String value = null;
        String manifestPath = classPath.substring(0, classPath.lastIndexOf("/io/goobi")) + "/META-INF/MANIFEST.MF";
        logger.trace(manifestPath);

        try (InputStream inputStream = new URL(manifestPath).openStream()) {
            StringWriter writer = new StringWriter();
            IOUtils.copy(inputStream, writer, "utf-8");
            String manifestString = writer.toString();
            value = manifestString;
        } catch (MalformedURLException e) {
            return null;
        } catch (IOException e) {
            return null;
        }
        return value;
    }

    private static String getInfo(String label, String infoText) {
        String regex = label + ": (.*)";
        Matcher matcher = Pattern.compile(regex).matcher(infoText);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "?";
    }

    /**
     * 
     * @return Version info as a single string
     */
    public static String asString() {
        return APPLICATION_NAME + " " + VERSION + " " + BUILDDATE + " " + BUILDVERSION;
    }

    /**
     * 
     * @return JSON object containing version info
     */
    public static String asJSON() {
        return new JSONObject().put("application", APPLICATION_NAME)
                .put("version", VERSION)
                .put("build-date", BUILDDATE)
                .put("git-revision", BUILDVERSION)
                .toString();
    }
}
