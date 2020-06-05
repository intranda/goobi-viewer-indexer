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
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Version class.
 * </p>
 *
 */
public class Version {
    
    private static final Logger logger = LoggerFactory.getLogger(Version.class);
    
    /** Constant <code>VERSION</code> */
    public final static String VERSION;
    /** Constant <code>BUILDVERSION</code> */
    public final static String BUILDVERSION;
    /** Constant <code>BUILDDATE</code> */
    public final static String BUILDDATE;

    static {
        String manifest = getManifestStringFromJar();
        if (StringUtils.isNotBlank(manifest)) {
            VERSION = getInfo("ApplicationName", manifest) + " " + getInfo("version", manifest);
            BUILDDATE = getInfo("Implementation-Build-Date", manifest);
            BUILDVERSION = getInfo("Implementation-Version", manifest);
        } else {
            VERSION = "unknown";
            BUILDDATE = new Date().toString();
            BUILDVERSION = "unknown";
        }
    }

    private static String getManifestStringFromJar() {
        Class clazz = Version.class;
        String className = clazz.getSimpleName() + ".class";
        String classPath = clazz.getResource(className).toString();
        logger.info("classpath: {}", classPath);
        String value = null;
        String manifestPath = classPath.substring(0, classPath.lastIndexOf(".jar")) + "/META-INF/MANIFEST.MF";
        logger.info(manifestPath);

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
        String regex = label + ": *(\\S*)";
        Matcher matcher = Pattern.compile(regex).matcher(infoText);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "?";
    }

}
