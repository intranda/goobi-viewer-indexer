
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

/**
 * <p>Version class.</p>
 *
 */
public class Version {
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
        String value = null;
        String manifestPath;
        if (classPath.contains("WEB-INF")) {
            // Server
            manifestPath = classPath.substring(0, classPath.lastIndexOf("/WEB-INF/")) + "/META-INF/MANIFEST.MF";
        } else {
            // Eclipse WTP
             manifestPath = classPath.substring(0, classPath.lastIndexOf("/classes/")) + "/m2e-wtp/web-resources/META-INF/MANIFEST.MF";
        }
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
