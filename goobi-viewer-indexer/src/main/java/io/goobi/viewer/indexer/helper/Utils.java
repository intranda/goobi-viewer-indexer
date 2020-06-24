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

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.xml.ws.http.HTTPException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.MetsIndexer;
import io.goobi.viewer.indexer.Version;
import io.goobi.viewer.indexer.model.FatalIndexerException;

/**
 * <p>
 * Utils class.
 * </p>
 *
 */
public class Utils {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    private static final int HTTP_TIMEOUT = 30000;

    /**
     * <p>
     * checkAndCreateDirectory.
     * </p>
     *
     * @param path a {@link java.nio.file.Path} object.
     * @return a boolean.
     */
    public static boolean checkAndCreateDirectory(Path path) {
        if (path == null) {
            return false;
        }
        if (!Files.isDirectory(path)) {
            try {
                Files.createDirectories(path);
                logger.info("Created folder: {}", path.toAbsolutePath());
            } catch (IOException e) {
                logger.error(e.getMessage());
                return false;
            }
        }

        return true;
    }

    /**
     * <p>
     * deleteDirectory.
     * </p>
     *
     * @param path {@link java.io.File}
     * @return boolean
     */
    public static boolean deleteDirectory(Path path) {
        if (Files.isDirectory(path)) {
            try {
                FileUtils.deleteDirectory(path.toFile());
                return true;
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        return false;
    }

    /**
     * 
     * @param pi
     * @param dataRepositoryName
     * @throws FatalIndexerException
     * @throws IOException
     * @throws ClientProtocolException
     * @throws HTTPException
     */
    public static void updateDataRepositoryCache(String pi, String dataRepositoryName)
            throws FatalIndexerException, HTTPException, ClientProtocolException, IOException {
        if (pi == null) {
            throw new IllegalArgumentException("pi may not be null");
        }
        if (dataRepositoryName == null) {
            throw new IllegalArgumentException("dataRepositoryName may not be null");
        }

        logger.info("Updating data repository cache...");
        Map<String, String> params = new HashMap<>(2);
        JSONObject json = new JSONObject();
        json.put("pi", pi);
        json.put("dataRepositoryName", dataRepositoryName);

        String url = Configuration.getInstance().getViewerUrl() + "/rest/tools/updatedatarepository?token="
                + Configuration.getInstance().getViewerAuthorizationToken();
        getWebContentPOST(url, params, null, json.toString(), ContentType.APPLICATION_JSON.getMimeType());
    }

    /**
     * <p>
     * getWebContentGET.
     * </p>
     *
     * @param urlString a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     * @throws org.apache.http.client.ClientProtocolException if any.
     * @throws java.io.IOException if any.
     * @throws io.goobi.viewer.exceptions.HTTPException if any.
     */
    public static String getWebContentGET(String urlString) throws ClientProtocolException, IOException, HTTPException {
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setSocketTimeout(HTTP_TIMEOUT)
                .setConnectTimeout(HTTP_TIMEOUT)
                .setConnectionRequestTimeout(HTTP_TIMEOUT)
                .build();
        try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build()) {
            HttpGet get = new HttpGet(urlString);
            try (CloseableHttpResponse response = httpClient.execute(get); StringWriter writer = new StringWriter()) {
                int code = response.getStatusLine().getStatusCode();
                if (code == HttpStatus.SC_OK) {
                    return EntityUtils.toString(response.getEntity(), TextHelper.DEFAULT_ENCODING);
                }
                logger.error("{}: {}\n{}", code, response.getStatusLine().getReasonPhrase(),
                        EntityUtils.toString(response.getEntity(), TextHelper.DEFAULT_ENCODING));
                return response.getStatusLine().getReasonPhrase();
            }
        }
    }

    /**
     * <p>
     * getWebContentPOST.
     * </p>
     *
     * @param url a {@link java.lang.String} object.
     * @param params a {@link java.util.Map} object.
     * @param cookies a {@link java.util.Map} object.
     * @param body Optional entity content.
     * @param contentType Optional mime type.
     * @return a {@link java.lang.String} object.
     * @throws org.apache.http.client.ClientProtocolException if any.
     * @throws java.io.IOException if any.
     * @throws io.goobi.viewer.exceptions.HTTPException if any.
     */
    public static String getWebContentPOST(String url, Map<String, String> params, Map<String, String> cookies, String body, String contentType)
            throws ClientProtocolException, IOException {
        if (url == null) {
            throw new IllegalArgumentException("url may not be null");
        }

        logger.debug("url: {}", url);
        List<NameValuePair> nameValuePairs = null;
        if (params == null) {
            nameValuePairs = new ArrayList<>(0);
        } else {
            nameValuePairs = new ArrayList<>(params.size());
            for (String key : params.keySet()) {
                nameValuePairs.add(new BasicNameValuePair(key, params.get(key)));
            }
        }
        HttpClientContext context = null;
        CookieStore cookieStore = new BasicCookieStore();
        if (cookies != null && !cookies.isEmpty()) {
            context = HttpClientContext.create();
            for (String key : cookies.keySet()) {
                BasicClientCookie cookie = new BasicClientCookie(key, cookies.get(key));
                cookie.setPath("/");
                cookie.setDomain("0.0.0.0");
                cookieStore.addCookie(cookie);
            }
            context.setCookieStore(cookieStore);
        }

        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setSocketTimeout(HTTP_TIMEOUT)
                .setConnectTimeout(HTTP_TIMEOUT)
                .setConnectionRequestTimeout(HTTP_TIMEOUT)
                .build();
        try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build()) {
            HttpPost post = new HttpPost(url);
            if (StringUtils.isNotEmpty(contentType)) {
                post.setHeader("Content-Type", contentType);
            }
            Charset.forName(TextHelper.DEFAULT_ENCODING);
            // TODO allow combinations of params + body
            if (StringUtils.isNotEmpty(body)) {
                post.setEntity(new ByteArrayEntity(body.getBytes(TextHelper.DEFAULT_ENCODING)));
            } else {
                post.setEntity(new UrlEncodedFormEntity(nameValuePairs));
            }
            try (CloseableHttpResponse response = (context == null ? httpClient.execute(post) : httpClient.execute(post, context));
                    StringWriter writer = new StringWriter()) {
                int code = response.getStatusLine().getStatusCode();
                if (code == HttpStatus.SC_OK) {
                    logger.trace("{}: {}", code, response.getStatusLine().getReasonPhrase());
                    return EntityUtils.toString(response.getEntity(), TextHelper.DEFAULT_ENCODING);
                }
                logger.error("{}: {}\n{}", code, response.getStatusLine().getReasonPhrase(),
                        EntityUtils.toString(response.getEntity(), TextHelper.DEFAULT_ENCODING));
                return response.getStatusLine().getReasonPhrase();
            }
        }
    }

    /**
     * <p>
     * postMail.
     * </p>
     *
     * @param recipients a {@link java.util.List} object.
     * @param subject a {@link java.lang.String} object.
     * @param body a {@link java.lang.String} object.
     * @param smtpServer a {@link java.lang.String} object.
     * @param smtpUser a {@link java.lang.String} object.
     * @param smtpPassword a {@link java.lang.String} object.
     * @param smtpSenderAddress a {@link java.lang.String} object.
     * @param smtpSenderName a {@link java.lang.String} object.
     * @param smtpSecurity a {@link java.lang.String} object.
     * @throws javax.mail.MessagingException
     * @throws java.io.UnsupportedEncodingException
     */
    public static void postMail(List<String> recipients, String subject, String body, String smtpServer, final String smtpUser,
            final String smtpPassword, String smtpSenderAddress, String smtpSenderName, String smtpSecurity)
            throws MessagingException, UnsupportedEncodingException {
        if (recipients == null) {
            throw new IllegalArgumentException("recipients may not be null");
        }
        if (subject == null) {
            throw new IllegalArgumentException("subject may not be null");
        }
        if (body == null) {
            throw new IllegalArgumentException("body may not be null");
        }
        if (smtpServer == null) {
            throw new IllegalArgumentException("smtpServer may not be null");
        }
        if (smtpSenderAddress == null) {
            throw new IllegalArgumentException("smtpSenderAddress may not be null");
        }
        if (smtpSenderName == null) {
            throw new IllegalArgumentException("smtpSenderName may not be null");
        }
        if (smtpSecurity == null) {
            throw new IllegalArgumentException("smtpSecurity may not be null");
        }

        boolean debug = false;

        boolean auth = true;
        if (StringUtils.isEmpty(smtpUser)) {
            auth = false;
        }
        Properties props = new Properties();
        switch (smtpSecurity.toUpperCase()) {
            case "STARTTLS":
                logger.debug("Using STARTTLS");
                props.setProperty("mail.transport.protocol", "smtp");
                props.setProperty("mail.smtp.port", "25");
                props.setProperty("mail.smtp.host", smtpServer);
                props.setProperty("mail.smtp.ssl.trust", "*");
                props.setProperty("mail.smtp.starttls.enable", "true");
                props.setProperty("mail.smtp.starttls.required", "true");
                break;
            case "SSL":
                logger.debug("Using SSL");
                props.setProperty("mail.transport.protocol", "smtp");
                props.setProperty("mail.smtp.host", smtpServer);
                props.setProperty("mail.smtp.port", "465");
                props.setProperty("mail.smtp.ssl.enable", "true");
                props.setProperty("mail.smtp.ssl.trust", "*");
                break;
            default:
                logger.debug("Using no SMTP security");
                props.setProperty("mail.transport.protocol", "smtp");
                props.setProperty("mail.smtp.port", "25");
                props.setProperty("mail.smtp.host", smtpServer);
        }
        props.setProperty("mail.smtp.connectiontimeout", "15000");
        props.setProperty("mail.smtp.timeout", "15000");
        props.setProperty("mail.smtp.auth", String.valueOf(auth));
        // logger.trace(props.toString());

        Session session;
        if (auth) {
            // with authentication
            session = Session.getInstance(props, new javax.mail.Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(smtpUser, smtpPassword);
                }
            });
        } else {
            // w/o authentication
            session = Session.getInstance(props, null);
        }
        session.setDebug(debug);

        Message msg = new MimeMessage(session);
        InternetAddress addressFrom = new InternetAddress(smtpSenderAddress, smtpSenderAddress);
        msg.setFrom(addressFrom);
        InternetAddress[] addressTo = new InternetAddress[recipients.size()];
        int i = 0;
        for (String recipient : recipients) {
            addressTo[i] = new InternetAddress(recipient);
            i++;
        }
        msg.setRecipients(Message.RecipientType.TO, addressTo);
        // Optional : You can also set your custom headers in the Email if you
        // Want
        // msg.addHeader("MyHeaderName", "myHeaderValue");
        msg.setSubject(subject);
        {
            // Message body
            MimeBodyPart messagePart = new MimeBodyPart();
            messagePart.setText(body, "utf-8");
            messagePart.setHeader("Content-Type", "text/plain; charset=\"utf-8\"");
            MimeMultipart multipart = new MimeMultipart();
            multipart.addBodyPart(messagePart);
            msg.setContent(multipart);
        }
        msg.setSentDate(new Date());
        Transport.send(msg);
    }

    /**
     * <p>
     * removeRecordImagesFromCache.
     * </p>
     *
     * @param pi a {@link java.lang.String} object.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @return a {@link java.lang.String} object.
     */
    public static String removeRecordImagesFromCache(String pi) throws FatalIndexerException {
        String viewerUrl = Configuration.getInstance().getViewerUrl();
        if (StringUtils.isEmpty(viewerUrl)) {
            return "<viewerUrl> is not configured.";
        }
        StringBuilder sbUrl = new StringBuilder(150);
        sbUrl.append(viewerUrl);
        if (!viewerUrl.endsWith("/")) {
            sbUrl.append('/');
        }
        sbUrl.append("tools?action=emptyCache&identifier=").append(pi).append("&fromContent=true&fromThumbs=true");

        try {
            return Utils.getWebContentGET(sbUrl.toString());
        } catch (IOException e) {
            return "Could not clear viewer cache: " + e.getMessage();
        }
    }

    /**
     * <p>
     * isUrn.
     * </p>
     *
     * @param urn a {@link java.lang.String} object.
     * @return a boolean.
     */
    public static boolean isUrn(String urn) {
        return StringUtils.isNotEmpty(urn) && !urn.startsWith("http");
    }

    /**
     * Creates the file Path to the updated anchor file
     *
     * @param destFolderPath a {@link java.lang.String} object.
     * @param baseName a {@link java.lang.String} object.
     * @param extension a {@link java.lang.String} object.
     * @param separator a {@link java.lang.String} object.
     * @return a {@link java.nio.file.Path} object.
     * @should construct path correctly and avoid collisions
     */
    public static Path getCollisionFreeDataFilePath(String destFolderPath, String baseName, String separator, String extension) {
        if (destFolderPath == null) {
            throw new IllegalArgumentException("destFolderPath may not be null");
        }
        if (baseName == null) {
            throw new IllegalArgumentException("pi may not be null");
        }
        if (extension == null) {
            throw new IllegalArgumentException("extension may not be null");
        }

        StringBuilder sbFilePath = new StringBuilder(baseName);
        Path path = Paths.get(destFolderPath, baseName + extension);
        if (Files.exists(path)) {
            // If an updated anchor file already exists, use a different file name
            int iteration = 0;
            while (Files.exists(path = Paths.get(destFolderPath, baseName + "#" + iteration + extension))) {
                iteration++;
            }
            if (StringUtils.isNotEmpty(separator)) {
                sbFilePath.append(separator);
            }
            sbFilePath.append(iteration).append(extension);
            path = Paths.get(destFolderPath, sbFilePath.toString());
        }

        return path;
    }

    /**
     * <p>
     * extractPiFromFileName.
     * </p>
     *
     * @param file a {@link java.nio.file.Path} object.
     * @should extract file name correctly
     * @return a {@link java.lang.String} object.
     */
    public static String extractPiFromFileName(Path file) {
        String fileExtension = FilenameUtils.getExtension(file.getFileName().toString());
        if (MetsIndexer.ANCHOR_UPDATE_EXTENSION.equals("." + fileExtension) || "delete".equals(fileExtension) || "purge".equals(fileExtension)) {
            String pi = FilenameUtils.getBaseName(file.getFileName().toString());
            if (pi.contains("#")) {
                pi = pi.substring(0, pi.indexOf("#"));
            }
            logger.info("Record identifier extracted from file name: {}", pi);
            return pi;
        }

        return null;
    }

    /**
     * <p>
     * getFileNameFromIiifUrl.
     * </p>
     *
     * @param url a {@link java.lang.String} object.
     * @should extract file name correctly
     * @return a {@link java.lang.String} object.
     */
    public static String getFileNameFromIiifUrl(String url) {
        if (StringUtils.isEmpty(url)) {
            return null;
        }

        String[] filePathSplit = url.split("/");
        if (filePathSplit.length < 5) {
            return null;
        }

        String baseFileName = FilenameUtils.getBaseName(filePathSplit[filePathSplit.length - 5]);
        String extension = FilenameUtils.getExtension(filePathSplit[filePathSplit.length - 1]);

        return baseFileName + "." + extension;
    }

    /**
     * 
     * @param prefix
     * @param order
     * @return
     * @should construct number correctly
     */
    public static int generateLongOrderNumber(int prefix, int count) {
        if (prefix < 1) {
            throw new IllegalArgumentException("prefix must be greater than 0");
        }
        if (count < 1) {
            throw new IllegalArgumentException("count must be greater than 0");
        }

        int prefixLength = (int) (Math.log10(prefix) + 1);
        int countLength = (int) (Math.log10(count) + 1);
        int zeroes = 9 - (prefixLength + countLength);
        if (zeroes < 0) {
            zeroes = 0;
        }
        StringBuilder sbOrder = new StringBuilder();
        sbOrder.append(prefix);
        for (int i = 0; i < zeroes; ++i) {
            sbOrder.append('0');
        }
        sbOrder.append(count);

        return Integer.valueOf(sbOrder.toString());
    }
    
    /**
     * Returns the application version number.
     *
     * @return a {@link java.lang.String} object.
     */
    public static String getVersion() {
        return Version.VERSION + " " + Version.BUILDDATE + " " + Version.BUILDVERSION;
    }
}
