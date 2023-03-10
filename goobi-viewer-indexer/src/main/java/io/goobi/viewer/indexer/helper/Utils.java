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
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import io.goobi.viewer.indexer.MetsIndexer;
import io.goobi.viewer.indexer.Version;
import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.model.SolrConstants;

/**
 * <p>
 * Utils class.
 * </p>
 *
 */
public class Utils {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(Utils.class);

    private static final int HTTP_TIMEOUT = 30000;

    private static final String HTTP_HEADER_CONTENT_TYPE = "Content-Type";

    private static final String HTTP_METHOD_DELETE = "DELETE";

    private static final String MAIL_PROPERTY_PROTOCOL = "mail.transport.protocol";
    private static final String MAIL_PROPERTY_SMTP_HOST = "mail.smtp.host";
    private static final String MAIL_PROPERTY_SMTP_PORT = "mail.smtp.port";

    /**
     * Private constructor.
     */
    private Utils() {
        //
    }

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
     * @throws HTTPException
     */
    public static void updateDataRepositoryCache(String pi, String dataRepositoryName)
            throws FatalIndexerException, IOException, HTTPException {
        updateDataRepositoryCache(pi, dataRepositoryName, Configuration.getInstance().getViewerUrl(),
                Configuration.getInstance().getViewerAuthorizationToken());
    }

    public static void prerenderPdfs(String pi, boolean forceUpdate) throws IOException, HTTPException, FatalIndexerException {
        if(StringUtils.isNotBlank(pi) && Configuration.getInstance().isPrerenderPdfsEnabled()) {            
            prerenderPdfs(pi, forceUpdate, Configuration.getInstance().getPrerenderPdfsConfig(),
                    Configuration.getInstance().getViewerUrl(),
                    Configuration.getInstance().getViewerAuthorizationToken());
        }
    }

    /**
     * 
     * @param pi
     * @param dataRepositoryName
     * @param viewerUrl
     * @param token
     * @throws FatalIndexerException
     * @throws IOException
     * @throws HTTPException
     */
    public static void updateDataRepositoryCache(String pi, String dataRepositoryName, String viewerUrl, String token)
            throws IOException, HTTPException {
        if (StringUtils.isEmpty(token)) {
            return;
        }

        if (pi == null) {
            throw new IllegalArgumentException("pi may not be null");
        }
        if (dataRepositoryName == null) {
            throw new IllegalArgumentException("dataRepositoryName may not be null");
        }

        logger.info("Updating data repository cache...");
        JSONObject json = new JSONObject();
        json.put("type", "UPDATE_DATA_REPOSITORY_NAMES");
        json.put("pi", pi);
        json.put("dataRepositoryName", dataRepositoryName);

        String url = viewerUrl + "/api/v1/tasks/";
        Map<String, String> headerParams = new HashMap<>(2);
        headerParams.put(HTTP_HEADER_CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        headerParams.put("token", token);
        getWebContentPOST(url, Collections.emptyMap(), null, json.toString(), headerParams);
    }

    public static void prerenderPdfs(String pi, boolean force, String config, String viewerUrl, String token)
            throws IOException, HTTPException {
        if (StringUtils.isEmpty(token)) {
            return;
        }

        if (pi == null) {
            throw new IllegalArgumentException("pi may not be null");
        }

        logger.info("Requesting prerender pdf task in viewer...");
        JSONObject json = new JSONObject();
        json.put("type", "PRERENDER_PDF");
        json.put("pi", pi);
        json.put("force", force);
        json.put("config", config);

        String url = viewerUrl + "/api/v1/tasks/";
        Map<String, String> headerParams = new HashMap<>(2);
        headerParams.put(HTTP_HEADER_CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        headerParams.put("token", token);
        getWebContentPOST(url, Collections.emptyMap(), null, json.toString(), headerParams);
    }

    /**
     * @param fileCount
     * @throws FatalIndexerException
     * @throws HTTPException
     * @throws ClientProtocolException
     * @throws IOException
     */
    public static void submitDataToViewer(long fileCount) throws FatalIndexerException {
        if (StringUtils.isEmpty(Configuration.getInstance().getViewerAuthorizationToken())) {
            return;
        }

        String url = Configuration.getInstance().getViewerUrl() + "/api/v1/indexer/version?token="
                + Configuration.getInstance().getViewerAuthorizationToken();
        try {
            JSONObject json = Version.asJSON();
            json.put("hotfolder-file-count", fileCount);
            getWebContentPUT(url, new HashMap<>(0), null, json.toString(),
                    Collections.singletonMap(HTTP_HEADER_CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType()));
        } catch (IOException | HTTPException e) {
            logger.warn("Version could not be submitted to Goobi viewer: {}", e.getMessage());
        }
    }

    /**
     * <p>
     * getWebContentGET.
     * </p>
     *
     * @param urlString a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     * @throws java.io.IOException if any.
     * @throws io.goobi.viewer.exceptions.HTTPException if any.
     */
    public static String getWebContentGET(String urlString) throws IOException, HTTPException {
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
                    return EntityUtils.toString(response.getEntity(), TextHelper.DEFAULT_CHARSET);
                }
                logger.trace("{}: {}", code, response.getStatusLine().getReasonPhrase());
                throw new HTTPException(code, response.getStatusLine().getReasonPhrase());
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
     * @throws java.io.IOException if any.
     * @throws io.goobi.viewer.exceptions.HTTPException if any.
     */
    public static String getWebContentPOST(String url, Map<String, String> params, Map<String, String> cookies, String body,
            Map<String, String> headerParams) throws IOException, HTTPException {
        return getWebContent("POST", url, params, cookies, body, headerParams);
    }

    /**
     * <p>
     * getWebContentPUT.
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
    public static String getWebContentPUT(String url, Map<String, String> params, Map<String, String> cookies, String body,
            Map<String, String> headerParams) throws IOException, HTTPException {
        return getWebContent("PUT", url, params, cookies, body, headerParams);
    }

    /**
     * <p>
     * getWebContentDELETE.
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
    public static String getWebContentDELETE(String url, Map<String, String> params, Map<String, String> cookies, String body,
            Map<String, String> headerParams) throws IOException, HTTPException {
        return getWebContent(HTTP_METHOD_DELETE, url, params, cookies, body, headerParams);
    }

    /**
     * <p>
     * getWebContent.
     * </p>
     *
     * @param method POST or PUT
     * @param url a {@link java.lang.String} object.
     * @param params a {@link java.util.Map} object.
     * @param cookies a {@link java.util.Map} object.
     * @param body Optional entity content.
     * @param headerParams Header parameters
     * @return a {@link java.lang.String} object.
     * @throws java.io.IOException if any.
     * @throws io.goobi.viewer.exceptions.HTTPException if any.
     */
    static String getWebContent(String method, String url, Map<String, String> params, Map<String, String> cookies, String body,
            Map<String, String> headerParams) throws IOException, HTTPException {
        if (method == null || !("POST".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method) || HTTP_METHOD_DELETE.equalsIgnoreCase(method))) {
            throw new IllegalArgumentException("Illegal method: " + method);
        }
        if (url == null) {
            throw new IllegalArgumentException("url may not be null");
        }

        logger.debug("url: {}", url);
        List<NameValuePair> nameValuePairs = null;
        if (params == null) {
            nameValuePairs = new ArrayList<>(0);
        } else {
            nameValuePairs = new ArrayList<>(params.size());
            for (Entry<String, String> entry : params.entrySet()) {
                nameValuePairs.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
        }
        HttpClientContext context = null;
        CookieStore cookieStore = new BasicCookieStore();
        if (cookies != null && !cookies.isEmpty()) {
            context = HttpClientContext.create();
            for (Entry<String, String> entry : cookies.entrySet()) {
                BasicClientCookie cookie = new BasicClientCookie(entry.getKey(), entry.getValue());
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
            HttpRequestBase requestBase;
            switch (method.toUpperCase()) {
                case HTTP_METHOD_DELETE:
                    requestBase = new HttpDelete(url);
                    break;
                case "POST":
                    requestBase = new HttpPost(url);
                    break;
                case "PUT":
                    requestBase = new HttpPut(url);
                    break;
                default:
                    return "";
            }
            if (headerParams != null) {
                for (Entry<String, String> entry : headerParams.entrySet()) {
                    requestBase.setHeader(entry.getKey(), entry.getValue());
                }
            }
            Charset.forName(TextHelper.DEFAULT_CHARSET);
            // TODO allow combinations of params + body
            if (requestBase instanceof HttpPost || requestBase instanceof HttpPut) {
                if (StringUtils.isNotEmpty(body)) {
                    ((HttpEntityEnclosingRequestBase) requestBase).setEntity(new ByteArrayEntity(body.getBytes(TextHelper.DEFAULT_CHARSET)));
                } else {
                    ((HttpEntityEnclosingRequestBase) requestBase).setEntity(new UrlEncodedFormEntity(nameValuePairs));
                }
            }
            try (CloseableHttpResponse response = (context == null ? httpClient.execute(requestBase) : httpClient.execute(requestBase, context));
                    StringWriter writer = new StringWriter()) {
                int code = response.getStatusLine().getStatusCode();
                if (code == HttpStatus.SC_OK) {
                    logger.trace("{}: {}", code, response.getStatusLine().getReasonPhrase());
                    return EntityUtils.toString(response.getEntity(), TextHelper.DEFAULT_CHARSET);
                }
                logger.error("Error calling URL '{}'; {}: {}\n{}", url, code, response.getStatusLine().getReasonPhrase(),
                        EntityUtils.toString(response.getEntity(), TextHelper.DEFAULT_CHARSET));
                throw new HTTPException(code, response.getStatusLine().getReasonPhrase());
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
     * @param smtpPort
     * @throws javax.mail.MessagingException
     * @throws java.io.UnsupportedEncodingException
     */
    public static void postMail(List<String> recipients, String subject, String body, String smtpServer, final String smtpUser,
            final String smtpPassword, String smtpSenderAddress, String smtpSenderName, String smtpSecurity, Integer smtpPort)
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
                if (smtpPort == null || smtpPort == -1) {
                    smtpPort = 25;
                }
                props.setProperty(MAIL_PROPERTY_PROTOCOL, "smtp");
                props.setProperty(MAIL_PROPERTY_SMTP_PORT, String.valueOf(smtpPort));
                props.setProperty(MAIL_PROPERTY_SMTP_HOST, smtpServer);
                props.setProperty("mail.smtp.ssl.trust", "*");
                props.setProperty("mail.smtp.starttls.enable", "true");
                props.setProperty("mail.smtp.starttls.required", "true");
                break;
            case "SSL":
                logger.debug("Using SSL");
                if (smtpPort == null || smtpPort == -1) {
                    smtpPort = 465;
                }
                props.setProperty(MAIL_PROPERTY_PROTOCOL, "smtp");
                props.setProperty(MAIL_PROPERTY_SMTP_HOST, smtpServer);
                props.setProperty(MAIL_PROPERTY_SMTP_PORT, String.valueOf(smtpPort));
                props.setProperty("mail.smtp.ssl.enable", "true");
                props.setProperty("mail.smtp.ssl.trust", "*");
                break;
            default:
                logger.debug("Using no SMTP security");
                if (smtpPort == null || smtpPort == -1) {
                    smtpPort = 25;
                }
                props.setProperty(MAIL_PROPERTY_PROTOCOL, "smtp");
                props.setProperty(MAIL_PROPERTY_SMTP_PORT, String.valueOf(smtpPort));
                props.setProperty(MAIL_PROPERTY_SMTP_HOST, smtpServer);
        }
        props.setProperty("mail.smtp.connectiontimeout", "15000");
        props.setProperty("mail.smtp.timeout", "15000");
        props.setProperty("mail.smtp.auth", String.valueOf(auth));

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
        msg.setSubject(subject);
        {
            // Message body
            MimeBodyPart messagePart = new MimeBodyPart();
            messagePart.setText(body, "utf-8");
            messagePart.setHeader(HTTP_HEADER_CONTENT_TYPE, "text/plain; charset=\"utf-8\"");
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
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @return a {@link java.lang.String} object.
     */
    public static String removeRecordImagesFromCache(String pi) throws FatalIndexerException {
        if (StringUtils.isEmpty(Configuration.getInstance().getViewerAuthorizationToken())) {
            return null;
        }

        String viewerUrl = Configuration.getInstance().getViewerUrl();
        if (StringUtils.isEmpty(viewerUrl)) {
            return "<viewerUrl> is not configured.";
        }
        StringBuilder sbUrl = new StringBuilder(150);
        sbUrl.append(viewerUrl);
        if (!viewerUrl.endsWith("/")) {
            sbUrl.append('/');
        }
        sbUrl.append("api/v1/cache/")
                .append(pi)
                .append("?content=true&thumbs=true&pdf=true")
                .append("&token=")
                .append(Configuration.getInstance().getViewerAuthorizationToken());

        try {
            String jsonString = Utils.getWebContentDELETE(sbUrl.toString(), new HashMap<>(0), null, null,
                    Collections.singletonMap(HTTP_HEADER_CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType()));
            if (StringUtils.isNotEmpty(jsonString)) {
                return (String) new JSONObject(jsonString).get("message");
            }
            throw new IOException("No JSON response");
        } catch (IOException | HTTPException e) {
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
            while (Files.exists(Paths.get(destFolderPath, baseName + "#" + iteration + extension))) {
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
        String fileExtension = "." + FilenameUtils.getExtension(file.getFileName().toString());
        if (MetsIndexer.ANCHOR_UPDATE_EXTENSION.equals(fileExtension) || Hotfolder.FILENAME_EXTENSION_DELETE.equals(fileExtension)
                || Hotfolder.FILENAME_EXTENSION_PURGE.equals(fileExtension)) {
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
     * 
     * @param fileName
     * @param regexes
     * @return true if fileName matches any of the regexes in the array; false otherwise
     * @should match correctly
     */
    public static boolean isFileNameMatchesRegex(String fileName, String[] regexes) {
        if (StringUtils.isEmpty(fileName) || regexes == null || regexes.length == 0) {
            return false;
        }

        for (String regex : regexes) {
            if (fileName.matches(regex)) {
                return true;
            }
        }

        return false;
    }

    /**
     * <p>
     * getFileNameFromIiifUrl.
     * </p>
     *
     * @param url a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     * @should extract file name correctly
     * @should extract escaped file name correctly
     */
    public static String getFileNameFromIiifUrl(String url) {
        if (StringUtils.isEmpty(url)) {
            return null;
        }

        try {
            url = URLDecoder.decode(url, TextHelper.DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage());
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
        logger.trace("generateLongOrderNumber({}, {})", prefix, count);
        if (prefix < 1) {
            throw new IllegalArgumentException("prefix must be greater than 0");
        }
        if (count < 1) {
            throw new IllegalArgumentException("count must be greater than 0");
        }

        return (int) (prefix * Math.pow(10, 4) + count);
    }

    /**
     * <p>
     * sortifyField.
     * </p>
     *
     * @param fieldName a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     * @should sortify correctly
     */
    public static String sortifyField(String fieldName) {
        return adaptField(fieldName, SolrConstants.PREFIX_SORT);
    }

    /**
     * 
     * @param fieldName
     * @param prefix
     * @return modified field name
     * @should apply prefix correctly
     * @should not apply prefix to regular fields if empty
     * @should not apply facet prefix to calendar fields
     * @should remove untokenized correctly
     */
    static String adaptField(String fieldName, String prefix) {
        if (fieldName == null) {
            return null;
        }
        if (prefix == null) {
            prefix = "";
        }

        switch (fieldName) {
            case SolrConstants.DC:
            case SolrConstants.DOCSTRCT:
            case SolrConstants.DOCSTRCT_SUB:
            case SolrConstants.DOCSTRCT_TOP:
                return prefix + fieldName;
            case SolrConstants.MONTHDAY:
            case SolrConstants.YEAR:
            case SolrConstants.YEARMONTH:
            case SolrConstants.YEARMONTHDAY:
                if (SolrConstants.PREFIX_SORT.equals(prefix)) {
                    return SolrConstants.PREFIX_SORTNUM + fieldName;
                }
                return fieldName;
            default:
                if (StringUtils.isNotEmpty(prefix)) {
                    if (fieldName.startsWith("MD_")) {
                        fieldName = fieldName.replace("MD_", prefix);
                    } else if (fieldName.startsWith("MD2_")) {
                        fieldName = fieldName.replace("MD2_", prefix);
                    } else if (fieldName.startsWith(SolrConstants.PREFIX_MDNUM)) {
                        if (SolrConstants.PREFIX_SORT.equals(prefix)) {
                            fieldName = fieldName.replace(SolrConstants.PREFIX_MDNUM, SolrConstants.PREFIX_SORTNUM);
                        } else {
                            fieldName = fieldName.replace(SolrConstants.PREFIX_MDNUM, prefix);
                        }
                    } else if (fieldName.startsWith("NE_")) {
                        fieldName = fieldName.replace("NE_", prefix);
                    } else if (fieldName.startsWith("BOOL_")) {
                        fieldName = fieldName.replace("BOOL_", prefix);
                    } else if (fieldName.startsWith(SolrConstants.PREFIX_SORT)) {
                        fieldName = fieldName.replace(SolrConstants.PREFIX_SORT, prefix);
                    }
                }
                fieldName = fieldName.replace(SolrConstants.SUFFIX_UNTOKENIZED, "");
                return fieldName;
        }
    }
}
