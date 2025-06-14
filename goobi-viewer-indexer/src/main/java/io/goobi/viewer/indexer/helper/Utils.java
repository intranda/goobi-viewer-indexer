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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import io.goobi.viewer.indexer.MetsIndexer;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.Version;
import io.goobi.viewer.indexer.exceptions.HTTPException;
import io.goobi.viewer.indexer.model.SolrConstants;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;

/**
 * <p>
 * Utils class.
 * </p>
 */
public final class Utils {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(Utils.class);

    private static final int HTTP_TIMEOUT = 30000;

    private static final String HTTP_HEADER_CONTENT_TYPE = "Content-Type";

    private static final String HTTP_METHOD_DELETE = "DELETE";

    private static final String MAIL_PROPERTY_PROTOCOL = "mail.transport.protocol";
    private static final String MAIL_PROPERTY_SMTP_HOST = "mail.smtp.host";
    private static final String MAIL_PROPERTY_SMTP_PORT = "mail.smtp.port";

    private static final char[] PI_ILLEGAL_CHARS = { '!', '?', '/', '\\', ':', ';', '(', ')', '@', '"', '\'' };

    // Simple image URI pattern (adjust extensions as needed)
    private static final Pattern PATTERN_IMAGE_URI = Pattern.compile("^https?://.+\\.(jpg|jpeg|png|gif|bmp|tiff?)$", Pattern.CASE_INSENSITIVE);

    // Simplified IIIF URI pattern (v2/v3 Image API)
    private static final Pattern PATTERN_IIIF_URI = Pattern.compile(
            "^(https?)://.+?/iiif/[^/]+/(full|\\d+,\\d+,\\d+,\\d+)/[^/]+/([^/]+)/\\.(jpg|png|tif|gif)$", Pattern.CASE_INSENSITIVE);

    // Alternate IIIF info.json pattern
    private static final Pattern PATTERN_IIIF_INFO_JSON = Pattern.compile("^https?://.+/info\\.json$", Pattern.CASE_INSENSITIVE);

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
     * <p>
     * updateDataRepositoryCache.
     * </p>
     *
     * @param pi a {@link java.lang.String} object
     * @param dataRepositoryName a {@link java.lang.String} object
     * @throws java.io.IOException
     * @throws io.goobi.viewer.indexer.exceptions.HTTPException
     */
    public static void updateDataRepositoryCache(String pi, String dataRepositoryName) throws IOException, HTTPException {
        updateDataRepositoryCache(pi, dataRepositoryName, SolrIndexerDaemon.getInstance().getConfiguration().getViewerUrl(),
                SolrIndexerDaemon.getInstance().getConfiguration().getViewerAuthorizationToken());
    }

    /**
     * <p>
     * prerenderPdfs.
     * </p>
     *
     * @param pi a {@link java.lang.String} object
     * @param forceUpdate a boolean
     * @throws java.io.IOException
     * @throws io.goobi.viewer.indexer.exceptions.HTTPException
     */
    public static void prerenderPdfs(String pi, boolean forceUpdate) throws IOException, HTTPException {
        if (StringUtils.isNotBlank(pi) && SolrIndexerDaemon.getInstance().getConfiguration().isPrerenderPdfsEnabled()) {
            prerenderPdfs(pi, forceUpdate, SolrIndexerDaemon.getInstance().getConfiguration().getPrerenderPdfsConfigVariant(),
                    SolrIndexerDaemon.getInstance().getConfiguration().getViewerUrl(),
                    SolrIndexerDaemon.getInstance().getConfiguration().getViewerAuthorizationToken());
        }
    }

    /**
     * <p>
     * updateDataRepositoryCache.
     * </p>
     *
     * @param pi a {@link java.lang.String} object
     * @param dataRepositoryName a {@link java.lang.String} object
     * @param viewerUrl a {@link java.lang.String} object
     * @param token a {@link java.lang.String} object
     * @throws java.io.IOException
     * @throws io.goobi.viewer.indexer.exceptions.HTTPException
     */
    public static void updateDataRepositoryCache(String pi, String dataRepositoryName, String viewerUrl, String token)
            throws IOException, HTTPException {
        if (StringUtils.isEmpty(token)) {
            return;
        }

        if (pi == null) {
            throw new IllegalArgumentException(StringConstants.ERROR_PI_MAY_NOT_BE_NULL);
        }
        if (dataRepositoryName == null) {
            throw new IllegalArgumentException("dataRepositoryName may not be null");
        }

        logger.info("Updating data repository cache...");
        JSONObject json = new JSONObject();
        json.put("type", "UPDATE_DATA_REPOSITORY_NAMES");
        json.put("pi", pi);
        json.put("dataRepositoryName", dataRepositoryName);

        String url = viewerUrl;
        if (!url.endsWith("/")) {
            url += "/";
        }
        url += "api/v1/tasks/";
        Map<String, String> headerParams = HashMap.newHashMap(2);
        headerParams.put(HTTP_HEADER_CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        headerParams.put("token", token);
        getWebContentPOST(url, Collections.emptyMap(), null, json.toString(), headerParams);
    }

    /**
     * <p>
     * prerenderPdfs.
     * </p>
     *
     * @param pi a {@link java.lang.String} object
     * @param force a boolean
     * @param config a {@link java.lang.String} object
     * @param viewerUrl a {@link java.lang.String} object
     * @param token a {@link java.lang.String} object
     * @throws java.io.IOException if any.
     * @throws io.goobi.viewer.indexer.exceptions.HTTPException if any.
     */
    public static void prerenderPdfs(String pi, boolean force, String config, String viewerUrl, String token)
            throws IOException, HTTPException {
        if (StringUtils.isEmpty(token)) {
            return;
        }

        if (pi == null) {
            throw new IllegalArgumentException(StringConstants.ERROR_PI_MAY_NOT_BE_NULL);
        }

        logger.info("Requesting prerender pdf task in viewer...");
        JSONObject json = new JSONObject();
        json.put("type", "PRERENDER_PDF");
        json.put("pi", pi);
        json.put("force", force);
        json.put("variant", config);

        String url = viewerUrl;
        if (!url.endsWith("/")) {
            url += "/";
        }
        url += "api/v1/tasks/";
        Map<String, String> headerParams = HashMap.newHashMap(2);
        headerParams.put(HTTP_HEADER_CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
        headerParams.put("token", token);
        getWebContentPOST(url, Collections.emptyMap(), null, json.toString(), headerParams);
    }

    /**
     * @param identifiers Identifier list of indexed records
     * @param fileCount
     */
    public static void submitDataToViewer(List<String> identifiers, long fileCount) {
        if (identifiers == null) {
            throw new IllegalArgumentException("identifiers may not be null");
        }
        if (StringUtils.isEmpty(SolrIndexerDaemon.getInstance().getConfiguration().getViewerAuthorizationToken())) {
            return;
        }

        String url = SolrIndexerDaemon.getInstance().getConfiguration().getViewerUrl();
        if (!url.endsWith("/")) {
            url += "/";
        }
        url += ("api/v1/indexer/version?token=" + SolrIndexerDaemon.getInstance().getConfiguration().getViewerAuthorizationToken());
        try {
            JSONObject json = Version.asJSON();
            json.put("hotfolder-file-count", fileCount);
            json.put("record-identifiers", identifiers);

            getWebContentPUT(url, HashMap.newHashMap(0), null, json.toString(),
                    Collections.singletonMap(HTTP_HEADER_CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType()));
            logger.info("Version and file count ({}) submitted to Goobi viewer.", fileCount);
            if (!identifiers.isEmpty()) {
                logger.info("Record identifier(s) submitted to Goobi viewer.");
            }
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
     * @throws io.goobi.viewer.indexer.exceptions.HTTPException if any.
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
     * @param headerParams Optional header params.
     * @return a {@link java.lang.String} object.
     * @throws java.io.IOException if any.
     * @throws io.goobi.viewer.indexer.exceptions.HTTPException if any.
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
     * @param headerParams Optional header params.
     * @return a {@link java.lang.String} object.
     * @throws java.io.IOException if any.
     * @throws io.goobi.viewer.indexer.exceptions.HTTPException if any.
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
     * @param headerParams Optional header params.
     * @return a {@link java.lang.String} object.
     * @throws java.io.IOException if any.
     * @throws io.goobi.viewer.indexer.exceptions.HTTPException if any.
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
                if (logger.isErrorEnabled()) {
                    logger.error("Error calling URL '{}'; {}: {}\n{}", url, code, response.getStatusLine().getReasonPhrase(),
                            EntityUtils.toString(response.getEntity(), TextHelper.DEFAULT_CHARSET));
                }
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
     * @param smtpPort a {@link java.lang.Integer} object
     * @throws jakarta.mail.MessagingException
     * @throws java.io.UnsupportedEncodingException
     */
    public static void postMail(List<String> recipients, String subject, String body, String smtpServer, final String smtpUser,
            final String smtpPassword, String smtpSenderAddress, String smtpSenderName, String smtpSecurity, final Integer smtpPort)
            throws MessagingException, UnsupportedEncodingException {
        logger.info("postMail");
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
        Integer usePort = smtpPort;
        Properties props = new Properties();
        switch (smtpSecurity.toUpperCase()) {
            case "STARTTLS":
                logger.debug("Using STARTTLS");
                if (usePort == null || usePort == -1) {
                    usePort = 25;
                }
                props.setProperty(MAIL_PROPERTY_PROTOCOL, "smtp");
                props.setProperty(MAIL_PROPERTY_SMTP_PORT, String.valueOf(usePort));
                props.setProperty(MAIL_PROPERTY_SMTP_HOST, smtpServer);
                props.setProperty("mail.smtp.ssl.trust", "*");
                props.setProperty("mail.smtp.starttls.enable", "true");
                props.setProperty("mail.smtp.starttls.required", "true");
                break;
            case "SSL":
                logger.debug("Using SSL");
                if (usePort == null || usePort == -1) {
                    usePort = 465;
                }
                props.setProperty(MAIL_PROPERTY_PROTOCOL, "smtp");
                props.setProperty(MAIL_PROPERTY_SMTP_HOST, smtpServer);
                props.setProperty(MAIL_PROPERTY_SMTP_PORT, String.valueOf(usePort));
                props.setProperty("mail.smtp.ssl.enable", "true");
                props.setProperty("mail.smtp.ssl.trust", "*");
                break;
            default:
                logger.debug("Using no SMTP security");
                if (usePort == null || usePort == -1) {
                    usePort = 25;
                }
                props.setProperty(MAIL_PROPERTY_PROTOCOL, "smtp");
                props.setProperty(MAIL_PROPERTY_SMTP_PORT, String.valueOf(usePort));
                props.setProperty(MAIL_PROPERTY_SMTP_HOST, smtpServer);
        }
        props.setProperty("mail.smtp.connectiontimeout", "15000");
        props.setProperty("mail.smtp.timeout", "15000");
        props.setProperty("mail.smtp.auth", String.valueOf(auth));

        Session session;
        if (auth) {
            // with authentication
            session = Session.getInstance(props, new jakarta.mail.Authenticator() {
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

        // Message body
        MimeBodyPart messagePart = new MimeBodyPart();
        messagePart.setText(body, "utf-8");
        messagePart.setHeader(HTTP_HEADER_CONTENT_TYPE, "text/plain; charset=\"utf-8\"");
        MimeMultipart multipart = new MimeMultipart();
        multipart.addBodyPart(messagePart);
        msg.setContent(multipart);

        msg.setSentDate(new Date());
        Transport.send(msg);
    }

    /**
     * <p>
     * removeRecordImagesFromCache.
     * </p>
     *
     * @param pi a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     */
    public static String removeRecordImagesFromCache(String pi) {
        if (StringUtils.isEmpty(SolrIndexerDaemon.getInstance().getConfiguration().getViewerAuthorizationToken()) || pi == null) {
            return null;
        }

        String viewerUrl = SolrIndexerDaemon.getInstance().getConfiguration().getViewerUrl();
        if (StringUtils.isEmpty(viewerUrl)) {
            return "<viewerUrl> is not configured.";
        }
        StringBuilder sbUrl = new StringBuilder(150);
        sbUrl.append(viewerUrl);
        if (!viewerUrl.endsWith("/")) {
            sbUrl.append('/');
        }
        sbUrl.append("api/v1/cache/")
                .append(URLEncoder.encode(pi, StandardCharsets.UTF_8))
                .append("?content=true&thumbs=true&pdf=true")
                .append("&token=")
                .append(SolrIndexerDaemon.getInstance().getConfiguration().getViewerAuthorizationToken());

        try {
            String jsonString = Utils.getWebContentDELETE(sbUrl.toString(), HashMap.newHashMap(0), null, null,
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
     * isValidURL.
     * </p>
     *
     * @param urlString URL to check
     * @return true if valid; false otherwise
     * @should return true if url starts with http
     * @should return true if url starts with https
     * @should return true if url starts with file
     * @should return false if not url
     */
    public static boolean isValidURL(String urlString) {
        try {
            URL url = new URI(urlString).toURL();
            url.toURI();
            return true;
        } catch (IllegalArgumentException | MalformedURLException | URISyntaxException e) {
            return false;
        }
    }

    /**
     * 
     * @param uri
     * @return true if image or IIIF uri; false otherwise
     * @should return true for image uri
     * @should return true for iiif uri
     * @should return true for iiif info json uri
     * @should return false for other uris
     */
    public static boolean isValidImageOrIiifURI(String uri) {
        return StringUtils.isNotEmpty(uri)
                && (PATTERN_IMAGE_URI.matcher(uri).matches()
                        || PATTERN_IIIF_URI.matcher(uri).matches()
                        || PATTERN_IIIF_INFO_JSON.matcher(uri).matches());
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
            throw new IllegalArgumentException(StringConstants.ERROR_PI_MAY_NOT_BE_NULL);
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
     * <p>
     * isFileNameMatchesRegex.
     * </p>
     *
     * @param fileName a {@link java.lang.String} object
     * @param regexes an array of {@link java.lang.String} objects
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
    public static String getFileNameFromIiifUrl(final String url) {
        if (StringUtils.isEmpty(url)) {
            return null;
        }

        String useUrl = url;
        try {
            useUrl = URLDecoder.decode(useUrl, TextHelper.DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage());
        }

        String[] filePathSplit = useUrl.split("/");
        if (filePathSplit.length < 5) {
            return null;
        }

        String baseFileName = FilenameUtils.getBaseName(filePathSplit[filePathSplit.length - 5]);
        String extension = FilenameUtils.getExtension(filePathSplit[filePathSplit.length - 1]);

        return baseFileName + "." + extension;
    }

    /**
     * <p>
     * generateLongOrderNumber.
     * </p>
     *
     * @param prefix a int
     * @param count a int
     * @should construct number correctly
     * @return a int
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
    static String adaptField(final String fieldName, final String prefix) {
        if (fieldName == null) {
            return null;
        }
        if (prefix == null) {
            throw new IllegalArgumentException("prefix may not be null");
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
                String ret = fieldName;
                if (StringUtils.isNotEmpty(prefix)) {
                    if (ret.startsWith("MD_")) {
                        ret = ret.replace("MD_", prefix);
                    } else if (ret.startsWith("MD2_")) {
                        ret = ret.replace("MD2_", prefix);
                    } else if (ret.startsWith(SolrConstants.PREFIX_MDNUM)) {
                        if (SolrConstants.PREFIX_SORT.equals(prefix)) {
                            ret = ret.replace(SolrConstants.PREFIX_MDNUM, SolrConstants.PREFIX_SORTNUM);
                        } else {
                            ret = ret.replace(SolrConstants.PREFIX_MDNUM, prefix);
                        }
                    } else if (ret.startsWith("NE_")) {
                        ret = ret.replace("NE_", prefix);
                    } else if (ret.startsWith("BOOL_")) {
                        ret = ret.replace("BOOL_", prefix);
                    } else if (ret.startsWith(SolrConstants.PREFIX_SORT)) {
                        ret = ret.replace(SolrConstants.PREFIX_SORT, prefix);
                    }
                }
                ret = ret.replace(SolrConstants.SUFFIX_UNTOKENIZED, "");
                return ret;
        }
    }

    /**
     * <p>
     * validatePi.
     * </p>
     *
     * @param pi a {@link java.lang.String} object.
     * @should return true if pi good
     * @should return false if pi empty, blank or null
     * @should return false if pi contains illegal characters
     * @return a boolean.
     */
    public static boolean validatePi(String pi) {
        if (StringUtils.isBlank(pi)) {
            return false;
        }

        return !StringUtils.containsAny(pi, PI_ILLEGAL_CHARS);
    }
}
