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
package de.intranda.digiverso.presentation.solr.helper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.presentation.solr.MetsIndexer;
import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;

public class Utils {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    /**
     * 
     * @param path
     * @return
     */
    public static boolean checkAndCreateDirectory(Path path) {
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
     * 
     * @param path {@link File}
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
     * @param recipients
     * @param subject
     * @param body
     * @param smtpServer
     * @param smtpUser
     * @param smtpPassword
     * @param smtpSenderAddress
     * @param smtpSenderName
     * @param smtpSecurity
     * @throws MessagingException
     * @throws UnsupportedEncodingException
     */
    public static void postMail(List<String> recipients, String subject, String body, String smtpServer, final String smtpUser,
            final String smtpPassword, String smtpSenderAddress, String smtpSenderName, String smtpSecurity) throws MessagingException,
            UnsupportedEncodingException {
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
     * 
     * @param pi
     * @return
     * @throws FatalIndexerException 
     */
    public static String removeRecordImagesFromCache(String pi) throws FatalIndexerException {
        String viewerUrl = Configuration.getInstance().getConfiguration("viewerUrl");
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
            return callUrl(sbUrl.toString());
        } catch (IOException e) {
            return "Could not clear viewer cache: " + e.getMessage();
        }
    }

    /**
     * 
     * @param url
     * @return
     * @throws IOException
     */
    public static String callUrl(String url) throws IOException {
        HttpGet httpGet = new HttpGet(url);
        RequestConfig defaultRequestConfig = RequestConfig.custom().setSocketTimeout(10000).setConnectTimeout(10000).setConnectionRequestTimeout(
                10000).build();
        try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build()) {
            try (CloseableHttpResponse response = httpClient.execute(httpGet); StringWriter writer = new StringWriter()) {
                switch (response.getStatusLine().getStatusCode()) {
                    case 200:
                        HttpEntity httpEntity = response.getEntity();
                        httpEntity.getContentLength();
                        IOUtils.copy(httpEntity.getContent(), writer);
                        return writer.toString();
                    default:
                        logger.error("Could not open URL '{}': {}", url, response.getStatusLine().getReasonPhrase());
                }
            }
        } finally {
            httpGet.releaseConnection();
        }

        return null;
    }

    /**
     * Create a JDOM document from an XML string.
     *
     * @param string
     * @return
     * @throws IOException
     * @throws JDOMException
     * @should build document correctly
     */
    public static Document getDocumentFromString(String string, String encoding) throws JDOMException, IOException {
        if (encoding == null) {
            encoding = "UTF-8";
            ;
        }

        byte[] byteArray = null;
        try {
            byteArray = string.getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
        }
        ByteArrayInputStream baos = new ByteArrayInputStream(byteArray);

        // Reader reader = new StringReader(hOCRText);
        SAXBuilder builder = new SAXBuilder();
        Document document = builder.build(baos);

        return document;
    }

    public static boolean isUrn(String urn) {
        return StringUtils.isNotEmpty(urn) && !urn.startsWith("http");
    }

    /**
     * Creates the file Path to the updated anchor file
     * 
     * @param destFolderPath
     * @param baseName
     * @param extension
     * @return
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
     * 
     * @param file
     * @return
     * @should extract file name correctly
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
}
