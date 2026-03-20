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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author florian
 *
 */
public class HttpConnector {

    private static final Logger logger = LogManager.getLogger(HttpConnector.class);

    //handles URL connection for downloading external images. Avoids having too many open http-connections
    private final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    private final int connectionTimeout;

    public HttpConnector(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * @param url
     * @param file
     * @deprecated Use {@link #downloadFile(URI, Path, long, List)} instead
     */
    @Deprecated(since = "26.03")
    public void downloadFile(URI url, Path file) throws IOException {
        downloadFile(url, file, 512L * 1024 * 1024, null);
    }

    /**
     * Downloads a file from the given URI, enforcing a maximum file size and validating redirect targets against SSRF.
     *
     * @param url the source URI
     * @param file the target file path
     * @param maxBytes maximum allowed download size in bytes (0 = unlimited)
     * @param allowedUrlPrefixes URL prefixes to validate redirect targets against (may be null)
     * @throws IOException if the download fails or limits are exceeded
     */
    public void downloadFile(URI url, Path file, long maxBytes, List<String> allowedUrlPrefixes) throws IOException {

        CloseableHttpClient client = HttpClientBuilder.create()
                .setConnectionManager(connectionManager)
                .setRedirectStrategy(new SafeRedirectStrategy(allowedUrlPrefixes))
                .build();
        HttpGet request = new HttpGet(url);
        RequestConfig config =
                RequestConfig.custom()
                        .setConnectionRequestTimeout(connectionTimeout)
                        .setSocketTimeout(connectionTimeout)
                        .setConnectTimeout(connectionTimeout)
                        .build();
        request.setConfig(config);
        try (CloseableHttpResponse response = client.execute(request)) {
            if (response.getStatusLine().getStatusCode() >= 400) {
                throw new IOException("Cannot resolve url '" + url + "' successfully. Status code = " + response.getStatusLine().getStatusCode());
            }

            // Check Content-Length header before downloading
            if (maxBytes > 0) {
                Header contentLengthHeader = response.getFirstHeader("Content-Length");
                if (contentLengthHeader != null) {
                    long contentLength = Long.parseLong(contentLengthHeader.getValue());
                    if (contentLength > maxBytes) {
                        throw new IOException(
                                "Response too large: " + contentLength + " bytes exceeds limit of " + maxBytes + " bytes for URL: " + url);
                    }
                }
            }

            long totalBytesRead = 0;
            try (InputStream inStream = response.getEntity().getContent();
                    OutputStream outStream = Files.newOutputStream(file)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = inStream.read(buffer)) != -1) {
                    totalBytesRead += bytesRead;
                    if (maxBytes > 0 && totalBytesRead > maxBytes) {
                        break;
                    }
                    outStream.write(buffer, 0, bytesRead);
                }
            }
            if (maxBytes > 0 && totalBytesRead > maxBytes) {
                Files.deleteIfExists(file);
                throw new IOException(
                        "Download exceeded maximum size of " + maxBytes + " bytes for URL: " + url);
            }
        }
    }

    /**
     * Redirect strategy that validates redirect targets against SSRF protection rules.
     */
    private static class SafeRedirectStrategy extends LaxRedirectStrategy {

        private final List<String> allowedUrlPrefixes;

        SafeRedirectStrategy(List<String> allowedUrlPrefixes) {
            this.allowedUrlPrefixes = allowedUrlPrefixes;
        }

        @Override
        public HttpUriRequest getRedirect(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
            HttpUriRequest redirect = super.getRedirect(request, response, context);
            URI redirectUri = redirect.getURI();
            if (!SsrfProtection.isRedirectAllowed(redirectUri, allowedUrlPrefixes)) {
                throw new ProtocolException("SSRF protection: redirect to disallowed URL blocked: " + redirectUri);
            }
            return redirect;
        }
    }
}
