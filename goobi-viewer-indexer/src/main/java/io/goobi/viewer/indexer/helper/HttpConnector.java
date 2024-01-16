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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * @author florian
 *
 */
public class HttpConnector {

    //handles URL connection for downloading external images. Avoids having too many open http-connections
    private final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    private final int connectionTimeout;

    public HttpConnector(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * @param url
     * @param file
     */
    public void downloadFile(URI url, Path file) throws IOException {

        CloseableHttpClient client = HttpClientBuilder.create()
                .setConnectionManager(connectionManager)
                .setRedirectStrategy(new LaxRedirectStrategy())
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
            try (InputStream inStream = response.getEntity().getContent()) {
                Files.copy(inStream, file, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }
}
