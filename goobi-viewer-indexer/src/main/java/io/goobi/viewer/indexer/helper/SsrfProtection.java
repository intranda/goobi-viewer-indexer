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
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Validates URLs before making HTTP requests to prevent Server-Side Request Forgery (SSRF) attacks.
 */
public final class SsrfProtection {

    private static final Logger logger = LogManager.getLogger(SsrfProtection.class);

    private SsrfProtection() {
        // utility class
    }

    /**
     * Validates a URL for safe external access. Checks against an allowlist (if configured) and blocks private/internal IP addresses.
     *
     * @param url the URL to validate
     * @param allowedUrlPrefixes list of allowed URL prefixes; if empty, all non-private URLs are allowed
     * @return true if the URL is safe to access
     * @should reject null url
     * @should reject blank url
     * @should reject non-HTTP scheme
     * @should reject localhost url
     * @should reject private IP ranges
     * @should reject AWS metadata endpoint
     * @should allow public url with empty allowlist
     * @should allow public url with null allowlist
     * @should reject url not in allowlist
     * @should allow url matching allowlist
     * @should reject private IP even if in allowlist
     */
    public static boolean isUrlAllowed(String url, List<String> allowedUrlPrefixes) {
        if (url == null || url.isBlank()) {
            return false;
        }

        // Only allow http and https schemes
        if (!url.startsWith("http://") && !url.startsWith("https://")) {
            logger.warn("SSRF protection: blocked non-HTTP URL scheme: {}", url);
            return false;
        }

        // If an allowlist is configured and non-empty, the URL must match one of the prefixes
        if (allowedUrlPrefixes != null && !allowedUrlPrefixes.isEmpty()) {
            boolean matched = allowedUrlPrefixes.stream().anyMatch(url::startsWith);
            if (!matched) {
                logger.warn("SSRF protection: URL not in allowlist: {}", url);
                return false;
            }
        }

        // Block private/internal IP addresses
        try {
            String host = new URI(url).getHost();
            if (host == null) {
                logger.warn("SSRF protection: could not extract host from URL: {}", url);
                return false;
            }
            if (!isHostAllowed(host)) {
                logger.warn("SSRF protection: blocked request to private/internal address: {} ({})", host, url);
                return false;
            }
        } catch (URISyntaxException e) {
            logger.warn("SSRF protection: invalid URL syntax: {}", url);
            return false;
        }

        return true;
    }

    /**
     * Checks whether a resolved host address is routable (not loopback, not private, not link-local).
     *
     * @param host hostname to check
     * @return true if the host resolves to a public address
     * @should reject loopback address
     * @should reject link-local address
     */
    static boolean isHostAllowed(String host) {
        try {
            InetAddress[] addresses = InetAddress.getAllByName(host);
            for (InetAddress addr : addresses) {
                if (addr.isLoopbackAddress() || addr.isSiteLocalAddress() || addr.isLinkLocalAddress()
                        || addr.isAnyLocalAddress() || addr.isMulticastAddress()) {
                    return false;
                }
                // Block AWS metadata endpoint (169.254.169.254)
                byte[] raw = addr.getAddress();
                if (raw.length == 4 && (raw[0] & 0xFF) == 169 && (raw[1] & 0xFF) == 254) {
                    return false;
                }
            }
            return true;
        } catch (UnknownHostException e) {
            logger.warn("SSRF protection: cannot resolve host: {}", host);
            return false;
        }
    }

    /**
     * Re-validates a redirect target URL. Used by the safe redirect strategy in HttpConnector.
     *
     * @param redirectUrl the redirect target URL
     * @param allowedUrlPrefixes list of allowed URL prefixes
     * @return true if the redirect is safe
     */
    public static boolean isRedirectAllowed(URI redirectUrl, List<String> allowedUrlPrefixes) {
        return isUrlAllowed(redirectUrl.toString(), allowedUrlPrefixes);
    }
}
