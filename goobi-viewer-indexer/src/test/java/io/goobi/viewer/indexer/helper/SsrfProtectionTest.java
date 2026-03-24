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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SsrfProtection} — validates SSRF protection logic including URL allowlisting,
 * private/internal IP blocking, scheme validation, and host resolution checks.
 */
class SsrfProtectionTest {

    /**
     * @see SsrfProtection#isUrlAllowed(String, List)
     * @verifies reject null url
     */
    @Test
    void isUrlAllowed_shouldRejectNull() {
        assertFalse(SsrfProtection.isUrlAllowed(null, Collections.emptyList()));
    }

    /**
     * @see SsrfProtection#isUrlAllowed(String, List)
     * @verifies reject blank url
     */
    @Test
    void isUrlAllowed_shouldRejectBlank() {
        assertFalse(SsrfProtection.isUrlAllowed("", Collections.emptyList()));
    }

    /**
     * @see SsrfProtection#isUrlAllowed(String, List)
     * @verifies reject non-HTTP scheme
     */
    @Test
    void isUrlAllowed_shouldRejectNonHttpScheme() {
        assertFalse(SsrfProtection.isUrlAllowed("file:///etc/passwd", Collections.emptyList()));
        assertFalse(SsrfProtection.isUrlAllowed("ftp://example.com/file", Collections.emptyList()));
    }

    /**
     * @see SsrfProtection#isUrlAllowed(String, List)
     * @verifies reject localhost url
     */
    @Test
    void isUrlAllowed_shouldRejectLocalhostUrl() {
        assertFalse(SsrfProtection.isUrlAllowed("http://localhost:8983/solr/admin/cores", Collections.emptyList()));
        assertFalse(SsrfProtection.isUrlAllowed("http://127.0.0.1:8983/solr/admin/cores", Collections.emptyList()));
    }

    /**
     * @see SsrfProtection#isUrlAllowed(String, List)
     * @verifies reject private IP ranges
     */
    @Test
    void isUrlAllowed_shouldRejectPrivateIpRanges() {
        assertFalse(SsrfProtection.isUrlAllowed("http://192.168.1.1/admin", Collections.emptyList()));
        assertFalse(SsrfProtection.isUrlAllowed("http://10.0.0.1/internal", Collections.emptyList()));
        assertFalse(SsrfProtection.isUrlAllowed("http://172.16.0.1/data", Collections.emptyList()));
    }

    /**
     * @see SsrfProtection#isUrlAllowed(String, List)
     * @verifies reject AWS metadata endpoint
     */
    @Test
    void isUrlAllowed_shouldRejectAwsMetadataEndpoint() {
        assertFalse(SsrfProtection.isUrlAllowed("http://169.254.169.254/latest/meta-data/", Collections.emptyList()));
    }

    /**
     * @see SsrfProtection#isUrlAllowed(String, List)
     * @verifies allow public url with empty allowlist
     */
    @Test
    void isUrlAllowed_shouldAllowPublicUrlWithEmptyAllowlist() {
        assertTrue(SsrfProtection.isUrlAllowed("https://example.com/image/1/full/max/0/default.jpg", Collections.emptyList()));
    }

    /**
     * @see SsrfProtection#isUrlAllowed(String, List)
     * @verifies allow public url with null allowlist
     */
    @Test
    void isUrlAllowed_shouldAllowPublicUrlWithNullAllowlist() {
        assertTrue(SsrfProtection.isUrlAllowed("https://example.com/image/1/full/max/0/default.jpg", null));
    }

    /**
     * @see SsrfProtection#isUrlAllowed(String, List)
     * @verifies reject url not in allowlist
     */
    @Test
    void isUrlAllowed_shouldRejectUrlNotInAllowlist() {
        List<String> allowlist = List.of("https://trusted-server.com/");
        assertFalse(SsrfProtection.isUrlAllowed("https://untrusted.com/image.jpg", allowlist));
    }

    /**
     * @see SsrfProtection#isUrlAllowed(String, List)
     * @verifies allow url matching allowlist
     */
    @Test
    void isUrlAllowed_shouldAllowUrlMatchingAllowlist() {
        List<String> allowlist = List.of("https://trusted-server.com/");
        assertTrue(SsrfProtection.isUrlAllowed("https://trusted-server.com/images/test.jpg", allowlist));
    }

    /**
     * @see SsrfProtection#isUrlAllowed(String, List)
     * @verifies reject private IP even if in allowlist
     */
    @Test
    void isUrlAllowed_shouldRejectPrivateIpEvenIfInAllowlist() {
        List<String> allowlist = List.of("http://localhost:8983/");
        assertFalse(SsrfProtection.isUrlAllowed("http://localhost:8983/solr/admin", allowlist));
    }

    /**
     * @see SsrfProtection#isHostAllowed(String)
     * @verifies reject loopback address
     */
    @Test
    void isHostAllowed_shouldRejectLoopback() {
        assertFalse(SsrfProtection.isHostAllowed("127.0.0.1"));
        assertFalse(SsrfProtection.isHostAllowed("localhost"));
    }

    /**
     * @see SsrfProtection#isHostAllowed(String)
     * @verifies reject link-local address
     */
    @Test
    void isHostAllowed_shouldRejectLinkLocal() {
        assertFalse(SsrfProtection.isHostAllowed("169.254.169.254"));
    }
}
