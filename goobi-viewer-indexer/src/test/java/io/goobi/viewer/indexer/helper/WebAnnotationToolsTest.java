package io.goobi.viewer.indexer.helper;

import java.net.URI;
import java.util.regex.Matcher;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WebAnnotationToolsTest {
    /**
     * @see WebAnnotationTools#getMatchingMatcher(URI)
     * @verifies pick correct matcher
     */
    @Test
    public void getMatchingMatcher_shouldPickCorrectMatcher() throws Exception {
        Matcher m = WebAnnotationTools
                .getMatchingMatcher(new URI("https://example.com/viewer/api/v1/records/PPN123/manifest/"));
        Assertions.assertNotNull(m);
        Assertions.assertEquals(WebAnnotationTools.TARGET_REGEX_MANIFEST, m.pattern().pattern());
    }
}