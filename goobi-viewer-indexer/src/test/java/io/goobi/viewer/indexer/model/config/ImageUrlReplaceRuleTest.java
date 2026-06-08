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
package io.goobi.viewer.indexer.model.config;

import java.util.regex.PatternSyntaxException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;

class ImageUrlReplaceRuleTest extends AbstractTest {

    /**
     * @see ImageUrlReplaceRule#ImageUrlReplaceRule(String,String)
     * @verifies throw IllegalArgumentException if condition or replacement is null
     */
    @Test
    void ImageUrlReplaceRule_shouldThrowIllegalArgumentExceptionIfConditionOrReplacementIsNull() throws Exception {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ImageUrlReplaceRule(null, "x"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ImageUrlReplaceRule("x", null));
    }

    /**
     * @see ImageUrlReplaceRule#ImageUrlReplaceRule(String,String)
     * @verifies throw PatternSyntaxException if condition is not a valid regex
     */
    @Test
    void ImageUrlReplaceRule_shouldThrowPatternSyntaxExceptionIfConditionIsNotValidRegex() throws Exception {
        Assertions.assertThrows(PatternSyntaxException.class, () -> new ImageUrlReplaceRule("[unclosed", "x"));
    }

    /**
     * @see ImageUrlReplaceRule#apply(String)
     * @verifies return null if url is null
     */
    @Test
    void apply_shouldReturnNullIfUrlIsNull() throws Exception {
        ImageUrlReplaceRule rule = new ImageUrlReplaceRule("^x$", "y");
        Assertions.assertNull(rule.apply(null));
    }

    /**
     * @see ImageUrlReplaceRule#apply(String)
     * @verifies return original url if condition does not match
     */
    @Test
    void apply_shouldReturnOriginalUrlIfConditionDoesNotMatch() throws Exception {
        ImageUrlReplaceRule rule = new ImageUrlReplaceRule("^https://nomatch\\.example\\.com/.*$", "https://other.example.com/");
        String url = "https://example.com/image.jpg";
        Assertions.assertEquals(url, rule.apply(url));
    }

    /**
     * @see ImageUrlReplaceRule#apply(String)
     * @verifies return replaced url if condition matches
     */
    @Test
    void apply_shouldReturnReplacedUrlIfConditionMatches() throws Exception {
        ImageUrlReplaceRule rule = new ImageUrlReplaceRule("^https://example\\.com/(.+)$", "https://other.example.com/$1");
        Assertions.assertEquals("https://other.example.com/image.jpg", rule.apply("https://example.com/image.jpg"));
    }

    /**
     * @see ImageUrlReplaceRule#apply(String)
     * @verifies support backreferences in replacement
     */
    @Test
    void apply_shouldSupportBackreferencesInReplacement() throws Exception {
        ImageUrlReplaceRule rule = new ImageUrlReplaceRule(
                "^https?://id\\.acdh\\.oeaw\\.ac\\.at/(.+?)\\?format=image/jpeg$",
                "https://loris.acdh.oeaw.ac.at/id.acdh.oeaw.ac.at/$1");
        Assertions.assertEquals(
                "https://loris.acdh.oeaw.ac.at/id.acdh.oeaw.ac.at/woldan/KVBlAM15/KVBlAM15_media/KVBlAM15_media_0001.tif",
                rule.apply("https://id.acdh.oeaw.ac.at/woldan/KVBlAM15/KVBlAM15_media/KVBlAM15_media_0001.tif?format=image/jpeg"));
    }
}
