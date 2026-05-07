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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * ImageUrlReplaceRule class. Holds a single regex condition and replacement, used to transform external image URLs at indexing time.
 * </p>
 */
public class ImageUrlReplaceRule {

    private final Pattern condition;
    private final String replacement;

    /**
     * Constructor.
     *
     * @param condition regex pattern to match against an image URL
     * @param replacement replacement string (may reference capture groups via $1, $2, ...)
     * @should throw IllegalArgumentException if condition or replacement is null
     * @should throw PatternSyntaxException if condition is not a valid regex
     */
    public ImageUrlReplaceRule(String condition, String replacement) {
        if (condition == null) {
            throw new IllegalArgumentException("condition may not be null");
        }
        if (replacement == null) {
            throw new IllegalArgumentException("replacement may not be null");
        }
        this.condition = Pattern.compile(condition);
        this.replacement = replacement;
    }

    /**
     * Applies this rule to the given URL. If the URL matches the condition, the replacement is applied; otherwise the URL is returned unchanged.
     *
     * @param url URL to test and potentially transform
     * @return transformed URL if the condition matched, otherwise the original URL
     * @should return null if url is null
     * @should return original url if condition does not match
     * @should return replaced url if condition matches
     * @should support backreferences in replacement
     */
    public String apply(String url) {
        if (url == null) {
            return null;
        }
        Matcher m = condition.matcher(url);
        if (m.matches()) {
            return m.replaceAll(replacement);
        }
        return url;
    }

    /**
     * @return the condition pattern
     */
    public Pattern getCondition() {
        return condition;
    }

    /**
     * @return the replacement string
     */
    public String getReplacement() {
        return replacement;
    }
}
