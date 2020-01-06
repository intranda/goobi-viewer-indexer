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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * Normalizes string values to the configured length, either by adding a filler character or by truncating.
 */
public class ValueNormalizer {

    /**
     * Position enumerator.
     */
    public enum ValueNormalizerPosition {
        FRONT,
        REAR;

        public static ValueNormalizerPosition getByName(String name) {
            if (name == null) {
                return FRONT;
            }

            switch (name.toLowerCase()) {
                case "rear":
                case "back":
                    return REAR;
                default:
                    return FRONT;
            }
        }
    }

    /** Target string length */
    private final int length;
    /** Filler character */
    private final char filler;
    /** Position at which to fill/truncate the string */
    private final ValueNormalizerPosition position;
    private final String relevantPartRegex;

    /**
     * Constructor.
     *
     * @param length Target string length
     * @param filler Filler character
     * @param position Position at which to fill/truncate the string
     * @param relevantPartRegex a {@link java.lang.String} object.
     */
    public ValueNormalizer(int length, char filler, ValueNormalizerPosition position, String relevantPartRegex) {
        this.length = length;
        this.filler = filler;
        this.position = position;
        this.relevantPartRegex = relevantPartRegex;
    }

    /**
     * Fills up the given string value with instances of <code>filler</code> until the string reaches the size of <code>length</code>, either at the
     * front or the rear <code>position</code>. Too long strings are truncated at <code>position</code>.
     *
     * @param s a {@link java.lang.String} object.
     * @return Normalized value
     * @should do nothing if length ok
     * @should normalize too short strings correctly
     * @should normalize too long strings correctly
     * @should keep parts not matching regex unchanged
     */
    public String normalize(String s) {
        if (s == null) {
            return null;
        }

        String relevantPart = s;

        // If a regex is provided, only normalize part matching it
        String prefix = "";
        String suffix = "";
        if (StringUtils.isNotEmpty(relevantPartRegex)) {
            Pattern p = Pattern.compile(relevantPartRegex);
            Matcher m = p.matcher(relevantPart);
            List<String> parts = new ArrayList<>();
            while (m.find()) {
                if (m.groupCount() > 0) {
                    // With capture group
                    relevantPart = m.group(1);
                    if (m.start(1) > 0) {
                        prefix = s.substring(0, m.start(1));
                    }
                    if (m.end(1) < s.length()) {
                        suffix = s.substring(m.end(1));
                    }
                } else {
                    // Without capture group
                    relevantPart = m.group();
                    if (m.start() > 0) {
                        prefix = s.substring(0, m.start());
                    }
                    if (m.end() < s.length()) {
                        suffix = s.substring(m.end());
                    }
                }
            }
        }

        if (relevantPart.length() == length) {
            return s;
        }

        if (relevantPart.length() < length) {
            StringBuilder sb = new StringBuilder();
            sb.append(prefix);
            for (int i = 0; i < length - relevantPart.length(); ++i) {
                sb.append(filler);
            }
            switch (position) {
                case FRONT:
                    sb.append(relevantPart);
                    break;
                case REAR:
                    sb.insert(prefix.length(), relevantPart);
                    break;
            }
            sb.append(suffix);
            return sb.toString();
        }

        switch (position) {
            case FRONT:
                return prefix + relevantPart.substring(relevantPart.length() - length) + suffix;
            case REAR:
                return prefix + relevantPart.substring(0, length) + suffix;
            default:
                return prefix + relevantPart + suffix;
        }
    }

    /**
     * <p>Getter for the field <code>length</code>.</p>
     *
     * @return the length
     */
    public int getLength() {
        return length;
    }

    /**
     * <p>Getter for the field <code>filler</code>.</p>
     *
     * @return the filler
     */
    public char getFiller() {
        return filler;
    }

    /**
     * <p>Getter for the field <code>position</code>.</p>
     *
     * @return the position
     */
    public ValueNormalizerPosition getPosition() {
        return position;
    }

}
