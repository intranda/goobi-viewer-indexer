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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ugh.dl.RomanNumeral;

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

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(ValueNormalizer.class);

    /** Target total string length */
    private int targetLength = 0;
    /** Filler character */
    private char filler = '0';
    /** Position at which to fill/truncate the string */
    private ValueNormalizerPosition position = ValueNormalizerPosition.FRONT;

    private String regex = ".*";

    private boolean convertRoman = false;

    /**
     * Fills up the given string value with instances of <code>filler</code> until the string reaches the size of <code>length</code>, either at the
     * front or the rear <code>position</code>. Too long strings are truncated at <code>position</code>.
     *
     * @param s a {@link java.lang.String} object.
     * @return Normalized value
     * @should do nothing if length ok
     * @should normalize too short strings correctly
     * @should normalize too long strings correctly
     * @should normalize regex groups correctly
     * @should keep parts not matching regex unchanged
     * @should convert roman numerals correctly
     */
    public String normalize(String s) {
        if (s == null || StringUtils.isEmpty(regex)) {
            return s;
        }

        // If a regex is provided, only normalize part matching it
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(s);
        if (m.find()) {

            // With capture group
            if (m.groupCount() > 0) {
                int[] groupStartIndices = new int[m.groupCount()];
                int[] groupEndIndices = new int[m.groupCount()];

                // Collect group indices first
                for (int i = 1; i <= m.groupCount(); ++i) {
                    groupStartIndices[i - 1] = m.start(i);
                    groupEndIndices[i - 1] = m.end(i);
                }

                // Modify and reassemble
                StringBuilder sb = new StringBuilder();
                if (groupStartIndices[0] > 0) {
                    sb.append(s.substring(0, groupStartIndices[0]));
                }
                for (int i = 0; i < groupStartIndices.length; ++i) {
                    // Apply modifications to group part
                    String groupPart = s.substring(groupStartIndices[i], groupEndIndices[i]);
                    if (convertRoman) {
                        // Roman numerals
                        try {
                            sb.append(convertRomanNumeral(groupPart));
                        } catch (NumberFormatException e) {
                            logger.warn("{}: {}", e.getMessage(), groupPart);
                            sb.append(groupPart);
                        }
                    } else {
                        // Filler
                        sb.append(applyFiller(groupPart));
                    }
                    if (i + 1 < groupStartIndices.length) {
                        // Append characters from the original string that are between this and the next group
                        sb.append(s.substring(groupEndIndices[i], groupStartIndices[i + 1]));
                    } else if (groupEndIndices[i] < s.length()) {
                        // Append remainder of the original string
                        sb.append(s.substring(groupEndIndices[i]));
                    }
                }

                return sb.toString();
            }

            // Without capture group
            String prefix = "";
            String suffix = "";
            String relevantPart = m.group();
            if (m.start() > 0) {
                prefix = s.substring(0, m.start());
            }
            if (m.end() < s.length()) {
                suffix = s.substring(m.end());
            }

            if (relevantPart.length() == targetLength) {
                return s;
            }

            if (relevantPart.length() < targetLength) {
                StringBuilder sb = new StringBuilder();
                sb.append(prefix);
                if (convertRoman) {
                    try {
                        sb.append(convertRomanNumeral(relevantPart));
                    } catch (NumberFormatException e) {
                        logger.warn("{}: {}", e.getMessage(), s);
                        sb.append(relevantPart);
                    }
                } else {
                    sb.append(applyFiller(relevantPart));
                }
                sb.append(suffix);
                return sb.toString();
            }

            // Shorten if original string than the target length
            switch (position) {
                case FRONT:
                    return prefix + relevantPart.substring(relevantPart.length() - targetLength) + suffix;
                case REAR:
                    return prefix + relevantPart.substring(0, targetLength) + suffix;
                default:
                    return prefix + relevantPart + suffix;
            }
        }

        return s;
    }

    /**
     * 
     * @param s
     * @return
     */
    String applyFiller(String s) {
        if (s == null || s.length() >= targetLength) {
            return s;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < targetLength - s.length(); ++i) {
            sb.append(filler);
        }

        switch (position) {
            case FRONT:
                return sb.toString() + s;
            case REAR:
                return s + sb.toString();
        }

        return s;
    }

    /**
     * 
     * @param s Roman numeral to convert
     * @return Same number in Arabic digits
     * @should convert correctly
     */
    static int convertRomanNumeral(String s) throws NumberFormatException {
        if (StringUtils.isEmpty(s)) {
            throw new IllegalArgumentException("s may not be empty");
        }

        RomanNumeral rn = new RomanNumeral(s);
        return rn.intValue();
    }

    /**
     * <p>
     * Getter for the field <code>targetLength</code>.
     * </p>
     *
     * @return the targetLength
     */
    public int getTargetLength() {
        return targetLength;
    }

    /**
     * @param targetLength the targetLength to set
     * @return this
     */
    public ValueNormalizer setTargetLength(int targetLength) {
        this.targetLength = targetLength;
        return this;
    }

    /**
     * <p>
     * Getter for the field <code>filler</code>.
     * </p>
     *
     * @return the filler
     */
    public char getFiller() {
        return filler;
    }

    /**
     * @param filler the filler to set
     * @return this
     */
    public ValueNormalizer setFiller(char filler) {
        this.filler = filler;
        return this;
    }

    /**
     * <p>
     * Getter for the field <code>position</code>.
     * </p>
     *
     * @return the position
     */
    public ValueNormalizerPosition getPosition() {
        return position;
    }

    /**
     * @param position the position to set
     * @return this
     */
    public ValueNormalizer setPosition(ValueNormalizerPosition position) {
        this.position = position;
        return this;
    }

    /**
     * @return the regex
     */
    public String getRegex() {
        return regex;
    }

    /**
     * @param regex the regex to set
     * @return this
     */
    public ValueNormalizer setRegex(String regex) {
        this.regex = regex;
        return this;
    }

    /**
     * @return the convertRoman
     */
    public boolean isConvertRoman() {
        return convertRoman;
    }

    /**
     * @param convertRoman the convertRoman to set
     * @return this
     */
    public ValueNormalizer setConvertRoman(boolean convertRoman) {
        this.convertRoman = convertRoman;
        return this;
    }
}
