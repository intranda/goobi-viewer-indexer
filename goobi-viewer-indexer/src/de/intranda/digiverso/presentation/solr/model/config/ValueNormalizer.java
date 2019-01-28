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
package de.intranda.digiverso.presentation.solr.model.config;

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

    /**
     * Constructor.
     * 
     * @param length Target string length
     * @param filler Filler character
     * @param position Position at which to fill/truncate the string
     */
    public ValueNormalizer(int length, char filler, ValueNormalizerPosition position) {
        this.length = length;
        this.filler = filler;
        this.position = position;
    }

    /**
     * Fills up the given string value with instances of <code>filler</code> until the string reaches the size of <code>length</code>, either at the
     * front or the rear <code>position</code>. Too long strings are truncated at <code>position</code>.
     * 
     * @param s
     * @return Normalized value
     * @should normalize too short strings correctly
     * @should normalize too long strings correctly
     */
    public String normalize(String s) {
        if (s == null) {
            return null;
        }

        if (s.length() == length) {
            return s;
        }

        if (s.length() < length) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < length - s.length(); ++i) {
                sb.append(filler);
            }
            switch (position) {
                case FRONT:
                    sb.append(s);
                    break;
                case REAR:
                    sb.insert(0, s);
                    break;
            }
            return sb.toString();
        }

        switch (position) {
            case FRONT:
                return s.substring(s.length() - length);
            case REAR:
                return s.substring(0, length);
            default:
                return s;
        }
    }

    /**
     * @return the length
     */
    public int getLength() {
        return length;
    }

    /**
     * @return the filler
     */
    public char getFiller() {
        return filler;
    }

    /**
     * @return the position
     */
    public ValueNormalizerPosition getPosition() {
        return position;
    }

}
