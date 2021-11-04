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

import org.junit.Assert;
import org.junit.Test;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.model.config.ValueNormalizer.ValueNormalizerPosition;

public class ValueNormalizerTest extends AbstractTest {

    /**
     * @see ValueNormalizer#normalize(String)
     * @verifies do nothing if length ok
     */
    @Test
    public void normalize_shouldDoNothingIfLengthOk() throws Exception {
        ValueNormalizer vn = new ValueNormalizer().setTargetLength(3);
        Assert.assertEquals("123", vn.normalize("123"));
    }

    /**
     * @see ValueNormalizer#normalize(String)
     * @verifies normalize too short strings correctly
     */
    @Test
    public void normalize_shouldNormalizeTooShortStringsCorrectly() throws Exception {
        {
            ValueNormalizer vn = new ValueNormalizer().setTargetLength(5);
            Assert.assertEquals("00123", vn.normalize("123"));
        }
        {
            ValueNormalizer vn = new ValueNormalizer().setTargetLength(5).setPosition(ValueNormalizerPosition.REAR);
            Assert.assertEquals("12300", vn.normalize("123"));
        }
    }

    /**
     * @see ValueNormalizer#normalize(String)
     * @verifies normalize too long strings correctly
     */
    @Test
    public void normalize_shouldNormalizeTooLongStringsCorrectly() throws Exception {
        {
            ValueNormalizer vn = new ValueNormalizer().setTargetLength(3);
            Assert.assertEquals("bar", vn.normalize("foobar"));
        }
        {
            ValueNormalizer vn = new ValueNormalizer().setTargetLength(3).setPosition(ValueNormalizerPosition.REAR);
            Assert.assertEquals("foo", vn.normalize("foobar"));
        }
    }

    /**
     * @see ValueNormalizer#normalize(String)
     * @verifies normalize regex groups correctly
     */
    @Test
    public void normalize_shouldNormalizeRegexGroupsCorrectly() throws Exception {
        {
            ValueNormalizer vn = new ValueNormalizer().setTargetLength(8).setRegex("[a-zA-Z]+[\\s]+([0-9]+)[.]([0-9]+).*$");
            Assert.assertEquals("foo 00004173.00000001 bar", vn.normalize("foo 4173.1 bar"));
            Assert.assertEquals("foo 00004173.00000010 bar", vn.normalize("foo 4173.10 bar"));
        }
        {
            ValueNormalizer vn = new ValueNormalizer().setTargetLength(8).setRegex("[a-zA-Z]+[\\s]+([0-9]+).*([0-9]+)$");
            Assert.assertEquals("foo 00000001 bar 00000002", vn.normalize("foo 1 bar 2"));
        }
    }

    /**
     * @see ValueNormalizer#normalize(String)
     * @verifies keep parts not matching regex unchanged
     */
    @Test
    public void normalize_shouldKeepPartsNotMatchingRegexUnchanged() throws Exception {
        {
            // front, too short
            ValueNormalizer vn = new ValueNormalizer().setTargetLength(5).setRegex("[0-9]+");
            Assert.assertEquals("foo00123bar", vn.normalize("foo123bar"));
        }
        {
            // rear, too short
            ValueNormalizer vn = new ValueNormalizer().setTargetLength(5)
                    .setPosition(ValueNormalizerPosition.REAR)
                    .setRegex("[0-9]+");
            Assert.assertEquals("foo12300bar", vn.normalize("foo123bar"));
        }
        {
            // front, too long
            ValueNormalizer vn = new ValueNormalizer().setTargetLength(2).setRegex("[0-9]+");
            Assert.assertEquals("foo23bar", vn.normalize("foo123bar"));
        }
        {
            // rear, too long
            ValueNormalizer vn = new ValueNormalizer().setTargetLength(2)
                    .setPosition(ValueNormalizerPosition.REAR)
                    .setRegex("[0-9]+");
            Assert.assertEquals("foo12bar", vn.normalize("foo123bar"));
        }
        {
            // with a specified capture group
            ValueNormalizer vn = new ValueNormalizer().setTargetLength(5).setRegex("1656\\/[0-9]\\/[0-9]\\/([0-9]+).*$");
            Assert.assertEquals("1656/1/2/00123a", vn.normalize("1656/1/2/123a"));
        }
    }

    /**
     * @see ValueNormalizer#normalize(String)
     * @verifies convert roman numerals correctly
     */
    @Test
    public void normalize_shouldConvertRomanNumeralsCorrectly() throws Exception {
        ValueNormalizer vn = new ValueNormalizer().setTargetLength(8).setRegex("foo ([C|I|M|V|X]+)[ ].*$").setConvertRoman(true);
        Assert.assertEquals("foo 1 f.17", vn.normalize("foo I f.17"));
        Assert.assertEquals("foo 8 f.1", vn.normalize("foo VIII f.1"));
    }

    /**
     * @see ValueNormalizer#convertRomanNumeral(String)
     * @verifies convert correctly
     */
    @Test
    public void convertRomanNumeral_shouldConvertCorrectly() throws Exception {
        Assert.assertEquals(1, ValueNormalizer.convertRomanNumeral("i"));
        Assert.assertEquals(2, ValueNormalizer.convertRomanNumeral("ii"));
        Assert.assertEquals(3, ValueNormalizer.convertRomanNumeral("iii"));
        Assert.assertEquals(4, ValueNormalizer.convertRomanNumeral("iv"));
        Assert.assertEquals(5, ValueNormalizer.convertRomanNumeral("v"));
        Assert.assertEquals(6, ValueNormalizer.convertRomanNumeral("vi"));
        Assert.assertEquals(7, ValueNormalizer.convertRomanNumeral("vii"));
        Assert.assertEquals(8, ValueNormalizer.convertRomanNumeral("viii"));
        Assert.assertEquals(9, ValueNormalizer.convertRomanNumeral("ix"));
        Assert.assertEquals(10, ValueNormalizer.convertRomanNumeral("x"));
    }
}