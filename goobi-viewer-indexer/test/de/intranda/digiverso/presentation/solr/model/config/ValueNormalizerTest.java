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

import org.junit.Assert;
import org.junit.Test;

import de.intranda.digiverso.presentation.solr.AbstractTest;
import de.intranda.digiverso.presentation.solr.model.config.ValueNormalizer.ValueNormalizerPosition;

public class ValueNormalizerTest extends AbstractTest {

    /**
     * @see ValueNormalizer#normalize(String)
     * @verifies do nothing if length ok
     */
    @Test
    public void normalize_shouldDoNothingIfLengthOk() throws Exception {
        ValueNormalizer vn = new ValueNormalizer(3, '0', ValueNormalizerPosition.FRONT, null);
        Assert.assertEquals("123", vn.normalize("123"));
    }

    /**
     * @see ValueNormalizer#normalize(String)
     * @verifies normalize too short strings correctly
     */
    @Test
    public void normalize_shouldNormalizeTooShortStringsCorrectly() throws Exception {
        {
            ValueNormalizer vn = new ValueNormalizer(5, '0', ValueNormalizerPosition.FRONT, null);
            Assert.assertEquals("00123", vn.normalize("123"));
        }
        {
            ValueNormalizer vn = new ValueNormalizer(5, '0', ValueNormalizerPosition.REAR, null);
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
            ValueNormalizer vn = new ValueNormalizer(3, '0', ValueNormalizerPosition.FRONT, null);
            Assert.assertEquals("bar", vn.normalize("foobar"));
        }
        {
            ValueNormalizer vn = new ValueNormalizer(3, '0', ValueNormalizerPosition.REAR, null);
            Assert.assertEquals("foo", vn.normalize("foobar"));
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
            ValueNormalizer vn = new ValueNormalizer(5, '0', ValueNormalizerPosition.FRONT, "[0-9]+");
            Assert.assertEquals("foo00123bar", vn.normalize("foo123bar"));
        }
        {
            // rear, too short
            ValueNormalizer vn = new ValueNormalizer(5, '0', ValueNormalizerPosition.REAR, "[0-9]+");
            Assert.assertEquals("foo12300bar", vn.normalize("foo123bar"));
        }
        {
            // front, too long
            ValueNormalizer vn = new ValueNormalizer(2, '0', ValueNormalizerPosition.FRONT, "[0-9]+");
            Assert.assertEquals("foo23bar", vn.normalize("foo123bar"));
        }
        {
            // rear, too long
            ValueNormalizer vn = new ValueNormalizer(2, '0', ValueNormalizerPosition.REAR, "[0-9]+");
            Assert.assertEquals("foo12bar", vn.normalize("foo123bar"));
        }
        {
            // with a specified capture group
            ValueNormalizer vn = new ValueNormalizer(5, '0', ValueNormalizerPosition.FRONT, "1656\\/[0-9]\\/[0-9]\\/([0-9]+).*$");
            Assert.assertEquals("1656/1/2/00123a", vn.normalize("1656/1/2/123a"));
        }
    }
}