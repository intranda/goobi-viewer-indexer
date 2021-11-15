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

public class SubfieldConfigTest {

    /**
     * @see SubfieldConfig#ingestXpaths(SubfieldConfig)
     * @verifies copy xpath expressions correctly
     */
    @Test
    public void ingestXpaths_shouldCopyXpathExpressionsCorrectly() throws Exception {
        SubfieldConfig config1 = new SubfieldConfig("MD_FOO", true, false);
        config1.getXpaths().add("foo:foo");

        SubfieldConfig config2 = new SubfieldConfig("MD_FOO", true, false);
        config2.getXpaths().add("foo:bar");

        Assert.assertEquals(1, config1.getXpaths().size());
        config1.ingestXpaths(config2);
        Assert.assertEquals(2, config1.getXpaths().size());
        Assert.assertEquals("foo:foo", config1.getXpaths().get(0));
        Assert.assertEquals("foo:bar", config1.getXpaths().get(1));
    }

    /**
     * @see SubfieldConfig#ingestXpaths(SubfieldConfig)
     * @verifies copy default values correctly
     */
    @Test
    public void ingestXpaths_shouldCopyDefaultValuesCorrectly() throws Exception {
        SubfieldConfig config1 = new SubfieldConfig("MD_FOO", true, false);
        config1.getDefaultValues().put("foo:foo", "foo");

        SubfieldConfig config2 = new SubfieldConfig("MD_FOO", true, false);
        config2.getDefaultValues().put("foo:bar", "bar");

        Assert.assertEquals(1, config1.getDefaultValues().size());
        config1.ingestXpaths(config2);
        Assert.assertEquals(2, config1.getDefaultValues().size());
        Assert.assertEquals("foo", config1.getDefaultValues().get("foo:foo"));
        Assert.assertEquals("bar", config1.getDefaultValues().get("foo:bar"));
    }
}