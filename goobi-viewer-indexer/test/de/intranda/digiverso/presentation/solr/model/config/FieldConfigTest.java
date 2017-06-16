/**
 * This file is part of the Goobi Solr Indexer - a content indexing tool for the Goobi Viewer and OAI-PMH/SRU interfaces.
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

import de.intranda.digiverso.presentation.solr.model.config.FieldConfig;

public class FieldConfigTest {
    
    /**
     * @see FieldConfig#ConfigurationItem(String)
     * @verifies set attributes correctly
     */
    @Test
    public void FieldConfig_shouldSetAttributesCorrectly() throws Exception {
        FieldConfig ci = new FieldConfig("field_name");
        Assert.assertEquals("field_name", ci.getFieldname());
    }
}