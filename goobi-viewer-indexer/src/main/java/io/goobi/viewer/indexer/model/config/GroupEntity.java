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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.goobi.viewer.indexer.model.SolrConstants.MetadataGroupType;

/**
 * Configuration element <groupEntity>.
 */
public class GroupEntity {

    private MetadataGroupType type;
    private boolean addAuthorityDataToDocstruct = false;
    private boolean addCoordsToDocstruct = false;
    private String url;
    private String xpath;
    private final Map<String, SubfieldConfig> subfields = new HashMap<>();
    private final List<GroupEntity> children = new ArrayList<>();

    /**
     * @return the type
     */
    public MetadataGroupType getType() {
        return type;
    }

    /**
     * @param type the type to set
     * @return this
     */
    public GroupEntity setType(MetadataGroupType type) {
        this.type = type;
        return this;
    }

    /**
     * @return the addAuthorityDataToDocstruct
     */
    public boolean isAddAuthorityDataToDocstruct() {
        return addAuthorityDataToDocstruct;
    }

    /**
     * @param addAuthorityDataToDocstruct the addAuthorityDataToDocstruct to set
     * @return this
     */
    public GroupEntity setAddAuthorityDataToDocstruct(boolean addAuthorityDataToDocstruct) {
        this.addAuthorityDataToDocstruct = addAuthorityDataToDocstruct;
        return this;
    }

    /**
     * @return the addCoordsToDocstruct
     */
    public boolean isAddCoordsToDocstruct() {
        return addCoordsToDocstruct;
    }

    /**
     * @param addCoordsToDocstruct the addCoordsToDocstruct to set
     * @return this
     */
    public GroupEntity setAddCoordsToDocstruct(boolean addCoordsToDocstruct) {
        this.addCoordsToDocstruct = addCoordsToDocstruct;
        return this;
    }

    /**
     * @return the url
     */
    public String getUrl() {
        return url;
    }

    /**
     * @param url the url to set
     * @return this
     */
    public GroupEntity setUrl(String url) {
        this.url = url;
        return this;
    }

    /**
     * @return the xpath
     */
    public String getXpath() {
        return xpath;
    }

    /**
     * @param xpath the xpath to set
     * @return this
     */
    public GroupEntity setXpath(String xpath) {
        this.xpath = xpath;
        return this;
    }

    /**
     * @return the subfields
     */
    public Map<String, SubfieldConfig> getSubfields() {
        return subfields;
    }

    /**
     * @return the children
     */
    public List<GroupEntity> getChildren() {
        return children;
    }
}
