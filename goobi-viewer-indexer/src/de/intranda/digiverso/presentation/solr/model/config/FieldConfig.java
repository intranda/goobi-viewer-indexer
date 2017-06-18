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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;

import de.intranda.digiverso.presentation.solr.model.NonSortConfiguration;

/**
 * Configuration object for a single metadata field configuration.
 */
public class FieldConfig {

    private String fieldname;
    private List<XPathConfig> xPathConfigurations;
    private String node; // first
    private String child; // false,all
    private String parents; // false,all,first
    private boolean addToDefault = false;
    private String constantValue;
    private String valuepostfix = "";
    private boolean oneToken = false;
    private boolean oneField = false;
    private boolean addUntokenizedVersion = true; // for the browsing menu
    private boolean lowercase = false;
    private String splittingCharacter = null;
    private boolean addSortField = false;
    private boolean normalizeYear = false;
    private int normalizeYearMinDigits = 3;
    private MultiMap groupEntityFields = new MultiValueMap();
    private Map<Object, String> replaceRules = new LinkedHashMap<>();
    private List<NonSortConfiguration> nonSortConfigurations;
    private boolean addToChildren = false;
    private boolean addToPages = false;

    /**
     * Constructor.
     * 
     * @param fieldname {@link String}
     * @should set attributes correctly
     */
    public FieldConfig(String fieldname) {
        super();
        this.fieldname = fieldname;
    }

    /**
     * @return the fieldname
     */
    public String getFieldname() {
        return fieldname;
    }

    /**
     * @param fieldname the fieldname to set
     */
    public void setFieldname(String fieldname) {
        this.fieldname = fieldname;
    }

    /**
     * @return the xPathConfigurations
     */
    public List<XPathConfig> getxPathConfigurations() {
        return xPathConfigurations;
    }

    /**
     * @param xPathConfigurations the xPathConfigurations to set
     */
    public void setxPathConfigurations(List<XPathConfig> xPathConfigurations) {
        this.xPathConfigurations = xPathConfigurations;
    }

    /**
     * @return the node
     */
    public String getNode() {
        if (node == null) {
            return "";
        }
        return node;
    }

    /**
     * @param node the node to set
     */
    public void setNode(String node) {
        this.node = node;
    }

    /**
     * 
     * @return {@link String} false,all
     */
    public String getChild() {
        if (child == null) {
            return "";
        }
        return child;
    }

    /**
     * @param child the child to set
     */
    public void setChild(String child) {
        this.child = child;
    }

    /**
     * @return the parents
     */
    public String getParents() {
        if (parents == null) {
            return "";
        }
        return parents;
    }

    /**
     * @param parents the parents to set
     */
    public void setParents(String parents) {
        this.parents = parents;
    }

    /**
     * @return the addToDefault
     */
    public boolean isAddToDefault() {
        return addToDefault;
    }

    /**
     * @param addToDefault the addToDefault to set
     */
    public void setAddToDefault(boolean addToDefault) {
        this.addToDefault = addToDefault;
    }

    /**
     * @return the constantValue
     */
    public String getConstantValue() {
        return constantValue;
    }

    /**
     * @param constantValue the constantValue to set
     */
    public void setConstantValue(String constantValue) {
        this.constantValue = constantValue;
    }

    /**
     * 
     * @param valuepostfix {@link String}
     */
    public void setValuepostfix(String valuepostfix) {
        this.valuepostfix = valuepostfix;
    }

    /**
     * 
     * @return {@link String}
     */
    public String getValuepostfix() {
        if (valuepostfix == null) {
            return "";
        }
        return valuepostfix;
    }

    /**
     * @param oneToken the oneToken to set
     */
    public void setOneToken(boolean oneToken) {
        this.oneToken = oneToken;
    }

    /**
     * @return the oneToken
     */
    public boolean isOneToken() {
        return oneToken;
    }

    public boolean isOneField() {
        return oneField;
    }

    public void setOneField(boolean oneField) {
        this.oneField = oneField;
    }

    /**
     * @return the addUntokenizedVersion
     */
    public boolean isAddUntokenizedVersion() {
        return addUntokenizedVersion;
    }

    /**
     * @param addUntokenizedVersion the addUntokenizedVersion to set
     */
    public void setAddUntokenizedVersion(boolean addUntokenizedVersion) {
        this.addUntokenizedVersion = addUntokenizedVersion;
    }

    /**
     * @return the lowercase
     */
    public boolean isLowercase() {
        return lowercase;
    }

    /**
     * @param lowercase the lowercase to set
     */
    public void setLowercase(boolean lowercase) {
        this.lowercase = lowercase;
    }

    /**
     * @return the splittingCharacter
     */
    public String getSplittingCharacter() {
        return splittingCharacter;
    }

    /**
     * @param splittingCharacter the splittingCharacter to set
     */
    public void setSplittingCharacter(String splittingCharacter) {
        this.splittingCharacter = splittingCharacter;
    }

    /**
     * @return the addSortField
     */
    public boolean isAddSortField() {
        return addSortField;
    }

    /**
     * @param addSortField the addSortField to set
     */
    public void setAddSortField(boolean addSortField) {
        this.addSortField = addSortField;
    }

    /**
     * @return the normalizeYear
     */
    public boolean isNormalizeYear() {
        return normalizeYear;
    }

    /**
     * @param normalizeYear the normalizeYear to set
     */
    public void setNormalizeYear(boolean normalizeYear) {
        this.normalizeYear = normalizeYear;
    }

    /**
     * @return the normalizeYearMinDigits
     */
    public int getNormalizeYearMinDigits() {
        return normalizeYearMinDigits;
    }

    /**
     * @param normalizeYearMinDigits the normalizeYearMinDigits to set
     */
    public void setNormalizeYearMinDigits(int normalizeYearMinDigits) {
        this.normalizeYearMinDigits = normalizeYearMinDigits;
    }

    public boolean isGroupEntity() {
        return groupEntityFields != null && !groupEntityFields.isEmpty();
    }

    /**
     * @return the groupEntityFields
     */
    public MultiMap getGroupEntityFields() {
        return groupEntityFields;
    }

    /**
     * @param groupEntityFields the groupEntityFields to set
     */
    public void setGroupEntityFields(MultiMap groupEntityFields) {
        this.groupEntityFields = groupEntityFields;
    }

    /**
     * @return the replaceRules
     */
    public Map<Object, String> getReplaceRules() {
        return replaceRules;
    }

    /**
     * @param replaceRules the replaceRules to set
     */
    public void setReplaceRules(Map<Object, String> replaceRules) {
        this.replaceRules = replaceRules;
    }

    /**
     * @return the nonSortConfigurations
     */
    public List<NonSortConfiguration> getNonSortConfigurations() {
        return nonSortConfigurations;
    }

    /**
     * @param nonSortConfigurations the nonSortConfigurations to set
     */
    public void setNonSortConfigurations(List<NonSortConfiguration> nonSortConfigurations) {
        this.nonSortConfigurations = nonSortConfigurations;
    }

    /**
     * @return the addToChildren
     */
    public boolean isAddToChildren() {
        return addToChildren;
    }

    /**
     * @param addToChildren the addToChildren to set
     */
    public void setAddToChildren(boolean addToChildren) {
        this.addToChildren = addToChildren;
    }

    /**
     * @return the addToPages
     */
    public boolean isAddToPages() {
        return addToPages;
    }

    /**
     * @param addToPages the addToPages to set
     */
    public void setAddToPages(boolean addToPages) {
        this.addToPages = addToPages;
    }
}
