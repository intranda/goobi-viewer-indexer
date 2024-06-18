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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;

/**
 * Configuration object for a single metadata field configuration.
 */
public class FieldConfig {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(FieldConfig.class);

    static final String DEFAULT_MULTIVALUE_SEPARATOR = " ; ";

    private String fieldname;
    private List<XPathConfig> xPathConfigurations = new ArrayList<>();
    private String node; // first
    private String child; // false,all
    private String parents; // false,all,first
    private boolean addToDefault = false;
    private String constantValue;
    private String valuepostfix = "";
    private boolean oneToken = false;
    private boolean oneField = false;
    private String oneFieldSeparator = DEFAULT_MULTIVALUE_SEPARATOR;
    private boolean addUntokenizedVersion = true; // for the browsing menu
    private boolean lowercase = false;
    private String splittingCharacter = null;
    private boolean addSortField = false;
    private boolean addSortFieldToTopstruct = false;
    private boolean addExistenceBoolean = false;
    private boolean normalizeYear = false;
    private boolean interpolateYears = false;
    private int normalizeYearMinDigits = 3;
    private String normalizeYearField;
    private GroupEntity groupEntity = null;
    private Map<Object, String> replaceRules = new LinkedHashMap<>();
    private List<NonSortConfiguration> nonSortConfigurations;
    private boolean addToPages = false;
    private boolean allowDuplicateValues = false;
    private final List<ValueNormalizer> valueNormalizers = new ArrayList<>();
    private String geoJSONSource;
    private String geoJSONSourceSeparator;
    private boolean geoJSONAddSearchField;
    private final Set<FileFormat> supportedFormats = new HashSet<>();

    /**
     * Constructor.
     *
     * @param fieldname {@link java.lang.String}
     * @should set attributes correctly
     */
    public FieldConfig(String fieldname) {
        super();
        this.fieldname = fieldname;
    }

    /**
     * 
     * @param xpath XPath exression to check
     * @should add FileFormats correctly
     */
    public void checkXpathSupportedFormats(String xpath) {
        if (xpath.contains("mets:xmlData") || xpath.startsWith("@OBJID")) {
            supportedFormats.add(FileFormat.METS);
            supportedFormats.add(FileFormat.METS_MARC);
            logger.debug("Field {} has METS/MODS and METS/MARC config.", fieldname);
        } else if (xpath.contains("ead:") || xpath.equals("@otherlevel")) {
            supportedFormats.add(FileFormat.EAD);
            logger.debug("Field {} has EAD config.", fieldname);
        } else if (xpath.contains("lido:")) {
            supportedFormats.add(FileFormat.LIDO);
            logger.debug("Field {} has LIDO config.", fieldname);
        } else if (xpath.contains("dc:")) {
            supportedFormats.add(FileFormat.DUBLINCORE);
            logger.debug("Field {} has Dublin Core config.", fieldname);
        } else if (xpath.contains("denkxweb:")) {
            supportedFormats.add(FileFormat.DENKXWEB);
            logger.debug("Field {} has DenkXWeb config.", fieldname);
        }
    }

    /**
     * <p>
     * Getter for the field <code>fieldname</code>.
     * </p>
     *
     * @return the fieldname
     */
    public String getFieldname() {
        return fieldname;
    }

    /**
     * <p>
     * Setter for the field <code>fieldname</code>.
     * </p>
     *
     * @param fieldname the fieldname to set
     */
    public void setFieldname(String fieldname) {
        this.fieldname = fieldname;
    }

    /**
     * <p>
     * Getter for the field <code>xPathConfigurations</code>.
     * </p>
     *
     * @return the xPathConfigurations
     */
    public List<XPathConfig> getxPathConfigurations() {
        return xPathConfigurations;
    }

    /**
     * <p>
     * Setter for the field <code>xPathConfigurations</code>.
     * </p>
     *
     * @param xPathConfigurations the xPathConfigurations to set
     */
    public void setxPathConfigurations(List<XPathConfig> xPathConfigurations) {
        this.xPathConfigurations = xPathConfigurations;
    }

    /**
     * <p>
     * Getter for the field <code>node</code>.
     * </p>
     *
     * @return the node
     */
    public String getNode() {
        if (node == null) {
            return "";
        }
        return node;
    }

    /**
     * <p>
     * Setter for the field <code>node</code>.
     * </p>
     *
     * @param node the node to set
     */
    public void setNode(String node) {
        this.node = node;
    }

    /**
     * <p>
     * Getter for the field <code>child</code>.
     * </p>
     *
     * @return {@link java.lang.String} false,all
     */
    public String getChild() {
        if (child == null) {
            return "";
        }
        return child;
    }

    /**
     * <p>
     * Setter for the field <code>child</code>.
     * </p>
     *
     * @param child the child to set
     */
    public void setChild(String child) {
        this.child = child;
    }

    /**
     * <p>
     * Getter for the field <code>parents</code>.
     * </p>
     *
     * @return the parents
     */
    public String getParents() {
        if (parents == null) {
            return "";
        }
        return parents;
    }

    /**
     * <p>
     * Setter for the field <code>parents</code>.
     * </p>
     *
     * @param parents the parents to set
     */
    public void setParents(String parents) {
        this.parents = parents;
    }

    /**
     * <p>
     * isAddToDefault.
     * </p>
     *
     * @return the addToDefault
     */
    public boolean isAddToDefault() {
        return addToDefault;
    }

    /**
     * <p>
     * Setter for the field <code>addToDefault</code>.
     * </p>
     *
     * @param addToDefault the addToDefault to set
     */
    public void setAddToDefault(boolean addToDefault) {
        this.addToDefault = addToDefault;
    }

    /**
     * <p>
     * Getter for the field <code>constantValue</code>.
     * </p>
     *
     * @return the constantValue
     */
    public String getConstantValue() {
        return constantValue;
    }

    /**
     * <p>
     * Setter for the field <code>constantValue</code>.
     * </p>
     *
     * @param constantValue the constantValue to set
     */
    public void setConstantValue(String constantValue) {
        this.constantValue = constantValue;
    }

    /**
     * <p>
     * Setter for the field <code>valuepostfix</code>.
     * </p>
     *
     * @param valuepostfix {@link java.lang.String}
     */
    public void setValuepostfix(String valuepostfix) {
        this.valuepostfix = valuepostfix;
    }

    /**
     * <p>
     * Getter for the field <code>valuepostfix</code>.
     * </p>
     *
     * @return {@link java.lang.String}
     */
    public String getValuepostfix() {
        if (valuepostfix == null) {
            return "";
        }
        return valuepostfix;
    }

    /**
     * <p>
     * Setter for the field <code>oneToken</code>.
     * </p>
     *
     * @param oneToken the oneToken to set
     */
    public void setOneToken(boolean oneToken) {
        this.oneToken = oneToken;
    }

    /**
     * <p>
     * isOneToken.
     * </p>
     *
     * @return the oneToken
     */
    public boolean isOneToken() {
        return oneToken;
    }

    /**
     * <p>
     * isOneField.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isOneField() {
        return oneField;
    }

    /**
     * <p>
     * Setter for the field <code>oneField</code>.
     * </p>
     *
     * @param oneField a boolean.
     */
    public void setOneField(boolean oneField) {
        this.oneField = oneField;
    }

    /**
     * @return the oneFieldSeparator
     */
    public String getOneFieldSeparator() {
        return oneFieldSeparator;
    }

    /**
     * @param oneFieldSeparator the oneFieldSeparator to set
     */
    public void setOneFieldSeparator(String oneFieldSeparator) {
        this.oneFieldSeparator = oneFieldSeparator;
    }

    /**
     * <p>
     * isAddUntokenizedVersion.
     * </p>
     *
     * @return the addUntokenizedVersion
     */
    public boolean isAddUntokenizedVersion() {
        return addUntokenizedVersion;
    }

    /**
     * <p>
     * Setter for the field <code>addUntokenizedVersion</code>.
     * </p>
     *
     * @param addUntokenizedVersion the addUntokenizedVersion to set
     */
    public void setAddUntokenizedVersion(boolean addUntokenizedVersion) {
        this.addUntokenizedVersion = addUntokenizedVersion;
    }

    /**
     * <p>
     * isLowercase.
     * </p>
     *
     * @return the lowercase
     */
    public boolean isLowercase() {
        return lowercase;
    }

    /**
     * <p>
     * Setter for the field <code>lowercase</code>.
     * </p>
     *
     * @param lowercase the lowercase to set
     */
    public void setLowercase(boolean lowercase) {
        this.lowercase = lowercase;
    }

    /**
     * <p>
     * Getter for the field <code>splittingCharacter</code>.
     * </p>
     *
     * @return the splittingCharacter
     */
    public String getSplittingCharacter() {
        return splittingCharacter;
    }

    /**
     * <p>
     * Setter for the field <code>splittingCharacter</code>.
     * </p>
     *
     * @param splittingCharacter the splittingCharacter to set
     */
    public void setSplittingCharacter(String splittingCharacter) {
        this.splittingCharacter = splittingCharacter;
    }

    /**
     * <p>
     * isAddSortField.
     * </p>
     *
     * @return the addSortField
     */
    public boolean isAddSortField() {
        return addSortField;
    }

    /**
     * <p>
     * Setter for the field <code>addSortField</code>.
     * </p>
     *
     * @param addSortField the addSortField to set
     */
    public void setAddSortField(boolean addSortField) {
        this.addSortField = addSortField;
    }

    /**
     * <p>
     * isAddSortFieldToTopstruct.
     * </p>
     *
     * @return the addSortFieldToTopstruct
     */
    public boolean isAddSortFieldToTopstruct() {
        return addSortFieldToTopstruct;
    }

    /**
     * <p>
     * Setter for the field <code>addSortFieldToTopstruct</code>.
     * </p>
     *
     * @param addSortFieldToTopstruct the addSortFieldToTopstruct to set
     */
    public void setAddSortFieldToTopstruct(boolean addSortFieldToTopstruct) {
        this.addSortFieldToTopstruct = addSortFieldToTopstruct;
    }

    /**
     * @return the addExistenceBoolean
     */
    public boolean isAddExistenceBoolean() {
        return addExistenceBoolean;
    }

    /**
     * @param addExistenceBoolean the addExistenceBoolean to set
     */
    public void setAddExistenceBoolean(boolean addExistenceBoolean) {
        this.addExistenceBoolean = addExistenceBoolean;
    }

    /**
     * <p>
     * isNormalizeYear.
     * </p>
     *
     * @return the normalizeYear
     */
    public boolean isNormalizeYear() {
        return normalizeYear;
    }

    /**
     * <p>
     * Setter for the field <code>normalizeYear</code>.
     * </p>
     *
     * @param normalizeYear the normalizeYear to set
     */
    public void setNormalizeYear(boolean normalizeYear) {
        this.normalizeYear = normalizeYear;
    }

    /**
     * <p>
     * Getter for the field <code>normalizeYearMinDigits</code>.
     * </p>
     *
     * @return the normalizeYearMinDigits
     */
    public int getNormalizeYearMinDigits() {
        return normalizeYearMinDigits;
    }

    /**
     * <p>
     * isInterpolateYears.
     * </p>
     *
     * @return the interpolateYears
     */
    public boolean isInterpolateYears() {
        return interpolateYears;
    }

    /**
     * <p>
     * Setter for the field <code>interpolateYears</code>.
     * </p>
     *
     * @param interpolateYears the interpolateYears to set
     */
    public void setInterpolateYears(boolean interpolateYears) {
        this.interpolateYears = interpolateYears;
    }

    /**
     * <p>
     * Setter for the field <code>normalizeYearMinDigits</code>.
     * </p>
     *
     * @param normalizeYearMinDigits the normalizeYearMinDigits to set
     */
    public void setNormalizeYearMinDigits(int normalizeYearMinDigits) {
        this.normalizeYearMinDigits = normalizeYearMinDigits;
    }

    /**
     * @return the normalizeYearField
     */
    public String getNormalizeYearField() {
        return normalizeYearField;
    }

    /**
     * @param normalizeYearField the normalizeYearField to set
     */
    public void setNormalizeYearField(String normalizeYearField) {
        this.normalizeYearField = normalizeYearField;
    }

    /**
     * <p>
     * isGroupEntity.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isGroupEntity() {
        return groupEntity != null;
    }

    /**
     * @return the groupEntity
     */
    public GroupEntity getGroupEntity() {
        return groupEntity;
    }

    /**
     * @param groupEntity the groupEntity to set
     */
    public void setGroupEntity(GroupEntity groupEntity) {
        this.groupEntity = groupEntity;
    }

    /**
     * <p>
     * Getter for the field <code>replaceRules</code>.
     * </p>
     *
     * @return the replaceRules
     */
    public Map<Object, String> getReplaceRules() {
        return replaceRules;
    }

    /**
     * <p>
     * Setter for the field <code>replaceRules</code>.
     * </p>
     *
     * @param replaceRules the replaceRules to set
     */
    public void setReplaceRules(Map<Object, String> replaceRules) {
        this.replaceRules = replaceRules;
    }

    /**
     * <p>
     * Getter for the field <code>nonSortConfigurations</code>.
     * </p>
     *
     * @return the nonSortConfigurations
     */
    public List<NonSortConfiguration> getNonSortConfigurations() {
        return nonSortConfigurations;
    }

    /**
     * <p>
     * Setter for the field <code>nonSortConfigurations</code>.
     * </p>
     *
     * @param nonSortConfigurations the nonSortConfigurations to set
     */
    public void setNonSortConfigurations(List<NonSortConfiguration> nonSortConfigurations) {
        this.nonSortConfigurations = nonSortConfigurations;
    }

    /**
     * <p>
     * isAddToPages.
     * </p>
     *
     * @return the addToPages
     */
    public boolean isAddToPages() {
        return addToPages;
    }

    /**
     * <p>
     * Setter for the field <code>addToPages</code>.
     * </p>
     *
     * @param addToPages the addToPages to set
     */
    public void setAddToPages(boolean addToPages) {
        this.addToPages = addToPages;
    }

    /**
     * @return the allowDuplicateValues
     */
    public boolean isAllowDuplicateValues() {
        return allowDuplicateValues;
    }

    /**
     * @param allowDuplicateValues the allowDuplicateValues to set
     */
    public void setAllowDuplicateValues(boolean allowDuplicateValues) {
        this.allowDuplicateValues = allowDuplicateValues;
    }

    /**
     * @return the valueNormalizers
     */
    public List<ValueNormalizer> getValueNormalizers() {
        return valueNormalizers;
    }

    /**
     * @return the geoJSONSource
     */
    public String getGeoJSONSource() {
        return geoJSONSource;
    }

    /**
     * @param geoJSONSource the geoJSONSource to set
     */
    public void setGeoJSONSource(String geoJSONSource) {
        this.geoJSONSource = geoJSONSource;
    }

    /**
     * @return the geoJSONSourceSeparator
     */
    public String getGeoJSONSourceSeparator() {
        return geoJSONSourceSeparator;
    }

    /**
     * @param geoJSONSourceSeparator the geoJSONSourceSeparator to set
     */
    public void setGeoJSONSourceSeparator(String geoJSONSourceSeparator) {
        this.geoJSONSourceSeparator = geoJSONSourceSeparator;
    }

    /**
     * @return the geoJSONAddSearchField
     */
    public boolean isGeoJSONAddSearchField() {
        return geoJSONAddSearchField;
    }

    /**
     * @param geoJSONAddSearchField the geoJSONAddSearchField to set
     */
    public void setGeoJSONAddSearchField(boolean geoJSONAddSearchField) {
        this.geoJSONAddSearchField = geoJSONAddSearchField;
    }

    /**
     * @return the supportedFormats
     */
    public Set<FileFormat> getSupportedFormats() {
        return supportedFormats;
    }
}
