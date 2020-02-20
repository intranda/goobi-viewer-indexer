package io.goobi.viewer.indexer.helper.language;

import java.io.File;
import java.util.Locale;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.WorldViewsIndexer;

/**
 * <p>LanguageHelper class.</p>
 *
 */
public class LanguageHelper {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(LanguageHelper.class);

    private static XMLConfiguration config;

    private static LanguageHelper helper;

    private Locale locale;

    /**
     * <p>getInstance.</p>
     *
     * @return a {@link io.goobi.viewer.indexer.helper.language.LanguageHelper} object.
     */
    public static LanguageHelper getInstance() {
        if (helper == null) {
            helper = new LanguageHelper();
            try {
                config = new XMLConfiguration("languages.xml");
            } catch (ConfigurationException e) {
                config = new XMLConfiguration();
            }
            config.setListDelimiter('&');
            config.setReloadingStrategy(new FileChangedReloadingStrategy());
            config.setExpressionEngine(new XPathExpressionEngine());
        }

        return helper;
    }

    /**
     * <p>getLanguage.</p>
     *
     * @param isoCode a {@link java.lang.String} object.
     * @return a {@link io.goobi.viewer.indexer.helper.language.Language} object.
     */
    public Language getLanguage(String isoCode) {
        SubnodeConfiguration languageConfig = null;
        try {
            if (isoCode.length() == 3) {
                languageConfig = (SubnodeConfiguration) config.configurationsAt("language[iso_639-2=\"" + isoCode + "\"]").get(0);
            } else if (isoCode.length() == 2) {
                languageConfig = (SubnodeConfiguration) config.configurationsAt("language[iso_639-1=\"" + isoCode + "\"]").get(0);
            }
        } catch (Throwable e) {
            throw new IllegalArgumentException("No matching language found for " + isoCode, e);
        }
        if (languageConfig == null) {
            throw new IllegalArgumentException("No matching language found for " + isoCode);
        }
        Language language = new Language();
        language.setIsoCode(languageConfig.getString("iso_639-2"));
        language.setIsoCodeOld(languageConfig.getString("iso_639-1"));
        language.setEnglishName(languageConfig.getString("eng"));
        language.setGermanName(languageConfig.getString("ger"));
        language.setFrenchName(languageConfig.getString("fre"));

        return language;
    }

}
