package io.goobi.viewer.indexer.helper.language;

import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.ReloadingFileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.reloading.PeriodicReloadingTrigger;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * <p>
 * LanguageHelper class.
 * </p>
 *
 */
public class LanguageHelper {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(LanguageHelper.class);

    ReloadingFileBasedConfigurationBuilder<XMLConfiguration> builder;

    private static LanguageHelper helper;

    /**
     * <p>
     * getInstance.
     * </p>
     *
     * @return a {@link io.goobi.viewer.indexer.helper.language.LanguageHelper} object.
     */
    public static LanguageHelper getInstance() {
        if (helper == null) {
            helper = new LanguageHelper();
            try {
                helper.builder =
                        new ReloadingFileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
                                .configure(new Parameters().properties()
                                        .setFileName("languages.xml")
                                        .setListDelimiterHandler(new DefaultListDelimiterHandler('&')) // TODO Why '&'?
                                        .setThrowExceptionOnMissing(false));
                helper.builder.getConfiguration().setExpressionEngine(new XPathExpressionEngine());
                PeriodicReloadingTrigger trigger = new PeriodicReloadingTrigger(helper.builder.getReloadingController(),
                        null, 10, TimeUnit.SECONDS);
                trigger.start();
            } catch (ConfigurationException e) {
                logger.error(e.getMessage());
            }
        }

        return helper;
    }

    private XMLConfiguration getConfig() {
        try {
            return builder.getConfiguration();
        } catch (ConfigurationException e) {
            logger.error(e.getMessage());
            return new XMLConfiguration();
        }
    }

    /**
     * <p>
     * getLanguage.
     * </p>
     *
     * @param isoCode a {@link java.lang.String} object.
     * @return a {@link io.goobi.viewer.indexer.helper.language.Language} object.
     */
    public Language getLanguage(String isoCode) {
        HierarchicalConfiguration<ImmutableNode> languageConfig = null;
        try {
            if (isoCode.length() == 3) {
                languageConfig = getConfig().configurationsAt("language[iso_639-2=\"" + isoCode + "\"]").get(0);
            } else if (isoCode.length() == 2) {
                languageConfig = getConfig().configurationsAt("language[iso_639-1=\"" + isoCode + "\"]").get(0);
            }
        } catch (Exception e) {
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
