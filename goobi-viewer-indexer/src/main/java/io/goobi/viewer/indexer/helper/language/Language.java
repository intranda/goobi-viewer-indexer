package io.goobi.viewer.indexer.helper.language;


/**
 * <p>Language class.</p>
 *
 */
public class Language {

    private String isoCode;
    private String isoCodeOld;
    private String englishName;
    private String frenchName;
    private String germanName;
    /**
     * <p>Getter for the field <code>isoCode</code>.</p>
     *
     * @return the language code according to iso 639-2/B
     */
    public String getIsoCode() {
        return isoCode;
    }
    /**
     * <p>Setter for the field <code>isoCode</code>.</p>
     *
     * @param isoCode a {@link java.lang.String} object.
     */
    public void setIsoCode(String isoCode) {
        this.isoCode = isoCode;
    }
    /**
     * <p>Getter for the field <code>isoCodeOld</code>.</p>
     *
     * @return the language code according to iso 639-1
     */
    public String getIsoCodeOld() {
        return isoCodeOld;
    }
    /**
     * <p>Setter for the field <code>isoCodeOld</code>.</p>
     *
     * @param isoCodeOld a {@link java.lang.String} object.
     */
    public void setIsoCodeOld(String isoCodeOld) {
        this.isoCodeOld = isoCodeOld;
    }
    /**
     * <p>Getter for the field <code>englishName</code>.</p>
     *
     * @return the englishName
     */
    public String getEnglishName() {
        return englishName;
    }
    /**
     * <p>Setter for the field <code>englishName</code>.</p>
     *
     * @param englishName the englishName to set
     */
    public void setEnglishName(String englishName) {
        this.englishName = englishName;
    }
    /**
     * <p>Getter for the field <code>frenchName</code>.</p>
     *
     * @return the frenchName
     */
    public String getFrenchName() {
        return frenchName;
    }
    /**
     * <p>Setter for the field <code>frenchName</code>.</p>
     *
     * @param frenchName the frenchName to set
     */
    public void setFrenchName(String frenchName) {
        this.frenchName = frenchName;
    }
    /**
     * <p>Getter for the field <code>germanName</code>.</p>
     *
     * @return the germanName
     */
    public String getGermanName() {
        return germanName;
    }
    /**
     * <p>Setter for the field <code>germanName</code>.</p>
     *
     * @param germanName the germanName to set
     */
    public void setGermanName(String germanName) {
        this.germanName = germanName;
    }
    
    
}
