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
package io.goobi.viewer.indexer.model;

/**
 * <p>
 * SolrConstants class.
 * </p>
 *
 */
public final class SolrConstants {

    public enum DocType {
        DOCSTRCT,
        PAGE,
        METADATA,
        EVENT,
        UGC,
        GROUP;
    }

    public enum MetadataGroupType {
        PERSON,
        CORPORATION,
        CONFERENCE,
        LOCATION,
        SUBJECT,
        ORIGININFO,
        RECORD,
        SHAPE,
        CITATION,
        OTHER;

        public static MetadataGroupType getByName(String name) {
            if (name != null) {
                switch (name) {
                    case "PERSON":
                        return PERSON;
                    case "CORPORATION":
                        return CORPORATION;
                    case "CONFERENCE":
                        return CONFERENCE;
                    case "LOCATION":
                        return LOCATION;
                    case "SUBJECT":
                        return SUBJECT;
                    case "ORIGININFO":
                        return ORIGININFO;
                    case "CITATION":
                        return CITATION;
                    case "OTHER":
                        return OTHER;
                    default:
                        return null;
                }

            }

            return null;
        }
    }

    /** Constant <code>ACCESSCONDITION="ACCESSCONDITION"</code> */
    public static final String ACCESSCONDITION = "ACCESSCONDITION";
    /** Constant <code>ALTO="ALTO"</code> */
    public static final String ALTO = "ALTO";
    /** Constant <code>CENTURY="CENTURY"</code> */
    public static final String CENTURY = "CENTURY";
    /** Constant <code>CMS_TEXT_ALL="CMS_TEXT_ALL"</code> */
    public static final String CMS_TEXT_ALL = "CMS_TEXT_ALL";
    /** Constant <code>COLORSPACE="COLORSPACE"</code> */
    public static final String COLORSPACE = "COLORSPACE";
    /** Constant <code>CURRENTNOSORT="CURRENTNOSORT"</code> */
    public static final String CURRENTNOSORT = "CURRENTNOSORT";
    /** Constant <code>DATAREPOSITORY="DATAREPOSITORY"</code> */
    public static final String DATAREPOSITORY = "DATAREPOSITORY";
    /** Constant <code>DATECREATED="DATECREATED"</code> */
    public static final String DATECREATED = "DATECREATED";
    /** Constant <code>DATEDELETED="DATEDELETED"</code> */
    public static final String DATEDELETED = "DATEDELETED";
    /** Constant <code>DATEUPDATED="DATEUPDATED"</code> */
    public static final String DATEUPDATED = "DATEUPDATED";
    /** Constant <code>DC="DC"</code> */
    public static final String DC = "DC";
    /** Constant <code>DEFAULT="DEFAULT"</code> */
    public static final String DEFAULT = "DEFAULT";
    /** Constant <code>DMDID="DMDID"</code> */
    public static final String DMDID = "DMDID";
    /** Constant <code>DOCSTRCT="DOCSTRCT"</code> */
    public static final String DOCSTRCT = "DOCSTRCT";
    /** Second instance of DOCSTRCT for alternative translations. */
    public static final String DOCSTRCT_ALT = "DOCSTRCT_ALT";
    /** Constant <code>DOCSTRCT_SUB="DOCSTRCT_SUB"</code> */
    public static final String DOCSTRCT_SUB = "DOCSTRCT_SUB";
    /** DOCSTRCT of the top document. */
    public static final String DOCSTRCT_TOP = "DOCSTRCT_TOP";
    /** Constant <code>DOCTYPE="DOCTYPE"</code> */
    public static final String DOCTYPE = "DOCTYPE";
    /** Constant <code>EVENTDATE="EVENTDATE"</code> */
    public static final String EVENTDATE = "EVENTDATE";
    /** Constant <code>EVENTDATEEND="EVENTDATEEND"</code> */
    public static final String EVENTDATEEND = "EVENTDATEEND";
    /** Constant <code>EVENTDATESTART="EVENTDATESTART"</code> */
    public static final String EVENTDATESTART = "EVENTDATESTART";
    /** Constant <code>EVENTTYPE="EVENTTYPE"</code> */
    public static final String EVENTTYPE = "EVENTTYPE";
    /** Constant <code>FILEIDROOT="FILEIDROOT"</code> */
    public static final String FILEIDROOT = "FILEIDROOT";
    /** Constant <code>FILENAME="FILENAME"</code> */
    public static final String FILENAME = "FILENAME";
    /** Constant <code>FILENAME_ALTO="FILENAME_ALTO"</code> */
    public static final String FILENAME_ALTO = "FILENAME_ALTO";
    /** Constant <code>FILENAME_FULLTEXT="FILENAME_FULLTEXT"</code> */
    public static final String FILENAME_FULLTEXT = "FILENAME_FULLTEXT";
    /** Constant <code>FILENAME_TEI="FILENAME_TEI"</code> */
    public static final String FILENAME_TEI = "FILENAME_TEI";
    /** Constant <code>FULLTEXT="FULLTEXT"</code> */
    public static final String FULLTEXT = "FULLTEXT";
    /** Constant <code>FULLTEXTAVAILABLE="FULLTEXTAVAILABLE"</code> */
    public static final String FULLTEXTAVAILABLE = "FULLTEXTAVAILABLE";
    /** Constant <code>GROUPFIELD="GROUPFIELD"</code> */
    public static final String GROUPFIELD = "GROUPFIELD";
    /** Constant <code>GROUPTYPE="GROUPTYPE"</code> */
    public static final String GROUPTYPE = "GROUPTYPE";
    /** Constant <code>HEIGHT="HEIGHT"</code> */
    public static final String HEIGHT = "HEIGHT";
    /** Constant <code>IDDOC="IDDOC"</code> */
    public static final String IDDOC = "IDDOC";
    /** Constant <code>IDDOC_OWNER="IDDOC_OWNER"</code> */
    public static final String IDDOC_OWNER = "IDDOC_OWNER";
    /** Constant <code>IDDOC_PARENT="IDDOC_PARENT"</code> */
    public static final String IDDOC_PARENT = "IDDOC_PARENT";
    /** Constant <code>IDDOC_TOPSTRUCT="IDDOC_TOPSTRUCT"</code> */
    public static final String IDDOC_TOPSTRUCT = "IDDOC_TOPSTRUCT";
    /** Constant <code>IMAGEURN="IMAGEURN"</code> */
    public static final String IMAGEURN = "IMAGEURN";
    /** Contains a list of all page URNs for a record. This is required for listing deleted records and their pages. */
    public static final String IMAGEURN_OAI = "IMAGEURN_OAI";
    /** Constant <code>ISANCHOR="ISANCHOR"</code> */
    public static final String ISANCHOR = "ISANCHOR";
    /** Constant <code>ISWORK="ISWORK"</code> */
    public static final String ISWORK = "ISWORK";
    /** Constant <code>LABEL="LABEL"</code> */
    public static final String LABEL = "LABEL";
    /** Constant <code>LANGUAGE="LANGUAGE"</code> */
    public static final String LANGUAGE = "LANGUAGE";
    /** Constant <code>LOGID="LOGID"</code> */
    public static final String LOGID = "LOGID";
    /** Constant <code>METADATATYPE="METADATATYPE"</code> */
    public static final String METADATATYPE = "METADATATYPE";
    /** Constant <code>MIMETYPE="MIMETYPE"</code> */
    public static final String MIMETYPE = "MIMETYPE";
    /** Constant <code>NORMDATATERMS="NORMDATATERMS"</code> */
    public static final String NORMDATATERMS = "NORMDATATERMS";
    /** Constant <code>NUMPAGES="NUMPAGES"</code> */
    public static final String NUMPAGES = "NUMPAGES";
    /** Constant <code>NUMVOLUMES="NUMVOLUMES"</code> */
    public static final String NUMVOLUMES = "NUMVOLUMES";
    /** Constant <code>OPACURL="OPACURL"</code> */
    public static final String OPACURL = "OPACURL";
    /** Constant <code>ORDER="ORDER"</code> */
    public static final String ORDER = "ORDER";
    /** Constant <code>ORDERLABEL="ORDERLABEL"</code> */
    public static final String ORDERLABEL = "ORDERLABEL";
    /** Constant <code>ORDERLABELFIRST="ORDERLABELFIRST"</code> */
    public static final String ORDERLABELFIRST = "ORDERLABELFIRST";
    /** Constant <code>ORDERLABELLAST="ORDERLABELLAST"</code> */
    public static final String ORDERLABELLAST = "ORDERLABELLAST";
    /** Constant <code>PHYSID="PHYSID"</code> */
    public static final String PHYSID = "PHYSID";
    /** Constant <code>PI="PI"</code> */
    public static final String PI = "PI";
    /** Constant <code>PI_ANCHOR="PI_ANCHOR"</code> */
    public static final String PI_ANCHOR = "PI_ANCHOR";
    /** Constant <code>PI_PARENT="PI_PARENT"</code> */
    public static final String PI_PARENT = "PI_PARENT";
    /** Constant <code>PI_TOPSTRUCT="PI_TOPSTRUCT"</code> */
    public static final String PI_TOPSTRUCT = "PI_TOPSTRUCT";
    /** Constant <code>SOURCEDOCFORMAT="SOURCEDOCFORMAT"</code> */
    public static final String SOURCEDOCFORMAT = "SOURCEDOCFORMAT";
    /** Constant <code>SUPERDEFAULT="SUPERDEFAULT"</code> */
    public static final String SUPERDEFAULT = "SUPERDEFAULT";
    /** Constant <code>SUPERFULLTEXT="SUPERFULLTEXT"</code> */
    public static final String SUPERFULLTEXT = "SUPERFULLTEXT";
    /** Constant <code>SUPERUGCTERMS="SUPERUGCTERMS"</code> */
    public static final String SUPERUGCTERMS = "SUPERUGCTERMS";
    /** Constant <code>THUMBNAIL="THUMBNAIL"</code> */
    public static final String THUMBNAIL = "THUMBNAIL";
    /** Constant <code>THUMBNAILREPRESENT="THUMBNAILREPRESENT"</code> */
    public static final String THUMBNAILREPRESENT = "THUMBNAILREPRESENT";
    /** Constant <code>THUMBPAGENO="THUMBPAGENO"</code> */
    public static final String THUMBPAGENO = "THUMBPAGENO";
    /** Constant <code>THUMBPAGENOLABEL="THUMBPAGENOLABEL"</code> */
    public static final String THUMBPAGENOLABEL = "THUMBPAGENOLABEL";
    /** Constant <code>UGCCOORDS="UGCCOORDS"</code> */
    public static final String UGCCOORDS = "UGCCOORDS";
    /** Constant <code>UGCTERMS="UGCTERMS"</code> */
    public static final String UGCTERMS = "UGCTERMS";
    /** Constant <code>UGCTYPE="UGCTYPE"</code> */
    public static final String UGCTYPE = "UGCTYPE";
    /** Constant <code>URN="URN"</code> */
    public static final String URN = "URN";
    /** Constant <code>YEAR="YEAR"</code> */
    public static final String YEAR = "YEAR";
    /** Constant <code>YEARMONTH="YEARMONTH"</code> */
    public static final String YEARMONTH = "YEARMONTH";
    /** Constant <code>YEARMONTHDAY="YEARMONTHDAY"</code> */
    public static final String YEARMONTHDAY = "YEARMONTHDAY";
    /** Constant <code>WIDTH="WIDTH"</code> */
    public static final String WIDTH = "WIDTH";

    /** Constant <code>OPEN_ACCESS_VALUE="OPENACCESS"</code> */
    public static final String OPEN_ACCESS_VALUE = "OPENACCESS";

    /** Constant <code>CMS_TEXT_="CMS_TEXT_"</code> */
    public static final String CMS_TEXT_ = "CMS_TEXT_";
    /** Constant <code>GROUPID_="GROUPID_"</code> */
    public static final String GROUPID_ = "GROUPID_";
    /** Constant <code>GROUPORDER_="GROUPORDER_"</code> */
    public static final String GROUPORDER_ = "GROUPORDER_";
    /** Constant <code>SORT_="SORT_"</code> */
    public static final String SORT_ = "SORT_";
    /** Constant <code>SORTNUM_="SORTNUM_"</code> */
    public static final String SORTNUM_ = "SORTNUM_";
    /** Constant <code>_LANG_="_LANG_"</code> */
    public static final String _LANG_ = "_LANG_";
    /** Constant <code>_NOESCAPE="_NOESCAPE"</code> */
    public static final String _NOESCAPE = "_NOESCAPE";
    /** Constant <code>_UNTOKENIZED="_UNTOKENIZED"</code> */
    public static final String _UNTOKENIZED = "_UNTOKENIZED";
    /** Constant <code>_UGC_TYPE_PERSON="PERSON"</code> */
    public static final String _UGC_TYPE_PERSON = "PERSON";
    /** Constant <code>_UGC_TYPE_CORPORATION="CORPORATION"</code> */
    public static final String _UGC_TYPE_CORPORATION = "CORPORATION";
    /** Constant <code>_UGC_TYPE_ADDRESS="ADDRESS"</code> */
    public static final String _UGC_TYPE_ADDRESS = "ADDRESS";
    /** Constant <code>_UGC_TYPE_COMMENT="COMMENT"</code> */
    public static final String _UGC_TYPE_COMMENT = "COMMENT";
    /** Constant <code>NAMEDENTITIES="NAMEDENTITIES"</code> */
    public static final String NAMEDENTITIES = "NAMEDENTITIES";

    /** Field containing true if a page contains a double image. */
    public static final String BOOL_DOUBLE_IMAGE = "BOOL_DOUBLE_IMAGE";
    /** Constant <code>MD_ANNOTATION_ID="MD_ANNOTATION_ID"</code> */
    public static final String MD_ANNOTATION_ID = "MD_ANNOTATION_ID";

}
