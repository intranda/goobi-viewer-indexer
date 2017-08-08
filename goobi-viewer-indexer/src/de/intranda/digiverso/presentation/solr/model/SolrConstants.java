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
package de.intranda.digiverso.presentation.solr.model;

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
        LOCATION,
        SUBJECT,
        ORIGININFO,
        OTHER;
    }

    public static final String ACCESSCONDITION = "ACCESSCONDITION";
    @Deprecated
    public static final String ALTO = "ALTO";
    public static final String CENTURY = "CENTURY";
    public static final String COLORSPACE = "COLORSPACE";
    public static final String CURRENTNOSORT = "CURRENTNOSORT";
    public static final String DATAREPOSITORY = "DATAREPOSITORY";
    public static final String DATECREATED = "DATECREATED";
    public static final String DATEDELETED = "DATEDELETED";
    public static final String DATEUPDATED = "DATEUPDATED";
    public static final String DC = "DC";
    public static final String DEFAULT = "DEFAULT";
    public static final String DMDID = "DMDID";
    public static final String DOCSTRCT = "DOCSTRCT";
    public static final String DOCTYPE = "DOCTYPE";
    public static final String EVENTDATE = "EVENTDATE";
    public static final String EVENTDATEEND = "EVENTDATEEND";
    public static final String EVENTDATESTART = "EVENTDATESTART";
    public static final String EVENTTYPE = "EVENTTYPE";
    public static final String FILEIDROOT = "FILEIDROOT";
    public static final String FILENAME = "FILENAME";
    public static final String FILENAME_ALTO = "FILENAME_ALTO";
    public static final String FILENAME_FULLTEXT = "FILENAME_FULLTEXT";
    public static final String FILENAME_TEI = "FILENAME_TEI";
    public static final String FULLTEXT = "FULLTEXT";
    public static final String FULLTEXTAVAILABLE = "FULLTEXTAVAILABLE";
    public static final String GROUPFIELD = "GROUPFIELD";
    public static final String GROUPTYPE = "GROUPTYPE";
    public static final String HEIGHT = "HEIGHT";
    public static final String IDDOC = "IDDOC";
    public static final String IDDOC_OWNER = "IDDOC_OWNER";
    public static final String IDDOC_PARENT = "IDDOC_PARENT";
    public static final String IDDOC_TOPSTRUCT = "IDDOC_TOPSTRUCT";
    public static final String IMAGEURN = "IMAGEURN";
    /** Contains a list of all page URNs for a record. This is required for listing deleted records and their pages. */
    public static final String IMAGEURN_OAI = "IMAGEURN_OAI";
    public static final String ISANCHOR = "ISANCHOR";
    public static final String ISWORK = "ISWORK";
    public static final String LANGUAGE = "LANGUAGE";
    public static final String LOGID = "LOGID";
    public static final String METADATATYPE = "METADATATYPE";
    public static final String MIMETYPE = "MIMETYPE";
    public static final String NORMDATATERMS = "NORMDATATERMS";
    public static final String NUMPAGES = "NUMPAGES";
    public static final String NUMVOLUMES = "NUMVOLUMES";
    public static final String OPACURL = "OPACURL";
    public static final String ORDER = "ORDER";
    public static final String ORDERLABEL = "ORDERLABEL";
    public static final String ORDERLABELFIRST = "ORDERLABELFIRST";
    public static final String ORDERLABELLAST = "ORDERLABELLAST";
    public static final String OVERVIEWPAGE_DESCRIPTION = "OVERVIEWPAGE_DESCRIPTION";
    public static final String OVERVIEWPAGE_PUBLICATIONTEXT = "OVERVIEWPAGE_PUBLICATIONTEXT";
    public static final String PARTNERID = "PARTNERID";
    public static final String PHYSID = "PHYSID";
    public static final String PI = "PI";
    public static final String PI_ANCHOR = "PI_ANCHOR";
    public static final String PI_PARENT = "PI_PARENT";
    public static final String PI_TOPSTRUCT = "PI_TOPSTRUCT";
    public static final String SOURCEDOCFORMAT = "SOURCEDOCFORMAT";
    public static final String SUPERDEFAULT = "SUPERDEFAULT";
    public static final String SUPERFULLTEXT = "SUPERFULLTEXT";
    public static final String THUMBNAIL = "THUMBNAIL";
    public static final String THUMBNAILREPRESENT = "THUMBNAILREPRESENT";
    public static final String THUMBPAGENO = "THUMBPAGENO";
    public static final String THUMBPAGENOLABEL = "THUMBPAGENOLABEL";
    public static final String UGCCOORDS = "UGCCOORDS";
    public static final String UGCTERMS = "UGCTERMS";
    public static final String UGCTYPE = "UGCTYPE";
    public static final String URN = "URN";
    public static final String YEAR = "YEAR";
    public static final String YEARMONTH = "YEARMONTH";
    public static final String YEARMONTHDAY = "YEARMONTHDAY";
    public static final String WIDTH = "WIDTH";

    public static final String LABEL = "LABEL";
    public static final String TYPO3_TYPE = "TYPE";

    public static final String OPEN_ACCESS_VALUE = "OPENACCESS";

    public static final String GROUPID_ = "GROUPID_";
    public static final String GROUPORDER_ = "GROUPORDER_";
    public static final String SORT_ = "SORT_";
    public static final String SORTNUM_ = "SORTNUM_";
    // public static final String VIDEO_ = "VIDEO_";
    public static final String _LANG_ = "_LANG_";
    public static final String _NOESCAPE = "_NOESCAPE";
    public static final String _UNTOKENIZED = "_UNTOKENIZED";
    public static final String _METS = "METS";
    public static final String _LIDO = "LIDO";
    public static final String _UGC_TYPE_PERSON = "PERSON";
    public static final String _UGC_TYPE_CORPORATION = "CORPORATION";
    public static final String _UGC_TYPE_ADDRESS = "ADDRESS";
    public static final String _UGC_TYPE_COMMENT = "COMMENT";
    public static final String _WORLDVIEWS = "WORLDVIEWS";
    public static final String NAMEDENTITIES = "NAMEDENTITIES";
}
