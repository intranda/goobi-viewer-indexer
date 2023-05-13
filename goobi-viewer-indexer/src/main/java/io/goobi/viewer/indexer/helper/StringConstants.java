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
package io.goobi.viewer.indexer.helper;

public class StringConstants {

    public static final String ERROR_CONFIG = "Configuration error, see log for details.";
    public static final String ERROR_DATAFOLDERS_MAY_NOT_BE_NULL = "dataFolders may not be null";
    public static final String ERROR_DOC_MAY_NOT_BE_NULL = "doc may not be null";
    public static final String ERROR_PI_MAY_NOT_BE_NULL = "pi may not be null";

    public static final String LOG_COULD_NOT_BE_DELETED = "'{}' could not be deleted.";

    private StringConstants() {
        //
    }
}
