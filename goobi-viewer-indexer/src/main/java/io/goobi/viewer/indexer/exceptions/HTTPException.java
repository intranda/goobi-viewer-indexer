/**
 * This file is part of the Goobi viewer - a content presentation and management application for digitized objects.
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
package io.goobi.viewer.indexer.exceptions;

import java.io.Serializable;

/**
 * <p>
 * HTTPException class.
 * </p>
 */
public class HTTPException extends Exception implements Serializable {

    private static final long serialVersionUID = -5840484445206784670L;

    private final int code;

    /**
     * <p>
     * Constructor for HTTPException.
     * </p>
     *
     * @param string {@link java.lang.String}
     * @param code a int.
     */
    public HTTPException(int code, String string) {
        super(string);
        this.code = code;
    }

    /**
     * <p>
     * Getter for the field <code>code</code>.
     * </p>
     *
     * @return the code
     */
    public int getCode() {
        return code;
    }
}
