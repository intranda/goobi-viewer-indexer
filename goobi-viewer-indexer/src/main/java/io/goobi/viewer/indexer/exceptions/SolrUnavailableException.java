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
package io.goobi.viewer.indexer.exceptions;

/**
 * Thrown if Solr could not be reached (or kept answering with a transient error) for the entire retry period. The affected record has not failed
 * and must be indexed again once Solr is available.
 * <p>
 * Unchecked on purpose: it must pass the indexers' catch blocks for record failures (which also catch {@link FatalIndexerException}) up to the
 * record task, which keeps the record file in the hotfolder. It must not extend <code>SolrException</code>, which the indexers treat as a
 * permanent record failure.
 */
public class SolrUnavailableException extends RuntimeException {

    private static final long serialVersionUID = -2730487717404283916L;

    /**
     * @param message Error message
     */
    public SolrUnavailableException(String message) {
        super(message);
    }
}
