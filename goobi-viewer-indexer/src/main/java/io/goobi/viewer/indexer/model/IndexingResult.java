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

import java.util.HashSet;
import java.util.Set;

public class IndexingResult {

    public enum IndexingResultStatus {
        OK,
        ERROR;
    }

    private IndexingResultStatus status = IndexingResultStatus.OK;
    private String pi;
    private String recordFileName;
    private String error;
    private boolean submitPiToViewer = false;
    private Set<String> mediaFileNames = new HashSet<>();

    /**
     * @return the status
     */
    public IndexingResultStatus getStatus() {
        return status;
    }

    /**
     * @param status the status to set
     * @return this
     */
    public IndexingResult setStatus(IndexingResultStatus status) {
        this.status = status;
        return this;
    }

    /**
     * @return the pi
     */
    public String getPi() {
        return pi;
    }

    /**
     * @param pi the pi to set
     * @return this
     */
    public IndexingResult setPi(String pi) {
        this.pi = pi;
        return this;
    }

    /**
     * @return the recordFileName
     */
    public String getRecordFileName() {
        return recordFileName;
    }

    /**
     * @param recordFileName the recordFileName to set
     * @return this
     */
    public IndexingResult setRecordFileName(String recordFileName) {
        this.recordFileName = recordFileName;
        return this;
    }

    /**
     * @return the error
     */
    public String getError() {
        return error;
    }

    /**
     * @param error the error to set
     * @return this
     */
    public IndexingResult setError(String error) {
        this.error = error;
        this.status = IndexingResultStatus.ERROR;
        return this;
    }

    /**
     * @return the submitPiToViewer
     */
    public boolean isSubmitPiToViewer() {
        return submitPiToViewer;
    }

    /**
     * @param submitPiToViewer the submitPiToViewer to set
     * @return this
     */
    public IndexingResult setSubmitPiToViewer(boolean submitPiToViewer) {
        this.submitPiToViewer = submitPiToViewer;
        return this;
    }

    /**
     * @return the mediaFileNames
     */
    public Set<String> getMediaFileNames() {
        return mediaFileNames;
    }

    /**
     * @param mediaFileNames the mediaFileNames to set
     */
    public void setMediaImageFileNames(Set<String> mediaFileNames) {
        this.mediaFileNames = mediaFileNames;
    }
}
