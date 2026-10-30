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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class IndexingResult {

    public enum IndexingResultStatus {
        OK,
        ERROR,
        /** Not indexed because a newer version of the record has been indexed from a higher-priority hotfolder. */
        SKIPPED;
    }

    private IndexingResultStatus status = IndexingResultStatus.OK;
    private String pi;
    private String recordFileName;
    private String error;
    private boolean submitPiToViewer = false;
    private boolean fulltextSuppressed = false;
    private Set<String> mediaFileNames = new HashSet<>();
    /** Docstruct type of the top structure element (for the per-record summary log line). */
    private String docStructType;
    /** Number of generated page documents (without shapes); null if the format has no pages. */
    private Integer numPages;
    /** Number of pages in the source document (e.g. METS physical structMap); null if the format has no pages. */
    private Integer numSourcePages;
    /** Number of files copied into the data repository per data folder param, in copy order. */
    private final Map<String, Integer> copiedDataFileCounts = new LinkedHashMap<>();

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
     * @return the fulltextSuppressed
     */
    public boolean isFulltextSuppressed() {
        return fulltextSuppressed;
    }

    /**
     * @param fulltextSuppressed the fulltextSuppressed to set
     * @return this
     */
    public IndexingResult setFulltextSuppressed(boolean fulltextSuppressed) {
        this.fulltextSuppressed = fulltextSuppressed;
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

    /**
     * @return the docStructType
     */
    public String getDocStructType() {
        return docStructType;
    }

    /**
     * @param docStructType the docStructType to set
     * @return this
     */
    public IndexingResult setDocStructType(String docStructType) {
        this.docStructType = docStructType;
        return this;
    }

    /**
     * @return the numPages
     */
    public Integer getNumPages() {
        return numPages;
    }

    /**
     * @param numPages the numPages to set
     * @return this
     */
    public IndexingResult setNumPages(Integer numPages) {
        this.numPages = numPages;
        return this;
    }

    /**
     * @return the numSourcePages
     */
    public Integer getNumSourcePages() {
        return numSourcePages;
    }

    /**
     * @param numSourcePages the numSourcePages to set
     * @return this
     */
    public IndexingResult setNumSourcePages(Integer numSourcePages) {
        this.numSourcePages = numSourcePages;
        return this;
    }

    /**
     * @return Number of files copied into the data repository per data folder param (e.g. {@code mediaFolder}), in copy order
     */
    public Map<String, Integer> getCopiedDataFileCounts() {
        return copiedDataFileCounts;
    }

    /**
     * Adds the given numbers of copied files per data folder param. Zero counts are ignored.
     *
     * @param counts Number of copied files per data folder param
     * @return this
     */
    public IndexingResult addCopiedDataFileCounts(Map<String, Integer> counts) {
        if (counts != null) {
            counts.forEach((param, count) -> {
                if (count != null && count > 0) {
                    copiedDataFileCounts.merge(param, count, Integer::sum);
                }
            });
        }
        return this;
    }
}
