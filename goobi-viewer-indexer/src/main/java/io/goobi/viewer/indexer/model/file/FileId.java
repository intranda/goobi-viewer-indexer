package io.goobi.viewer.indexer.model.file;

import org.apache.commons.lang3.StringUtils;

public class FileId {

    private final boolean prefix;
    private final boolean suffix;
    private final char separator;
    private final String root;
    private final String fileGroup;

    public static FileId getFileId(String idString, String fileGroup) throws IllegalArgumentException {

        boolean fileGroupPrefix = false; // FILEID starts with the file group name
        boolean fileGroupSuffix = false; // FILEID ends with the file group name
        char fileIdSeparator = '_'; // Separator character between the group name and the rest and the file ID

        // Determine the FILEID root (part of the FILEID that doesn't change for different mets:fileGroups)
        String fileIdRoot = null;
        // Remove the file group part from the file ID
        if (StringUtils.isNotBlank(idString)) {
            if (idString.startsWith(fileGroup + '_')) {
                fileGroupPrefix = true;
            } else if (idString.startsWith(fileGroup + '.')) {
                fileGroupPrefix = true;
                fileIdSeparator = '.';
            } else if (idString.endsWith('_' + fileGroup)) {
                fileGroupSuffix = true;
            } else if (idString.endsWith('.' + fileGroup)) {
                fileGroupSuffix = true;
                fileIdSeparator = '.';
            }
            if (fileGroupPrefix) {
                fileIdRoot = idString.replace(fileGroup + fileIdSeparator, "");
            } else if (fileGroupSuffix) {
                fileIdRoot = idString.replace(fileIdSeparator + fileGroup, "");
            } else {
                fileIdRoot = idString;
            }
            return new FileId(fileIdRoot, fileGroup, fileGroupPrefix, fileGroupSuffix, fileIdSeparator);
        }
        return null;
    }

    public String getRoot() {
        return root;
    }

    public String getFileGroup() {
        return fileGroup;
    }

    public String getFullId() {
        if (this.prefix) {
            return this.fileGroup + this.separator + this.root;
        } else if (this.suffix) {
            return this.root + this.separator + this.fileGroup;
        }
        return this.root;
    }

    public String getFullId(String fileGroupId) {
        if (this.prefix) {
            return fileGroupId + this.separator + this.root;
        } else if (this.suffix) {
            return this.root + this.separator + fileGroupId;
        }
        return this.root;
    }

    @Override
    public String toString() {
        return getFullId();
    }

    private FileId(String root, String fileGroup, boolean prefix, boolean suffix, char separator) {
        super();
        this.prefix = prefix;
        this.suffix = suffix;
        this.root = root;
        this.fileGroup = fileGroup;
        this.separator = separator;
    }

}
