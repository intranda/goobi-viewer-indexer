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
package io.goobi.viewer.indexer.helper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.goobi.viewer.indexer.AbstractTest;

class FileToolsTest extends AbstractTest {

    /**
     * @see FileTools#compressGzipFile(File,File)
     * @verifies throw FileNotFoundException if file not found
     */
    @Test
    void compressGzipFile_shouldThrowFileNotFoundExceptionIfFileNotFound() {
        File file = new File("notfound.txt");
        Assertions.assertFalse(file.exists());
        Assertions.assertThrows(FileNotFoundException.class, () -> FileTools.compressGzipFile(file, new File("target/test.tar.gz")));
    }

    /**
     * @see FileTools#decompressGzipFile(File,File)
     * @verifies throw FileNotFoundException if file not found
     */
    @Test
    void decompressGzipFile_shouldThrowFileNotFoundExceptionIfFileNotFound() {
        File gzipFile = new File("notfound.tar.gz");
        Assertions.assertFalse(gzipFile.exists());
        Assertions.assertThrows(FileNotFoundException.class, () -> FileTools.decompressGzipFile(gzipFile, new File("target/target.bla")));
    }

    /**
     * @see FileTools#getFileFromString(String,File,String,boolean)
     * @verifies write file correctly
     */
    @Test
    void getFileFromString_shouldWriteFileCorrectly(@TempDir File tempDir) throws Exception {
        Assertions.assertTrue(tempDir.exists());
        File file = new File(tempDir, "temp.txt");
        String text = "Lorem ipsum dolor sit amet";
        FileTools.getFileFromString(text, file.getAbsolutePath(), null, false);
        Assertions.assertTrue(file.isFile());
    }

    /**
     * @see FileTools#getCharset(InputStream)
     * @verifies detect charset correctly
     */
    @Test
    void getCharset_shouldDetectCharsetCorrectly() throws Exception {
        File file = new File("src/test/resources/stopwords_de_en.txt");
        try (FileInputStream fis = new FileInputStream(file)) {
            Assertions.assertEquals("UTF-8", FileTools.getCharset(fis));
        }
    }

    /**
     * @see FileTools#readFileToString(File,String)
     * @verifies read file correctly
     */
    @Test
    void readFileToString_shouldReadFileCorrectly() throws Exception {
        File file = new File("src/test/resources/stopwords_de_en.txt");
        Assertions.assertTrue(file.isFile());
        String text = FileTools.readFileToString(file, null);
        Assertions.assertTrue(StringUtils.isNotEmpty(text));
    }

    /**
     * @see FileTools#readFileToString(File,String)
     * @verifies throw IOException if file not found
     */
    @Test
    void readFileToString_shouldThrowIOExceptionIfFileNotFound() {
        File file = new File("src/test/resources/filenotfound.txt");
        Assertions.assertFalse(file.isFile());
        Assertions.assertThrows(IOException.class, () -> FileTools.readFileToString(file, null));
    }

    /**
     * @see FileTools#deleteUnsupportedDataFolders(Path,String)
     * @verifies only delete folders that match fileNameRoot
     */
    @Test
    void deleteUnsupportedDataFolders_shouldOnlyDeleteFoldersThatMatchFileNameRoot(@TempDir File tempDir) throws Exception {
        Assertions.assertTrue(tempDir.isDirectory());
        Path yes1 = Files.createDirectory(Paths.get(tempDir.getAbsolutePath(), "PPN123_foo"));
        Assertions.assertTrue(Files.isDirectory(yes1));
        Path yes2 = Files.createDirectory(Paths.get(tempDir.getAbsolutePath(), "PPN123_foobar"));
        Assertions.assertTrue(Files.isDirectory(yes2));
        Path no1 = Files.createDirectory(Paths.get(tempDir.getAbsolutePath(), "PPN123_0001_foo"));
        Assertions.assertTrue(Files.isDirectory(no1));
        Path no2 = Files.createDirectory(Paths.get(tempDir.getAbsolutePath(), "PPN456_foo"));
        Assertions.assertTrue(Files.isDirectory(no2));

        FileTools.deleteUnsupportedDataFolders(tempDir.toPath(), "PPN123");

        Assertions.assertFalse(Files.exists(yes1));
        Assertions.assertFalse(Files.exists(yes2));
        Assertions.assertTrue(Files.exists(no1));
        Assertions.assertTrue(Files.exists(no2));
    }

    /**
     * @see FileTools#deleteUnsupportedDataFolders(Path,String,java.util.Collection)
     * @verifies only check candidate names if given
     */
    @Test
    void deleteUnsupportedDataFolders_shouldOnlyCheckCandidateNamesIfGiven(@TempDir File tempDir) throws Exception {
        Path candidate = Files.createDirectory(Paths.get(tempDir.getAbsolutePath(), "PPN123_foo"));
        Path notACandidate = Files.createDirectory(Paths.get(tempDir.getAbsolutePath(), "PPN123_bar"));
        Path otherRecord = Files.createDirectory(Paths.get(tempDir.getAbsolutePath(), "PPN456_foo"));

        FileTools.deleteUnsupportedDataFolders(tempDir.toPath(), "PPN123", List.of("PPN123_foo", "PPN456_foo", "PPN123_missing"));

        Assertions.assertFalse(Files.exists(candidate));
        Assertions.assertTrue(Files.exists(notACandidate));
        Assertions.assertTrue(Files.exists(otherRecord));
    }

    /**
     * @see FileTools#moveOrCopyDirectoryContents(Path,Path)
     * @verifies move files if on same file store
     */
    @Test
    void moveOrCopyDirectoryContents_shouldMoveFilesIfOnSameFileStore(@TempDir File tempDir) throws Exception {
        Path src = Files.createDirectory(Paths.get(tempDir.getAbsolutePath(), "PPN123_media"));
        Path srcFile = Files.writeString(src.resolve("00000001.tif"), "image");
        Path subDir = Files.createDirectory(src.resolve("sub"));
        Files.writeString(subDir.resolve("00000002.tif"), "image2");
        Object srcFileKey = Files.readAttributes(srcFile, BasicFileAttributes.class).fileKey();
        Path target = Paths.get(tempDir.getAbsolutePath(), "media", "PPN123");
        Files.createDirectory(target.getParent());

        Assertions.assertEquals(2, FileTools.moveOrCopyDirectoryContents(src, target));

        Path movedFile = target.resolve("00000001.tif");
        Assertions.assertEquals("image", Files.readString(movedFile));
        Assertions.assertEquals("image2", Files.readString(target.resolve("00000002.tif")));
        // Renamed rather than copied: same inode, and the source file is gone
        Assertions.assertEquals(srcFileKey, Files.readAttributes(movedFile, BasicFileAttributes.class).fileKey());
        Assertions.assertFalse(Files.exists(srcFile));
    }

    /**
     * @see FileTools#moveOrCopyDirectoryContents(Path,Path)
     * @verifies replace existing files
     */
    @Test
    void moveOrCopyDirectoryContents_shouldReplaceExistingFiles(@TempDir File tempDir) throws Exception {
        Path src = Files.createDirectory(Paths.get(tempDir.getAbsolutePath(), "PPN123_media"));
        Files.writeString(src.resolve("00000001.tif"), "new");
        Path target = Files.createDirectory(Paths.get(tempDir.getAbsolutePath(), "PPN123"));
        Files.writeString(target.resolve("00000001.tif"), "old");
        Files.writeString(target.resolve("00000002.tif"), "keep");

        Assertions.assertEquals(1, FileTools.moveOrCopyDirectoryContents(src, target));

        Assertions.assertEquals("new", Files.readString(target.resolve("00000001.tif")));
        Assertions.assertEquals("keep", Files.readString(target.resolve("00000002.tif")));
    }

}