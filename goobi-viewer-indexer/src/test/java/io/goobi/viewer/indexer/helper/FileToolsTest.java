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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.goobi.viewer.indexer.AbstractTest;

class FileToolsTest extends AbstractTest {

    private File tempDir = new File("target/temp");

    @AfterEach
    public void tearDown() throws Exception {
        if (tempDir.exists()) {
            FileUtils.deleteQuietly(tempDir);
        }
    }

    /**
     * @see FileTools#compressGzipFile(File,File)
     * @verifies throw FileNotFoundException if file not found
     */
    @Test
    void compressGzipFile_shouldThrowFileNotFoundExceptionIfFileNotFound() throws Exception {
        File file = new File("notfound.txt");
        Assertions.assertFalse(file.exists());
        Assertions.assertThrows(FileNotFoundException.class, () -> FileTools.compressGzipFile(file, new File("target/test.tar.gz")));
    }

    /**
     * @see FileTools#decompressGzipFile(File,File)
     * @verifies throw FileNotFoundException if file not found
     */
    @Test
    void decompressGzipFile_shouldThrowFileNotFoundExceptionIfFileNotFound() throws Exception {
        File gzipFile = new File("notfound.tar.gz");
        Assertions.assertFalse(gzipFile.exists());
        Assertions.assertThrows(FileNotFoundException.class, () -> FileTools.decompressGzipFile(gzipFile, new File("target/target.bla")));
    }

    /**
     * @see FileTools#getFileFromString(String,File,String,boolean)
     * @verifies write file correctly
     */
    @Test
    void getFileFromString_shouldWriteFileCorrectly() throws Exception {
        Assertions.assertTrue(tempDir.mkdirs());
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
    void readFileToString_shouldThrowIOExceptionIfFileNotFound() throws Exception {
        File file = new File("src/test/resources/filenotfound.txt");
        Assertions.assertFalse(file.isFile());
        Assertions.assertThrows(IOException.class, () -> FileTools.readFileToString(file, null));
    }

    /**
     * @see FileTools#deleteUnsupportedDataFolders(Path,String)
     * @verifies only delete folders that match fileNameRoot
     */
    @Test
    void deleteUnsupportedDataFolders_shouldOnlyDeleteFoldersThatMatchFileNameRoot() throws Exception {
        Assertions.assertTrue(tempDir.mkdirs());
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

}