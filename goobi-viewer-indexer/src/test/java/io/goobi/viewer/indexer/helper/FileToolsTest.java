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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import io.goobi.viewer.indexer.AbstractTest;

public class FileToolsTest extends AbstractTest {

    private File tempDir = new File("target/temp");

    @After
    public void tearDown() throws Exception {
        if (tempDir.exists()) {
            FileUtils.deleteQuietly(tempDir);
        }
    }

    /**
     * @see FileTools#compressGzipFile(File,File)
     * @verifies throw FileNotFoundException if file not found
     */
    @Test(expected = FileNotFoundException.class)
    public void compressGzipFile_shouldThrowFileNotFoundExceptionIfFileNotFound() throws Exception {
        File file = new File("notfound.txt");
        Assert.assertFalse(file.exists());
        FileTools.compressGzipFile(file, new File("target/test.tar.gz"));
    }

    /**
     * @see FileTools#decompressGzipFile(File,File)
     * @verifies throw FileNotFoundException if file not found
     */
    @Test(expected = FileNotFoundException.class)
    public void decompressGzipFile_shouldThrowFileNotFoundExceptionIfFileNotFound() throws Exception {
        File gzipFile = new File("notfound.tar.gz");
        Assert.assertFalse(gzipFile.exists());
        FileTools.decompressGzipFile(gzipFile, new File("target/target.bla"));
    }

    /**
     * @see FileTools#getFileFromString(String,File,String,boolean)
     * @verifies write file correctly
     */
    @Test
    public void getFileFromString_shouldWriteFileCorrectly() throws Exception {
        Assert.assertTrue(tempDir.mkdirs());
        File file = new File(tempDir, "temp.txt");
        String text = "Lorem ipsum dolor sit amet";
        FileTools.getFileFromString(text, file.getAbsolutePath(), null, false);
        Assert.assertTrue(file.isFile());
    }

    /**
     * @see FileTools#getCharset(InputStream)
     * @verifies detect charset correctly
     */
    @Test
    public void getCharset_shouldDetectCharsetCorrectly() throws Exception {
        File file = new File("src/test/resources/stopwords_de_en.txt");
        try (FileInputStream fis = new FileInputStream(file)) {
            Assert.assertEquals("UTF-8", FileTools.getCharset(fis));
        }
    }

    /**
     * @see FileTools#readFileToString(File,String)
     * @verifies read file correctly
     */
    @Test
    public void readFileToString_shouldReadFileCorrectly() throws Exception {
        File file = new File("src/test/resources/stopwords_de_en.txt");
        Assert.assertTrue(file.isFile());
        String text = FileTools.readFileToString(file, null);
        Assert.assertTrue(StringUtils.isNotEmpty(text));
    }

    /**
     * @see FileTools#readFileToString(File,String)
     * @verifies throw IOException if file not found
     */
    @Test
    public void readFileToString_shouldThrowIOExceptionIfFileNotFound() throws Exception {
        File file = new File("src/test/resources/filenotfound.txt");
        Assert.assertFalse(file.isFile());
        FileTools.readFileToString(file, null);
    }

}