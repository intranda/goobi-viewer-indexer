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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;

/**
 * File I/O utilities.
 */
public final class FileTools {

    private static final Logger logger = LogManager.getLogger(FileTools.class);

    /** Constant <code>TXT_EXTENSION=".txt"</code> */
    public static final String TXT_EXTENSION = ".txt";
    /** Constant <code>XML_EXTENSION=".xml"</code> */
    public static final String XML_EXTENSION = ".xml";

    /** Private constructor. */
    private FileTools() {
        //
    }

    /** Constant <code>FILENAME_FILTER_XML</code> */
    public static final FilenameFilter FILENAME_FILTER_XML =
            (dir, name) -> XML_EXTENSION.equals("." + FilenameUtils.getExtension(name.toLowerCase()));

    /**
     * Reads a String from a byte array
     *
     * @param bytes an array of {@link java.lang.Byte} objects.
     * @param encoding a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     */
    public static String getStringFromByteArray(byte[] bytes, final String encoding) {
        String result = "";

        StringBuilder text = new StringBuilder();
        String nl = System.getProperty("line.separator");
        try (Scanner scanner = new Scanner(new ByteArrayInputStream(bytes), encoding != null ? encoding : TextHelper.DEFAULT_CHARSET)) {
            while (scanner.hasNextLine()) {
                text.append(scanner.nextLine()).append(nl);
            }
        }
        result = text.toString();
        return result.trim();
    }

    /**
     * <p>
     * readFileToString.
     * </p>
     *
     * @param file a {@link java.io.File} object.
     * @param convertFileToCharset a {@link java.lang.String} object.
     * @throws java.io.IOException
     * @should read file correctly
     * @should throw IOException if file not found
     * @return a {@link java.lang.String} object.
     */
    public static String readFileToString(File file, String convertFileToCharset) throws IOException {
        StringBuilder sb = new StringBuilder();
        logger.trace("readFileToString: {}", file.getAbsolutePath());
        try (FileInputStream fis = new FileInputStream(file)) {
            boolean charsetDetected = true;
            String charset = getCharset(new FileInputStream(file));
            // logger.debug("{} charset: {}", file.getAbsolutePath(), charset); //NOSONAR This is needed for debugging sometimes
            if (charset == null) {
                charsetDetected = false;
                charset = TextHelper.DEFAULT_CHARSET;
            }
            try (InputStreamReader in = new InputStreamReader(fis, charset); BufferedReader r = new BufferedReader(in)) {
                String line = null;
                while ((line = r.readLine()) != null) {
                    sb.append(line).append('\n');
                }
            }
            // Force conversion to target charset, if different charset detected
            if (charsetDetected && convertFileToCharset != null && !charset.equals(convertFileToCharset)) {
                try {
                    Charset toCharset = Charset.forName(convertFileToCharset);
                    FileUtils.write(file, sb.toString(), toCharset);
                    logger.debug("File '{}' has been converted from {} to {}.", file.getAbsolutePath(), charset, convertFileToCharset);
                } catch (UnsupportedEncodingException e) {
                    logger.error("Cannot convert file '{}' - Unsupported target encoding '{}'.", file.getAbsolutePath(), e.getMessage());
                }

            }
        } catch (UnsupportedEncodingException e) {
            logger.error("Unsupported encoding '{}' in '{}'.", e.getMessage(), file.getAbsolutePath());
        }

        return sb.toString();
    }

    /**
     * Uses ICU4J to determine the charset of the given InputStream.
     *
     * @param input a {@link java.io.InputStream} object.
     * @return Detected charset name; null if not detected.
     * @throws java.io.IOException
     * @should detect charset correctly
     */
    public static String getCharset(InputStream input) throws IOException {
        CharsetDetector cd = new CharsetDetector();
        try (BufferedInputStream bis = new BufferedInputStream(input)) {
            cd.setText(bis);
            CharsetMatch cm = cd.detect();
            if (cm != null) {
                return cm.getName();
            }
        }

        return null;
    }

    /**
     * Simply write a String into a text file.
     *
     * @param string The String to write
     * @param filePath The file path to write to (will be created if it doesn't exist)
     * @param encoding The character encoding to use. If null, a standard utf-8 encoding will be used
     * @param append Whether to append the text to an existing file (true), or to overwrite it (false)
     * @throws java.io.IOException
     * @should write file correctly
     * @should append to file correctly
     * @return a {@link java.io.File} object.
     */
    public static File getFileFromString(String string, String filePath, final String encoding, boolean append) throws IOException {
        if (string == null) {
            throw new IllegalArgumentException("string may not be null");
        }
        File file = new File(filePath);
        try (FileWriterWithEncoding writer = FileWriterWithEncoding.builder()
                .setFile(file)
                .setCharset(encoding != null ? encoding : TextHelper.DEFAULT_CHARSET)
                .setAppend(append)
                .get()) {
            writer.write(string);
        }

        return file;
    }

    /**
     * <p>
     * decompressGzipFile.
     * </p>
     *
     * @param gzipFile a {@link java.io.File} object.
     * @param newFile a {@link java.io.File} object.
     * @throws java.io.IOException if file not found
     * @should throw FileNotFoundException if file not found
     */
    public static void decompressGzipFile(File gzipFile, File newFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(gzipFile); GZIPInputStream gis = new GZIPInputStream(fis);
                FileOutputStream fos = new FileOutputStream(newFile)) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) != -1) {
                fos.write(buffer, 0, len);
            }
        }
    }

    /**
     * <p>
     * compressGzipFile.
     * </p>
     *
     * @param file a {@link java.io.File} object.
     * @param gzipFile a {@link java.io.File} object.
     * @throws java.io.IOException
     * @should throw FileNotFoundException if file not found
     */
    public static void compressGzipFile(File file, File gzipFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(file); FileOutputStream fos = new FileOutputStream(gzipFile);
                GZIPOutputStream gzipOS = new GZIPOutputStream(fos)) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) != -1) {
                gzipOS.write(buffer, 0, len);
            }
        }
    }

    /**
     * <p>
     * compressZipFile.
     * </p>
     *
     * @param files a {@link java.util.List} object.
     * @param zipFile a {@link java.io.File} object.
     * @param level a {@link java.lang.Integer} object.
     * @throws java.io.IOException
     * @should throw FileNotFoundException if file not found
     */
    public static void compressZipFile(List<File> files, File zipFile, Integer level) throws IOException {
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("files may not be empty or null");
        }
        if (zipFile == null) {
            throw new IllegalArgumentException("zipFile may not be empty or null");
        }

        try (FileOutputStream fos = new FileOutputStream(zipFile); ZipOutputStream zos = new ZipOutputStream(fos)) {
            if (level != null) {
                zos.setLevel(level);
            }
            for (File file : files) {
                try (FileInputStream fis = new FileInputStream(file)) {
                    zos.putNextEntry(new ZipEntry(file.getName()));
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = fis.read(buffer)) != -1) {
                        zos.write(buffer, 0, len);
                    }
                }
            }
        }
    }

    /**
     * <p>
     * compressZipFile.
     * </p>
     *
     * @param contentMap a {@link java.util.Map} object.
     * @param zipFile a {@link java.io.File} object.
     * @param level a {@link java.lang.Integer} object.
     * @throws java.io.IOException
     * @should throw FileNotFoundException if file not found
     */
    public static void compressZipFile(Map<Path, String> contentMap, File zipFile, Integer level) throws IOException {
        if (contentMap == null || contentMap.isEmpty()) {
            throw new IllegalArgumentException("texts may not be empty or null");
        }
        if (zipFile == null) {
            throw new IllegalArgumentException("zipFile may not be empty or null");
        }

        try (FileOutputStream fos = new FileOutputStream(zipFile); ZipOutputStream zos = new ZipOutputStream(fos)) {
            if (level != null) {
                zos.setLevel(level);
            }
            for (Entry<Path, String> entry : contentMap.entrySet()) {
                try (InputStream in = IOUtils.toInputStream(entry.getValue(), StandardCharsets.UTF_8)) {
                    zos.putNextEntry(new ZipEntry(entry.getKey().getFileName().toString()));
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = in.read(buffer)) != -1) {
                        zos.write(buffer, 0, len);
                    }
                }
            }
        }
    }

    /**
     * <p>
     * checkPathExistance.
     * </p>
     *
     * @param path a {@link java.nio.file.Path} object.
     * @param create a boolean.
     * @return true if folder found or created; false if not found
     * @throws java.io.IOException
     */
    public static boolean checkPathExistance(Path path, boolean create) throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("path may not be null");
        }

        if (Files.exists(path)) {
            return true;
        }
        if (create) {
            Files.createDirectory(path);
            logger.info("Created folder: {}", path.toAbsolutePath());
            return true;
        }
        logger.error("Folder not found: {}", path.toAbsolutePath());
        return false;
    }

    /**
     * <p>
     * copyStream.
     * </p>
     *
     * @param output a {@link java.io.OutputStream} object.
     * @param input a {@link java.io.InputStream} object.
     * @throws java.io.IOException if any.
     */
    public static void copyStream(OutputStream output, InputStream input) throws IOException {
        byte[] buf = new byte[1024];
        int len;
        while ((len = input.read(buf)) > 0) {
            output.write(buf, 0, len);
        }
    }

    /**
     * <p>
     * copyDirectory.
     * </p>
     *
     * @param sourceLocation {@link java.nio.file.Path}
     * @param targetLocation {@link java.nio.file.Path}
     * @return number of copied files.
     * @throws java.io.IOException in case of errors.
     */
    public static int copyDirectory(Path sourceLocation, Path targetLocation) throws IOException {
        if (sourceLocation == null) {
            throw new IllegalArgumentException("targetsourceLocationLocation may not be null");
        }
        if (targetLocation == null) {
            throw new IllegalArgumentException("targetLocation may not be null");
        }

        int count = sourceLocation.toFile().listFiles().length;
        if (count > 0) {
            if (!Files.exists(targetLocation)) {
                Files.createDirectory(targetLocation);
            }
            try (Stream<Path> walk = Files.walk(sourceLocation)) {
                walk.filter(Files::isRegularFile).forEach(file -> {
                    Path newFile = Paths.get(targetLocation.toAbsolutePath().toString(), file.getFileName().toString());
                    try {
                        Files.copy(file, newFile, StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                });
            }
        }

        return count;
    }

    /**
     * <p>
     * isFolderEmpty.
     * </p>
     *
     * @param folder a {@link java.nio.file.Path} object.
     * @return true if folder empty; false otherwise
     * @throws java.io.IOException
     */
    public static boolean isFolderEmpty(final Path folder) throws IOException {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(folder)) {
            return !ds.iterator().hasNext();
        }
    }

    /**
     * 
     * @param hotfolderPath
     * @param fileNameRoot
     * @throws IOException
     * @should only delete folders that match fileNameRoot
     */
    public static void deleteUnsupportedDataFolders(Path hotfolderPath, String fileNameRoot) throws IOException {
        Pattern p = Pattern.compile(fileNameRoot + "_[a-z]+");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(hotfolderPath, new DirectoryStream.Filter<Path>() {
            @Override
            public boolean accept(Path entry) throws IOException {
                Matcher m = p.matcher(entry.getFileName().toString());
                return m.matches();
            }
        })) {
            for (Path path : stream) {
                if (Files.isDirectory(path)) {
                    if (Utils.deleteDirectory(path)) {
                        logger.info("Deleted unsupported folder '{}'.", path.getFileName());
                    } else {
                        logger.warn(StringConstants.LOG_COULD_NOT_BE_DELETED, path.toAbsolutePath());
                    }
                }
            }
        }
    }

    /**
     * <p>
     * isImageUrl.
     * </p>
     *
     * @param fileName
     * @return true if this is an image file name; false otherwise
     * @should return true for image file names
     */
    public static boolean isImageFile(String fileName) {
        if (StringUtils.isEmpty(fileName)) {
            return false;
        }

        String extension = FilenameUtils.getExtension(fileName);
        if (StringUtils.isEmpty(extension)) {
            return false;
        }

        switch (extension.toLowerCase()) {
            case "tif":
            case "tiff":
            case "jpg":
            case "jpeg":
            case "gif":
            case "png":
            case "jp2":
                return true;
            default:
                return false;
        }
    }

}
