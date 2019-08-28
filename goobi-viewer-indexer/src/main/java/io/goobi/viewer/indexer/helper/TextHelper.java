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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.FileUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.XMLOutputter;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;

import de.intranda.digiverso.ocr.alto.model.structureclasses.logical.AltoDocument;
import de.intranda.digiverso.ocr.alto.utils.HyphenationLinker;
import de.intranda.digiverso.ocr.conversion.ConvertAbbyyToAltoStaX;
import de.intranda.digiverso.ocr.conversion.ConvertTeiToAlto;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.FatalIndexerException;
import io.goobi.viewer.indexer.model.SolrConstants;

public final class TextHelper {

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(TextHelper.class);

    public static final String DEFAULT_ENCODING = "UTF-8";

    private static final String ALTO_WIDTH = "WIDTH";
    private static final String ALTO_HEIGHT = "HEIGHT";
    private static final String ALTO_CONTENT = "CONTENT";
    private static final String ALTO_SUBS_CONTENT = "SUBS_CONTENT";
    private static final String ALTO_SUBS_TYPE = "SUBS_TYPE";
    private static final String ALTO_SUBS_TYPE_FIRST_WORD = "HypPart1";
    private static final String ALTO_SUBS_TYPE_SECOND_WORD = "HypPart2";

    /**
     * 
     * @param s
     * @return
     */
    public static String normalizeSequence(String s) {
        if (s == null) {
            return null;
        }

        return Normalizer.normalize(s, Normalizer.Form.NFC);
    }

    /**
     * Uses ICU4J to determine the charset of the given InputStream.
     * 
     * @param input
     * @return Detected charset name; null if not detected.
     * @throws IOException
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
     * Converts a <code>String</code> from one given encoding to the other.
     * 
     * @param string The string to convert.
     * @param from Source encoding.
     * @param to Destination encoding.
     * @return The converted string.
     */
    public static String convertStringEncoding(String string, String from, String to) {
        try {
            Charset charsetFrom = Charset.forName(from);
            Charset charsetTo = Charset.forName(to);
            CharsetEncoder encoder = charsetFrom.newEncoder();
            CharsetDecoder decoder = charsetTo.newDecoder();
            ByteBuffer bbuf = encoder.encode(CharBuffer.wrap(string));
            CharBuffer cbuf = decoder.decode(bbuf);
            return cbuf.toString();
        } catch (CharacterCodingException e) {
            logger.error(e.getMessage(), e);
        }

        return string;
    }

    /**
     * @param file
     * @param convertFileToCharset
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     * @should read file correctly
     * @should throw IOException if file not found
     */
    public static String readFileToString(File file, String convertFileToCharset) throws FileNotFoundException, IOException {
        StringBuilder sb = new StringBuilder();
        logger.trace("readFileToString: {}", file.getAbsolutePath());
        try (FileInputStream fis = new FileInputStream(file)) {
            boolean charsetDetected = true;
            String charset = getCharset(new FileInputStream(file));
            // logger.debug("{} charset: {}", file.getAbsolutePath(), charset);
            if (charset == null) {
                charsetDetected = false;
                charset = DEFAULT_ENCODING;
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
                    logger.info("File '{}' has been converted from {} to {}.", file.getAbsolutePath(), charset, convertFileToCharset);
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
     * 
     * @param file
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     * @throws JDOMException
     * @should read XML file correctly
     * @should throw IOException if file not found
     */
    public static Document readXmlFileToDoc(File file) throws FileNotFoundException, IOException, JDOMException {
        try (FileInputStream fis = new FileInputStream(file)) {
            org.jdom2.Document doc = new SAXBuilder().build(fis);
            return doc;
        }
    }

    /**
     * Reads the ALTO document in the file with the given file name.
     * 
     * @param fileName Name of the ALTO file.
     * @param folder Folder in which the file is contained.
     * @return String[] with the content { raw ALTO, plain full-text, page width, page height, named entities }.
     * @throws FileNotFoundException
     * @throws IOException
     * @throws JDOMException
     * @should read ALTO document correctly
     * @should extract fulltext correctly
     * @should extract page dimensions correctly
     * @should throw FileNotFoundException if file not found
     */
    public static Map<String, Object> readAltoFile(File file) throws FileNotFoundException, IOException, JDOMException {
        AltoDocument altoDoc = AltoDocument.getDocumentFromFile(file);
        HyphenationLinker linker = new HyphenationLinker();
        linker.linkWords(altoDoc);
        Document doc = new Document(altoDoc.writeToDom());
        return readAltoDoc(doc, file.getAbsolutePath());
    }

    /**
     * Reads the given ALTO document. If a word has a line break, the complete word is written into each CONTENT attribute.
     * 
     * @param doc
     * @param filePath
     * @return
     */
    public static Map<String, Object> readAltoDoc(Document doc, String filePath) {
        if (doc == null) {
            throw new IllegalArgumentException("doc may not be null.");
        }
        Map<String, Object> ret = new HashMap<>();
        List<String> namedEntityFields = new ArrayList<>();

        StringBuilder sbFulltext = new StringBuilder();
        String width = null;
        String height = null;
        try {
            Element elePage = doc.getRootElement().getChild("Layout", null).getChild("Page", null);
            // Extract page dimensions
            if (elePage.getAttribute(ALTO_WIDTH) != null) {
                try {
                    int w = Integer.valueOf(elePage.getAttributeValue(ALTO_WIDTH));
                    width = String.valueOf(w);
                } catch (NumberFormatException e) {
                    // Float value (since ALTO 2.1)
                    try {
                        float w = Float.valueOf(elePage.getAttributeValue(ALTO_WIDTH));
                        width = String.valueOf((int) w);
                    } catch (NumberFormatException e1) {
                        logger.error("WIDTH not a number: {}", elePage.getAttributeValue(ALTO_WIDTH));
                    }
                }
            }
            if (elePage.getAttribute(ALTO_HEIGHT) != null) {
                try {
                    int h = Integer.valueOf(elePage.getAttributeValue(ALTO_HEIGHT));
                    height = String.valueOf(h);
                } catch (NumberFormatException e) {
                    // Float value (since ALTO 2.1)
                    try {
                        float h = Float.valueOf(elePage.getAttributeValue(ALTO_HEIGHT));
                        height = String.valueOf((int) h);
                    } catch (NumberFormatException e1) {
                        logger.error("HEIGHT not a number: {}", elePage.getAttributeValue(ALTO_HEIGHT));
                    }
                }
            }
            List<Element> elePrintSpaceList = elePage.getChildren("PrintSpace", null);
            if (elePrintSpaceList != null) {
                // Only attempt to read coordinates and text if a 'PrintSpace' block is available
                for (Element elePrintSpace : elePrintSpaceList) {
                    List<Element> blocks = elePrintSpace.getChildren();
                    if (blocks != null) {
                        WordWrapper wordWrapper = new WordWrapper();
                        for (Element eleBlock : blocks) {
                            switch (eleBlock.getName()) {
                                case "TextBlock":
                                    readAltoTextBlock(eleBlock, sbFulltext, wordWrapper);
                                    break;
                                case "ComposedBlock":
                                    handleAltoComposedBlock(eleBlock, sbFulltext, wordWrapper);
                                    //                                    List<Element> textBlocks = eleBlock.getChildren("TextBlock", null);
                                    //                                    if (textBlocks != null) {
                                    //                                        for (Element eleTextBlock : textBlocks) {
                                    //                                            readAltoTextBlock(eleTextBlock, sbFulltext, wordWrapper);
                                    //                                        }
                                    //                                    }
                                    break;
                                default:
                                    // nothing
                            }
                        }
                    }
                }
            }

            //Get Named entities here
            if (doc.getRootElement().getChild("Tags", null) != null) {
                for (Element tag : doc.getRootElement().getChild("Tags", null).getChildren("NamedEntityTag", null)) {
                    //                    namedEntityFields.add(createJSONNamedEntityTag(tag));
                    namedEntityFields.add(createSimpleNamedEntityTag(tag));
                }
            }
        } catch (NullPointerException e) {
            logger.error("Could not parse ALTO document: {}", filePath);
            logger.error(e.getMessage(), e);
        }

        XMLOutputter out = new XMLOutputter();
        ret.put(SolrConstants.ALTO, out.outputString(doc));
        ret.put(SolrConstants.FULLTEXT, sbFulltext.toString());
        ret.put(SolrConstants.WIDTH, width);
        ret.put(SolrConstants.HEIGHT, height);
        ret.put("NAMEDENTITIES", namedEntityFields);

        return ret;
    }

    /**
     * An ALTO ComposedBlock can theoretically contain n levels of nested ComposedBlocks. Collect all words from contained TextBlocks recursively.
     *
     * @param eleComposedBlock
     * @return
     * @should return all words from nested ComposedBlocks
     */
    public static void handleAltoComposedBlock(Element eleComposedBlock, StringBuilder sbFulltext, WordWrapper wordWrapper) {
        List<Element> words = new ArrayList<>();

        // Words from TextBlocks
        for (Element eleTextBlock : eleComposedBlock.getChildren("TextBlock", null)) {
            readAltoTextBlock(eleTextBlock, sbFulltext, wordWrapper);

        }

        // Nested ComposedBlocks
        List<Element> eleListNextedComposedBlocks = eleComposedBlock.getChildren("ComposedBlock", null);
        if (eleListNextedComposedBlocks != null) {
            for (Element eleNestedComposedBlock : eleComposedBlock.getChildren("ComposedBlock", null)) {
                handleAltoComposedBlock(eleNestedComposedBlock, sbFulltext, wordWrapper);
            }
        }
    }

    /**
     * @param tag
     * @return
     */
    private static String createSimpleNamedEntityTag(Element tag) {
        String neLabel = tag.getAttributeValue("LABEL");
        String neType = tag.getAttributeValue("TYPE");

        StringBuilder sb = new StringBuilder();
        sb.append(neType);
        sb.append('_');
        sb.append(neLabel);

        return sb.toString();
    }

    /**
     * 
     * @param eleTextBlock
     * @param sbFulltext
     * @param eleWord1
     * @param word1
     * @param fileName
     */
    private static void readAltoTextBlock(Element eleTextBlock, StringBuilder sbFulltext, WordWrapper wordWrapper) {
        List<Element> lines = eleTextBlock.getChildren("TextLine", null);
        for (Element eleLine : lines) {
            List<Element> eleWordList = eleLine.getChildren("String", null);
            if (eleWordList == null || eleWordList.isEmpty()) {
                continue;
            }

            if (sbFulltext.length() > 0) {
                // Add a line break in the full-text if a new line starts
                sbFulltext.append('\n');
            }
            // Add unaltered words to the full-text
            for (Element eleWord : eleWordList) {
                if (eleWordList.indexOf(eleWord) > 0) {
                    sbFulltext.append(' ');
                }
                /*for hyphenated words, only add the SUBS_CONTENT (content of complete word) 
                of the first word to the fulltext, and ignore the second word
                */
                if (ALTO_SUBS_TYPE_FIRST_WORD.equals(eleWord.getAttributeValue(ALTO_SUBS_TYPE))) {
                    sbFulltext.append(eleWord.getAttributeValue(ALTO_SUBS_CONTENT));
                } else if (!ALTO_SUBS_TYPE_SECOND_WORD.equals(eleWord.getAttributeValue(ALTO_SUBS_TYPE))) {
                    sbFulltext.append(eleWord.getAttributeValue(ALTO_CONTENT));
                }
            }
            if (wordWrapper.word1 != null) {
                Element eleWord2 = eleWordList.get(0);
                String word2 = eleWord2.getAttributeValue(ALTO_CONTENT);
                // If the current text block has one one line, assume it's the page number and do not combine
                if (word2 != null && eleWordList.size() > 1) {
                    word2 = word2.trim();
                    String newWord = wordWrapper.word1 + word2;
                    wordWrapper.eleWord1.setAttribute(ALTO_CONTENT, newWord);
                    eleWord2.setAttribute(ALTO_CONTENT, newWord);
                    //                        logger.debug("Combined ALTO words in file '" + fileName + "': " + word1 + " + " + word2 + " ==> " + newWord);
                }
                wordWrapper.eleWord1 = null;
                wordWrapper.word1 = null;
            }

            // Analyze the last word of every line
            Element eleLastWord = eleWordList.get(eleWordList.size() - 1);
            String word = eleLastWord.getAttributeValue(ALTO_CONTENT);
            if (word != null) {
                word = word.trim();
                // logger.info("Last word: " + word);
                // Check whether the last word on the current line ends with a hyphen etc.
                // if (word.length() > 1 && (word.endsWith("¬") || word.endsWith("-") || word.endsWith("­"))) {
                //                    if (word.length() > 1 && (word.endsWith("\u00AC") || word.endsWith("\u002D") || word.endsWith("\u00AD"))) {
                //                        wordWrapper.eleWord1 = eleLastWord;
                //                        wordWrapper.word1 = word.substring(0, word.length() - 1);
                //                        // logger.info("Found hyphenated last word: " + word1);
                //                    }
            }
        }
        if (sbFulltext.length() > 0) {
            // Add an extra line break in the full-text if a new block starts
            sbFulltext.append('\n');
        }
    }

    /**
     * Wrapper class for passing around Element and String objects between methods.
     */
    static class WordWrapper {
        Element eleWord1;
        String word1;
    }

    /**
     * Converts the document from the given TEI file to ALTO.
     * 
     * @param file
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     * @throws JDOMException
     * @throws FatalIndexerException
     * @should convert to ALTO correctly
     * @should throw IOException given wrong document format
     */
    public static Map<String, Object> readTeiToAlto(File file) throws FileNotFoundException, IOException, JDOMException, FatalIndexerException {
        logger.info("readTei: {}", file.getAbsolutePath());
        if (!file.exists()) {
            throw new FileNotFoundException("File not found: " + file.getAbsolutePath());
        }
        if (!FileFormat.TEI.equals(JDomXP.determineFileFormat(file))) {
            throw new IOException(file.getAbsolutePath() + " is not a valid TEI document.");
        }
        Map<String, Object> ret = new HashMap<>();

        // Convert to ALTO
        try {
            Element alto = new ConvertTeiToAlto().convert(file.toPath());
            if (alto != null) {
                Document altoDoc = new Document();
                altoDoc.setRootElement(alto);
                ret = readAltoDoc(altoDoc, file.getAbsolutePath());
                logger.info("Converted TEI to ALTO: {}", file.getName());
            } else {
                logger.warn("Could not convert TEI to ALTO: {}", file.getName());
            }
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }

        return ret;
    }

    /**
     * Extracts page width and height attributes from the given ABBYY XML file and converts the document to ALTO.
     * 
     * @param file
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     * @throws JDOMException
     * @throws XMLStreamException
     * @throws FatalIndexerException
     * @should convert to ALTO correctly
     * @should throw IOException given wrong document format
     */
    public static Map<String, Object> readAbbyyToAlto(File file)
            throws FileNotFoundException, IOException, XMLStreamException, FatalIndexerException {
        logger.trace("readAbbyy: {}", file.getAbsolutePath());
        if (!FileFormat.ABBYYXML.equals(JDomXP.determineFileFormat(file))) {
            throw new IOException(file.getAbsolutePath() + " is not a valid ABBYY XML document.");
        }
        Map<String, Object> ret = new HashMap<>();

        // Convert to ALTO
        Element alto = new ConvertAbbyyToAltoStaX().convert(file, new Date(file.lastModified()));
        if (alto != null) {
            Document altoDoc = new Document();
            altoDoc.setRootElement(alto);
            ret = readAltoDoc(altoDoc, file.getAbsolutePath());
            logger.debug("Converted ABBYY XML to ALTO: {}", file.getName());
        }

        return ret;
    }

    /**
     * Extracts image width, height and color space attributes from the given MIX file.
     * 
     * @param file
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     * @throws JDOMException
     * @throws FatalIndexerException
     * @should return correct values
     * @should throw FileNotFoundException if file not found
     */
    public static Map<String, String> readMix(File file) throws FileNotFoundException, IOException, JDOMException, FatalIndexerException {
        Map<String, String> ret = new HashMap<>();

        try (FileInputStream fis = new FileInputStream(file)) {
            Document doc = new SAXBuilder().build(fis);
            // Image width
            List<String> values =
                    JDomXP.evaluateToStringListStatic("mix:mix/mix:BasicImageInformation/mix:BasicImageCharacteristics/mix:imageWidth", doc);
            if (values != null && !values.isEmpty()) {
                try {
                    String str = values.get(0);
                    Integer.parseInt(str);
                    ret.put(SolrConstants.WIDTH, str);
                    // logger.info(SolrConstants.WIDTH + ":" + str);
                } catch (NumberFormatException e) {
                }
            }
            // Image height
            values = JDomXP.evaluateToStringListStatic("mix:mix/mix:BasicImageInformation/mix:BasicImageCharacteristics/mix:imageHeight", doc);
            if (values != null && !values.isEmpty()) {
                try {
                    String str = values.get(0);
                    Integer.parseInt(str);
                    ret.put(SolrConstants.HEIGHT, str);
                    // logger.info(SolrConstants.HEIGHT + ":" + str);
                } catch (NumberFormatException e) {
                }
            }
            // Color space
            values = JDomXP.evaluateToStringListStatic(
                    "mix:mix/mix:BasicImageInformation/mix:BasicImageCharacteristics/mix:PhotometricInterpretation/mix:colorSpace", doc);
            if (values != null && !values.isEmpty()) {
                String str = values.get(0);
                ret.put(SolrConstants.COLORSPACE, str);
            }
            // logger.info(SolrConstants.COLORSPACE + ":" + str);
        }

        return ret;
    }

    /**
     * 
     * @param fileName
     * @param folder
     * @param warnIfMissing
     * @return
     * @should return text if fulltext file exists
     * @should return null if fulltext folder exists but no file
     * @should return null of fulltext folder does not exist
     */
    public static String generateFulltext(String fileName, Path folder, boolean warnIfMissing) {
        if (!Files.isDirectory(folder)) {
            return null;
        }

        String text = null;
        Path txt = Paths.get(folder.toAbsolutePath().toString(), fileName);
        if (Files.isRegularFile(txt)) {
            try {
                text = TextHelper.readFileToString(txt.toFile(), DEFAULT_ENCODING);
            } catch (IOException e) {
                logger.error("{}: {}", e.getMessage(), txt.toAbsolutePath());
            }
        } else if (warnIfMissing) {
            logger.warn("File not found : {}", txt.toAbsolutePath());
        }

        return text;
    }

    /**
     * Strips the given string of HTML tags, etc.
     * 
     * @param text
     * @return
     * @should clean up string correctly
     */
    public static String cleanUpHtmlTags(String text) {
        // Workaround for strings containing just opening brackets because JSoup cuts off everything that comes after
        if (text.contains("<") && !text.contains(">")) {
            text = text.replace("<", "");
        }

        return Jsoup.parse(text).text();
    }
}
