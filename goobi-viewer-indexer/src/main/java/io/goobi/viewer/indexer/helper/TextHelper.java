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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
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

import org.apache.commons.lang3.StringUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.output.XMLOutputter;
import org.jsoup.Jsoup;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import de.intranda.digiverso.ocr.alto.model.structureclasses.logical.AltoDocument;
import de.intranda.digiverso.ocr.alto.utils.HyphenationLinker;
import de.intranda.digiverso.ocr.conversion.ConvertAbbyyToAltoStaX;
import de.intranda.digiverso.ocr.conversion.ConvertTeiToAlto;
import io.goobi.viewer.indexer.exceptions.FatalIndexerException;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;
import io.goobi.viewer.indexer.model.SolrConstants;

/**
 * <p>
 * TextHelper class.
 * </p>
 *
 */
public final class TextHelper {

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(TextHelper.class);

    /** Constant <code>DEFAULT_ENCODING="UTF-8"</code> */
    public static final String DEFAULT_CHARSET = StandardCharsets.UTF_8.name();

    private static final String ALTO_WIDTH = "WIDTH";
    private static final String ALTO_HEIGHT = "HEIGHT";
    private static final String ALTO_COMPOSEDBLOCK = "ComposedBlock";
    private static final String ALTO_CONTENT = "CONTENT";
    private static final String ALTO_SUBS_CONTENT = "SUBS_CONTENT";
    private static final String ALTO_SUBS_TYPE = "SUBS_TYPE";
    private static final String ALTO_SUBS_TYPE_FIRST_WORD = "HypPart1";
    private static final String ALTO_SUBS_TYPE_SECOND_WORD = "HypPart2";

    /**
     * 
     */
    private TextHelper() {
        //
    }

    /**
     * <p>
     * normalizeSequence.
     * </p>
     *
     * @param s a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     */
    public static String normalizeSequence(String s) {
        if (s == null) {
            return null;
        }

        return Normalizer.normalize(s, Normalizer.Form.NFC);
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
     * Reads the ALTO document in the file with the given file name, links hyphenated words and extracts plain text.
     *
     * @return String[] with the content { raw ALTO, plain full-text, page width, page height, named entities }.
     * @throws java.io.FileNotFoundException
     * @throws java.io.IOException
     * @throws org.jdom2.JDOMException
     * @should read ALTO document correctly
     * @should extract fulltext correctly
     * @should extract page dimensions correctly
     * @should throw FileNotFoundException if file not found
     * @param file a {@link java.io.File} object.
     */
    public static Map<String, Object> readAltoFile(File file) throws IOException, JDOMException {
        AltoDocument altoDoc;
        try {
            altoDoc = AltoDocument.getDocumentFromFile(file);
        } catch (IllegalArgumentException e) {
            throw new IOException(e);
        }
        HyphenationLinker linker = new HyphenationLinker();
        linker.linkWords(altoDoc);
        Document doc = new Document(altoDoc.writeToDom());
        return readAltoDoc(doc);
    }

    /**
     * Reads the given ALTO document. If a word has a line break, the complete word is written into each CONTENT attribute.
     *
     * @param doc a {@link org.jdom2.Document} object.
     * @return a {@link java.util.Map} object.
     */
    public static Map<String, Object> readAltoDoc(Document doc) {
        if (doc == null) {
            throw new IllegalArgumentException(StringConstants.ERROR_DOC_MAY_NOT_BE_NULL);
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
                    int w = Integer.parseInt(elePage.getAttributeValue(ALTO_WIDTH));
                    width = String.valueOf(w);
                } catch (NumberFormatException e) {
                    // Float value (since ALTO 2.1)
                    try {
                        float w = Float.parseFloat(elePage.getAttributeValue(ALTO_WIDTH));
                        width = String.valueOf((int) w);
                    } catch (NumberFormatException e1) {
                        logger.error("WIDTH not a number: {}", elePage.getAttributeValue(ALTO_WIDTH));
                    }
                }
            }
            if (elePage.getAttribute(ALTO_HEIGHT) != null) {
                try {
                    int h = Integer.parseInt(elePage.getAttributeValue(ALTO_HEIGHT));
                    height = String.valueOf(h);
                } catch (NumberFormatException e) {
                    // Float value (since ALTO 2.1)
                    try {
                        float h = Float.parseFloat(elePage.getAttributeValue(ALTO_HEIGHT));
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
                    if (blocks == null) {
                        continue;
                    }
                    for (Element eleBlock : blocks) {
                        switch (eleBlock.getName()) {
                            case "TextBlock":
                                readAltoTextBlock(eleBlock, sbFulltext);
                                break;
                            case ALTO_COMPOSEDBLOCK:
                                handleAltoComposedBlock(eleBlock, sbFulltext);
                                break;
                            default:
                                // nothing
                        }
                    }
                }
            }

            // Get Named entities here
            if (doc.getRootElement().getChild("Tags", null) != null) {
                for (Element eleTag : doc.getRootElement().getChild("Tags", null).getChildren("NamedEntityTag", null)) {
                    String neTag = createSimpleNamedEntityTag(eleTag);
                    if (StringUtils.isNotEmpty(neTag)) {
                        namedEntityFields.add(neTag);
                    }
                }
            }
        } catch (NullPointerException e) {
            logger.error("Could not parse ALTO document.");
            logger.error(e.getMessage(), e);
        }

        XMLOutputter out = new XMLOutputter();
        ret.put(SolrConstants.ALTO, out.outputString(doc));
        ret.put(SolrConstants.FULLTEXT, sbFulltext.toString());
        ret.put(SolrConstants.WIDTH, width);
        ret.put(SolrConstants.HEIGHT, height);
        ret.put(SolrConstants.NAMEDENTITIES, namedEntityFields);

        return ret;
    }

    /**
     * An ALTO ComposedBlock can theoretically contain n levels of nested ComposedBlocks. Collect all words from contained TextBlocks recursively.
     *
     * @param eleComposedBlock a {@link org.jdom2.Element} object.
     * @should return all words from nested ComposedBlocks
     * @param sbFulltext a {@link java.lang.StringBuilder} object.
     */
    public static void handleAltoComposedBlock(Element eleComposedBlock, StringBuilder sbFulltext) {
        // Words from TextBlocks
        for (Element eleTextBlock : eleComposedBlock.getChildren("TextBlock", null)) {
            readAltoTextBlock(eleTextBlock, sbFulltext);

        }

        // Nested ComposedBlocks
        List<Element> eleListNextedComposedBlocks = eleComposedBlock.getChildren(ALTO_COMPOSEDBLOCK, null);
        if (eleListNextedComposedBlocks != null) {
            for (Element eleNestedComposedBlock : eleComposedBlock.getChildren(ALTO_COMPOSEDBLOCK, null)) {
                handleAltoComposedBlock(eleNestedComposedBlock, sbFulltext);
            }
        }
    }

    /**
     * @param eleTag JDOM2 element containing tag attributes
     * @return String containing type and label
     * @should uppercase type
     */
    static String createSimpleNamedEntityTag(Element eleTag) {
        String neLabel = eleTag.getAttributeValue("LABEL");
        String neType = eleTag.getAttributeValue("TYPE");

        if (neType == null || neLabel == null) {
            return null;
        }

        return neType.toUpperCase() + "_" + neLabel;
    }

    /**
     * Reads words from a TextBlock element. No hyphenation linking happens here (those words have already been linked on the ALTO document
     * beforehand).
     * 
     * @param eleTextBlock
     * @param sbFulltext
     */
    private static void readAltoTextBlock(Element eleTextBlock, StringBuilder sbFulltext) {
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
            int count = 0;
            for (Element eleWord : eleWordList) {
                if (count > 0) {
                    sbFulltext.append(' ');
                }
                /* for hyphenated words, only add the SUBS_CONTENT (content of complete word) 
                of the first word to the full-text, and ignore the second word
                */
                if (ALTO_SUBS_TYPE_FIRST_WORD.equals(eleWord.getAttributeValue(ALTO_SUBS_TYPE))) {
                    sbFulltext.append(eleWord.getAttributeValue(ALTO_SUBS_CONTENT));
                } else if (!ALTO_SUBS_TYPE_SECOND_WORD.equals(eleWord.getAttributeValue(ALTO_SUBS_TYPE))) {
                    sbFulltext.append(eleWord.getAttributeValue(ALTO_CONTENT));
                }
                count++;
            }
        }
        if (sbFulltext.length() > 0) {
            // Add an extra line break in the full-text if a new block starts
            sbFulltext.append('\n');
        }
    }

    /**
     * Converts the document from the given TEI file to ALTO.
     *
     * @param file a {@link java.io.File} object.
     * @throws java.io.IOException
     * @should convert to ALTO correctly
     * @should throw IOException given wrong document format
     * @return a {@link java.util.Map} object.
     */
    public static Map<String, Object> readTeiToAlto(File file) throws IOException {
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
                ret = readAltoDoc(altoDoc);
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
     * @param file a {@link java.io.File} object.
     * @throws java.io.IOException
     * @throws javax.xml.stream.XMLStreamException
     * @should convert to ALTO correctly
     * @should throw IOException given wrong document format
     * @return a {@link java.util.Map} object.
     */
    public static Map<String, Object> readAbbyyToAlto(File file) throws IOException, XMLStreamException {
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
            ret = readAltoDoc(altoDoc);
            logger.debug("Converted ABBYY XML to ALTO: {}", file.getName());
        }

        return ret;
    }

    /**
     * Extracts image width, height and color space attributes from the given MIX file.
     *
     * @param file a {@link java.io.File} object.
     * @throws java.io.IOException
     * @throws org.jdom2.JDOMException
     * @throws io.goobi.viewer.indexer.exceptions.FatalIndexerException
     * @should return correct values
     * @should throw FileNotFoundException if file not found
     * @return a {@link java.util.Map} object.
     */
    public static Map<String, String> readMix(File file) throws IOException, JDOMException, FatalIndexerException {
        Map<String, String> ret = new HashMap<>();

        try (FileInputStream fis = new FileInputStream(file)) {
            Document doc = XmlTools.getSAXBuilder().build(fis);
            // Image width
            List<String> values =
                    JDomXP.evaluateToStringListStatic("mix:mix/mix:BasicImageInformation/mix:BasicImageCharacteristics/mix:imageWidth", doc);
            if (values != null && !values.isEmpty()) {
                try {
                    String str = values.get(0);
                    Integer.parseInt(str);
                    ret.put(SolrConstants.WIDTH, str);
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
        }

        return ret;
    }

    /**
     * <p>
     * generateFulltext.
     * </p>
     *
     * @param fileName a {@link java.lang.String} object.
     * @param folder a {@link java.nio.file.Path} object.
     * @param warnIfMissing a boolean.
     * @param forceDefaultCharset If true, files will be force converted to UTF-8, if different charset detected
     * @should return text if fulltext file exists
     * @should return null if fulltext folder exists but no file
     * @should return null of fulltext folder does not exist
     * @return a {@link java.lang.String} object.
     */
    public static String generateFulltext(String fileName, Path folder, boolean warnIfMissing, boolean forceDefaultCharset) {
        if (!Files.isDirectory(folder)) {
            return null;
        }

        String text = null;
        Path txt = Paths.get(folder.toAbsolutePath().toString(), fileName);
        if (Files.isRegularFile(txt)) {
            try {
                text = FileTools.readFileToString(txt.toFile(), forceDefaultCharset ? DEFAULT_CHARSET : null);
            } catch (IOException e) {
                logger.error("{}: {}", e.getMessage(), txt.toAbsolutePath());
            }
        } else if (warnIfMissing) {
            logger.warn("File not found: {}", txt.toAbsolutePath());
        }

        return text;
    }

    /**
     * Strips the given string of HTML tags, etc.
     *
     * @param text a {@link java.lang.String} object.
     * @should clean up string correctly
     * @return a {@link java.lang.String} object.
     */
    public static String cleanUpHtmlTags(String text) {
        // Workaround for strings containing just opening brackets because JSoup cuts off everything that comes after
        if (text.contains("<") && !text.contains(">")) {
            text = text.replace("<", "");
        }

        return Jsoup.parse(text).text();
    }
}
