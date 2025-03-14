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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.filter.Filter;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.jdom2.xpath.XPathBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

/**
 * XML utilities.
 */
public final class XmlTools {

    private static final Logger logger = LogManager.getLogger(XmlTools.class);

    /** Private constructor. */
    private XmlTools() {
    }

    public static SAXBuilder getSAXBuilder() {
        SAXBuilder builder = new SAXBuilder();
        // Disable access to external entities
        builder.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        builder.setFeature("http://xml.org/sax/features/external-general-entities", false);
        builder.setFeature("http://xml.org/sax/features/external-parameter-entities", false);

        return builder;
    }

    /**
     * <p>
     * readXmlFile.
     * </p>
     *
     * @param filePath a {@link java.lang.String} object.
     * @return a {@link org.jdom2.Document} object.
     * @throws java.io.IOException in case of errors
     * @throws org.jdom2.JDOMException
     * @should build document from string correctly
     * @should throw FileNotFoundException if file not found
     */
    public static Document readXmlFile(String filePath) throws IOException, JDOMException {
        try (FileInputStream fis = new FileInputStream(new File(filePath))) {
            return getSAXBuilder().build(fis);
        }
    }

    /**
     * Reads an XML document from the given URL and returns a JDOM2 document. Works with XML files within JARs.
     *
     * @param url a {@link java.net.URL} object.
     * @return a {@link org.jdom2.Document} object.
     * @throws java.io.IOException
     * @throws org.jdom2.JDOMException
     * @should build document from url correctly
     */
    public static Document readXmlFile(URL url) throws IOException, JDOMException {
        try (InputStream is = url.openStream()) {
            return getSAXBuilder().build(is);
        }
    }

    /**
     * <p>
     * readXmlFile.
     * </p>
     *
     * @param path a {@link java.nio.file.Path} object.
     * @return a {@link org.jdom2.Document} object.
     * @throws java.io.IOException
     * @throws org.jdom2.JDOMException
     * @should build document from path correctly
     * @should throw IOException if file not found
     */
    public static Document readXmlFile(Path path) throws IOException, JDOMException {
        try (InputStream is = Files.newInputStream(path)) {
            return getSAXBuilder().build(is);
        }
    }

    /**
     * <p>
     * writeXmlFile.
     * </p>
     *
     * @param doc a {@link org.jdom2.Document} object.
     * @param filePath a {@link java.lang.String} object.
     * @return a {@link java.io.File} object.
     * @throws java.io.IOException
     * @should write file correctly
     * @should throw FileNotFoundException if file is directory
     */
    public static File writeXmlFile(Document doc, String filePath) throws IOException {
        return FileTools.getFileFromString(getStringFromElement(doc, TextHelper.DEFAULT_CHARSET), filePath, TextHelper.DEFAULT_CHARSET, false);
    }

    /**
     * Create a JDOM document from an XML string.
     *
     * @param string a {@link java.lang.String} object.
     * @param encoding a {@link java.lang.String} object.
     * @return a {@link org.jdom2.Document} object.
     * @throws java.io.IOException
     * @throws org.jdom2.JDOMException
     * @should build document correctly
     */
    public static Document getDocumentFromString(String string, final String encoding) throws JDOMException, IOException {
        byte[] byteArray = null;
        try {
            byteArray = string.getBytes(encoding != null ? encoding : TextHelper.DEFAULT_CHARSET);
            ByteArrayInputStream baos = new ByteArrayInputStream(byteArray);
            return getSAXBuilder().build(baos);
        } catch (UnsupportedEncodingException e) {
            logger.error(e.getMessage());
        }

        return null;
    }

    /**
     * <p>
     * getStringFromElement.
     * </p>
     *
     * @param element a {@link java.lang.Object} object.
     * @param encoding a {@link java.lang.String} object.
     * @should return XML string correctly for documents
     * @should return XML string correctly for elements
     * @return a {@link java.lang.String} object.
     */
    public static String getStringFromElement(Object element, final String encoding) {
        if (element == null) {
            throw new IllegalArgumentException("element may not be null");
        }

        Format format = Format.getRawFormat();
        XMLOutputter outputter = new XMLOutputter(format);
        Format xmlFormat = outputter.getFormat();
        xmlFormat.setEncoding(encoding != null ? encoding : TextHelper.DEFAULT_CHARSET);
        xmlFormat.setExpandEmptyElements(true);
        outputter.setFormat(xmlFormat);

        String docString = null;
        if (element instanceof Document doc) {
            docString = outputter.outputString(doc);
        } else if (element instanceof Element e) {
            docString = outputter.outputString(e);
        }

        return docString;
    }

    /**
     * Evaluates the given XPath expression to a list of elements.
     *
     * @param expr XPath expression to evaluate.
     * @param namespaces a {@link java.util.List} object.
     * @return {@link java.util.ArrayList} or null
     * @should return all values
     * @param element a {@link org.jdom2.Element} object.
     */
    public static List<Element> evaluateToElements(String expr, Element element, List<Namespace> namespaces) {
        List<Element> retList = new ArrayList<>();

        List<Object> list = evaluate(expr, element, Filters.element(), namespaces);
        if (list == null) {
            return retList;
        }
        for (Object object : list) {
            if (object instanceof Element e) {
                retList.add(e);
            }
        }

        return retList;
    }

    /**
     * XPath evaluation with a given return type filter.
     *
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @param filter Return type filter.
     * @param namespaces a {@link java.util.List} object.
     * @return a {@link java.util.List} object.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static List<Object> evaluate(String expr, Object parent, Filter filter, List<Namespace> namespaces) {
        XPathBuilder<Object> builder = new XPathBuilder<>(expr.trim().replace("\n", ""), filter);

        if (namespaces != null && !namespaces.isEmpty()) {
            builder.setNamespaces(namespaces);
        }

        XPathExpression<Object> xpath = builder.compileWith(XPathFactory.instance());
        return xpath.evaluate(parent);

    }

    /**
     * <p>
     * determineFileFormat.
     * </p>
     *
     * @param xml a {@link java.lang.String} object.
     * @param encoding a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     * @throws org.jdom2.JDOMException if any.
     * @throws java.io.IOException if any.
     */
    public static String determineFileFormat(String xml, String encoding) throws JDOMException, IOException {
        if (xml == null) {
            return null;
        }
        Document doc = getDocumentFromString(xml, encoding);
        return determineFileFormat(doc);
    }

    /**
     * Determines the format of the given XML file by checking for namespaces.
     *
     * @should detect mets files correctly
     * @should detect lido files correctly
     * @should detect abbyy files correctly
     * @should detect tei files correctly
     * @param doc a {@link org.jdom2.Document} object.
     * @return a {@link java.lang.String} object.
     */
    public static String determineFileFormat(Document doc) {
        if (doc == null || doc.getRootElement() == null) {
            return null;
        }

        if (doc.getRootElement().getNamespace("mets") != null) {
            return "METS";
        }
        if (doc.getRootElement().getNamespace("lido") != null) {
            return "LIDO";
        }
        if (doc.getRootElement().getNamespace().getURI().contains("abbyy")) {
            return "ABBYYXML";
        }
        if (doc.getRootElement().getName().equals("TEI") || doc.getRootElement().getName().equals("TEI.2")) {
            return "TEI";
        }

        return null;
    }
}
