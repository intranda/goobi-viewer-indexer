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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdom2.Attribute;
import org.jdom2.CDATA;
import org.jdom2.Comment;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.Text;
import org.jdom2.filter.Filter;
import org.jdom2.filter.Filters;
import org.jdom2.output.XMLOutputter;
import org.jdom2.xpath.XPathBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import io.goobi.viewer.indexer.SolrIndexerDaemon;

/**
 * <p>
 * JDomXP class.
 * </p>
 */
public class JDomXP {

    public enum FileFormat {
        UNKNOWN,
        METS,
        METS_MARC,
        LIDO,
        EAD,
        EAD3,
        DENKXWEB,
        DUBLINCORE,
        WORLDVIEWS,
        CMS,
        ALTO,
        ABBYYXML,
        TEI,
        MIX;

        public static FileFormat getByName(String name) {
            if (name == null) {
                return UNKNOWN;
            }

            switch (name.toUpperCase()) {
                case "METS":
                    return METS;
                case "METS_MARC":
                    return METS_MARC;
                case "LIDO":
                    return LIDO;
                case "EAD":
                    return EAD;
                case "DENKXWEB":
                    return DENKXWEB;
                case "DUBLINCORE":
                    return DUBLINCORE;
                case "WORLDVIEWS":
                    return WORLDVIEWS;
                case "CMS":
                    return CMS;
                case "ABBYY", "ABBYYXML":
                    return ABBYYXML;
                case "TEI":
                    return TEI;
                case "MIX":
                    return MIX;
                default:
                    return UNKNOWN;
            }
        }
    }

    /** Logger for this class. */
    private static final Logger logger = LogManager.getLogger(JDomXP.class);

    private static final String XPATH_TEXT = "/text()"; //NOSONAR XPath expression, not URI

    private Document doc;

    /**
     * Constructor that reads a Document from the given file.
     *
     * @param file a {@link java.io.File} object.
     * @throws org.jdom2.JDOMException in case of errors.
     * @throws java.io.IOException in case of errors.
     */
    public JDomXP(File file) throws JDOMException, IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            doc = XmlTools.getSAXBuilder().build(fis);
        }
    }

    /**
     * Constructor that takes an existing Document.
     *
     * @param doc a {@link org.jdom2.Document} object.
     */
    public JDomXP(Document doc) {
        this.doc = doc;
    }

    /**
     *
     * Generic return type XPath evaluation.
     *
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @return {@link java.util.List}
     */
    public List<Object> evaluate(String expr, final Object parent) {
        return evaluate(expr, parent != null ? parent : doc, Filters.fpassthrough());
    }

    /**
     * XPath evaluation with a given return type filter.
     * 
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @param filter Return type filter.
     * @return List<Object>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static List<Object> evaluate(String expr, Object parent, Filter filter) {
        if (expr == null) {
            throw new IllegalArgumentException("expr may not be null");
        }

        XPathBuilder<Object> builder = new XPathBuilder<>(expr.trim().replace("\n", ""), filter);
        // Add all namespaces
        for (String key : SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().keySet()) {
            Namespace value = SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get(key);
            builder.setNamespace(value.getPrefix(), value.getURI());
        }
        XPathExpression<Object> xpath = builder.compileWith(XPathFactory.instance());
        return xpath.evaluate(parent);

    }

    /**
     * Evaluates the given XPath expression to a list of elements. Defaults to root element if no node was given in <code>parent</code>.
     *
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @return {@link java.util.ArrayList} or null
     * @should return all values
     */
    public List<Element> evaluateToElements(String expr, final Object parent) {
        return evaluateToElementsStatic(expr, parent != null ? parent : doc);
    }

    /**
     * Evaluates the given XPath expression to a list of elements.
     *
     * @param expr XPath expression to evaluate.
     * @param parent The expression is evaluated relative to this element.
     * @return {@link java.util.ArrayList} or null
     */
    public static List<Element> evaluateToElementsStatic(String expr, Object parent) {
        List<Element> retList = new ArrayList<>();

        List<Object> list = evaluate(expr, parent, Filters.element());
        if (list == null) {
            return Collections.emptyList();
        }
        for (Object object : list) {
            if (object instanceof Element element) {
                retList.add(element);
            }
        }

        return retList;
    }

    /**
     * Evaluates the given XPath expression to a list of attributes.
     *
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @return {@link java.util.ArrayList} or null
     * @should return all values
     */
    public List<Attribute> evaluateToAttributes(String expr, final Object parent) {
        List<Attribute> retList = new ArrayList<>();
        List<Object> list = evaluate(expr, parent != null ? parent : doc, Filters.attribute());
        if (list == null) {
            return Collections.emptyList();
        }
        for (Object object : list) {
            if (object instanceof Attribute attr) {
                retList.add(attr);
            }
        }

        return retList;
    }

    /**
     * Evaluates the given XPath expression to a string value of an XML attribute particularly.
     *
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @return a {@link java.lang.String} object.
     * @should return value correctly
     */
    public String evaluateToAttributeStringValue(String expr, final Object parent) {
        List<Object> list = evaluate(expr, parent != null ? parent : doc, Filters.attribute());
        if (list == null || list.isEmpty()) {
            return null;
        }
        if (objectToString(list.get(0)) != null) {
            return objectToString(list.get(0));
        }

        return "";
    }

    /**
     * Evaluates the given XPath expression to a single string. Does not work for attribute values (use <code>evaluateToAttributeStringValue</code>).
     *
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @return {@link java.lang.String} or null
     * @should return value correctly
     * @should convert string to NFC
     */
    public String evaluateToString(final String expr, final Object parent) {
        if (expr == null) {
            return "";
        }
        
        // JDOM2 requires '/text()' for string evaluation
        String expression  = expr;
        if (!expression.endsWith(XPATH_TEXT)) {
            expression += XPATH_TEXT;
        }
        List<Object> list = evaluate(expression, parent != null ? parent : doc, Filters.text());
        if (list == null || list.isEmpty()) {
            return null;
        }
        if (objectToString(list.get(0)) != null) {
            return objectToString(list.get(0));
        }
        return "";
    }

    /**
     * Evaluates the given XPath expression to a list of strings.
     *
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @return {@link java.util.ArrayList} or null
     * @should return all values
     * @should convert strings to NFC
     */
    public List<String> evaluateToStringList(String expr, final Object parent) {
        return evaluateToStringListStatic(expr, parent != null ? parent : doc);
    }

    /**
     * Evaluates the given XPath expression to a list of strings.
     *
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @return {@link java.util.ArrayList} or null
     */
    public static List<String> evaluateToStringListStatic(String expr, Object parent) {
        List<Object> list = evaluate(expr, parent, Filters.fpassthrough());
        if (list == null) {
            return Collections.emptyList();
        }

        List<String> retList = new ArrayList<>();
        for (Object object : list) {
            retList.add(objectToString(object));
        }

        return retList;
    }

    /**
     * Evaluates given XPath expression to the first found CDATA element.
     *
     * @param expr a {@link java.lang.String} object.
     * @param parent a {@link java.lang.Object} object.
     * @return a {@link java.lang.String} object.
     * @should return value correctly
     */
    public String evaluateToCdata(final String expr, final Object parent) {
        if (expr == null) {
            return "";
        }
        // JDOM2 requires '/text()' for string evaluation
        String useExpr = expr;
        if (!useExpr.endsWith(XPATH_TEXT)) {
            useExpr += XPATH_TEXT;
        }
        List<Object> list = evaluate(useExpr, parent != null ? parent : doc, Filters.cdata());
        if (list == null || list.isEmpty()) {
            return null;
        }
        if (objectToString(list.get(0)) != null) {
            return objectToString(list.get(0));
        }
        return "";
    }

    /**
     * Returns the string value of the given XML node object, depending on its type.
     *
     * @param object a {@link java.lang.Object} object.
     * @return String
     * @should convert string to NFC
     */
    public static String objectToString(Object object) {
        if (object instanceof Element ele) {
            return TextHelper.normalizeSequence(ele.getText());
        } else if (object instanceof Attribute attr) {
            return TextHelper.normalizeSequence(attr.getValue());
        } else if (object instanceof Text text) {
            return TextHelper.normalizeSequence(text.getText());
        } else if (object instanceof CDATA cdata) {
            return TextHelper.normalizeSequence(cdata.getText());
        } else if (object instanceof Comment comment) {
            return TextHelper.normalizeSequence(comment.getText());
        } else if (object instanceof Double) {
            return String.valueOf(object);
        } else if (object instanceof Boolean) {
            return String.valueOf(object);
        } else if (object instanceof String s) {
            return TextHelper.normalizeSequence(s);
        } else if (object != null) {
            logger.error("Unknown object type: {}", object.getClass().getName());
            return null;
        } else {
            return null;
        }

    }

    /**
     * Outputs the XML document contained in this object to a file.
     *
     * @param filename a {@link java.lang.String} object.
     */
    public void writeDocumentToFile(String filename) {
        writeXmlFile(doc, filename);
    }

    /**
     * <p>
     * getRootElement.
     * </p>
     *
     * @return a {@link org.jdom2.Element} object.
     */
    public Element getRootElement() {
        if (doc != null) {
            return doc.getRootElement();
        }

        return null;
    }

    /**
     * Returns the mets:dmdSec element with the given DMDID
     *
     * @param dmdId a {@link java.lang.String} object.
     * @return a {@link org.jdom2.Element} object.
     * @should return mdWrap correctly
     */
    public Element getMdWrap(String dmdId) {
        List<Element> ret = evaluateToElements("mets:mets/mets:dmdSec[@ID='" + dmdId + "']/mets:mdWrap[@MDTYPE='MODS' or @MDTYPE='MARC']", null);
        if (ret != null && !ret.isEmpty()) {
            return ret.get(0);
        }

        return null;
    }

    /**
     * Splits a multi-record LIDO document into a list of individual record documents.
     *
     * @param file a {@link java.io.File} object.
     * @return a {@link java.util.List} object.
     * @should split multi record documents correctly
     * @should leave single record documents as is
     * @should return empty list for non lido documents
     * @should return empty list for non-existing files
     * @should return empty list given null
     */
    public static List<Document> splitLidoFile(File file) {
        if (file == null) {
            return Collections.emptyList();
        }

        try {
            JDomXP xp = new JDomXP(file);
            if (xp.doc.getRootElement().getName().equals("lidoWrap")) {
                // Multiple LIDO document file
                Namespace nsXsi = Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
                List<Element> lidoElements =
                        xp.doc.getRootElement().getChildren("lido", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("lido"));
                if (lidoElements != null) {
                    List<Document> ret = new ArrayList<>(lidoElements.size());
                    for (Element eleLidoDoc : lidoElements) {
                        Document doc = new Document();
                        doc.setRootElement(eleLidoDoc.clone());
                        doc.getRootElement().addNamespaceDeclaration(nsXsi);
                        doc.getRootElement()
                                .setAttribute(new Attribute("schemaLocation",
                                        "http://www.lido-schema.org http://www.lido-schema.org/schema/v1.0/lido-v1.0.xsd", nsXsi));
                        ret.add(doc);
                    }
                    return ret;
                }
            } else if (xp.doc.getRootElement().getName().equals("lido")) {
                // Single LIDO document file
                return Collections.singletonList(xp.doc);
            } else {
                logger.warn("Unknown root element: {}", xp.doc.getRootElement().getName());
            }
        } catch (IOException | JDOMException e) {
            logger.error(e.getMessage());
        }

        return Collections.emptyList();
    }

    /**
     * Splits a multi-record DenkXweb document into a list of individual record documents.
     *
     * @param file a {@link java.io.File} object.
     * @return a {@link java.util.List} object.
     * @should split multi record documents correctly
     * @should leave single record documents as is
     * @should return empty list for non lido documents
     * @should return empty list for non-existing files
     * @should return empty list given null
     */
    public static List<Document> splitDenkXwebFile(File file) {
        if (file == null) {
            return Collections.emptyList();
        }

        try {
            JDomXP xp = new JDomXP(file);
            if (xp.doc.getRootElement().getName().equals("monuments")) {
                // Multiple DenkXweb document file
                List<Element> eleListRecord =
                        xp.doc.getRootElement()
                                .getChildren("monument", SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("denkxweb"));
                if (eleListRecord != null) {
                    List<Document> ret = new ArrayList<>(eleListRecord.size());
                    for (Element eleDoc : eleListRecord) {
                        Document doc = new Document();
                        doc.setRootElement(eleDoc.clone());
                        ret.add(doc);
                    }
                    return ret;
                }
            } else if (xp.doc.getRootElement().getName().equals("monument")) {
                // Single DenkXweb document file
                return Collections.singletonList((xp.doc));
            } else {
                logger.warn("Unknown root element: {}", xp.doc.getRootElement().getName());
            }
        } catch (IOException | JDOMException e) {
            logger.error(e.getMessage());
        }

        return Collections.emptyList();
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
     * @should build document correctly
     */
    public static Document readXmlFile(String filePath) throws IOException, JDOMException {
        try (FileInputStream fis = new FileInputStream(new File(filePath))) {
            return XmlTools.getSAXBuilder().build(fis);
        }
    }

    /**
     * Writes the given Document to an XML file with the given path.
     *
     * @param doc The document to write.
     * @param filePath The file path to which to write the document.
     * @return true if successful; false otherwise.
     * @should write xml file correctly
     */
    public static boolean writeXmlFile(Document doc, String filePath) {
        if (doc != null) {
            try (FileWriterWithEncoding writer =
                    FileWriterWithEncoding.builder()
                            .setFile(filePath)
                            .setCharset(StandardCharsets.UTF_8)
                            .setCharsetEncoder(StandardCharsets.UTF_8.newEncoder())
                            .setAppend(false)
                            .get()) {
                new XMLOutputter().output(doc, writer);
                return true;
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        return false;
    }

    /**
     * Determines the format of the given XML file by checking for namespaces.
     *
     * @param file a {@link java.io.File} object.
     * @return a {@link io.goobi.viewer.indexer.helper.JDomXP.FileFormat} object.
     * @throws java.io.IOException
     * @should detect mets mods files correctly
     * @should detect mets marc files correctly
     * @should detect lido files correctly
     * @should detect ead2 files correctly
     * @should detect ead3 files correctly
     * @should detect denkxweb files correctly
     * @should detect dublin core files correctly
     * @should detect worldviews files correctly
     * @should detect abbyy files correctly
     * @should detect tei files correctly
     * @should detect cms files correctly
     */
    public static FileFormat determineFileFormat(File file) throws IOException {
        try {
            JDomXP xp = new JDomXP(file);
            if (xp.doc.getRootElement() == null) {
                return FileFormat.UNKNOWN;
            }

            if (xp.doc.getRootElement().getNamespace("mets") != null) {
                // Records containing both MODS and MARC should be treated as METS/MODS
                List<Element> elements = evaluateToElementsStatic("mets:dmdSec/mets:mdWrap[@MDTYPE='MODS']", xp.doc.getRootElement());
                if (elements != null && !elements.isEmpty()) {
                    return FileFormat.METS;
                }
                elements = evaluateToElementsStatic("mets:dmdSec/mets:mdWrap[@MDTYPE='MARC']", xp.doc.getRootElement());
                if (elements != null && !elements.isEmpty()) {
                    return FileFormat.METS_MARC;
                }
                return FileFormat.METS;
            }
            if (xp.doc.getRootElement().getNamespace("lido") != null) {
                return FileFormat.LIDO;
            }
            if (xp.doc.getRootElement().getNamespace() != null
                    && (xp.doc.getRootElement().getNamespace().getURI().equals("urn:isbn:1-931666-22-9"))) {
                return FileFormat.EAD;
            }
            if (xp.doc.getRootElement().getNamespace() != null
                    && (xp.doc.getRootElement().getNamespace().getURI().equals("http://ead3.archivists.org/schema/")
                            || xp.doc.getRootElement().getNamespace().getURI().equals("https://archivists.org/ns/ead/v4"))) {
                return FileFormat.EAD3;
            }
            if (xp.doc.getRootElement().getNamespace() != null
                    && xp.doc.getRootElement().getNamespace().getURI().equals("http://denkxweb.de/")) {
                return FileFormat.DENKXWEB;
            }
            if (xp.doc.getRootElement().getNamespace("dc") != null) {
                return FileFormat.DUBLINCORE;
            }
            if (xp.doc.getRootElement().getName().equals("worldviews")) {
                return FileFormat.WORLDVIEWS;
            }
            if (xp.doc.getRootElement().getNamespace().getURI().contains("abbyy")) {
                return FileFormat.ABBYYXML;
            }
            if (xp.doc.getRootElement().getName().equals("TEI.2")) {
                return FileFormat.TEI;
            }
            if (xp.doc.getRootElement().getName().equals("cmsPage")) {
                return FileFormat.CMS;
            }
        } catch (JDOMException e) {
            logger.error(e.getMessage());
        }

        return FileFormat.UNKNOWN;
    }
}
