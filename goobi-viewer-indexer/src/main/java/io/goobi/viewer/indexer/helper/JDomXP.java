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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.output.FileWriterWithEncoding;
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
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.XMLOutputter;
import org.jdom2.xpath.XPathBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.goobi.viewer.indexer.model.FatalIndexerException;

/**
 * <p>JDomXP class.</p>
 *
 */
public class JDomXP {

    public enum FileFormat {
        UNKNOWN,
        METS,
        LIDO,
        DENKXWEB,
        WORLDVIEWS,
        ALTO,
        ABBYYXML,
        TEI;

        public static FileFormat getByName(String name) {
            if (name == null) {
                return UNKNOWN;
            }

            switch (name.toUpperCase()) {
                case "METS":
                    return METS;
                case "LIDO":
                    return LIDO;
                case "DENKXWEB":
                    return DENKXWEB;
                case "WORLDVIEWS":
                    return WORLDVIEWS;
                case "ABBYY":
                case "ABBYYXML":
                    return ABBYYXML;
                case "TEI":
                    return TEI;
                default:
                    return UNKNOWN;
            }
        }
    }

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(JDomXP.class);

    private Document doc;

    /**
     * Constructor that reads a Document from the given file.
     *
     * @param file a {@link java.io.File} object.
     * @throws org.jdom2.JDOMException in case of errors.
     * @throws java.io.FileNotFoundException in case of errors.
     * @throws java.io.IOException in case of errors.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     */
    public JDomXP(File file) throws JDOMException, FileNotFoundException, IOException, FatalIndexerException {
        SAXBuilder builder = new SAXBuilder();
        try (FileInputStream fis = new FileInputStream(file)) {
            doc = builder.build(fis);
        }
    }

    /**
     * Constructor that takes an existing Document.
     *
     * @param doc a {@link org.jdom2.Document} object.
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     */
    public JDomXP(Document doc) throws FatalIndexerException {
        this.doc = doc;
    }

    /**
     *
     * Generic return type XPath evaluation.
     *
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @return {@link java.util.List}
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     */
    public List<Object> evaluate(String expr, Object parent) throws FatalIndexerException {
        if (parent == null) {
            parent = doc;
        }
        return evaluate(expr, parent, Filters.fpassthrough());
    }

    /**
     * XPath evaluation with a given return type filter.
     * 
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @param filter Return type filter.
     * @return
     * @throws FatalIndexerException
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static List<Object> evaluate(String expr, Object parent, Filter filter) throws FatalIndexerException {
        XPathBuilder<Object> builder = new XPathBuilder<>(expr.trim().replace("\n", ""), filter);
        // Add all namespaces
        for (String key : Configuration.getInstance().getNamespaces().keySet()) {
            Namespace value = Configuration.getInstance().getNamespaces().get(key);
            builder.setNamespace(key, value.getURI());
        }
        XPathExpression<Object> xpath = builder.compileWith(XPathFactory.instance());
        return xpath.evaluate(parent);

    }

    /**
     * Evaluates the given XPath expression to a list of elements.
     *
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @return {@link java.util.ArrayList} or null
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should return all values
     */
    public List<Element> evaluateToElements(String expr, Object parent) throws FatalIndexerException {
        List<Element> retList = new ArrayList<>();
        if (parent == null) {
            parent = doc;
        }
        List<Object> list = evaluate(expr, parent, Filters.element());
        if (list == null) {
            return null;
        }
        for (Object object : list) {
            if (object instanceof Element) {
                Element element = (Element) object;
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
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should return all values
     */
    public List<Attribute> evaluateToAttributes(String expr, Object parent) throws FatalIndexerException {
        List<Attribute> retList = new ArrayList<>();
        if (parent == null) {
            parent = doc;
        }
        List<Object> list = evaluate(expr, parent, Filters.attribute());
        if (list == null) {
            return null;
        }
        for (Object object : list) {
            if (object instanceof Attribute) {
                Attribute attr = (Attribute) object;
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
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should return value correctly
     * @return a {@link java.lang.String} object.
     */
    public String evaluateToAttributeStringValue(String expr, Object parent) throws FatalIndexerException {
        if (parent == null) {
            parent = doc;
        }
        List<Object> list = evaluate(expr, parent, Filters.attribute());
        if (list == null || list.isEmpty()) {
            return null;
        }
        if (objectToString(list.get(0)) != null) {
            return objectToString(list.get(0));
        }

        return "";
    }

    /**
     * Evaluates the given XPath expression to a single string.
     *
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @return {@link java.lang.String} or null
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should return value correctly
     * @should convert string to NFC
     */
    public String evaluateToString(String expr, Object parent) throws FatalIndexerException {
        if (parent == null) {
            parent = doc;
        }
        // JDOM2 requires '/text()' for string evaluation
        if (expr != null && !expr.endsWith("/text()")) {
            expr += "/text()";
        }
        List<Object> list = evaluate(expr, parent, Filters.text());
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
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should return all values
     * @should convert strings to NFC
     */
    public List<String> evaluateToStringList(String expr, Object parent) throws FatalIndexerException {
        if (parent == null) {
            parent = doc;
        }
        
        return evaluateToStringListStatic(expr, parent);
    }
    
    /**
     * Evaluates the given XPath expression to a list of strings.
     *
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @return {@link java.util.ArrayList} or null
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     */
    public static List<String> evaluateToStringListStatic(String expr, Object parent) throws FatalIndexerException {
        List<Object> list = evaluate(expr, parent, Filters.fpassthrough());
        if (list == null) {
            return null;
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
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should return value correctly
     * @return a {@link java.lang.String} object.
     */
    public String evaluateToCdata(String expr, Object parent) throws FatalIndexerException {
        if (parent == null) {
            parent = doc;
        }
        // JDOM2 requires '/text()' for string evaluation
        if (expr != null && !expr.endsWith("/text()")) {
            expr += "/text()";
        }
        List<Object> list = evaluate(expr, parent, Filters.cdata());
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
        if (object instanceof Element) {
            return TextHelper.normalizeSequence(((Element) object).getText());
        } else if (object instanceof Attribute) {
            return TextHelper.normalizeSequence(((Attribute) object).getValue());
        } else if (object instanceof Text) {
            return TextHelper.normalizeSequence(((Text) object).getText());
        } else if (object instanceof CDATA) {
            return TextHelper.normalizeSequence(((CDATA) object).getText());
        } else if (object instanceof Comment) {
            return TextHelper.normalizeSequence(((Comment) object).getText());
        } else if (object instanceof Double) {
            return String.valueOf(object);
        } else if (object instanceof Boolean) {
            return String.valueOf(object);
        } else if (object instanceof String) {
            return TextHelper.normalizeSequence((String) object);
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
     * @throws java.io.IOException
     */
    public void writeDocumentToFile(String filename) throws IOException {
        writeXmlFile(doc, filename);
    }

    /**
     * <p>getRootElement.</p>
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
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should return mdWrap correctly
     * @return a {@link org.jdom2.Element} object.
     */
    public Element getMdWrap(String dmdId) throws FatalIndexerException {
        List<Element> ret = evaluateToElements("mets:mets/mets:dmdSec[@ID='" + dmdId + "']/mets:mdWrap[@MDTYPE='MODS']", null);
        if (ret != null && !ret.isEmpty()) {
            return ret.get(0);
        }

        return null;
    }

    /**
     * Splits a multi-record LIDO document into a list of individual record documents.
     *
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should split multi record documents correctly
     * @should leave single record documents as is
     * @should return empty list for non lido documents
     * @should return empty list for non-existing files
     * @should return empty list given null
     * @param file a {@link java.io.File} object.
     * @return a {@link java.util.List} object.
     */
    public static List<Document> splitLidoFile(File file) throws FatalIndexerException {
        if (file == null) {
            return Collections.emptyList();
        }

        try {
            JDomXP xp = new JDomXP(file);
            if (xp.doc.getRootElement().getName().equals("lidoWrap")) {
                // Multiple LIDO document file
                Namespace nsXsi = Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
                List<Element> lidoElements = xp.doc.getRootElement().getChildren("lido", Configuration.getInstance().getNamespaces().get("lido"));
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
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (JDOMException e) {
            logger.error(e.getMessage());
        }

        return Collections.emptyList();
    }

    /**
     * Splits a multi-record DenkXweb document into a list of individual record documents.
     *
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should split multi record documents correctly
     * @should leave single record documents as is
     * @should return empty list for non lido documents
     * @should return empty list for non-existing files
     * @should return empty list given null
     * @param file a {@link java.io.File} object.
     * @return a {@link java.util.List} object.
     */
    public static List<Document> splitDenkXwebFile(File file) throws FatalIndexerException {
        if (file == null) {
            return Collections.emptyList();
        }

        try {
            JDomXP xp = new JDomXP(file);
            if (xp.doc.getRootElement().getName().equals("monuments")) {
                // Multiple DenkXweb document file
                //                Namespace nsXsi = Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
                List<Element> eleListRecord =
                        xp.doc.getRootElement().getChildren("monument", Configuration.getInstance().getNamespaces().get("denkxweb"));
                if (eleListRecord != null) {
                    List<Document> ret = new ArrayList<>(eleListRecord.size());
                    for (Element eleDoc : eleListRecord) {
                        Document doc = new Document();
                        doc.setRootElement(eleDoc.clone());
                        //                        doc.getRootElement().addNamespaceDeclaration(nsXsi);
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
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (JDOMException e) {
            logger.error(e.getMessage());
        }

        return Collections.emptyList();
    }

    /**
     * <p>readXmlFile.</p>
     *
     * @param filePath a {@link java.lang.String} object.
     * @throws java.io.FileNotFoundException if file not found
     * @throws java.io.IOException in case of errors
     * @throws org.jdom2.JDOMException
     * @should build document correctly
     * @should throw FileNotFoundException if file not found
     * @return a {@link org.jdom2.Document} object.
     */
    public static Document readXmlFile(String filePath) throws FileNotFoundException, IOException, JDOMException {
        try (FileInputStream fis = new FileInputStream(new File(filePath))) {
            return new SAXBuilder().build(fis);
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
            try (FileWriterWithEncoding writer = new FileWriterWithEncoding(filePath, "UTF8")) {
                new XMLOutputter().output(doc, writer);
                return true;
            } catch (FileNotFoundException e) {
                logger.error(e.getMessage());
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
     * @throws java.io.IOException
     * @throws io.goobi.viewer.indexer.model.FatalIndexerException
     * @should detect mets files correctly
     * @should detect lido files correctly
     * @should detect denkxweb files correctly
     * @should detect worldviews files correctly
     * @should detect abbyy files correctly
     * @should detect tei files correctly
     * @return a {@link io.goobi.viewer.indexer.helper.JDomXP.FileFormat} object.
     */
    public static FileFormat determineFileFormat(File file) throws IOException, FatalIndexerException {
        try {
            JDomXP xp = new JDomXP(file);
            if (xp.doc.getRootElement() == null) {
                return FileFormat.UNKNOWN;
            }

            if (xp.doc.getRootElement().getNamespace("mets") != null) {
                return FileFormat.METS;
            }
            if (xp.doc.getRootElement().getNamespace("lido") != null) {
                return FileFormat.LIDO;
            }
            if (xp.doc.getRootElement().getNamespace() != null
                    && xp.doc.getRootElement().getNamespace().getURI().toString().equals("http://denkxweb.de/")) {
                return FileFormat.DENKXWEB;
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
        } catch (JDOMException e) {
            logger.error(e.getMessage());
        }

        return FileFormat.UNKNOWN;
    }
}
