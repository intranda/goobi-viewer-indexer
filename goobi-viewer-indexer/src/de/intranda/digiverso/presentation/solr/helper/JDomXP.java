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
package de.intranda.digiverso.presentation.solr.helper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import de.intranda.digiverso.presentation.solr.model.FatalIndexerException;

public class JDomXP {

    public enum FileFormat {
        UNKNOWN,
        METS,
        LIDO,
        ALTO,
        ABBYYXML,
        TEI;

        public static FileFormat getByName(String name) {
            if ("METS".equalsIgnoreCase(name)) {
                return METS;
            } else if ("LIDO".equalsIgnoreCase(name)) {
                return LIDO;
            } else if ("ABBYY".equalsIgnoreCase(name) || "ABBYYXML".equalsIgnoreCase(name)) {
                return LIDO;
            } else if ("TEI".equalsIgnoreCase(name)) {
                return LIDO;
            }
            return UNKNOWN;
        }
    }

    /** Logger for this class. */
    private static final Logger logger = LoggerFactory.getLogger(JDomXP.class);

    private Document doc;

    private static Map<String, Namespace> namespaces;

    /**
     * Constructor that reads a Document from the given file.
     * 
     * @param file
     * @throws JDOMException in case of errors.
     * @throws FileNotFoundException in case of errors.
     * @throws IOException in case of errors.
     * @throws FatalIndexerException
     */
    public JDomXP(File file) throws JDOMException, FileNotFoundException, IOException, FatalIndexerException {
        if (namespaces == null) {
            initNamespaces();
        }

        SAXBuilder builder = new SAXBuilder();
        try (FileInputStream fis = new FileInputStream(file)) {
            doc = builder.build(fis);
        }
    }

    /**
     * Constructor that takes an existing Document.
     * 
     * @param doc
     * @throws FatalIndexerException
     */
    public JDomXP(Document doc) throws FatalIndexerException {
        if (namespaces == null) {
            initNamespaces();
        }
        this.doc = doc;
    }

    /**
     * Adds relevant XML namespaces to the list of available namespace objects.
     * 
     * @throws FatalIndexerException
     */
    public static void initNamespaces() throws FatalIndexerException {
        namespaces = new HashMap<>();
        getNamespaces().put("mets", Namespace.getNamespace("mets", "http://www.loc.gov/METS/"));
        getNamespaces().put("mods", Namespace.getNamespace("mods", "http://www.loc.gov/mods/v3"));
        getNamespaces().put("gdz", Namespace.getNamespace("gdz", "http://gdz.sub.uni-goettingen.de/"));
        getNamespaces().put("xlink", Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink"));
        getNamespaces().put("dv", Namespace.getNamespace("dv", "http://dfg-viewer.de/"));
        getNamespaces().put("lido", Namespace.getNamespace("lido", "http://www.lido-schema.org"));
        getNamespaces().put("mix", Namespace.getNamespace("mix", "http://www.loc.gov/mix/v20"));
        getNamespaces().put("mm", Namespace.getNamespace("mm", "http://www.mycore.de/metsmaker/v1"));

        Map<String, String> additionalNamespaces = Configuration.getInstance().getListConfiguration("init.namespaces");
        if (additionalNamespaces != null) {
            for (String key : additionalNamespaces.keySet()) {
                getNamespaces().put(key, Namespace.getNamespace(key, additionalNamespaces.get(key)));
                logger.info("Added custom namespace '{}'.", key);
            }
        }
    }

    /***
     * Generic return type XPath evaluation.
     * 
     * @param expr XPath expression to evaluate.
     * @param parent If not null, the expression is evaluated relative to this element.
     * @return {@link List}
     */
    public List<Object> evaluate(String expr, Object parent) {
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
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static List<Object> evaluate(String expr, Object parent, Filter filter) {
        XPathBuilder<Object> builder = new XPathBuilder<>(expr.trim().replace("\n", ""), filter);
        // Add all namespaces
        for (String key : getNamespaces().keySet()) {
            Namespace value = getNamespaces().get(key);
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
     * @return {@link ArrayList} or null
     * @should return all values
     */
    public List<Element> evaluateToElements(String expr, Object parent) {
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
     * @return {@link ArrayList} or null
     * @should return all values
     */
    public List<Attribute> evaluateToAttributes(String expr, Object parent) {
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
     * @return
     * @should return value correctly
     */
    public String evaluateToAttributeStringValue(String expr, Object parent) {
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
     * @return {@link String} or null
     * @should return value correctly
     */
    public String evaluateToString(String expr, Object parent) {
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
     * @return {@link ArrayList} or null
     * @should return all values
     */
    public List<String> evaluateToStringList(String expr, Object parent) {
        if (parent == null) {
            parent = doc;
        }

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
     * @param expr
     * @param parent
     * @return
     * @should return value correctly
     */
    public String evaluateToCdata(String expr, Object parent) {
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
     * @param object
     * @return String
     */
    public static String objectToString(Object object) {
        if (object instanceof Element) {
            return ((Element) object).getText();
        } else if (object instanceof Attribute) {
            return ((Attribute) object).getValue();
        } else if (object instanceof Text) {
            return ((Text) object).getText();
        } else if (object instanceof CDATA) {
            return ((CDATA) object).getText();
        } else if (object instanceof Comment) {
            return ((Comment) object).getText();
        } else if (object instanceof Double) {
            return String.valueOf(object);
        } else if (object instanceof Boolean) {
            return String.valueOf(object);
        } else if (object instanceof String) {
            return (String) object;
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
     * @param filename
     * @throws IOException
     */
    public void writeDocumentToFile(String filename) throws IOException {
        writeXmlFile(doc, filename);
    }

    public Element getRootElement() {
        if (doc != null) {
            return doc.getRootElement();
        }

        return null;
    }

    /**
     * Returns the mets:dmdSec element with the given DMDID
     * 
     * @param dmdId
     * @return
     * @should return mdWrap correctly
     */
    public Element getMdWrap(String dmdId) {
        List<Element> ret = evaluateToElements("mets:mets/mets:dmdSec[@ID='" + dmdId + "']/mets:mdWrap[@MDTYPE='MODS']", null);
        if (ret != null && !ret.isEmpty()) {
            return ret.get(0);
        }

        return null;
    }

    public static Map<String, Namespace> getNamespaces() {
        return namespaces;
    }

    /**
     * Splits a multi-record LIDO document into a list of individual record documents.
     * 
     * @param lidoFile
     * @return
     * @throws FatalIndexerException
     * @should split multi record documents correctly
     * @should leave single record documents as is
     * @should return empty list for non lido documents
     * @should return empty list for non-exsting files
     * @should return empty list given null
     */
    public static List<Document> splitLidoFile(File lidoFile) throws FatalIndexerException {
        List<Document> ret = new ArrayList<>();

        if (lidoFile != null) {
            try {
                JDomXP xp = new JDomXP(lidoFile);
                if (xp.doc.getRootElement().getName().equals("lidoWrap")) {
                    // Multiple LIDO document file
                    Namespace nsXsi = Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
                    List<Element> lidoElements = xp.doc.getRootElement().getChildren("lido", namespaces.get("lido"));
                    if (lidoElements != null) {
                        for (Element eleLidoDoc : lidoElements) {
                            Document doc = new Document();
                            doc.setRootElement(eleLidoDoc.clone());
                            doc.getRootElement().addNamespaceDeclaration(nsXsi);
                            doc.getRootElement().setAttribute(new Attribute("schemaLocation",
                                    "http://www.lido-schema.org http://www.lido-schema.org/schema/v1.0/lido-v1.0.xsd", nsXsi));
                            ret.add(doc);
                        }
                    }
                } else if (xp.doc.getRootElement().getName().equals("lido")) {
                    // Single LIDO document file
                    ret.add(xp.doc);
                } else {
                    logger.error("Unknown root element: {}", xp.doc.getRootElement().getName());
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            } catch (JDOMException e) {
                logger.error(e.getMessage());
            }
        }

        return ret;
    }

    /**
     * 
     * @param filePath
     * @return
     * @throws FileNotFoundException if file not found
     * @throws IOException in case of errors
     * @throws JDOMException
     * @should build document correctly
     * @should throw FileNotFoundException if file not found
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
     * @param file
     * @return
     * @throws IOException
     * @throws FatalIndexerException
     * @should detect mets files correctly
     * @should detect lido files correctly
     * @should detect abbyy files correctly
     * @should detect tei files correctly
     */
    public static FileFormat determineFileFormat(File file) throws IOException, FatalIndexerException {
        try {
            JDomXP xp = new JDomXP(file);
            if (xp.doc.getRootElement() != null) {
                if (xp.doc.getRootElement().getNamespace("mets") != null) {
                    return FileFormat.METS;
                }
                if (xp.doc.getRootElement().getNamespace("lido") != null) {
                    return FileFormat.LIDO;
                }
                if (xp.doc.getRootElement().getNamespace().getURI().contains("abbyy")) {
                    return FileFormat.ABBYYXML;
                }
                if (xp.doc.getRootElement().getName().equals("TEI.2")) {
                    return FileFormat.TEI;
                }
            }
        } catch (JDOMException e) {
            logger.error(e.getMessage());
        }

        return FileFormat.UNKNOWN;
    }
}
