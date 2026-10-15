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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.goobi.viewer.indexer.AbstractTest;
import io.goobi.viewer.indexer.Ead3Indexer;
import io.goobi.viewer.indexer.EadIndexer;
import io.goobi.viewer.indexer.SolrIndexerDaemon;
import io.goobi.viewer.indexer.helper.JDomXP.FileFormat;

class JDomXPTest extends AbstractTest {

    private static final String EAD2_FILE = "src/test/resources/EAD/Akte_Koch.xml";
    private static final String EAD3_FILE = "src/test/resources/EAD/EAD3_example.xml";
    private static final String EAD_C_XPATH = "ead:ead/ead:archdesc/ead:dsc/ead:c";

    /**
     * @see JDomXP#addNamespace(Namespace)
     * @verifies override global namespace for same prefix
     */
    @Test
    void addNamespace_shouldOverrideGlobalNamespaceForSamePrefix() throws Exception {
        // EAD3 document: its 'ead:' elements live in the EAD3 namespace.
        // Without an instance binding the 'ead' prefix resolves to the global default (EAD2) and matches nothing.
        JDomXP ead3 = new JDomXP(new File(EAD3_FILE));
        Assertions.assertTrue(ead3.evaluateToElements(EAD_C_XPATH, null).isEmpty());
        // With an EAD3 instance binding the same expression resolves correctly.
        ead3.addNamespace(Ead3Indexer.NAMESPACE_EAD3);
        Assertions.assertFalse(ead3.evaluateToElements(EAD_C_XPATH, null).isEmpty());

        // Cross-namespace guard: the element name 'archdesc' exists in both the EAD2 and EAD3 namespaces.
        // The EAD2 document's elements must only match when the prefix is bound to the EAD2 namespace,
        // never when it is bound to EAD3 (the previous namespace-blind behaviour would have matched either).
        JDomXP ead2 = new JDomXP(new File(EAD2_FILE));
        ead2.addNamespace(EadIndexer.NAMESPACE_EAD2);
        Assertions.assertFalse(ead2.evaluateToElements("ead:ead/ead:archdesc", null).isEmpty());

        JDomXP ead2BoundToEad3 = new JDomXP(new File(EAD2_FILE));
        ead2BoundToEad3.addNamespace(Ead3Indexer.NAMESPACE_EAD3);
        Assertions.assertTrue(ead2BoundToEad3.evaluateToElements("ead:ead/ead:archdesc", null).isEmpty());
    }

    /**
     * @see JDomXP#addNamespace(Namespace)
     * @verifies not affect global configuration
     */
    @Test
    void addNamespace_shouldNotAffectGlobalConfiguration() throws Exception {
        Namespace globalBefore = SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("ead");

        JDomXP ead3 = new JDomXP(new File(EAD3_FILE));
        ead3.addNamespace(Ead3Indexer.NAMESPACE_EAD3);
        ead3.evaluateToElements(EAD_C_XPATH, null);

        Namespace globalAfter = SolrIndexerDaemon.getInstance().getConfiguration().getNamespaces().get("ead");
        Assertions.assertEquals(globalBefore, globalAfter);
        // The global 'ead' prefix must remain bound to EAD2 - the instance binding must not leak into shared state.
        Assertions.assertEquals(EadIndexer.NAMESPACE_EAD2, globalAfter);
    }

    /**
     * @see JDomXP#newLruCache(int)
     * @verifies evict least recently used entry beyond max entries
     */
    @Test
    void newLruCache_shouldEvictLeastRecentlyUsedEntryBeyondMaxEntries() {
        Map<String, String> cache = JDomXP.newLruCache(2);
        cache.put("a", "A");
        cache.put("b", "B");
        // Accessing "a" makes "b" the least recently used entry
        Assertions.assertEquals("A", cache.get("a"));
        cache.put("c", "C");

        Assertions.assertEquals(2, cache.size());
        Assertions.assertTrue(cache.containsKey("a"));
        Assertions.assertTrue(cache.containsKey("c"));
        Assertions.assertFalse(cache.containsKey("b"));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect mets mods files correctly
     */
    @Test
    void determineFileFormat_shouldDetectMetsModsFilesCorrectly() throws Exception {
        Assertions.assertEquals(FileFormat.METS, JDomXP.determineFileFormat(new File("src/test/resources/METS/H030001_mets.xml")));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect mets marc files correctly
     */
    @Test
    void determineFileFormat_shouldDetectMetsMarcFilesCorrectly() throws Exception {
        Assertions.assertEquals(FileFormat.METS_MARC,
                JDomXP.determineFileFormat(new File("src/test/resources/METS/VoorbeeldMETS_9940609919905131.xml")));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect lido files correctly
     */
    @Test
    void determineFileFormat_shouldDetectLidoFilesCorrectly() throws Exception {
        Assertions.assertEquals(FileFormat.LIDO, JDomXP.determineFileFormat(new File("src/test/resources/LIDO/V0011127.xml")));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect ead2 files correctly
     */
    @Test
    void determineFileFormat_shouldDetectEad2FilesCorrectly() throws Exception {
        Assertions.assertEquals(FileFormat.EAD, JDomXP.determineFileFormat(new File(EAD2_FILE)));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect ead3 files correctly
     */
    @Test
    void determineFileFormat_shouldDetectEad3FilesCorrectly() throws Exception {
        Assertions.assertEquals(FileFormat.EAD3, JDomXP.determineFileFormat(new File(EAD3_FILE)));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect denkxweb files correctly
     */
    @Test
    void determineFileFormat_shouldDetectDenkxwebFilesCorrectly() throws Exception {
        Assertions.assertEquals(FileFormat.DENKXWEB, JDomXP.determineFileFormat(new File("src/test/resources/DenkXweb/10973880.xml")));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect dublin core files correctly
     */
    @Test
    void determineFileFormat_shouldDetectDublinCoreFilesCorrectly() throws Exception {
        Assertions.assertEquals(FileFormat.DUBLINCORE, JDomXP.determineFileFormat(new File("src/test/resources/DC/record.xml")));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect worldviews files correctly
     */
    @Test
    void determineFileFormat_shouldDetectWorldviewsFilesCorrectly() throws Exception {
        Assertions.assertEquals(FileFormat.WORLDVIEWS,
                JDomXP.determineFileFormat(new File("src/test/resources/WorldViews/gei_test_sthe_quelle_01.xml")));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect abbyy files correctly
     */
    @Test
    void determineFileFormat_shouldDetectAbbyyFilesCorrectly() throws Exception {
        Assertions.assertEquals(FileFormat.ABBYYXML, JDomXP.determineFileFormat(new File("src/test/resources/ABBYYXML/00000001.xml")));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect tei files correctly
     */
    @Test
    void determineFileFormat_shouldDetectTeiFilesCorrectly() throws Exception {
        Assertions.assertEquals(FileFormat.TEI,
                JDomXP.determineFileFormat(new File("src/test/resources/METS/kleiuniv_PPN517154005/kleiuniv_PPN517154005_wc/00000001.xml")));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies detect cms files correctly
     */
    @Test
    void determineFileFormat_shouldDetectCmsFilesCorrectly() throws Exception {
        Assertions.assertEquals(FileFormat.CMS, JDomXP.determineFileFormat(new File("src/test/resources/cms/cms1.xml")));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies return unknown for files with doctype
     */
    @Test
    void determineFileFormat_shouldReturnUnknownForFilesWithDoctype(@TempDir Path tempDir) throws Exception {
        Path file = Files.writeString(tempDir.resolve("doctype.xml"),
                "<?xml version=\"1.0\"?><!DOCTYPE lido [<!ENTITY x \"y\">]><lido:lido xmlns:lido=\"http://www.lido-schema.org\">&x;</lido:lido>");
        Assertions.assertEquals(FileFormat.UNKNOWN, JDomXP.determineFileFormat(file.toFile()));
    }

    /**
     * @see JDomXP#determineFileFormat(File)
     * @verifies return same results as full document parsing
     */
    @Test
    void determineFileFormat_shouldReturnSameResultsAsFullDocumentParsing() throws Exception {
        List<Path> files;
        try (Stream<Path> walk = Files.walk(Paths.get("src/test/resources"))) {
            files = walk.filter(p -> Files.isRegularFile(p) && p.getFileName().toString().endsWith(".xml")).toList();
        }
        Assertions.assertFalse(files.isEmpty());
        for (Path file : files) {
            Assertions.assertEquals(determineFileFormatFromDocument(file.toFile()), JDomXP.determineFileFormat(file.toFile()), file.toString());
        }
    }

    /**
     * Previous implementation of {@link JDomXP#determineFileFormat(File)}, which parses the complete document; used as a reference.
     *
     * @param file
     * @return Detected {@link FileFormat}
     * @throws Exception
     */
    private static FileFormat determineFileFormatFromDocument(File file) throws Exception {
        Document doc;
        try (FileInputStream fis = new FileInputStream(file)) {
            doc = XmlTools.getSAXBuilder().build(fis);
        } catch (JDOMException e) {
            return FileFormat.UNKNOWN;
        }
        Element root = doc.getRootElement();
        if (root.getNamespace("mets") != null) {
            List<Element> elements = JDomXP.evaluateToElementsStatic("mets:dmdSec/mets:mdWrap[@MDTYPE='MODS']", root);
            if (elements != null && !elements.isEmpty()) {
                return FileFormat.METS;
            }
            elements = JDomXP.evaluateToElementsStatic("mets:dmdSec/mets:mdWrap[@MDTYPE='MARC']", root);
            if (elements != null && !elements.isEmpty()) {
                return FileFormat.METS_MARC;
            }
            return FileFormat.METS;
        }
        if (root.getNamespace("lido") != null) {
            return FileFormat.LIDO;
        }
        String uri = root.getNamespace().getURI();
        if (uri.equals("urn:isbn:1-931666-22-9")) {
            return FileFormat.EAD;
        }
        if (uri.equals("http://ead3.archivists.org/schema/") || uri.equals("https://archivists.org/ns/ead/v4")) {
            return FileFormat.EAD3;
        }
        if (uri.equals("http://denkxweb.de/")) {
            return FileFormat.DENKXWEB;
        }
        if (root.getNamespace("dc") != null) {
            return FileFormat.DUBLINCORE;
        }
        if (root.getName().equals("worldviews")) {
            return FileFormat.WORLDVIEWS;
        }
        if (uri.contains("abbyy")) {
            return FileFormat.ABBYYXML;
        }
        if (root.getName().equals("TEI.2")) {
            return FileFormat.TEI;
        }
        if (root.getName().equals("cmsPage")) {
            return FileFormat.CMS;
        }
        return FileFormat.UNKNOWN;
    }
}
