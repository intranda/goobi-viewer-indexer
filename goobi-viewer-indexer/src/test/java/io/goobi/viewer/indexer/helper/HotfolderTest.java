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

import static org.junit.Assert.assertFalse;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import io.goobi.viewer.indexer.AbstractSolrEnabledTest;
import io.goobi.viewer.indexer.SolrIndexerDaemon;

/**
 * TODO "Connection pool closed" error if more than one test (Jenkins only)
 */
public class HotfolderTest extends AbstractSolrEnabledTest {

    /**
     * @see Hotfolder#countRecordFiles()
     * @verifies count files correctly
     */
    @Test
    public void countRecordFiles_shouldCountFilesCorrectly() throws Exception {
        hotfolder = new Hotfolder(SolrIndexerDaemon.getInstance().getConfiguration().getHotfolderPath());
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("performance.countHotfolderFiles", true);
        Assert.assertTrue(SolrIndexerDaemon.getInstance().getConfiguration().isCountHotfolderFiles());

        {
            Path path = Paths.get("target/viewer/hotfolder", "1.xml");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "1A.XML");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "2.delete");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "3.purge");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "4.docupdate");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "5.UPDATED");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }

        // Uncounted files
        {
            Path path = Paths.get("target/viewer/hotfolder", "6.exe");
            Files.createFile(path);
            Assert.assertTrue(Files.isRegularFile(path));
        }
        {
            Path path = Paths.get("target/viewer/hotfolder", "7");
            Files.createDirectories(path);
            Assert.assertTrue(Files.isDirectory(path));
        }

        Assert.assertEquals(6, hotfolder.countRecordFiles());

    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if body contains no error
     */
    @Test
    public void checkAndSendErrorReport_shouldReturnFalseIfBodyContainsNoError() throws Exception {
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "bar"));
    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if recipients not configured
     */
    @Test
    public void checkAndSendErrorReport_shouldReturnFalseIfRecipientsNotConfigured() throws Exception {
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "ERROR bar"));
    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if smtpServer not configured
     */
    @Test
    public void checkAndSendErrorReport_shouldReturnFalseIfSmtpServerNotConfigured() throws Exception {
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.recipients", "foo@bar.com");
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "ERROR bar"));
    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if smtpSenderAddress not configured
     */
    @Test
    public void checkAndSendErrorReport_shouldReturnFalseIfSmtpSenderAddressNotConfigured() throws Exception {
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.recipients", "foo@bar.com");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpServer", "bar.com");
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "ERROR bar"));
    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if smtpSenderName not configured
     */
    @Test
    public void checkAndSendErrorReport_shouldReturnFalseIfSmtpSenderNameNotConfigured() throws Exception {
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.recipients", "user@example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpServer", "smtp.example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpSenderAddress", "indexer@example.foo");
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "ERROR bar"));
    }

    /**
     * @see Hotfolder#checkAndSendErrorReport(String,String)
     * @verifies return false if smtpSecurity not configured
     */
    @Test
    public void checkAndSendErrorReport_shouldReturnFalseIfSmtpSecurityNotConfigured() throws Exception {
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.recipients", "user@example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpServer", "smtp.example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpSenderAddress", "indexer@example.foo");
        SolrIndexerDaemon.getInstance().getConfiguration().overrideValue("init.email.smtpSenderName", "Goobi viewer Indexer");
        assertFalse(Hotfolder.checkAndSendErrorReport("foo", "ERROR bar"));
    }
}