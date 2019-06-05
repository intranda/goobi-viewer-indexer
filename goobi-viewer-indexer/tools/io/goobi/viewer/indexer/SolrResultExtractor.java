/*************************************************************************
 * 
 * Copyright intranda GmbH
 * 
 * ************************* CONFIDENTIAL ********************************
 * 
 * [2003] - [2013] intranda GmbH, Bertha-von-Suttner-Str. 9, 37085 Göttingen, Germany 
 * 
 * All Rights Reserved.
 * 
 * NOTICE: All information contained herein is protected by copyright. 
 * The source code contained herein is proprietary of intranda GmbH. 
 * The dissemination, reproduction, distribution or modification of 
 * this source code, without prior written permission from intranda GmbH, 
 * is expressly forbidden and a violation of international copyright law.
 * 
 *************************************************************************/
package io.goobi.viewer.indexer;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPath;

/**
 * Extracts raw strings from XML files.
 * 
 */
public class SolrResultExtractor {

	private static final String FILEPATH_IN = "select.xml";
	private static final String FILEPATH_OUT = "result.txt";

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		StringBuilder sb = new StringBuilder();
		FileInputStream fis = null;
		BufferedWriter out = null;
		try {
			fis = new FileInputStream(FILEPATH_IN);
			Document doc = new SAXBuilder().build(fis);
			if (doc != null) {
				XPath xp = XPath.newInstance("response/result/doc/str");
				List<Element> result = (List<Element>) xp.selectNodes(doc);
				if (result != null) {
					for (Element ele : result) {
						sb.append(ele.getTextTrim() + "\n");
						System.out.println(ele.getTextTrim());
					}
				}
				out = new BufferedWriter(new FileWriter(FILEPATH_OUT));
				out.write(sb.toString());
			}
		} catch (JDOMException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
