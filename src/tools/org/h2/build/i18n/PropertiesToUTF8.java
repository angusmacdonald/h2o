/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.build.i18n;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.Enumeration;
import java.util.Properties;

import org.h2.build.code.CheckTextFiles;
import org.h2.build.indexer.HtmlConverter;
import org.h2.util.IOUtils;
import org.h2.util.SortedProperties;
import org.h2.util.StringUtils;

/**
 * This class converts a file stored in the UTF-8 encoding format to a properties file and vice versa.
 */
public class PropertiesToUTF8 {
	
	private PropertiesToUTF8() {
		// utility class
	}
	
	/**
	 * This method is called when executing this application from the command line.
	 * 
	 * @param args
	 *            the command line parameters
	 */
	public static void main(String[] args) throws Exception {
		convert("bin/org/h2/res");
		convert("bin/org/h2/server/web/res");
	}
	
	/**
	 * Convert a properties file to a UTF-8 text file.
	 * 
	 * @param source
	 *            the name of the properties file
	 * @param target
	 *            the target file name
	 */
	static void propertiesToTextUTF8(String source, String target) throws Exception {
		if ( !new File(source).exists() ) {
			return;
		}
		Properties prop = SortedProperties.loadProperties(source);
		FileOutputStream out = new FileOutputStream(target);
		PrintWriter writer = new PrintWriter(new OutputStreamWriter(out, "UTF-8"));
		// keys is sorted
		for ( Enumeration en = prop.keys(); en.hasMoreElements(); ) {
			String key = (String) en.nextElement();
			String value = prop.getProperty(key, null);
			writer.println("@" + key);
			writer.println(value);
			writer.println();
		}
		writer.close();
	}
	
	/**
	 * Convert a translation file (in UTF-8) to a properties file (without special characters).
	 * 
	 * @param source
	 *            the source file name
	 * @param target
	 *            the target file name
	 */
	static void textUTF8ToProperties(String source, String target) throws Exception {
		if ( !new File(source).exists() ) {
			return;
		}
		LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream(source), "UTF-8"));
		try {
			Properties prop = new SortedProperties();
			StringBuilder buff = new StringBuilder();
			String key = null;
			boolean found = false;
			while ( true ) {
				String line = reader.readLine();
				if ( line == null ) {
					break;
				}
				line = line.trim();
				if ( line.length() == 0 ) {
					continue;
				}
				if ( line.startsWith("@") ) {
					if ( key != null ) {
						prop.setProperty(key, buff.toString());
						buff.setLength(0);
					}
					found = true;
					key = line.substring(1);
				} else {
					if ( buff.length() > 0 ) {
						buff.append(System.getProperty("line.separator"));
					}
					buff.append(line);
				}
			}
			if ( found ) {
				prop.setProperty(key, buff.toString());
			}
			storeProperties(prop, target);
		} finally {
			reader.close();
		}
	}
	
	private static void convert(String source) throws Exception {
		File[] list = new File(source).listFiles();
		for ( int i = 0; list != null && i < list.length; i++ ) {
			File f = list[i];
			if ( !f.getName().endsWith(".properties") ) {
				continue;
			}
			FileInputStream in = new FileInputStream(f);
			InputStreamReader r = new InputStreamReader(in, "UTF-8");
			String s = IOUtils.readStringAndClose(r, -1);
			in.close();
			String name = f.getName();
			String utf8, html;
			if ( name.startsWith("utf8") ) {
				utf8 = HtmlConverter.convertHtmlToString(s);
				html = HtmlConverter.convertStringToHtml(utf8);
				RandomAccessFile out = new RandomAccessFile("_" + name.substring(4), "rw");
				out.write(html.getBytes());
				out.setLength(out.getFilePointer());
				out.close();
			} else {
				new CheckTextFiles().checkOrFixFile(f, false, false);
				html = s;
				utf8 = HtmlConverter.convertHtmlToString(html);
				// s = unescapeHtml(s);
				utf8 = StringUtils.javaDecode(utf8);
				FileOutputStream out = new FileOutputStream("_utf8" + f.getName());
				OutputStreamWriter w = new OutputStreamWriter(out, "UTF-8");
				w.write(utf8);
				w.close();
				out.close();
			}
			String java = StringUtils.javaEncode(utf8);
			java = StringUtils.replaceAll(java, "\\r", "\r");
			java = StringUtils.replaceAll(java, "\\n", "\n");
			RandomAccessFile out = new RandomAccessFile("_java." + name, "rw");
			out.write(java.getBytes());
			out.setLength(out.getFilePointer());
			out.close();
		}
	}
	
	/**
	 * Store a properties file.
	 * 
	 * @param p
	 *            the properties
	 * @param fileName
	 *            the file name
	 */
	static void storeProperties(Properties p, String fileName) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		p.store(out, null);
		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		InputStreamReader reader = new InputStreamReader(in, "ISO8859-1");
		LineNumberReader r = new LineNumberReader(reader);
		FileWriter w = new FileWriter(fileName);
		PrintWriter writer = new PrintWriter(new BufferedWriter(w));
		while ( true ) {
			String line = r.readLine();
			if ( line == null ) {
				break;
			}
			if ( !line.startsWith("#") ) {
				writer.println(line);
			}
		}
		writer.close();
	}
	
}
