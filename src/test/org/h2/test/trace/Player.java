/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership. The ASF licenses this file to You under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the License.
 */
package org.h2.test.trace;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;

/**
 * This tool can re-run Java style log files. There is no size limit.
 */
public class Player {
	
	// TODO support InputStream;
	// TODO support Reader;
	// TODO support int[];
	// TODO support Blob and Clob;
	// TODO support Calendar
	// TODO support Object
	// TODO support Object[]
	// TODO support URL
	// TODO support Array
	// TODO support Ref
	// TODO support SQLInput, SQLOutput
	// TODO support Properties
	// TODO support Map
	// TODO support SQLXML
	
	private static final String[] IMPORTED_PACKAGES = { "", "java.lang.", "java.sql.", "javax.sql." };
	
	private boolean trace;
	
	private HashMap objects = new HashMap();
	
	/**
	 * Execute a trace file using the command line. The log file name to execute (replayed) must be specified as the last parameter. The
	 * following optional command line parameters are supported:
	 * <ul>
	 * <li><code>-log</code> to enable logging the executed statement to System.out
	 * </ul>
	 * 
	 * @param args
	 *            the arguments of the application
	 */
	public static void main(String[] args) throws IOException {
		new Player().run(args);
	}
	
	/**
	 * Execute a trace file.
	 * 
	 * @param fileName
	 *            the file name
	 * @param log
	 *            print debug information
	 * @param checkResult
	 *            if the result of each method should be compared against the result in the file
	 */
	public static void execute(String fileName, boolean log, boolean checkResult) throws IOException {
		new Player().runFile(fileName, log);
	}
	
	private void run(String[] args) throws IOException {
		String fileName = "test.log.db";
		try {
			fileName = args[args.length - 1];
			for ( int i = 0; i < args.length - 1; i++ ) {
				if ( "-trace".equals(args[i]) ) {
					trace = true;
				} else {
					throw new Error("Unknown setting: " + args[i]);
				}
			}
		} catch ( Exception e ) {
			e.printStackTrace();
			System.out.println("Usage: java " + getClass().getName() + " [-trace] <fileName>");
			return;
		}
		runFile(fileName, trace);
	}
	
	private void runFile(String fileName, boolean trace) throws IOException {
		this.trace = trace;
		LineNumberReader reader = new LineNumberReader(new BufferedReader(new FileReader(fileName)));
		while ( true ) {
			String line = reader.readLine();
			if ( line == null ) {
				break;
			}
			runLine(line.trim());
		}
	}
	
	/**
	 * Write trace information if trace is enabled.
	 * 
	 * @param s
	 *            the message to write
	 */
	void trace(String s) {
		if ( trace ) {
			System.out.println(s);
		}
	}
	
	private void runLine(String line) {
		if ( !line.startsWith("/**/") ) {
			return;
		}
		line = line.substring("/**/".length()) + ";";
		Statement s = Parser.parseStatement(this, line);
		trace("> " + s.toString());
		try {
			s.execute();
		} catch ( Exception e ) {
			e.printStackTrace();
			trace("error: " + e.toString());
		}
	}
	
	/**
	 * Get the class for the given class name. Only a limited set of classes is supported.
	 * 
	 * @param className
	 *            the class name
	 * @return the class
	 */
	static Class getClass(String className) {
		for ( int i = 0; i < IMPORTED_PACKAGES.length; i++ ) {
			try {
				return Class.forName(IMPORTED_PACKAGES[i] + className);
			} catch ( ClassNotFoundException e ) {
				// ignore
			}
		}
		throw new Error("Class not found: " + className);
	}
	
	/**
	 * Assign an object to a variable.
	 * 
	 * @param variableName
	 *            the variable name
	 * @param obj
	 *            the object
	 */
	void assign(String variableName, Object obj) {
		objects.put(variableName, obj);
	}
	
	/**
	 * Get an object.
	 * 
	 * @param name
	 *            the variable name
	 * @return the object
	 */
	Object getObject(String name) {
		return objects.get(name);
	}
	
}
