/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import org.h2.util.IOUtils;
import org.h2.util.StartBrowser;
import org.h2.util.StringUtils;

/**
 * The link checker makes sure that each link in the documentation points to an existing target.
 */
public class LinkChecker {
	
	private static final boolean OPEN_EXTERNAL_LINKS = false;
	
	private static final String[] IGNORE_MISSING_LINKS_TO = new String[] { "SysProperties", "ErrorCode" };
	
	private HashMap targets = new HashMap();
	
	private HashMap links = new HashMap();
	
	/**
	 * This method is called when executing this application from the command line.
	 * 
	 * @param args
	 *            the command line parameters
	 */
	public static void main(String[] args) throws Exception {
		new LinkChecker().run(args);
	}
	
	private void run(String[] args) throws Exception {
		String dir = "docs";
		for ( int i = 0; i < args.length; i++ ) {
			if ( "-dir".equals(args[i]) ) {
				dir = args[++i];
			}
		}
		process(dir);
		listExternalLinks();
		listBadLinks();
	}
	
	private void listExternalLinks() {
		for ( Iterator it = links.keySet().iterator(); it.hasNext(); ) {
			String link = (String) it.next();
			if ( link.startsWith("http") ) {
				if ( link.indexOf("//localhost") > 0 ) {
					continue;
				}
				if ( OPEN_EXTERNAL_LINKS ) {
					System.out.println(link);
					StartBrowser.openURL(link);
					try {
						Thread.sleep(100);
					} catch ( InterruptedException e ) {
						// ignore
					}
				}
			}
		}
	}
	
	private void listBadLinks() throws Exception {
		ArrayList errors = new ArrayList();
		for ( Iterator it = links.keySet().iterator(); it.hasNext(); ) {
			String link = (String) it.next();
			if ( !link.startsWith("http") && !link.endsWith("h2.pdf") && link.indexOf("_ja.") < 0 ) {
				if ( targets.get(link) == null ) {
					errors.add(links.get(link) + ": missing link " + link);
				}
			}
		}
		for ( Iterator it = links.keySet().iterator(); it.hasNext(); ) {
			String link = (String) it.next();
			if ( !link.startsWith("http") ) {
				targets.remove(link);
			}
		}
		for ( Iterator it = targets.keySet().iterator(); it.hasNext(); ) {
			String name = (String) it.next();
			if ( targets.get(name).equals("name") ) {
				boolean ignore = false;
				for ( int i = 0; i < IGNORE_MISSING_LINKS_TO.length; i++ ) {
					if ( name.indexOf(IGNORE_MISSING_LINKS_TO[i]) >= 0 ) {
						ignore = true;
						break;
					}
				}
				if ( !ignore ) {
					errors.add("No link to " + name);
				}
			}
		}
		Collections.sort(errors);
		for ( int i = 0; i < errors.size(); i++ ) {
			System.out.println(errors.get(i));
		}
		if ( errors.size() > 0 ) {
			throw new Exception("Problems where found by the Link Checker");
		}
	}
	
	private void process(String path) throws Exception {
		if ( path.endsWith("/CVS") || path.endsWith("/.svn") ) {
			return;
		}
		File file = new File(path);
		if ( file.isDirectory() ) {
			String[] list = file.list();
			for ( int i = 0; i < list.length; i++ ) {
				process(path + "/" + list[i]);
			}
		} else {
			processFile(path);
		}
	}
	
	private void processFile(String path) throws Exception {
		targets.put(path, "file");
		String lower = StringUtils.toLowerEnglish(path);
		if ( !lower.endsWith(".html") && !lower.endsWith(".htm") ) {
			return;
		}
		String fileName = new File(path).getName();
		String parent = path.substring(0, path.lastIndexOf('/'));
		String html = IOUtils.readStringAndClose(new FileReader(path), -1);
		int idx = -1;
		while ( true ) {
			idx = html.indexOf(" id=\"", idx + 1);
			if ( idx < 0 ) {
				break;
			}
			int start = idx + 4;
			int end = html.indexOf("\"", start + 1);
			if ( end < 0 ) {
				error(fileName, "expected \" after id= " + html.substring(idx, idx + 100));
			}
			String ref = html.substring(start + 1, end);
			targets.put(path + "#" + ref, "id");
		}
		idx = -1;
		while ( true ) {
			idx = html.indexOf("<a ", idx + 1);
			if ( idx < 0 ) {
				break;
			}
			int equals = html.indexOf("=", idx);
			if ( equals < 0 ) {
				error(fileName, "expected = after <a at " + html.substring(idx, idx + 100));
			}
			String type = html.substring(idx + 2, equals).trim();
			int start = html.indexOf("\"", idx);
			if ( start < 0 ) {
				error(fileName, "expected \" after <a at " + html.substring(idx, idx + 100));
			}
			int end = html.indexOf("\"", start + 1);
			if ( end < 0 ) {
				error(fileName, "expected \" after <a at " + html.substring(idx, idx + 100));
			}
			String ref = html.substring(start + 1, end);
			if ( type.equals("href") ) {
				if ( ref.startsWith("http:") || ref.startsWith("https:") ) {
					// ok
				} else if ( ref.startsWith("javascript:") ) {
					ref = null;
					// ok
				} else if ( ref.length() == 0 ) {
					ref = null;
					// ok
				} else if ( ref.startsWith("#") ) {
					ref = path + ref;
				} else {
					String p = parent;
					while ( ref.startsWith(".") ) {
						if ( ref.startsWith("./") ) {
							ref = ref.substring(2);
						} else if ( ref.startsWith("../") ) {
							ref = ref.substring(3);
							p = p.substring(0, p.lastIndexOf('/'));
						}
					}
					ref = p + "/" + ref;
				}
				if ( ref != null ) {
					links.put(ref, path);
				}
			} else if ( type.equals("name") ) {
				targets.put(path + "#" + ref, "name");
			} else {
				error(fileName, "unsupported <a ?: " + html.substring(idx, idx + 100));
			}
		}
	}
	
	private void error(String fileName, String string) {
		System.out.println("ERROR with " + fileName + ": " + string);
	}
	
}
