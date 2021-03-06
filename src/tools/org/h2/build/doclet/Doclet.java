/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.build.doclet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;

import org.h2.util.StringUtils;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.ParamTag;
import com.sun.javadoc.Parameter;
import com.sun.javadoc.RootDoc;
import com.sun.javadoc.Tag;
import com.sun.javadoc.ThrowsTag;
import com.sun.javadoc.Type;

/**
 * This class is a custom doclet implementation to generate the Javadoc for this product.
 */
public class Doclet {
	
	private static final boolean INTERFACES_ONLY = Boolean.getBoolean("h2.interfacesOnly");
	
	private int errorCount;
	
	private HashSet errors = new HashSet();
	
	/**
	 * This method is called by the javadoc framework and is required for all doclets.
	 * 
	 * @param root
	 *            the root
	 * @return true if successful
	 */
	public static boolean start(RootDoc root) throws IOException {
		return new Doclet().startDoc(root);
	}
	
	private boolean startDoc(RootDoc root) throws IOException {
		ClassDoc[] classes = root.classes();
		String[][] options = root.options();
		String destDir = System.getProperty("h2.destDir", "docs/javadoc");
		for ( int i = 0; i < options.length; i++ ) {
			if ( options[i][0].equals("destdir") ) {
				destDir = options[i][1];
			}
		}
		for ( int i = 0; i < classes.length; ++i ) {
			ClassDoc clazz = classes[i];
			processClass(destDir, clazz);
		}
		if ( errorCount > 0 ) {
			throw new IOException("FAILED: " + errorCount + " errors found");
		}
		return true;
	}
	
	private static String getClass(ClassDoc clazz) {
		String name = clazz.name();
		if ( clazz.qualifiedName().indexOf(".jdbc.") > 0 && name.startsWith("Jdbc") ) {
			return name.substring(4);
		}
		return name;
	}
	
	private void processClass(String destDir, ClassDoc clazz) throws IOException {
		String packageName = clazz.containingPackage().name();
		String dir = destDir + "/" + packageName.replace('.', '/');
		( new File(dir) ).mkdirs();
		String fileName = dir + "/" + clazz.name() + ".html";
		String className = getClass(clazz);
		FileWriter out = new FileWriter(fileName);
		PrintWriter writer = new PrintWriter(new BufferedWriter(out));
		writer.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\" "
				+ "\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">");
		String language = "en";
		writer.println("<html xmlns=\"http://www.w3.org/1999/xhtml\" " + "lang=\"" + language + "\" xml:lang=\"" + language + "\">");
		writer.println("<head><meta http-equiv=\"Content-Type\" content=\"text/html;charset=utf-8\" /><title>");
		writer.println(className);
		writer.println("</title><link rel=\"stylesheet\" type=\"text/css\" href=\"../../../stylesheet.css\" /></head><body>");
		writer.println("<table class=\"content\"><tr class=\"content\"><td class=\"content\"><div class=\"contentDiv\">");
		writer.println("<h1>" + className + "</h1>");
		writer.println(formatText(clazz.commentText()) + "<br /><br />");
		
		// method overview
		MethodDoc[] methods = clazz.methods();
		Arrays.sort(methods, new Comparator() {
			
			public int compare(Object a, Object b) {
				return ( (MethodDoc) a ).name().compareTo(( (MethodDoc) b ).name());
			}
		});
		boolean hasMethods = false;
		for ( int i = 0; i < methods.length; i++ ) {
			MethodDoc method = methods[i];
			String name = method.name();
			if ( skipMethod(method) ) {
				continue;
			}
			if ( !hasMethods ) {
				writer.println("<table><tr><th colspan=\"2\">Methods</th></tr>");
				hasMethods = true;
			}
			String type = getTypeName(method.isStatic(), method.returnType());
			writer.println("<tr><td class=\"return\">" + type + "</td><td class=\"method\">");
			Parameter[] params = method.parameters();
			StringBuilder buff = new StringBuilder();
			buff.append('(');
			for ( int j = 0; j < params.length; j++ ) {
				if ( j > 0 ) {
					buff.append(", ");
				}
				buff.append(getTypeName(false, params[j].type()));
				buff.append(' ');
				buff.append(params[j].name());
			}
			buff.append(')');
			if ( isDeprecated(method) ) {
				name = "<span class=\"deprecated\">" + name + "</span>";
			}
			writer.println("<a href=\"#r" + i + "\">" + name + "</a>" + buff.toString());
			String firstSentence = getFirstSentence(method.firstSentenceTags());
			if ( firstSentence != null ) {
				writer.println("<div class=\"methodText\">" + formatText(firstSentence) + "</div>");
			}
			writer.println("</td></tr>");
		}
		if ( hasMethods ) {
			writer.println("</table>");
		}
		
		// field overview
		FieldDoc[] fields = clazz.fields();
		if ( clazz.interfaces().length > 0 ) {
			fields = clazz.interfaces()[0].fields();
		}
		Arrays.sort(fields, new Comparator() {
			
			public int compare(Object a, Object b) {
				return ( (FieldDoc) a ).name().compareTo(( (FieldDoc) b ).name());
			}
		});
		int fieldId = 0;
		for ( int i = 0; i < fields.length; i++ ) {
			FieldDoc field = fields[i];
			if ( skipField(clazz, field) ) {
				continue;
			}
			String name = field.name();
			String text = field.commentText();
			if ( text == null || text.trim().length() == 0 ) {
				addError("Undocumented field (" + clazz.name() + ".java:" + field.position().line() + ") " + name);
			}
			if ( text != null && text.startsWith("INTERNAL") ) {
				continue;
			}
			if ( fieldId == 0 ) {
				writer.println("<br /><table><tr><th colspan=\"2\">Fields</th></tr>");
			}
			String type = getTypeName(true, field.type());
			writer.println("<tr><td class=\"return\">" + type + "</td><td class=\"method\">");
			String constant = field.constantValueExpression();
			
			// add a link (a name) if there is a <code> tag
			String link = getFieldLink(text, constant, clazz, name);
			writer.print("<a href=\"#" + link + "\">" + name + "</a>");
			if ( constant == null ) {
				writer.println();
			} else {
				writer.println(" = " + constant);
			}
			writer.println("</td></tr>");
			fieldId++;
		}
		if ( fieldId > 0 ) {
			writer.println("</table>");
		}
		
		// method details
		for ( int i = 0; i < methods.length; i++ ) {
			MethodDoc method = methods[i];
			String name = method.name();
			if ( skipMethod(method) ) {
				continue;
			}
			String type = getTypeName(method.isStatic(), method.returnType());
			writer.println("<a name=\"r" + i + "\"></a>");
			Parameter[] params = method.parameters();
			StringBuilder buff = new StringBuilder();
			buff.append('(');
			for ( int j = 0; j < params.length; j++ ) {
				if ( j > 0 ) {
					buff.append(", ");
				}
				buff.append(getTypeName(false, params[j].type()));
				buff.append(' ');
				buff.append(params[j].name());
			}
			buff.append(')');
			ClassDoc[] exceptions = method.thrownExceptions();
			if ( exceptions.length > 0 ) {
				buff.append(" throws ");
				for ( int k = 0; k < exceptions.length; k++ ) {
					if ( k > 0 ) {
						buff.append(", ");
					}
					buff.append(exceptions[k].typeName());
				}
			}
			if ( isDeprecated(method) ) {
				name = "<span class=\"deprecated\">" + name + "</span>";
			}
			writer.println("<h4>" + type + " <span class=\"methodName\">" + name + "</span>" + buff.toString() + "</h4>");
			boolean hasComment = method.commentText() != null && method.commentText().trim().length() != 0;
			writer.println(formatText(method.commentText()));
			ParamTag[] paramTags = method.paramTags();
			ThrowsTag[] throwsTags = method.throwsTags();
			boolean hasThrowsTag = throwsTags != null && throwsTags.length > 0;
			boolean space = false;
			if ( paramTags.length != params.length ) {
				if ( hasComment && !method.commentText().startsWith("[") && !hasThrowsTag ) {
					// [Not supported] and such are not problematic
					// also not problematic are methods that always throw an exception
					addError("Undocumented parameter(s) (" + clazz.name() + ".java:" + method.position().line() + ") " + name
							+ " documented: " + paramTags.length + " params: " + params.length);
				}
			}
			for ( int j = 0; j < paramTags.length; j++ ) {
				if ( !space ) {
					writer.println("<br /><br />");
					space = true;
				}
				String paramName = paramTags[j].parameterName();
				String comment = paramTags[j].parameterComment();
				if ( comment.trim().length() == 0 ) {
					addError("Undocumented parameter (" + clazz.name() + ".java:" + method.position().line() + ") " + name + " "
							+ paramName);
				}
				String p = paramName + " - " + comment;
				if ( j == 0 ) {
					writer.println("<div class=\"itemTitle\">Parameters:</div>");
				}
				writer.println("<div class=\"item\">" + p + "</div>");
			}
			Tag[] returnTags = method.tags("return");
			if ( returnTags != null && returnTags.length > 0 ) {
				if ( !space ) {
					writer.println("<br /><br />");
					space = true;
				}
				writer.println("<div class=\"itemTitle\">Returns:</div>");
				String returnComment = returnTags[0].text();
				if ( returnComment.trim().length() == 0 ) {
					addError("Undocumented return value (" + clazz.name() + ".java:" + method.position().line() + ") " + name);
				}
				writer.println("<div class=\"item\">" + returnComment + "</div>");
			} else if ( !method.returnType().toString().equals("void") ) {
				if ( hasComment && !method.commentText().startsWith("[") && !hasThrowsTag ) {
					// [Not supported] and such are not problematic
					// also not problematic are methods that always throw an exception
					addError("Undocumented return value (" + clazz.name() + ".java:" + method.position().line() + ") " + name + " "
							+ method.returnType());
				}
			}
			if ( hasThrowsTag ) {
				if ( !space ) {
					writer.println("<br /><br />");
					space = true;
				}
				writer.println("<div class=\"itemTitle\">Throws:</div>");
				for ( int j = 0; j < throwsTags.length; j++ ) {
					String p = throwsTags[j].exceptionName();
					String c = throwsTags[j].exceptionComment();
					if ( c.length() > 0 ) {
						p += " - " + c;
					}
					writer.println("<div class=\"item\">" + p + "</div>");
				}
			}
			writer.println("<hr />");
		}
		
		// field details
		Arrays.sort(fields, new Comparator() {
			
			public int compare(Object a, Object b) {
				FieldDoc fa = (FieldDoc) a;
				FieldDoc fb = (FieldDoc) b;
				String ca = fa.constantValueExpression();
				String cb = fb.constantValueExpression();
				if ( ca != null && cb != null ) {
					return ca.compareTo(cb);
				}
				return fa.name().compareTo(fb.name());
			}
		});
		for ( int i = 0; i < fields.length; i++ ) {
			FieldDoc field = fields[i];
			if ( skipField(clazz, field) ) {
				continue;
			}
			String text = field.commentText();
			if ( text.startsWith("INTERNAL") ) {
				continue;
			}
			String name = field.name();
			String constant = field.constantValueExpression();
			String link = getFieldLink(text, constant, clazz, name);
			writer.println("<a name=\"" + link + "\"></a>");
			writer.println("<h4><span class=\"methodName\">" + name);
			if ( constant == null ) {
				writer.println();
			} else {
				writer.println(" = " + constant);
			}
			writer.println("</span></h4>");
			writer.println("<div class=\"item\">" + formatText(text) + "</div>");
			writer.println("<hr />");
		}
		
		writer.println("</div></td></tr></table></body></html>");
		writer.close();
		out.close();
	}
	
	private String getFieldLink(String text, String constant, ClassDoc clazz, String name) {
		String link = constant != null ? constant : name.toLowerCase();
		int linkStart = text.indexOf("<code>");
		if ( linkStart >= 0 ) {
			int linkEnd = text.indexOf("</code>", linkStart);
			link = text.substring(linkStart + "<code>".length(), linkEnd);
			if ( constant != null && !constant.equals(link) ) {
				System.out.println("Wrong code tag? " + clazz.name() + "." + name + " code: " + link + " constant: " + constant);
				errorCount++;
			}
		}
		if ( Character.isDigit(link.charAt(0)) ) {
			link = "c" + link;
		}
		return link;
	}
	
	private static String formatText(String text) {
		if ( text == null ) {
			return text;
		}
		text = StringUtils.replaceAll(text, "\n </pre>", "</pre>");
		return text;
	}
	
	private static boolean skipField(ClassDoc clazz, FieldDoc field) {
		if ( field.isPrivate() || field.containingClass() != clazz ) {
			return true;
		}
		return false;
	}
	
	private boolean skipMethod(MethodDoc method) {
		ClassDoc clazz = method.containingClass();
		boolean isInterface = clazz.isInterface() || ( clazz.isAbstract() && method.isAbstract() );
		if ( INTERFACES_ONLY && !isInterface ) {
			return true;
		}
		String name = method.name();
		if ( method.isPrivate() || name.equals("finalize") ) {
			return true;
		}
		if ( method.getRawCommentText().trim().startsWith("@deprecated INTERNAL") ) {
			return true;
		}
		String firstSentence = getFirstSentence(method.firstSentenceTags());
		String raw = method.getRawCommentText();
		if ( firstSentence != null && firstSentence.startsWith("INTERNAL") ) {
			return true;
		}
		if ( ( firstSentence == null || firstSentence.trim().length() == 0 ) && raw.indexOf("{@inheritDoc}") < 0 ) {
			if ( !doesOverride(method) ) {
				boolean setterOrGetter = name.startsWith("set") && method.parameters().length == 1;
				setterOrGetter |= name.startsWith("get") && method.parameters().length == 0;
				setterOrGetter |= name.startsWith("is") && method.parameters().length == 0
						&& method.returnType().toString().equals("boolean");
				if ( !setterOrGetter ) {
					addError("Undocumented method " + " (" + clazz.name() + ".java:" + method.position().line() + ") " + clazz + "." + name
							+ " " + raw);
					return true;
				}
			}
		}
		return false;
	}
	
	private void addError(String s) {
		if ( errors.add(s) ) {
			System.out.println(s);
			errorCount++;
		}
	}
	
	private boolean doesOverride(MethodDoc method) {
		ClassDoc clazz = method.containingClass();
		int parameterCount = method.parameters().length;
		return foundMethod(clazz, false, method.name(), parameterCount);
	}
	
	private boolean foundMethod(ClassDoc clazz, boolean include, String methodName, int parameterCount) {
		if ( include ) {
			MethodDoc[] ms = clazz.methods();
			for ( int j = 0; j < ms.length; j++ ) {
				MethodDoc m = ms[j];
				if ( m.name().equals(methodName) && m.parameters().length == parameterCount ) {
					return true;
				}
			}
		}
		ClassDoc[] ifs = clazz.interfaces();
		for ( int i = 0; i < ifs.length; i++ ) {
			if ( foundMethod(ifs[i], true, methodName, parameterCount) ) {
				return true;
			}
		}
		clazz = clazz.superclass();
		return clazz != null && foundMethod(clazz, true, methodName, parameterCount);
	}
	
	private static String getFirstSentence(Tag[] tags) {
		String firstSentence = null;
		if ( tags.length > 0 ) {
			Tag first = tags[0];
			firstSentence = first.text();
		}
		return firstSentence;
	}
	
	private static String getTypeName(boolean isStatic, Type type) {
		String s = type.typeName() + type.dimension();
		if ( isStatic ) {
			s = "static " + s;
		}
		return s;
	}
	
	private static boolean isDeprecated(MethodDoc method) {
		Tag[] tags = method.tags();
		boolean deprecated = false;
		for ( int j = 0; j < tags.length; j++ ) {
			Tag t = tags[j];
			if ( t.kind().equals("@deprecated") ) {
				deprecated = true;
			}
		}
		return deprecated;
	}
}
