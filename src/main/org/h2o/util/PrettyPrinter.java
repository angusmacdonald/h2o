package org.h2o.util;

import java.util.Collection;

public class PrettyPrinter {
	public static String printSet(Collection<?> set){
		StringBuilder sb = new StringBuilder();

		int i = 0;
		for (Object element: set){
			if (i > 0) sb.append(", ");
			sb.append(element);
			i++;
		}
		return sb.toString();
	}
}
