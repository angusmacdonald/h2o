package org.h2.command;

import java.util.HashSet;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class QPMMap<T> extends HashSet<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public boolean add(T e) {
		System.out.println("(size: " + (super.size()+1) + "): Adding new element: " + e);
		return super.add(e);
	}

	@Override
	public boolean remove(Object o) {
		System.out.println("(size: " + (super.size()+1) + "): Removing element: " + o);
		return super.remove(o);
	}

	@Override
	public void clear() {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Cleared set entirely.");
		super.clear();
	}

}
