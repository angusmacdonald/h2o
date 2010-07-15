/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.message.Message;

/**
 * A class to iterate over all permutations of an array.
 * The algorithm is from Applied Combinatorics, by Alan Tucker as implemented in
 * http://www.koders.com/java/fidD3445CD11B1DC687F6B8911075E7F01E23171553.aspx
 */
public class Permutations {

	private Object[] in;
	private Object[] out;
	private int n, m;
	private int[] index;
	private boolean hasNext = true;

	/**
	 * Create a new permutations object.
	 *
	 * @param in the source array
	 * @param out the target array
	 */
	public Permutations(Object[] in, Object[] out) {
		this(in, out, in.length);
	}

	/**
	 * Create a new permutations object.
	 *
	 * @param in the source array
	 * @param out the target array
	 * @param m the number of output elements to generate
	 */
	public Permutations(Object[] in, Object[] out, int m) {
		this.n = in.length;
		this.m = m;
		if (n < m || m < 0) {
			Message.throwInternalError("n < m or m < 0");
		}
		this.in = in;
		this.out = out;
		index = new int[n];
		for (int i = 0; i < n; i++) {
			index[i] = i;
		}

		// The elements from m to n are always kept ascending right to left.
		// This keeps the dip in the interesting region.
		reverseAfter(m - 1);
	}

	/**
	 * Move the index forward a notch. The algorithm first finds the rightmost
	 * index that is less than its neighbor to the right. This is the dip point.
	 * The algorithm next finds the least element to the right of the dip that
	 * is greater than the dip. That element is switched with the dip. Finally,
	 * the list of elements to the right of the dip is reversed.
	 * For example, in a permutation of 5 items, the index may be {1, 2, 4, 3,
	 * 0}. The dip is 2 the rightmost element less than its neighbor on its
	 * right. The least element to the right of 2 that is greater than 2 is 3.
	 * These elements are swapped, yielding {1, 3, 4, 2, 0}, and the list right
	 * of the dip point is reversed, yielding {1, 3, 0, 2, 4}.
	 */
	private void moveIndex() {
		// find the index of the first element that dips
		int i = rightmostDip();
		if (i < 0) {
			hasNext = false;
			return;
		}

		// find the least greater element to the right of the dip
		int leastToRightIndex = i + 1;
		for (int j = i + 2; j < n; j++) {
			if (index[j] < index[leastToRightIndex] && index[j] > index[i]) {
				leastToRightIndex = j;
			}
		}

		// switch dip element with least greater element to its right
		int t = index[i];
		index[i] = index[leastToRightIndex];
		index[leastToRightIndex] = t;

		if (m - 1 > i) {
			// reverse the elements to the right of the dip
			reverseAfter(i);

			// reverse the elements to the right of m - 1
			reverseAfter(m - 1);
		}
	}

	/**
	 * Get the index of the first element from the right that is less
	 * than its neighbor on the right.
	 *
	 * @return the index or -1 if non is found
	 */
	private int rightmostDip() {
		for (int i = n - 2; i >= 0; i--) {
			if (index[i] < index[i + 1]) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * Reverse the elements to the right of the specified index.
	 *
	 * @param i the index
	 */
	private void reverseAfter(int i) {
		int start = i + 1;
		int end = n - 1;
		while (start < end) {
			int t = index[start];
			index[start] = index[end];
			index[end] = t;
			start++;
			end--;
		}
	}

	/**
	 * Go to the next lineup, and if available, fill the target array.
	 *
	 * @return if a new lineup is available
	 */
	public boolean next() {
		if (!hasNext) {
			return false;
		}
		for (int i = 0; i < m; i++) {
			out[i] = in[index[i]];
		}
		moveIndex();
		return true;
	}

}
