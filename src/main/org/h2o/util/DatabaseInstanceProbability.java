package org.h2o.util;

import java.io.Serializable;

public class DatabaseInstanceProbability implements Serializable,
		Comparable<DatabaseInstanceProbability> {

	private static final long serialVersionUID = 2642261932912933106L;

	private double probability;

	public DatabaseInstanceProbability(double probability) {
		this.probability = probability;
	}

	public double getProbability() {
		return probability;
	}

	public void setProbability(double probability) {
		this.probability = probability;
	}

	@Override
	public int compareTo(DatabaseInstanceProbability o) {
		if (this.getProbability() > o.getProbability())
			return 1;
		else if (this.getProbability() < o.getProbability())
			return -1;
		else
			return 0;
	}
}
