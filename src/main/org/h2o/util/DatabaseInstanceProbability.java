package org.h2o.util;

import java.io.Serializable;

public class DatabaseInstanceProbability implements Serializable, Comparable<DatabaseInstanceProbability> {

    private static final long serialVersionUID = 2642261932912933106L;

    private double probability;

    public DatabaseInstanceProbability(final double probability) {

        this.probability = probability;
    }

    public double getProbability() {

        return probability;
    }

    public void setProbability(final double probability) {

        this.probability = probability;
    }

    @Override
    public int compareTo(final DatabaseInstanceProbability o) {

        if (probability > o.getProbability()) { return 1; }
        if (probability < o.getProbability()) { return -1; }
        return 0;
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(probability);
        result = prime * result + (int) (temp ^ temp >>> 32);
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final DatabaseInstanceProbability other = (DatabaseInstanceProbability) obj;
        if (Double.doubleToLongBits(probability) != Double.doubleToLongBits(other.probability)) { return false; }
        return true;
    }

}
