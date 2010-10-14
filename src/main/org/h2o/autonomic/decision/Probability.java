/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.autonomic.decision;

public class Probability {

    public float high, medium_high, medium, medium_low, low;

    public static enum Interval {
        high, medium_high, medium, medium_low, low
    };

    /**
     * @param high
     *            The probability that utilization of this resource is high.
     * @param medium_high
     *            The probability that utilization of this resource is medium-high.
     * @param medium
     *            The probability that utilization of this resource is medium.
     * @param medium_low
     *            The probability that utilization of this resource is medium-low.
     * @param low
     *            The probability that utilization of this resource is low.
     */
    public Probability(float high, float medium_high, float medium, float medium_low, float low) {

        this.high = high;
        this.medium_high = medium_high;
        this.medium = medium;
        this.medium_low = medium_low;
        this.low = low;
    }

    /**
     * Get the value of this probability according to a metric which defines the value of this resource in general, and the amount of
     * information needed.
     * 
     * @param valueOfMeasurement
     *            The value of this probability to the metric.
     * @param upTo
     *            The number of intervals used to obtain the value. For example, if 'medium' is specified here the value will be calculated
     *            based on the probability that something is between medium and low utilization.
     * @return The value of this resource.
     */
    public double getValue(double valueOfMeasurement, Interval upTo) {

        float toMeasure = 0;

        switch (upTo) {
            case high:
                toMeasure += high;
            case medium_high:
                toMeasure += medium_high;
            case medium:
                toMeasure += medium;
            case medium_low:
                toMeasure += medium_low;
            case low:
                toMeasure += low;
        }

        return low * valueOfMeasurement;
    }
}
