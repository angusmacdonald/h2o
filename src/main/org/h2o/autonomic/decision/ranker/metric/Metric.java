/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.autonomic.decision.ranker.metric;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import uk.ac.standrews.cs.numonic.data.ResourceType;

public class Metric implements Serializable {

    private static final long serialVersionUID = -3656189458012849935L;

    /*
     * ####################### PROBABILITY-BASED. ####################### cpu + memory + network + disk should add up to one.
     */

    private final Map<EnumResourceRequest, Long> requestInformation = new HashMap<EnumResourceRequest, Long>();

    private final Map<ResourceType, Double> importanceOfResources = new HashMap<ResourceType, Double>();

    public Metric(final long expectedTimeToCompletion, final long immediateDiskSpace, final double cpu, final double memory, final double network, final double disk) {

        requestInformation.put(EnumResourceRequest.EXPECTED_TIME_TO_COMPLETE, expectedTimeToCompletion);
        requestInformation.put(EnumResourceRequest.DISK_SPACE_NEEDED, immediateDiskSpace);

        //TODO fix.
        //        importanceOfResources.put(ResourceType.CPU_UTIL, cpu);
        //        importanceOfResources.put(ResourceType.MEMORY_UTIL, memory);
        //        importanceOfResources.put(ResourceType.NETWORK_UTIL, network);
        //        importanceOfResources.put(ResourceType.DISK_UTIL, disk);
    }

    public Map<ResourceType, Double> getResourceImportanceMap() {

        return importanceOfResources;
    }

    public static Metric getDefaultSystemTableMetric() {

        return new Metric(0, 0, 0.25, 0.25, 0.25, 0.25);
    }
}
