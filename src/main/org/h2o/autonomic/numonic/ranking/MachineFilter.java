package org.h2o.autonomic.numonic.ranking;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Filters out suitable machines according to requirements.
 *
 * @author Alan Dearle (al@cs.st-andrews.ac.uk)
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class MachineFilter {

    public Set<MachineMonitoringData> selectSuitableMachines(final Collection<MachineMonitoringData> machineData, final Requirements requirements) {

        final Set<MachineMonitoringData> suitable_machines = new HashSet<MachineMonitoringData>();

        for (final MachineMonitoringData machine : machineData) {
            if (machine.meetsRequirements(requirements)) {
                suitable_machines.add(machine);
            }
        }

        return suitable_machines;
    }

}
