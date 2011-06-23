package org.h2o.eval.coordinator.instructions;

public class StartMachineInstruction {

    /**
     * ID to be given to this machine being started.
     */
    public final Integer id;

    /**
     * Optional field. How long in milliseconds before the machine is terminated.
     */
    public final String fail_after;

    public StartMachineInstruction(final Integer id, final String fail_after) {

        this.id = id;
        this.fail_after = fail_after;
    }

}
