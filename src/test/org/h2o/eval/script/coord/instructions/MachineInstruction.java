package org.h2o.eval.script.coord.instructions;

public class MachineInstruction {

    /**
     * ID to be given to this machine being started.
     */
    public final Integer id;

    /**
     * Optional field. How long in milliseconds before the machine is terminated.
     */
    public final Long fail_after;

    public MachineInstruction(final Integer id, final Long fail_after) {

        this.id = id;
        this.fail_after = fail_after;
    }

}
