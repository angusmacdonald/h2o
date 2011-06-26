package org.h2o.eval.script.coord.instructions;

import java.io.Serializable;

public abstract class Instruction implements Serializable {

    private static final long serialVersionUID = 1497017264425067258L;

    /**
     * Machine which should execute a given instruction.
     */
    public final String id;

    /**
     * Whether this instruction is to execute a workload, or if it is a single query.
     */
    private final boolean isWorkload;

    public Instruction(final String id, final boolean isWorkload) {

        this.id = id;
        this.isWorkload = isWorkload;
    }

    public boolean isWorkload() {

        return isWorkload;
    }

    public abstract String getData();

}
