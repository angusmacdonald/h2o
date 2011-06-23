package org.h2o.eval.coordinator.instructions;

public class WorkloadInstruction extends Instruction {

    private static final long serialVersionUID = -5175906689517121113L;

    /**
     * Workload to be executed.
     */
    public final String workloadFile;

    /**
     * How long the workload should be executed for.
     */
    public final Long duration;

    public WorkloadInstruction(final String id, final String workloadFile, final Long duration) {

        super(id, true);

        this.workloadFile = workloadFile;
        this.duration = duration == null ? 0 : duration;

    }

    @Override
    public String getData() {

        return workloadFile;
    }

}
