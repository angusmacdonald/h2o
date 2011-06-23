package org.h2o.eval.coordinator.instructions;

public class QueryInstruction extends Instruction {

    private static final long serialVersionUID = -2808571499092300184L;

    /**
     * Query to be executed.
     */
    public final String query;

    public QueryInstruction(final String id, final String query) {

        super(id, false);
        this.query = query;
    }

    @Override
    public String getData() {

        return query;
    }

}
