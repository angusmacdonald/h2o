package org.h2o.eval.coordinator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.h2o.eval.coordinator.instructions.Instruction;
import org.h2o.eval.coordinator.instructions.QueryInstruction;
import org.h2o.eval.coordinator.instructions.StartMachineInstruction;
import org.h2o.eval.coordinator.instructions.WorkloadInstruction;
import org.h2o.util.exceptions.WorkloadParseException;

public class CoordinationScriptExecutor {

    protected static StartMachineInstruction parseStartMachine(final String action) throws WorkloadParseException {

        //format: {start_machine id="<machine-id>" fail-after=<time_to_failure>}
        //example format: {start_machine id="0" fail-after="30000"}

        final Pattern p = Pattern.compile("\\{start_machine id=\"(\\d+)\"(?: fail-after=\"(\\d+)\")?\\}");

        final Matcher matcher = p.matcher(action);

        String fail_after = null;
        Integer id = null;

        if (matcher.matches()) {
            id = Integer.valueOf(matcher.group(1));
            fail_after = matcher.group(2);
        }
        else {
            throw new WorkloadParseException("Invalid syntax in : " + action);
        }

        return new StartMachineInstruction(id, fail_after);
    }

    protected static Instruction parseQuery(final String action) throws WorkloadParseException {

        //format: {<machine-id>} [query | execute workload operation]
        //example format: {0} CREATE TABLE test (id int);
        //example format: {0} {execute_workload="src/test/org/h2o/eval/workloads/test.workload"}

        final Pattern p = Pattern.compile("\\{(\\d+)\\} ((.)*)");

        final Matcher matcher = p.matcher(action);

        String query = null;
        String id = null;

        if (matcher.matches()) {
            id = matcher.group(1);
            query = matcher.group(2);
        }
        else {
            throw new WorkloadParseException("Invalid syntax in : " + action);
        }

        if (query != null && query.contains("execute_workload")) {
            return parseWorkloadRequest(query, id);
        }
        else {
            return new QueryInstruction(id, query);
        }

    }

    protected static WorkloadInstruction parseWorkloadRequest(final String query, final String id) throws WorkloadParseException {

        //example format: {0} {execute_workload="src/test/org/h2o/eval/workloads/test.workload"}

        final Pattern p = Pattern.compile("\\{execute_workload=\"([^\"]*)\"(?: duration=\"(\\d+)\")?\\}");

        final Matcher matcher = p.matcher(query);
        String workloadFile = null;
        String duration = null;

        if (matcher.matches()) {
            workloadFile = matcher.group(1);
            duration = matcher.group(2);
        }
        else {
            throw new WorkloadParseException("Invalid syntax in : " + query);
        }

        return new WorkloadInstruction(id, workloadFile, duration);

    }

    protected static int parseSleepOperation(final String action) throws WorkloadParseException {

        //example format: {sleep=5000}

        final Pattern p = Pattern.compile("\\{sleep=\"(\\d+)\"\\}");

        final Matcher matcher = p.matcher(action);
        String sleepTime = null;

        if (matcher.matches()) {
            sleepTime = matcher.group(1);
        }
        else {
            throw new WorkloadParseException("Invalid syntax in : " + action);
        }

        return Integer.valueOf(sleepTime);
    }
}
