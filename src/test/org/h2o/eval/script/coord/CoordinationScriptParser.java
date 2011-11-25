package org.h2o.eval.script.coord;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.h2o.eval.script.coord.instructions.CheckMetaReplFactorInstruction;
import org.h2o.eval.script.coord.instructions.CheckReplFactorInstruction;
import org.h2o.eval.script.coord.instructions.CreateTableInstruction;
import org.h2o.eval.script.coord.instructions.Instruction;
import org.h2o.eval.script.coord.instructions.MachineInstruction;
import org.h2o.eval.script.coord.instructions.QueryInstruction;
import org.h2o.eval.script.coord.instructions.SleepInstruction;
import org.h2o.eval.script.coord.instructions.WorkloadInstruction;
import org.h2o.util.exceptions.WorkloadParseException;

/**
 * Use the {@link #parse(String)} method to parse a single line of a co-ordinator script.
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 *
 */
public class CoordinationScriptParser {

    /**
     * Example format: {sleep="5000"}
     */
    private static final String SLEEP_REGEX = "\\{sleep=\"(\\d+)\"\\}";

    /**
     * Example format: {0} {execute_workload="src/test/org/h2o/eval/workloads/test.workload"}
     */
    private static final String EXECUTE_WORKLOAD_REGEX = "\\{execute_workload=\"([^\"]*)\"(?:\\s+duration=\"(\\d+)\")?\\}";

    /**
     * Format: {<machine-id>} [query | execute workload operation]
     * Example format: {0} CREATE TABLE test (id int);
     * Example format: {0} {execute_workload="src/test/org/h2o/eval/workloads/test.workload"}
     */
    private static final String QUERY_REGEX = "\\{(\\d+)\\} ((.)*)";

    /**
     * Example format: {terminate_machine id="0"}

     */
    private static final String TERMINATE_MACHINE_REGEX = "\\{terminate_machine id=\"(\\d+)\"\\}";

    /**
     * Format: {start_machine id="<machine-id>" fail-after=<time_to_failure> [block-workloads="<boolean>"]}
     * n.b. blocking is optional, and false by default.
     * Example format: {start_machine id="0" fail-after="30000" block-workloads="true"}
     */
    private static final String START_MACHINE_REGEX = "\\{start_machine id=\"(\\d+)\"(?:\\s+fail-after=\"(\\d+)\")?(?:\\s+block-workloads=\"(true|false)\")?\\}";

    /**
     * Example format: {create_table id="1" name="test0" schema="id int, id int, str_a varchar(40), int_a BIGINT" prepopulate_with="300"}
     * 
     * prepopulate_with is the only optional value.
     */
    private static final String CREATE_TABLE_REGEX = "\\{create_table id=\"(\\d+)\"(?:\\s+name=\"([^\"]+)\")(?:\\s+schema=\"([^\"]+)\")(?:\\s+prepopulate_with=\"(\\d+)\")?\\}";

    /**
     * Example format: {check_repl_factor table="name" expected="3"}

     */
    private static final String CHECK_REPL_REGEX = "\\{check_repl_factor (?:name=\"([^\"]+)\")\\s+expected=\"(\\d+)\"\\}";

    /**
     * Example format: {check_meta_repl_factor table="name" expected="3"}

     */
    private static final String CHECK_META_REPL_REGEX = "\\{check_repl_factor (?:name=\"([^\"]+)\")?\\s*expected=\"(\\d+)\"\\}";

    private static final Pattern start_machine_pattern = Pattern.compile(START_MACHINE_REGEX);
    private static final Pattern terminate_machine_pattern = Pattern.compile(TERMINATE_MACHINE_REGEX);
    private static final Pattern query_pattern = Pattern.compile(QUERY_REGEX);
    private static final Pattern workload_pattern = Pattern.compile(EXECUTE_WORKLOAD_REGEX);
    private static final Pattern sleep_pattern = Pattern.compile(SLEEP_REGEX);
    private static final Pattern create_table_pattern = Pattern.compile(CREATE_TABLE_REGEX);
    private static final Pattern check_replication_factor_pattern = Pattern.compile(CHECK_REPL_REGEX);
    private static final Pattern check_meta_replication_factor_pattern = Pattern.compile(CHECK_REPL_REGEX);

    public static MachineInstruction parseStartMachine(final String action) throws WorkloadParseException {

        //format: {start_machine id="<machine-id>" fail-after=<time_to_failure> [block-workloads="<boolean>"]}
        //n.b. blocking is optional, and false by default.
        //example format: {start_machine id="0" fail-after="30000" block-workloads="true"}

        final Matcher matcher = start_machine_pattern.matcher(action);

        Long fail_after;
        Integer id;
        boolean blockWorkloads;

        if (matcher.matches()) {
            id = Integer.valueOf(matcher.group(1));
            final String failAfterString = matcher.group(2);
            fail_after = failAfterString != null ? Long.valueOf(failAfterString) : null;

            final String blockWorkloadsString = matcher.group(3);
            blockWorkloads = blockWorkloadsString != null ? Boolean.valueOf(blockWorkloadsString) : false;
        }
        else {
            throw new WorkloadParseException("Invalid syntax in : " + action);
        }

        return new MachineInstruction(id, fail_after, blockWorkloads, true);
    }

    public static MachineInstruction parseTerminateMachine(final String action) throws WorkloadParseException {

        return new MachineInstruction(parseSingleIDPattern(action, terminate_machine_pattern), null, false, false);
    }

    public static Instruction parseQueryOrWorkload(final String action) throws WorkloadParseException {

        final Matcher matcher = query_pattern.matcher(action);

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

    public static Instruction parseCreateTableOperation(final String action) throws WorkloadParseException {

        final Matcher matcher = create_table_pattern.matcher(action);

        String tableName = null;
        String schema = null;
        String prepopulateWith = null;
        String id = null;
        if (matcher.matches()) {
            id = matcher.group(1);
            tableName = matcher.group(2);
            schema = matcher.group(3);
            prepopulateWith = matcher.group(4);

        }
        else {
            throw new WorkloadParseException("Invalid syntax in : " + action);
        }

        return new CreateTableInstruction(id, tableName, schema, prepopulateWith);
    }

    public static Instruction parseCheckReplOperation(final String action) throws WorkloadParseException {

        final Matcher matcher = check_replication_factor_pattern.matcher(action);

        String tableName = null;
        int expected = 0;
        if (matcher.matches()) {
            tableName = matcher.group(1);
            expected = Integer.valueOf(matcher.group(2));

        }
        else {
            throw new WorkloadParseException("Invalid syntax in : " + action);
        }

        return new CheckReplFactorInstruction(tableName, expected);
    }

    public static Instruction parseCheckMetaReplOperation(final String action) throws WorkloadParseException {

        final Matcher matcher = check_meta_replication_factor_pattern.matcher(action);

        String tableName = null;
        int expected = 0;
        if (matcher.matches()) {
            tableName = matcher.group(1);
            expected = Integer.valueOf(matcher.group(2));

        }
        else {
            throw new WorkloadParseException("Invalid syntax in : " + action);
        }

        return new CheckMetaReplFactorInstruction(tableName, expected);
    }

    protected static WorkloadInstruction parseWorkloadRequest(final String query, final String id) throws WorkloadParseException {

        final Matcher matcher = workload_pattern.matcher(query);
        String workloadFile = null;
        Long duration = null;

        if (matcher.matches()) {
            workloadFile = matcher.group(1);
            duration = Long.valueOf(matcher.group(2));
        }
        else {
            throw new WorkloadParseException("Invalid syntax in : " + query);
        }

        return new WorkloadInstruction(id, workloadFile, duration);

    }

    public static SleepInstruction parseSleepOperation(final String action) throws WorkloadParseException {

        final Matcher matcher = sleep_pattern.matcher(action);
        String sleepTime = null;

        if (matcher.matches()) {
            sleepTime = matcher.group(1);
        }
        else {
            throw new WorkloadParseException("Invalid syntax in : " + action);
        }

        return new SleepInstruction(Integer.valueOf(sleepTime));
    }

    private static Integer parseSingleIDPattern(final String action, final Pattern pattern) throws WorkloadParseException {

        final Matcher matcher = pattern.matcher(action);

        Integer id = null;

        if (matcher.matches()) {
            id = Integer.valueOf(matcher.group(1));

        }
        else {
            throw new WorkloadParseException("Invalid syntax in : " + action);
        }

        return id;
    }

    /**
     * Create an instruction object by parsing the given line of a coordination script.
     * @param action
     * @return Returns null for comments and blank lines.
     * @throws WorkloadParseException
     */
    public static Instruction parse(final String action) throws WorkloadParseException {

        if (action.startsWith("#") || action.trim().equals("")) { // {machines-to-start="2"}
            //Comment... ignore.
            return null;
        }
        else if (start_machine_pattern.matcher(action).matches()) {

            return parseStartMachine(action);

        }
        else if (terminate_machine_pattern.matcher(action).matches()) {

            return parseTerminateMachine(action);

        }
        else if (sleep_pattern.matcher(action).matches()) {
            return parseSleepOperation(action);

        }
        else if (create_table_pattern.matcher(action).matches()) {
            return parseCreateTableOperation(action);

        }
        else if (check_replication_factor_pattern.matcher(action).matches()) {
            return parseCheckReplOperation(action);

        }
        else if (check_meta_replication_factor_pattern.matcher(action).matches()) {
            return parseCheckMetaReplOperation(action);

        }
        else {
            //Execute a query
            return parseQueryOrWorkload(action);

        }

    }

}
