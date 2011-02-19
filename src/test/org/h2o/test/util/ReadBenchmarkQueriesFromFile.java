package org.h2o.test.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class ReadBenchmarkQueriesFromFile {

    /**
     * Reads in SQL statements from a specified external file and 'cleans' them, meaning it takes out any extraneous starting
     * characters such as '>', and changes prepared statements to regular sql statements. For example, the query 
     * <i>'INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES ( ?, ?, ?) {1: 3001, 2: 9, 3: 1};'</i> becomes
     * <i>'INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES ( 3001, 9, 1);'</i>
     * @param fileName
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static ArrayList<String> getSQLQueriesFromFile(final String fileName) throws FileNotFoundException, IOException {

        return cleanSQL(readFromFile(fileName));
    }

    private static void printSQL(final ArrayList<String> cleansedSQL) {

        for (final String sqlString : cleansedSQL) {
            System.out.println(sqlString);
        }
    }

    /**
    * Removes any text before the SQL query string and resolves prepared statements to become general SQL statements.
     * @param lines SQL statements, one per entry.
     * @return list of SQL statements that can be executed by a database.
     */
    private static ArrayList<String> cleanSQL(final ArrayList<String> lines) {

        final ArrayList<String> cleansedSQL = new ArrayList<String>(lines.size());

        for (final String sqlString : lines) {

            String cleansedSQLString = removeStartingCharacters(sqlString);

            cleansedSQLString = adjustPreparedStatements(cleansedSQLString);

            cleansedSQL.add(cleansedSQLString);

        }

        return cleansedSQL;
    }

    /**
     * Very primitive parsing of SQL string to convert prepared statements to a regular SQL statement. It assumes
     * that there are no question marks within quotes in the query.
     */
    private static String adjustPreparedStatements(final String sqlString) {

        //Example query: INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES ( ?, ?, ?) {1: 3001, 2: 9, 3: 1};
        //This example is used throughout comments in this method.

        final String questionMarkCharacterInUnicode = "\u003F";

        if (!sqlString.contains(questionMarkCharacterInUnicode)) { return sqlString; } //Not a prepared statement if it doesn't have this.

        String newSQLString = sqlString.substring(0, sqlString.indexOf("{") - 1); //of the form: 'INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES ( ?, ?, ?)'

        final String preparedStatementParameters = sqlString.substring(sqlString.indexOf("{"), sqlString.indexOf("}")); //of the form: '1: 3001, 2: 9, 3: 1'
        final String[] parameters = preparedStatementParameters.split(",");

        for (final String parameter : parameters) {
            // of the form: 1: '3001'  (number: value).
            final String parameterValue = parameter.substring(parameter.indexOf(":") + 2).trim(); //of the form: '3001'

            newSQLString = newSQLString.replaceFirst("\\?", parameterValue);
        }

        return newSQLString;

    }

    public static String removeStartingCharacters(final String sqlString) {

        String cleansedSQLString = sqlString;

        if (sqlString.startsWith("> ")) {
            cleansedSQLString = cleansedSQLString.substring("> ".length());
        }
        return cleansedSQLString;
    }

    /**
     * Reads text from the specified file and returns it as an arraylist, each entry being a single line from the file.
     * @param fileName  File to be opened.
     * @return List of lines from the file.
     * @throws FileNotFoundException    
     * @throws IOException
     */
    public static ArrayList<String> readFromFile(final String fileName) throws FileNotFoundException, IOException {

        BufferedReader in = null;

        final ArrayList<String> linesFromFile = new ArrayList<String>();
        try {
            in = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = in.readLine()) != null) {
                linesFromFile.add(line);
            }
        }
        finally {
            if (in != null) {
                in.close();
            }
        }

        return linesFromFile;
    }

    /**
     * Very basic test.
     */
    public static void main(final String[] args) {

        try {
            final String fileName = "C:\\Users\\Angus\\workspace\\h2o\\testQueries\\test.txt";
            final ArrayList<String> lines = readFromFile(fileName);

            final ArrayList<String> cleansedSQL = cleanSQL(lines);

            printSQL(cleansedSQL);
        }
        catch (final IOException e) {
        }
    }

}
