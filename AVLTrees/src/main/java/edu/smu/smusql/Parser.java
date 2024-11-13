package edu.smu.smusql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
 * This is a rudimentary parser you may want to use to parse smuSQL statements.
 * Use of this parser is optional.
 * You may decide to use your own parser.
 * Example usage of some code in this parser can be found in the sample implementation.
 * However, the sample implementation does not have a 'Parser' class.
 */
public class Parser {

    // public List<Object> parseInsert(String[] tokens) {
    //     String tableName = tokens[2]; // The name of the table to be inserted into.
    //     String valueList = queryBetweenParentheses(tokens, 4); // Create values list between parentheses
    //     List<String> values = Arrays.asList(valueList.split(",")); // The values between parentheses.
    //     List<Object> data = new ArrayList<>();
    //     data.add(tableName);
    //     data.add(values);
    //     return data;
    // }
        

    public void parseDelete(String[] tokens) {
        // Eg delete from student where sid = 657889
        String tableName = tokens[2]; // The name of the table to be deleted from.

        List<String[]> whereClauseConditions = new ArrayList<>(); // Array for storing conditions from the where clause.

        // Parse WHERE clause conditions
        if (tokens.length > 3 && tokens[3].toUpperCase().equals("WHERE")) {
            for (int i = 4; i < tokens.length; i++) {
                if (tokens[i].toUpperCase().equals("AND") || tokens[i].toUpperCase().equals("OR")) {
                    // Add AND/OR conditions
                    whereClauseConditions.add(new String[] {tokens[i].toUpperCase(), null, null, null});
                } else if (isOperator(tokens[i])) {
                    // Add condition with operator (column, operator, value)
                    String column = tokens[i - 1];
                    String operator = tokens[i];
                    String value = tokens[i + 1];
                    whereClauseConditions.add(new String[] {null, column, operator, value});
                    i += 1; // Skip the value since it has been processed
    }
}
        }
    }
    

// Method to parse the UPDATE statement
public void parseUpdate(String[] tokens) {

    String tableName = tokens[1]; // name of the table to be updated

    String setColumn = tokens[3]; // column to be updated
    String newValue = tokens[5]; // new value for above column
    
    // Initialize whereClauseConditions list
    List<String[]> whereClauseConditions = new ArrayList<>();

        // Parse WHERE clause conditions
        if (tokens.length > 6 && tokens[6].equalsIgnoreCase("WHERE")) {
            for (int i = 5; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("AND") || tokens[i].equalsIgnoreCase("OR")) {
                    // Add AND/OR conditions
                    whereClauseConditions.add(new String[] {tokens[i].toUpperCase(), null, null, null});
                } else if (isOperator(tokens[i])) {
                    // Add condition with operator (column, operator, value)
                    String column = tokens[i - 1];
                    String operator = tokens[i];
                    String value = tokens[i + 1];
                    whereClauseConditions.add(new String[] {null, column, operator, value});
                    i += 1; // Skip the value since it has been processed
                }
            }
        }

}

    // Method to parse the CREATE TABLE statement
    public List<String> parseCreate(String[] tokens) {
        List<String> columns = new ArrayList<>();        

        // Find the starting index of the column definitions
        int startIndex = 3; // Assuming tokens[0] is "CREATE", tokens[1] is "TABLE", tokens[2] is the table name

        for (int i = startIndex; i < tokens.length; i++) {
            String token = tokens[i].replaceAll("[(),]", "").trim(); // Remove parentheses and commas
            if (!token.isEmpty()) {
                columns.add(token);
            }
        }

        return columns;
    }

public List<String[]> parseWhereClause(String[] tokens) {
    List<String[]> whereClauseConditions = new ArrayList<>();

    // Find the starting index of the WHERE clause
    int whereIndex = -1;
    for (int i = 0; i < tokens.length; i++) {
        if (tokens[i].equalsIgnoreCase("WHERE")) {
            whereIndex = i;
            break;
        }
    }

    // If WHERE clause is found, parse the conditions
    if (whereIndex != -1) {
        for (int i = whereIndex + 1; i < tokens.length; i++) {
            if (tokens[i].equalsIgnoreCase("AND") || tokens[i].equalsIgnoreCase("OR")) {
                // Add AND/OR conditions
                whereClauseConditions.add(new String[] {tokens[i].toUpperCase(), null, null, null});
            } else if (isOperator(tokens[i])) {
                // Add condition with operator (column, operator, value)
                String column = tokens[i - 1];
                String operator = tokens[i];
                String value = tokens[i + 1];
                whereClauseConditions.add(new String[] {null, column, operator, value});
                i += 1; // Skip the value since it has been processed
            }
        }
    }
    return whereClauseConditions;
    
}


    // Helper method to extract content inside parentheses
    private String queryBetweenParentheses(String[] tokens, int startIndex) {
        StringBuilder result = new StringBuilder();
        for (int i = startIndex; i < tokens.length; i++) {
            result.append(tokens[i]).append(" ");
        }
        return result.toString().trim().replaceAll("\\(", "").replaceAll("\\)", "");
    }

    // Helper method to determine if a string is an operator
    private boolean isOperator(String token) {
        return token.equals("=") || token.equals(">") || token.equals("<") || token.equals(">=") || token.equals("<=");
    }

    }
