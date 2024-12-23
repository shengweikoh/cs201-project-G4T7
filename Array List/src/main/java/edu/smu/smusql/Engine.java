package edu.smu.smusql;

import java.util.*;

public class Engine {
    private Database database = new Database();
    private Parser parser = new Parser();

    public Database getDatabase() {
        return database;
    }

    public String executeSQL(String query) {
        String[] tokens = query.trim().split("\\s+");
        String command = tokens[0].toUpperCase();

        switch (command) {
            case "CREATE":
                return create(tokens);
            case "INSERT":
                return insert(tokens);
            case "SELECT":
                return select(tokens);
            case "UPDATE":
                return update(tokens);
            case "DELETE":
                return delete(tokens);
            default:
                return "ERROR: Unknown command";
        }
    }

    public String insert(String[] tokens) {
        // Parse tokens
        List<Object> parsedData = parser.parseInsert(tokens);

        // Get table name and list of values from parsed data
        String tableName = (String) parsedData.get(0);
        List<Object> values = (List<Object>) parsedData.get(1);

        // Get table from database
        Table table = null;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }

        // Get primary key from values
        String primaryKey = values.get(0).toString(); // Convert primary key to String

        // Insert row to table
        try {
            table.insertRow(primaryKey, values); // Pass raw values, conversion happens in insertRow
        } catch (IllegalArgumentException e) {
            return "ERROR: " + e.getMessage(); // Return specific error messages
        }

        return "Insertion Successful";
    }

    public String update(String[] tokens) {
        // Check syntax
        if (!tokens[2].equalsIgnoreCase("SET")) {
            return "ERROR: Invalid UPDATE syntax";
        }

        String tableName = tokens[1];

        Table table = null;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return "ERROR: " + e.getMessage() + ": " + tableName;
        }

        // Parse the columns and values to be updated
        String updatedColumn = tokens[3];
        if (!table.getColumns().contains(updatedColumn)) {
            return "ERROR: Column not found: " + updatedColumn;
        }
        if (!tokens[4].equals("=")) {
            return "ERROR: Invalid assignment in SET clause";
        }
        String updatedValue = tokens[5];

        // Check if there's a WHERE clause
        List<String[]> whereClauseConditions = new ArrayList<>();
        List<Boolean> andOrConditions = new ArrayList<>();

        // Parse WHERE clause conditions
        if (tokens.length > 6 && tokens[6].equalsIgnoreCase("WHERE")) {
            for (int i = 7; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("AND")) {
                    // Store True for AND
                    andOrConditions.add(true);
                } else if (tokens[i].equalsIgnoreCase("OR")) {
                    // Store False for OR
                    andOrConditions.add(false);
                } else if (isOperator(tokens[i])) {
                    // Add condition with operator (column, operator, value)
                    String column = tokens[i - 1];
                    if (!table.getColumns().contains(column)) {
                        return "ERROR: Column not found: " + column;
                    }
                    String operator = tokens[i];
                    String value = tokens[i + 1];
                    whereClauseConditions.add(new String[]{column, operator, value});
                    i += 1; // Skip the value since it has been processed
                }
            }
        }

        // Get rows that satisfy the WHERE clause
        Set<Map<String, Object>> rowsToUpdate;
        if (whereClauseConditions.isEmpty()) {
            // No WHERE clause: update all rows
            rowsToUpdate = new HashSet<>(table.getAllRows());
        } else {
            rowsToUpdate = evaluateWhereCondition(whereClauseConditions.get(0), table);
            for (int i = 1; i < whereClauseConditions.size(); i++) {
                Set<Map<String, Object>> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
                if (andOrConditions.get(i - 1)) {
                    rowsToUpdate.retainAll(newRows);
                } else {
                    rowsToUpdate.addAll(newRows);
                }
            }
        }

        // Update the rows
        for (Map<String, Object> row : rowsToUpdate) {
            row.put(updatedColumn, updatedValue);
        }

        return rowsToUpdate.size() + " row(s) updated in " + tableName;
    }

    public String delete(String[] tokens) {
        // Check syntax
        if (!tokens[1].equalsIgnoreCase("FROM")) {
            return "ERROR: Invalid DELETE syntax";
        }

        String tableName = tokens[2];

        // Check if table doesn't exist
        if (!database.listTables().contains(tableName)) {
            return "ERROR: No such table";
        }

        // Fetch the table
        Table table = null;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }

        // Initialize whereClauseConditions list
        List<String[]> whereClauseConditions = new ArrayList<>();
        List<Boolean> andOrConditions = new ArrayList<>();

        // Parse WHERE clause conditions
        if (tokens.length > 3 && tokens[3].equalsIgnoreCase("WHERE")) {
            for (int i = 4; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("AND")) {
                    // Store True for AND
                    andOrConditions.add(true);
                } else if (tokens[i].equalsIgnoreCase("OR")) {
                    // Store False for OR
                    andOrConditions.add(false);
                } else if (isOperator(tokens[i])) {
                    // Add condition with operator (column, operator, value)
                    String column = tokens[i - 1];
                    if (!table.getColumns().contains(column)) {
                        return "ERROR: Column not found: " + column;
                    }
                    String operator = tokens[i];
                    String value = tokens[i + 1];
                    whereClauseConditions.add(new String[]{column, operator, value});
                    i += 1; // Skip the value since it has been processed
                }
            }
        }

        // Evaluate WHERE conditions to get rows to delete
        Set<Map<String, Object>> rowsToDelete;
        if (whereClauseConditions.isEmpty()) {
            // No WHERE clause: delete all rows
            rowsToDelete = new HashSet<>(table.getAllRows());
        } else {
            rowsToDelete = evaluateWhereCondition(whereClauseConditions.get(0), table);
            for (int i = 1; i < whereClauseConditions.size(); i++) {
                Set<Map<String, Object>> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
                if (andOrConditions.get(i - 1)) {
                    rowsToDelete.retainAll(newRows);
                } else {
                    rowsToDelete.addAll(newRows);
                }
            }
        }

        // Delete the matched rows
        for (Map<String, Object> rowToDelete : rowsToDelete) {
            String primaryKeyValue = rowToDelete.get(table.getPrimaryKey()).toString();
            table.deleteRow(primaryKeyValue);
        }

        return rowsToDelete.size() + " row(s) deleted from " + tableName;
    }

    public String select(String[] tokens) {
        // Check if the query syntax is valid
        if (!tokens[1].equals("*") || !tokens[2].equalsIgnoreCase("FROM")) {
            return "ERROR: Invalid SELECT syntax";
        }

        // Get the table name from the query
        String tableName = tokens[3];

        // Check if the table exists in the database
        if (!database.listTables().contains(tableName)) {
            return "ERROR: No such table";
        }

        // Retrieve the table
        Table table = null;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }

        List<String> columns = table.getColumns();

        // Initialize whereClauseConditions list
        List<String[]> whereClauseConditions = new ArrayList<>();
        List<Boolean> andOrConditions = new ArrayList<>();

        Set<Map<String, Object>> rows;

        if (tokens.length == 4) {
            // No WHERE clause: select all rows
            rows = new HashSet<>(table.getAllRows());
        } else {
            // Parse WHERE clause conditions
            if (tokens.length > 4 && tokens[4].equalsIgnoreCase("WHERE")) {
                for (int i = 5; i < tokens.length; i++) {
                    if (tokens[i].equalsIgnoreCase("AND")) {
                        // Store True for AND
                        andOrConditions.add(true);
                    } else if (tokens[i].equalsIgnoreCase("OR")) {
                        // Store False for OR
                        andOrConditions.add(false);
                    } else if (isOperator(tokens[i])) {
                        // Add condition with operator (column, operator, value)
                        String column = tokens[i - 1];
                        if (!table.getColumns().contains(column)) {
                            return "ERROR: Column not found: " + column;
                        }
                        String operator = tokens[i];
                        String value = tokens[i + 1];
                        whereClauseConditions.add(new String[]{column, operator, value});
                        i += 1; // Skip the value since it has been processed
                    }
                }
            }

            if (whereClauseConditions.isEmpty()) {
                return "ERROR: Invalid WHERE clause";
            }

            rows = evaluateWhereCondition(whereClauseConditions.get(0), table);

            for (int i = 1; i < whereClauseConditions.size(); i++) {
                Set<Map<String, Object>> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
                if (andOrConditions.get(i - 1)) {
                    rows.retainAll(newRows);
                } else {
                    rows.addAll(newRows);
                }
            }
        }

        StringBuilder result = new StringBuilder();
        result.append(String.join("\t", columns)).append("\n"); // Print column headers

        // Loop through the rows and append the values without the extra tab at the end
        for (Map<String, Object> row : rows) {
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                Object value = row.getOrDefault(column, "NULL"); // Use "NULL" if the value is missing

                result.append(value); // Append the value directly

                // Append a tab only if it's not the last column
                if (i < columns.size() - 1) {
                    result.append("\t");
                }
            }
            result.append("\n"); // Move to the next line after each row
        }

        return result.toString();
    }

    public String create(String[] tokens) {
        if (!tokens[1].equalsIgnoreCase("TABLE")) {
            return "ERROR: Invalid CREATE TABLE syntax";
        }

        String tableName = tokens[2];
        if (database.listTables().contains(tableName)) {
            return "ERROR: Table already exists";
        }

        String columnList = queryBetweenParentheses(tokens, 3);
        List<String> columns = Arrays.asList(columnList.split(","));
        columns.replaceAll(String::trim);

        if (columns.isEmpty()) {
            return "ERROR: No columns specified";
        }

        // Create the table with the table name and columns
        database.createTable(tableName, columns);

        return "Table " + tableName + " created";
    }

    // HELPER METHODS

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

    // Helper method to determine if a string is numeric
    private boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private Object parseValue(String valueStr) {
        valueStr = valueStr.trim();

        // Check if the value is numeric
        if (isNumeric(valueStr)) {
            return Double.parseDouble(valueStr); // Always return numeric values as Double
        }

        // Handle Boolean values
        if (valueStr.equalsIgnoreCase("true") || valueStr.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(valueStr);
        }

        // Otherwise, treat it as a raw string
        return valueStr;
    }

    private Set<Map<String, Object>> evaluateWhereCondition(String[] whereClauseCondition, Table table) {
        String column = whereClauseCondition[0].trim(); // Column name (e.g., "gpa")
        String operator = whereClauseCondition[1].trim(); // Operator (e.g., ">", "<", "=", etc.)
        String valueStr = whereClauseCondition[2].trim(); // Value (e.g., "3.8")

        Set<Map<String, Object>> matchingRows = new HashSet<>();

        // Get all rows from the table
        List<Map<String, Object>> allRows = table.getAllRows();

        for (Map<String, Object> row : allRows) {
            Object cellValue = row.get(column);

            if (cellValue == null) continue;

            int comparison = compareValues(cellValue.toString(), valueStr);

            switch (operator) {
                case "=":
                    if (comparison == 0) {
                        matchingRows.add(row);
                    }
                    break;
                case ">":
                    if (comparison > 0) {
                        matchingRows.add(row);
                    }
                    break;
                case ">=":
                    if (comparison >= 0) {
                        matchingRows.add(row);
                    }
                    break;
                case "<":
                    if (comparison < 0) {
                        matchingRows.add(row);
                    }
                    break;
                case "<=":
                    if (comparison <= 0) {
                        matchingRows.add(row);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported operator: " + operator);
            }
        }

        return matchingRows;
    }

    // Helper method to compare two values as numbers or strings
    private int compareValues(String val1, String val2) {
        if (isNumeric(val1) && isNumeric(val2)) {
            Double num1 = Double.parseDouble(val1);
            Double num2 = Double.parseDouble(val2);
            return num1.compareTo(num2);
        } else {
            return val1.compareTo(val2);
        }
    }
}
