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
        // Check syntax
        if (!tokens[1].toUpperCase().equals("INTO")) {
            return "ERROR: Invalid INSERT INTO syntax";
        }

        // Get table name and list of values from parsed data
        String tableName = tokens[2];

        // Get table from database
        Table table = null;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return "ERROR: " + e.getMessage() + ": " + tableName;
        }

        // Extract values between parentheses
        String valueList = queryBetweenParentheses(tokens, 4);
        List<String> values = Arrays.asList(valueList.split(","));

        // Get primary key from values
        String primaryKey = values.get(0).toString(); // Convert primary key to String

        try {
            table.insertRow(primaryKey, values); // Pass raw values, conversion happens in insertRow
        } catch (IllegalArgumentException e) {
            return "ERROR: " + e.getMessage(); // Return specific error messages
        }

        return "Row inserted into " + tableName;
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
                    whereClauseConditions.add(new String[] { column, operator, value });
                    i += 1; // Skip the value since it has been processed
                }
            }
        }

        // Get rows that satisfy the WHERE clause
        Set<String> rowsToUpdate;
        if (tokens.length == 6) {
            // No WHERE clause: update all rows
            rowsToUpdate = table.getPrimaryKeyMap().keySet();
        } else {
            rowsToUpdate = evaluateWhereCondition(whereClauseConditions.get(0), table);
            for (int i = 1; i < whereClauseConditions.size(); i++) {
                Set<String> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
                if (andOrConditions.get(i - 1)) {
                    rowsToUpdate.retainAll(newRows);
                } else {
                    rowsToUpdate.addAll(newRows);
                }
            }
        }

        TreeMap<String, List<String>> columnMap = table.getColumnTreeMap(updatedColumn);
        Map<String, Map<String, String>> rows = table.getPrimaryKeyMap();

        // Remove id from the column TreeMap
        for (String primaryKey : rowsToUpdate) {
            Map<String, String> row = rows.get(primaryKey);
            // Remove id from previous key
            columnMap.get(row.get(updatedColumn)).remove(primaryKey);
            // Update the row with the new value
            row.put(updatedColumn, updatedValue);
        }

        // Add ids to the new key in the column TreeMap
        if (columnMap.containsKey(updatedValue)) {
            columnMap.get(updatedValue).addAll(rowsToUpdate);
        } else {
            columnMap.put(updatedValue, new ArrayList<>(rowsToUpdate));
        }

        return "Table " + tableName + " updated." + rowsToUpdate.size() + " row(s) affected.";
    }

    public String delete(String[] tokens) {
        // Check syntax
        if (!tokens[1].toUpperCase().equals("FROM") || !tokens[3].toUpperCase().equals("WHERE")) {
            return "ERROR: Invalid DELETE syntax";
        }

        String tableName = tokens[2];

        // Fetch the table
        Table table = null;
        try {
            table = database.getTable(tableName); // database is where tables are stored after being created - not the
                                                  // same as cache
        } catch (IllegalArgumentException e) {
            return e.getMessage() + ": " + tableName;
        }

        // Initialize whereClauseConditions list
        List<String[]> whereClauseConditions = new ArrayList<>();
        List<Boolean> andOrConditions = new ArrayList<>();

        // Iterate through the where condition
        if (tokens.length > 3 && tokens[3].toUpperCase().equals("WHERE")) {
            for (int i = 4; i < tokens.length; i++) {
                if (tokens[i].toUpperCase().equals("AND")) {
                    // true for AND
                    andOrConditions.add(true);
                } else if (tokens[i].toUpperCase().equals("OR")) {
                    // false for OR
                    andOrConditions.add(false);
                } else if (isOperator(tokens[i])) {
                    // eg where gpa < 2.0
                    // col, operator, value

                    String column = tokens[i - 1]; // idx is at operator so -1 to go back

                    if (!table.getColumns().contains(column)) {
                        return "ERROR: Column not found: " + column;
                    }

                    String operator = tokens[i];
                    String value = tokens[i + 1];

                    whereClauseConditions.add(new String[] { column, operator, value });

                    i++; // increment i by 1 because i+1 stored in token
                }
            }
        }

        // Evaluate WHERE conditions to get rows to delete
        Set<String> rowsToDelete = evaluateWhereCondition(whereClauseConditions.get(0), table);

        for (int i = 1; i < whereClauseConditions.size(); i++) {
            Set<String> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
            if (andOrConditions.get(i - 1)) {
                rowsToDelete.retainAll(newRows); // AND condition
            } else {
                rowsToDelete.addAll(newRows); // OR condition
            }
        }

        List<String> columns = table.getColumns();
        Map<String, Map<String, String>> rows = table.getPrimaryKeyMap();

        // Delete the rows from the column TreeMaps
        for (String column : columns) {
            TreeMap<String, List<String>> columnMap = table.getColumnTreeMap(column);
            for (String rowId : rowsToDelete) {
                Map<String, String> row = rows.get(rowId);
                columnMap.get(row.get(column)).remove(rowId);
            }
        }

        // Delete the rows from the primaryKeyMap
        for (String rowId : rowsToDelete) {
            rows.remove(rowId);
        }

        return "Rows deleted from " + tableName + ". " + rowsToDelete.size() + " row(s) affected.";
    }

    public String select(String[] tokens) {
        // Check if the query syntax is valid
        if (!tokens[1].equals("*") || !tokens[2].toUpperCase().equals("FROM")) {
            return "ERROR: Invalid SELECT syntax";
        }

        // Get the table name from the query
        String tableName = tokens[3];

        // Retrieve the table
        Table table = null;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return "ERROR: " + e.getMessage() + ": " + tableName;
        }

        // List of columns from the table
        List<String> columns = table.getColumns();

        // Initialize whereClauseConditions list
        List<String[]> whereClauseConditions = new ArrayList<>();
        List<Boolean> andOrConditions = new ArrayList<>();

        // Fetch all rows from the primary key map
        Map<String, Map<String, String>> allRows = table.getPrimaryKeyMap();

        if (tokens.length == 4) {
            // No WHERE clause: use all rows
            return buildResultWithRows(columns, allRows.values());
        }

        // Parse WHERE clause conditions
        if (tokens.length > 4 && tokens[4].equalsIgnoreCase("WHERE")) {
            for (int i = 5; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("AND")) {
                    andOrConditions.add(true);
                } else if (tokens[i].equalsIgnoreCase("OR")) {
                    andOrConditions.add(false);
                } else if (isOperator(tokens[i])) {
                    String column = tokens[i - 1];
                    if (!table.getColumns().contains(column)) {
                        return "ERROR: Column not found: " + column;
                    }
                    String operator = tokens[i];
                    String value = tokens[i + 1];
                    whereClauseConditions.add(new String[] { column, operator, value });
                    i += 1;
                }
            }
        }

        // Evaluate WHERE conditions
        Set<String> rows = evaluateWhereCondition(whereClauseConditions.get(0), table);
        for (int i = 1; i < whereClauseConditions.size(); i++) {
            Set<String> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
            if (andOrConditions.get(i - 1)) {
                rows.retainAll(newRows);
            } else {
                rows.addAll(newRows);
            }
        }

        // Filtered rows based on WHERE conditions
        List<Map<String, String>> filteredRows = new ArrayList<>();
        for (String rowId : rows) {
            filteredRows.add(allRows.get(rowId));
        }

        return buildResultWithRows(columns, filteredRows);
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
        database.createTable(tableName, columns, false); // Set to true if want to use BTree

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

    private Set<String> evaluateWhereCondition(String[] whereClauseCondition, Table table) {
        String column = whereClauseCondition[0].trim(); // Column name (e.g., "gpa")
        String operator = whereClauseCondition[1].trim(); // Operator (e.g., ">", "<", "=", etc.)
        String valueStr = whereClauseCondition[2].trim(); // Value (e.g., "3.8")

        // We will store the keys to the matching rows in a TreeSet to avoid duplicates
        // and auto sort keys in ascending order
        Set<String> matchingRows = new TreeSet<>();

        // Assume we are dealing with TreeMap that stores keys as Strings
        TreeMap<String, List<String>> columnTreeMap = table.getColumnTreeMap(column);
        if (columnTreeMap == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        // System.out.println(columnTreeMap.toString());

        // Handle different operators
        switch (operator) {
            case "=":
                // Exact match
                List<String> exactMatches = columnTreeMap.get(valueStr); // Use the original string key for exact match
                if (exactMatches != null) {
                    matchingRows.addAll(exactMatches);
                }
                break;
            case ">":
            case ">=":
            case "<":
            case "<=":
                // For range queries, we need to convert the String keys to the correct type for
                // comparison
                SortedMap<String, List<String>> subMap;
                if (operator.equals(">")) {
                    subMap = columnTreeMap.tailMap(valueStr, false); // Get all values greater than valueStr
                } else if (operator.equals(">=")) {
                    subMap = columnTreeMap.tailMap(valueStr, true); // Get all values greater than or equal to valueStr
                } else if (operator.equals("<")) {
                    subMap = columnTreeMap.headMap(valueStr, false); // Get all values less than valueStr
                } else {
                    subMap = columnTreeMap.headMap(valueStr, true); // Get all values less than or equal to valueStr
                }

                for (List<String> rows : subMap.values()) {
                    matchingRows.addAll(rows);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator);
        }

        return matchingRows;
    }

    // Helper function to build result string with rows
    private String buildResultWithRows(List<String> columns, Collection<Map<String, String>> rows) {
        StringBuilder result = new StringBuilder();
        result.append(String.join("\t", columns)).append("\n"); // Print column headers

        for (Map<String, String> rowData : rows) {
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                String value = rowData.getOrDefault(column, "NULL"); // Use "NULL" if the value is missing

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
}