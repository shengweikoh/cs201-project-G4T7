package edu.smu.smusql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

public class Engine {
    private Database database = new Database();
    private TransactionLog transactionLog = new TransactionLog();
    private List<String> queryHistory = new ArrayList<>();

    public Database getDatabase() {
        return database;
    }

    public String executeSQL(String query) {
        String[] tokens = query.trim().split("\\s+");
        String command = tokens[0].toUpperCase();

        // Save the typed query to history ("SELECT * FROM table WHERE column = value")
        if (!command.equals("HISTORY") && !command.equals("EXECUTE") && !command.equals("CLEAR")) {
            queryHistory.add(query);
        }

        // If history reached 25 queries, remove the oldest query
        if (queryHistory.size() > 25) {
            queryHistory.remove(0); // Remove the oldest entry
        }

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
            case "UNDO":
                return undo();
            case "EXECUTE":
                return executeHistory(tokens);
            case "CLEAR":
                return clearHistory(tokens);
            case "HISTORY":
                return showHistory();
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

        // Get columns
        List<String> columns = table.getColumns();

        // Convert all values to Strings within this method
        Map<String, String> row = new HashMap<>();

        try {
            if (values.size() != columns.size()) {
                throw new IllegalArgumentException("Number of values doesn't match number of columns");
            }

            for (int i = 0; i < columns.size(); i++) {
                String value = values.get(i).toString().trim(); // Convert each value to String and trim whitespace
                row.put(columns.get(i), value);
            }

            table.insertRow(primaryKey, row); // Pass raw values, conversion happens in insertRow
        } catch (IllegalArgumentException e) {
            return "ERROR: " + e.getMessage(); // Return specific error messages
        }

        // After successful insertion, log the transaction
        Set<String> primaryKeys = new HashSet<>();
        primaryKeys.add(primaryKey);
        transactionLog.addTransaction(new Transaction("INSERT", tableName, primaryKeys, null)); // No previous state

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

        Map<String, Map<String, String>> rows = table.getPrimaryKeyMap();
        Map<String, Map<String, String>> previousStates = new HashMap<>();

        for (String primaryKey : rowsToUpdate) {
            previousStates.put(primaryKey, new HashMap<>(rows.get(primaryKey))); // Deep copy
        }

        // Use the updateRows method in Table
        table.updateRows(rowsToUpdate, updatedColumn, updatedValue);

        // Log the transaction
        transactionLog
                .addTransaction(new Transaction("UPDATE", tableName, rowsToUpdate, previousStates, updatedColumn));

        return "Table " + tableName + " updated. " + rowsToUpdate.size() + " row(s) affected.";
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

        Map<String, Map<String, String>> rows = table.getPrimaryKeyMap();// Capture previous states of rows to delete
        Map<String, Map<String, String>> previousStates = new HashMap<>();

        for (String primaryKey : rowsToDelete) {
            previousStates.put(primaryKey, new HashMap<>(rows.get(primaryKey))); // Deep copy
        }

        table.deleteRows(rowsToDelete);

        // Log the transaction
        transactionLog.addTransaction(new Transaction("DELETE", tableName, rowsToDelete, previousStates));

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

    private Set<String> evaluateWhereCondition(String[] whereClauseCondition, Table table) {
        String column = whereClauseCondition[0].trim(); // Column name (e.g., "gpa")
        String operator = whereClauseCondition[1].trim(); // Operator (e.g., ">", "<", "=", etc.)
        String valueStr = whereClauseCondition[2].trim(); // Value (e.g., "3.8")

        // We will store the keys to the matching rows in a TreeSet to avoid duplicates
        // and auto sort keys in ascending order
        Set<String> matchingRows = new TreeSet<>();

        // Use TreeMap (Red-Black Tree) indexing
        TreeMap<String, List<String>> columnTreeMap = table.getColumnTreeMap(column);
        if (columnTreeMap == null) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        // Handle different operators for TreeMap
        switch (operator) {
            case "=":
                List<String> exactMatches = columnTreeMap.get(valueStr);
                if (exactMatches != null) {
                    matchingRows.addAll(exactMatches);
                }
                break;
            case ">":
            case ">=":
            case "<":
            case "<=":
                SortedMap<String, List<String>> subMap;

                if (operator.equals(">")) {
                    subMap = columnTreeMap.tailMap(valueStr, false);
                } else if (operator.equals(">=")) {
                    subMap = columnTreeMap.tailMap(valueStr, true);
                } else if (operator.equals("<")) {
                    subMap = columnTreeMap.headMap(valueStr, false);
                } else {
                    subMap = columnTreeMap.headMap(valueStr, true);
                }

                for (List<String> rows : subMap.values()) {
                    matchingRows.addAll(rows);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
        // }

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

    // X factor - Undo & Show history
    public String undo() {
        Transaction lastTransaction = transactionLog.getLastTransaction();
        if (lastTransaction == null) {
            return "No operation to undo.";
        }

        String tableName = lastTransaction.getTableName();
        Table table = database.getTable(tableName);
        Set<String> primaryKeys = lastTransaction.getPrimaryKeys();

        switch (lastTransaction.getOperationType()) {
            case "INSERT":
                // Undo insert: delete the inserted rows
                table.deleteRows(primaryKeys);
                return "Undo successful: Last insert has been removed from " + tableName + ".";

            case "UPDATE":
                // Undo update: restore each row to its previous state
                Map<String, Map<String, String>> previousStates = lastTransaction.getPreviousStates();
                String updatedColumn = lastTransaction.getColumnName();
                for (Map.Entry<String, Map<String, String>> entry : previousStates.entrySet()) {
                    Set<String> singleRowSet = Collections.singleton(entry.getKey());
                    String previousValue = entry.getValue().get(updatedColumn); // Retrieve previous value of updated
                                                                                // column
                    table.updateRows(singleRowSet, updatedColumn, previousValue); // Restore previous state for each row
                }
                return "Undo successful: Last update has been reverted in " + tableName + ".";

            case "DELETE":
                // Undo delete: reinsert each deleted row
                previousStates = lastTransaction.getPreviousStates();
                for (Map.Entry<String, Map<String, String>> entry : previousStates.entrySet()) {
                    table.insertRow(entry.getKey(), entry.getValue()); // Reinsert deleted row
                }
                return "Undo successful: Last delete has been restored in " + tableName + ".";

            default:
                return "ERROR: Unknown operation type.";
        }
    }

    public String showHistory() {
        StringBuilder result = new StringBuilder();
        int index = 1;
        for (String q : queryHistory) {
            result.append(index).append(": ").append(q).append("\n");
            index++;
        }
        return result.toString();
    }

    // Method to execute a query from history
    public String executeHistory(String[] tokens) {
        if (tokens.length < 2) {
            return "ERROR: EXECUTE command requires an index";
        }
        try {
            int historyIndex = Integer.parseInt(tokens[1]) - 1; // Convert to zero-based index
            if (historyIndex < 0 || historyIndex >= queryHistory.size()) {
                return "ERROR: Invalid history index";
            }
            String historicalQuery = queryHistory.get(historyIndex);
            System.out.println("Executing: " + historicalQuery);
            String result = executeSQL(historicalQuery);
            return result;
        } catch (NumberFormatException e) {
            return "ERROR: Invalid index format";
        }
    }

    // Method to clear query history
    public String clearHistory(String[] tokens) {
        if (tokens.length >= 2 && tokens[1].equalsIgnoreCase("HISTORY")) {
            queryHistory.clear();
            return "Query history cleared.";
        }
        return "ERROR: Unknown command";
    }
}