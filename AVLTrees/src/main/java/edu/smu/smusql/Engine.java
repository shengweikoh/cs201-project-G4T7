package edu.smu.smusql;

import java.lang.reflect.Array;
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
    if (!tokens[1].equalsIgnoreCase("INTO")) {
        return "ERROR: Invalid INSERT syntax";
    }

    String tableName = tokens[2];
    Table table;
    try {
        table = database.getTable(tableName);
    } catch (IllegalArgumentException e) {
        return e.getMessage();
    }

    if (!tokens[3].equalsIgnoreCase("VALUES")) {
        return "ERROR: Invalid INSERT syntax";
    }

    // Extract values from the tokens
    String valuesString = String.join(" ", Arrays.copyOfRange(tokens, 4, tokens.length));
    valuesString = valuesString.replaceAll("[()]", ""); // Remove parentheses
    String[] valuesArray = valuesString.split(",");

    // Convert valuesArray to a list of objects
    List<Object> values = new ArrayList<>();
    for (String value : valuesArray) {
        values.add(value.trim());
    }

    // Get primary key from values
    String primaryKey = values.get(0).toString(); // Assuming the first value is the primary key

    // Insert row into table
    try {
        table.insertRow(primaryKey, values);
    } catch (IllegalArgumentException e) {
        return "ERROR: " + e.getMessage();
    }

    return "Insertion Successful";
}

    public String update(String[] tokens) {
        if (!tokens[2].equalsIgnoreCase("SET")) {
            return "ERROR: Invalid UPDATE syntax";
        }
    
        String tableName = tokens[1];
        String[] setClause = tokens[3].split("=");
        if (setClause.length != 2) {
            return "ERROR: Invalid SET clause syntax";
        }
    
        String columnToUpdate = setClause[0].trim();
        String newValue = setClause[1].trim();
    
        Table table;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
    
        if (!table.getColumns().contains(columnToUpdate)) {
            return "ERROR: Column not found: " + columnToUpdate;
        }
    
        List<String[]> whereClauseConditions = parser.parseWhereClause(tokens);
        if (whereClauseConditions.isEmpty()) {
            return "ERROR: Invalid WHERE clause";
        }
    
        Set<Map<String, Object>> matchingRows = evaluateWhereCondition(whereClauseConditions.get(0), table);
    
        for (int i = 1; i < whereClauseConditions.size(); i++) {
            Set<Map<String, Object>> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
            matchingRows.retainAll(newRows); // Use AND for simplicity in this example
        }
    
        int updatedRowCount = 0;
        for (Map<String, Object> row : matchingRows) {
            row.put(columnToUpdate, newValue); // Update the column with the new value
            updatedRowCount++;
        }
    
        return updatedRowCount + " row(s) updated in " + tableName;
    }

    public String delete(String[] tokens) {
        if (!tokens[1].equalsIgnoreCase("FROM") || !tokens[3].equalsIgnoreCase("WHERE")) {
            return "ERROR: Invalid DELETE syntax";
        }

        String tableName = tokens[2];
        Table table;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
    
        List<String[]> whereClauseConditions;
        try {
            whereClauseConditions = parser.parseWhereClause(tokens);
        } catch (IllegalArgumentException e) {
            return "ERROR: " + e.getMessage();
        }
    
        Set<Map<String, Object>> rowsToDelete = evaluateWhereCondition(whereClauseConditions.get(0), table);
    
        for (int i = 1; i < whereClauseConditions.size(); i++) {
            Set<Map<String, Object>> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
            rowsToDelete.retainAll(newRows); // Using AND logic for simplicity
        }
    
        // Collect primary keys to delete
        List<String> primaryKeysToDelete = new ArrayList<>();
        for (Map<String, Object> row : rowsToDelete) {
            String primaryKey = row.get(table.getPrimaryKeyColumn()).toString();
            primaryKeysToDelete.add(primaryKey);
        }
    
        // Delete rows by primary keys
        for (String primaryKey : primaryKeysToDelete) {
            try {
                table.deleteRow(primaryKey); // Remove from AVL Tree and row data
            } catch (IllegalArgumentException e) {
                // Handle the case where the primary key is not found
                return "ERROR: " + e.getMessage();
            }
        }
    
        return primaryKeysToDelete.size() + " row(s) deleted from " + tableName;
    }

    public String select(String[] tokens) {
        if (!tokens[1].equals("*") || !tokens[2].equalsIgnoreCase("FROM")) {
            return "ERROR: Invalid SELECT syntax";
        }

        String tableName = tokens[3];
        Table table;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }

        if (tokens.length == 4) {
            return table.selectAll().toString(); // Select all rows
        }

        List<String[]> whereClauseConditions = parser.parseWhereClause(tokens);
        Set<Map<String, Object>> rows = evaluateWhereCondition(whereClauseConditions.get(0), table);

        for (int i = 1; i < whereClauseConditions.size(); i++) {
            Set<Map<String, Object>> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
            rows.retainAll(newRows); // AND condition
        }

        return rows.toString();
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
        columns.replaceAll(String::trim); // Remove leading/trailing whitespace

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
    

    private Set<Map<String, Object>> evaluateWhereCondition(String[] condition, Table table) {
        Set<Map<String, Object>> resultSet = new HashSet<>();
    
        if (condition == null || condition.length < 4) {
            return resultSet; // Return empty set if condition is null or malformed
        }
    
        String column = condition[1];
        String operator = condition[2];
        String value = condition[3];
    
        for (Map<String, Object> row : table.getRows()) {
            Object columnValue = row.get(column);
            if (columnValue != null && evaluateCondition(columnValue.toString(), operator, value)) {
                resultSet.add(row);
            }
        }
    
        return resultSet;
    }
    
    private boolean evaluateCondition(String columnValue, String operator, String value) {
        switch (operator) {
            case "=":
                return columnValue.equals(value);
            case "!=":
                return !columnValue.equals(value);
            case "<":
                return Double.parseDouble(columnValue) < Double.parseDouble(value);
            case ">":
                return Double.parseDouble(columnValue) > Double.parseDouble(value);
            case "<=":
                return Double.parseDouble(columnValue) <= Double.parseDouble(value);
            case ">=":
                return Double.parseDouble(columnValue) >= Double.parseDouble(value);
            default:
                return false;
        }
    }


}