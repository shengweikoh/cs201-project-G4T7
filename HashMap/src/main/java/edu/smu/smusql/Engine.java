package edu.smu.smusql;

import java.util.*;

public class Engine {
    private final Database database;

    public Engine() {
        this.database = new Database();
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

    // CREATE TABLE command
    private String create(String[] tokens) {
        if (tokens.length < 4 || !tokens[1].equalsIgnoreCase("TABLE")) {
            return "ERROR: Invalid CREATE TABLE syntax";
        }
        String tableName = tokens[2];
        if (database.tableExists(tableName)) {
            return "ERROR: Table already exists";
        }

        String columnList = queryBetweenParentheses(tokens, 3);
        List<String> columns = Arrays.asList(columnList.split(","));
        columns.replaceAll(String::trim);

        database.createTable(tableName, columns);
        return "Table " + tableName + " created";
    }

    // INSERT INTO command
    private String insert(String[] tokens) {
        if (tokens.length < 5 || !tokens[1].equalsIgnoreCase("INTO")) {
            return "ERROR: Invalid INSERT INTO syntax";
        }
        String tableName = tokens[2];
        if (!database.tableExists(tableName)) {
            return "ERROR: Table does not exist";
        }

        Table table = database.getTable(tableName);

        String valuesList = queryBetweenParentheses(tokens, 4);
        List<Object> values = Arrays.asList(valuesList.split(","));
        for (int i = 0; i < values.size(); i++) {
            values.set(i, values.get(i).toString().trim());
        }

        return table.insertRow(values);
    }

    // SELECT * FROM command with optional WHERE
    private String select(String[] tokens) {
        if (tokens.length < 4 || !tokens[1].equals("*") || !tokens[2].equalsIgnoreCase("FROM")) {
            return "ERROR: Invalid SELECT syntax";
        }
        String tableName = tokens[3];
        if (!database.tableExists(tableName)) {
            return "ERROR: Table does not exist";
        }

        Table table = database.getTable(tableName);
        List<List<Object>> resultRows = table.getAllRows();

        if (tokens.length > 4 && tokens[4].equalsIgnoreCase("WHERE")) {
            resultRows = applyWhereClause(table, tokens, 4);  // Use refactored method here
        }

        return formatRows(resultRows, table.getColumns());
    }

    // UPDATE command
    private String update(String[] tokens) {
        if (tokens.length < 7 || !tokens[2].equalsIgnoreCase("SET")) {
            return "ERROR: Invalid UPDATE syntax";
        }
        String tableName = tokens[1];
        if (!database.tableExists(tableName)) {
            return "ERROR: Table does not exist";
        }

        Table table = database.getTable(tableName);
        String targetColumn = tokens[3];
        String newValue = tokens[5];

        // Parse WHERE clause if provided
        List<List<Object>> rowsToUpdate = table.getAllRows();
        if (tokens.length > 6 && tokens[6].equalsIgnoreCase("WHERE")) {
            rowsToUpdate = applyWhereClause(table, tokens, 6);
        }

        // Perform the update
        for (List<Object> row : rowsToUpdate) {
            table.updateRow(row, targetColumn, newValue);
        }

        return "UPDATE successful";
    }

    // DELETE FROM command
    // DELETE FROM command
private String delete(String[] tokens) {
    if (tokens.length < 4 || !tokens[1].equalsIgnoreCase("FROM")) {
        return "ERROR: Invalid DELETE syntax";
    }
    String tableName = tokens[2];
    if (!database.tableExists(tableName)) {
        return "ERROR: Table does not exist";
    }

    Table table = database.getTable(tableName);

    // Check if there is a WHERE clause
    if (tokens.length > 4 && tokens[3].equalsIgnoreCase("WHERE")) {
        String columnName = tokens[4];
        String operator = tokens[5];
        String value = tokens[6];

        // Use deleteWhere to delete rows that match the WHERE condition
        return table.deleteWhere(columnName, operator, value);
    }  else {
        return "ERROR: DELETE without WHERE is not allowed";  // Prevents full deletion
    }
}



    // Helper method to format rows for SELECT output
    private String formatRows(List<List<Object>> rows, List<String> columns) {
        StringBuilder result = new StringBuilder(String.join("\t", columns)).append("\n");
        for (List<Object> row : rows) {
            for (Object value : row) {
                result.append(value).append("\t");
            }
            result.append("\n");
        }
        return result.toString();
    }

    // Helper method to extract content inside parentheses
    private String queryBetweenParentheses(String[] tokens, int startIndex) {
        StringBuilder result = new StringBuilder();
        for (int i = startIndex; i < tokens.length; i++) {
            result.append(tokens[i]).append(" ");
        }
        return result.toString().trim().replaceAll("\\(", "").replaceAll("\\)", "");
    }

    private List<List<Object>> applyWhereClause(Table table, String[] tokens, int whereIndex) {
        if (tokens.length <= whereIndex + 3) {
            throw new IllegalArgumentException("ERROR: Incomplete WHERE clause.");
        }
    
        String columnName = tokens[whereIndex + 1];
        String operator = tokens[whereIndex + 2];
        String value = tokens[whereIndex + 3];
    
        int columnIndex = table.getColumns().indexOf(columnName);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("ERROR: Column " + columnName + " does not exist");
        }
    
        List<List<Object>> filteredRows = new ArrayList<>();
        for (List<Object> row : table.getAllRows()) {
            String cellValue = row.get(columnIndex).toString();
            if (evaluateCondition(cellValue, operator, value)) {
                filteredRows.add(row);
            }
        }
        return filteredRows;
    }
    private boolean evaluateCondition(String cellValue, String operator, String value) {
        try {
            // Try to parse as numbers
            double cellNumber = Double.parseDouble(cellValue);
            double compareValue = Double.parseDouble(value);
    
            switch (operator) {
                case "=":
                    return cellNumber == compareValue;
                case ">":
                    return cellNumber > compareValue;
                case "<":
                    return cellNumber < compareValue;
                case ">=":
                    return cellNumber >= compareValue;
                case "<=":
                    return cellNumber <= compareValue;
                default:
                    throw new IllegalArgumentException("ERROR: Unsupported operator " + operator);
            }
        } catch (NumberFormatException e) {
            // If parsing as numbers fails, treat them as strings
            switch (operator) {
                case "=":
                    return cellValue.equals(value);
                case "!=":
                    return !cellValue.equals(value);
                default:
                    throw new IllegalArgumentException("ERROR: Unsupported operator " + operator + " for non-numeric values");
            }
        }
    }
    
}
