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

        // Parse WHERE clause
        if (tokens.length > 4 && tokens[4].equalsIgnoreCase("WHERE")) {
            String columnName = tokens[5];
            String operator = tokens[6];
            String value = tokens[7];
            if (!operator.equals("=")) {
                return "ERROR: Only '=' operator is supported in WHERE clause";
            }
            resultRows = table.selectWhere(columnName, value);
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

        // Parse WHERE clause
        String columnName = tokens.length > 7 ? tokens[7] : null;
        String value = tokens.length > 8 ? tokens[9] : null;

        return table.updateWhere(columnName, value, targetColumn, newValue);
    }

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
        String columnName = tokens.length > 4 ? tokens[4] : null;
        String value = tokens.length > 5 ? tokens[6] : null;

        return table.deleteWhere(columnName, value);
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
}
