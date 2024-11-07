package edu.smu.smusql;

import java.util.*;

public class Engine {
    private Database database = new Database();

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
                return insert(tokens, query);
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

    // Insert method
    public String insert(String[] tokens, String query) {
        // Check syntax
        if (tokens.length < 4 || !tokens[1].equalsIgnoreCase("INTO")) {
            return "ERROR: Invalid INSERT syntax";
        }
    
        String tableName = tokens[2];
    
        // Extract values
        int valuesIndex = query.toUpperCase().indexOf("VALUES");
        if (valuesIndex == -1) {
            return "ERROR: Invalid INSERT syntax, missing VALUES keyword";
        }
    
        String valuesPart = query.substring(valuesIndex + 6).trim();
        valuesPart = valuesPart.replaceAll("\\(", "").replaceAll("\\)", "");
        String[] valuesArray = valuesPart.split(",");
    
        List<Object> values = new ArrayList<>();
        for (String value : valuesArray) {
            values.add(value.trim().replace("'", "")); // Remove any single quotes
        }
    
        // Get table from database
        Table table;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
    
        // Insert row to table
        String result = table.insertRow(values);
        if (result.startsWith("ERROR")) {
            return result; // Propagate the error message
        }
    
        return "Insertion Successful";
    }
    

    // Update method
    public String update(String[] tokens) {
        if (tokens.length < 5 || !tokens[2].equalsIgnoreCase("SET")) {
            return "ERROR: Invalid UPDATE syntax";
        }

        String tableName = tokens[1];
        Table table;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }

        String updatedColumn = tokens[3];
        if (!table.getColumns().contains(updatedColumn)) {
            return "ERROR: Column not found: " + updatedColumn;
        }
        if (!tokens[4].equals("=")) {
            return "ERROR: Invalid assignment in SET clause";
        }
        String updatedValue = tokens[5].replace("'", "");

        // Check for WHERE clause
        List<String[]> whereClauseConditions = new ArrayList<>();
        List<Boolean> andOrConditions = new ArrayList<>();

        if (tokens.length > 6 && tokens[6].equalsIgnoreCase("WHERE")) {
            for (int i = 7; i < tokens.length; i++) {
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
                    String value = tokens[i + 1].replace("'", "");
                    whereClauseConditions.add(new String[]{column, operator, value});
                    i += 1;
                }
            }
        }

        Set<List<Object>> rowsToUpdate = getRowsToUpdate(table, whereClauseConditions, andOrConditions);
        int updatedColumnIndex = table.getColumns().indexOf(updatedColumn);

        for (List<Object> row : rowsToUpdate) {
            row.set(updatedColumnIndex, updatedValue);
        }

        return rowsToUpdate.size() + " row(s) updated in " + tableName;
    }

    // Delete method
    public String delete(String[] tokens) {
        if (tokens.length < 3 || !tokens[1].equalsIgnoreCase("FROM")) {
            return "ERROR: Invalid DELETE syntax";
        }

        String tableName = tokens[2];
        Table table;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }

        List<String[]> whereClauseConditions = new ArrayList<>();
        List<Boolean> andOrConditions = new ArrayList<>();

        if (tokens.length > 3 && tokens[3].equalsIgnoreCase("WHERE")) {
            for (int i = 4; i < tokens.length; i++) {
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
                    String value = tokens[i + 1].replace("'", "");
                    whereClauseConditions.add(new String[]{column, operator, value});
                    i += 1;
                }
            }
        }

        Set<List<Object>> rowsToDelete = getRowsToDelete(table, whereClauseConditions, andOrConditions);
        for (List<Object> rowToDelete : rowsToDelete) {
            String primaryKeyValue = rowToDelete.get(0).toString(); // Assuming primary key is at index 0
            table.deleteRow(primaryKeyValue);
        }

        return rowsToDelete.size() + " row(s) deleted from " + tableName;
    }

    // Select method
    public String select(String[] tokens) {
        if (tokens.length < 4 || !tokens[2].equalsIgnoreCase("FROM")) {
            return "ERROR: Invalid SELECT syntax";
        }
    
        String tableName = tokens[3];
        Table table;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
    
        List<String> columns = table.getColumns();
        Set<List<Object>> rows;
    
        if (tokens.length == 4) {
            // No WHERE clause: select all rows
            rows = new HashSet<>(table.selectAllRows());
        } else {
            List<String[]> whereClauseConditions = new ArrayList<>();
            List<Boolean> andOrConditions = new ArrayList<>();
    
            // Ensure that there are enough tokens for the WHERE clause
            if (tokens.length > 4 && tokens[4].equalsIgnoreCase("WHERE")) {
                for (int i = 5; i < tokens.length; i++) {
                    if (tokens[i].equalsIgnoreCase("AND")) {
                        andOrConditions.add(true);
                    } else if (tokens[i].equalsIgnoreCase("OR")) {
                        andOrConditions.add(false);
                    } else if (isOperator(tokens[i])) {
                        // Ensure there's a valid column and value
                        if (i - 1 >= 0 && i + 1 < tokens.length) {
                            String column = tokens[i - 1];
                            if (!table.getColumns().contains(column)) {
                                return "ERROR: Column not found: " + column;
                            }
                            String operator = tokens[i];
                            String value = tokens[i + 1].replace("'", "");
                            whereClauseConditions.add(new String[]{column, operator, value});
                            i++; // Skip the value since it has been processed
                        } else {
                            return "ERROR: Incomplete WHERE condition";
                        }
                    }
                }
            }
    
            // Ensure at least one WHERE condition exists
            if (whereClauseConditions.isEmpty()) {
                return "ERROR: Invalid WHERE clause";
            }
    
            rows = getRowsToSelect(table, whereClauseConditions, andOrConditions);
        }
    
        StringBuilder result = new StringBuilder();
        result.append(String.join("\t", columns)).append("\n");
        for (List<Object> row : rows) {
            result.append(String.join("\t", row.stream().map(Object::toString).toArray(String[]::new))).append("\n");
        }
    
        return result.toString();
    }
    

    // Create method
    public String create(String[] tokens) {
        if (tokens.length < 4 || !tokens[1].equalsIgnoreCase("TABLE")) {
            return "ERROR: Invalid CREATE TABLE syntax";
        }
    
        String tableName = tokens[2];
        // Check if the table already exists
        if (database.getTable(tableName) != null) {
            return "ERROR: Table " + tableName + " already exists";
        }
    
        String columnsPart = String.join(" ", Arrays.copyOfRange(tokens, 3, tokens.length));
        columnsPart = columnsPart.trim();
        if (!columnsPart.startsWith("(") || !columnsPart.endsWith(")")) {
            return "ERROR: Invalid CREATE TABLE syntax, missing parentheses";
        }
    
        columnsPart = columnsPart.substring(1, columnsPart.length() - 1);
        String[] columnsArray = columnsPart.split(",");
        List<String> columns = new ArrayList<>();
        for (String column : columnsArray) {
            columns.add(column.trim());
        }
    
        if (columns.isEmpty()) {
            return "ERROR: No columns specified";
        }
    
        try {
            database.createTable(tableName, columns);
        } catch (IllegalArgumentException e) {
            return "ERROR: " + e.getMessage();
        }
    
        return "Table " + tableName + " created";
    }
    

    private Set<List<Object>> getRowsToUpdate(Table table, List<String[]> whereClauseConditions, List<Boolean> andOrConditions) {
        Set<List<Object>> rowsToUpdate = evaluateWhereCondition(whereClauseConditions.get(0), table);
        for (int i = 1; i < whereClauseConditions.size(); i++) {
            Set<List<Object>> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
            if (andOrConditions.get(i - 1)) {
                rowsToUpdate.retainAll(newRows);
            } else {
                rowsToUpdate.addAll(newRows);
            }
        }
        return rowsToUpdate;
    }

    private Set<List<Object>> getRowsToDelete(Table table, List<String[]> whereClauseConditions, List<Boolean> andOrConditions) {
        Set<List<Object>> rowsToDelete = evaluateWhereCondition(whereClauseConditions.get(0), table);
        for (int i = 1; i < whereClauseConditions.size(); i++) {
            Set<List<Object>> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
            if (andOrConditions.get(i - 1)) {
                rowsToDelete.retainAll(newRows);
            } else {
                rowsToDelete.addAll(newRows);
            }
        }
        return rowsToDelete;
    }

    private Set<List<Object>> getRowsToSelect(Table table, List<String[]> whereClauseConditions, List<Boolean> andOrConditions) {
        Set<List<Object>> rowsToSelect = evaluateWhereCondition(whereClauseConditions.get(0), table);
        for (int i = 1; i < whereClauseConditions.size(); i++) {
            Set<List<Object>> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
            if (andOrConditions.get(i - 1)) {
                rowsToSelect.retainAll(newRows);
            } else {
                rowsToSelect.addAll(newRows);
            }
        }
        return rowsToSelect;
    }

    private boolean isOperator(String token) {
        return token.equals("=") || token.equals(">") || token.equals("<") || token.equals(">=") || token.equals("<=");
    }

    private Set<List<Object>> evaluateWhereCondition(String[] whereClauseCondition, Table table) {
        String column = whereClauseCondition[0].trim(); // Column name
        String operator = whereClauseCondition[1].trim(); // Operator
        String valueStr = whereClauseCondition[2].trim(); // Value

        Set<List<Object>> matchingRows = new HashSet<>();
        int columnIndex = table.getColumns().indexOf(column);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        // Get all rows from the table
        List<List<Object>> allRows = table.getAllRows();

        for (List<Object> row : allRows) {
            Object cellValue = row.get(columnIndex);

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

    private boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

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

