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
        String newValue = tokens[5].replaceAll("^['\"]|['\"]$", "");
    
        // Parse WHERE clause if provided
        List<List<Object>> rowsToUpdate = table.getAllRows();
        if (tokens.length > 6 && tokens[6].equalsIgnoreCase("WHERE")) {
            rowsToUpdate = applyWhereClause(table, tokens, 6); // Apply WHERE conditions to filter rows
        }
    
        // Track if any rows were updated
        int updatedRows = 0;
    
        // Perform the update on filtered rows
        for (List<Object> row : rowsToUpdate) {
            table.updateRow(row, targetColumn, newValue);
            updatedRows++; // Increment if a row was updated
        }
    
        // Return a message based on whether rows were updated
        if (updatedRows > 0) {
            return "UPDATE successful, updated " + updatedRows + " rows";
        } else {
            return "No rows updated";
        }
    }

    private String delete(String[] tokens) {
        // Ensure correct syntax
        if (tokens.length < 3 || !tokens[1].equalsIgnoreCase("FROM")) {
            return "ERROR: Invalid DELETE syntax";
        }
    
        String tableName = tokens[2];
    
        // Check if table exists
        if (!database.tableExists(tableName)) {
            return "ERROR: Table does not exist";
        }
    
        Table table = database.getTable(tableName);
    
        // Fetch rows to delete
        List<List<Object>> rowsToDelete = table.getAllRows();
        if (tokens.length > 3 && tokens[3].equalsIgnoreCase("WHERE")) {
            rowsToDelete = applyWhereClause(table, tokens, 3); // Apply WHERE clause
        }
    
        if (rowsToDelete.isEmpty()) {
            return tokens.length <= 3 
                ? "No rows to delete" 
                : "No rows matched the condition, nothing to delete";
        }
    
        // Delete rows directly from the HashMap using an iterator
        int deletedCount = 0;
        Iterator<Map.Entry<Object, List<Object>>> iterator = table.getRows().entrySet().iterator();
    
        while (iterator.hasNext()) {
            Map.Entry<Object, List<Object>> entry = iterator.next();
            if (rowsToDelete.contains(entry.getValue())) {
                iterator.remove();
                deletedCount++;
            }
        }
    
        return "Deleted " + deletedCount + " row(s) from " + tableName;
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
        List<List<Object>> filteredRows = table.getAllRows();
        List<String> columnNames = new ArrayList<>();
        List<String> operators = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        List<String> logicalOperators = new ArrayList<>();
    
        int i = whereIndex + 1;  // Start processing after the WHERE keyword
        while (i < tokens.length) {
            if (tokens[i].equalsIgnoreCase("AND") || tokens[i].equalsIgnoreCase("OR")) {
                logicalOperators.add(tokens[i].toUpperCase());  // Save logical operators like AND/OR
                i++;
                continue;
            }
    
            // Ensure that we have at least three tokens: column, operator, value
            if (i + 2 >= tokens.length) {
                System.out.println("ERROR: Incomplete condition in WHERE clause.");
                return new ArrayList<>(); // Prevent further processing if the condition is incomplete
            }
    
            String columnName = tokens[i];
            String operator = tokens[i + 1];
            String value = tokens[i + 2].replaceAll("^['\"]|['\"]$", "");  // Remove any surrounding quotes from value
    
            System.out.println("Parsing condition: " + columnName + " " + operator + " " + value);
    
            // Check if the operator is valid
            if (!isValidOperator(operator)) {
                System.out.println("ERROR: Invalid operator in WHERE clause: " + operator);
                return new ArrayList<>();  // Return empty if invalid operator found
            }
    
            columnNames.add(columnName);
            operators.add(operator);
            values.add(value);
    
            i += 3;  // Move to the next condition or logical operator
        }
    
        // Now, filter rows based on the extracted conditions and logical operators
        return filterRowsWithLogic(table, columnNames, operators, values, logicalOperators);
    }
    

    // private List<List<Object>> applyWhereClause(Table table, String[] tokens, int whereIndex) {
    //     List<List<Object>> filteredRows = table.getAllRows();
    //     List<String> columnNames = new ArrayList<>();
    //     List<String> operators = new ArrayList<>();
    //     List<Object> values = new ArrayList<>();
    //     List<String> logicalOperators = new ArrayList<>();
    
    //     int i = whereIndex + 1;
    //     while (i < tokens.length) {
    //         if (tokens[i].equalsIgnoreCase("AND") || tokens[i].equalsIgnoreCase("OR")) {
    //             logicalOperators.add(tokens[i].toUpperCase());
    //             i++;
    //             continue;
    //         }
    
    //         if (i + 2 >= tokens.length) {
    //             System.out.println("ERROR: Incomplete condition in WHERE clause.");
    //             return new ArrayList<>(); // Prevent deletion if syntax is incorrect
    //         }
    
    //         String columnName = tokens[i];
    //         String operator = tokens[i + 1];
    //         String value = tokens[i + 2].replaceAll("^['\"]|['\"]$", ""); // Remove any quotes around the value

    //         System.out.println("Parsing condition: " + columnName + " " + operator + " " + value);

    
    //         // Check if the operator is valid
    //         if (!isValidOperator(operator)) {
    //             System.out.println("ERROR: Invalid operator in WHERE clause: " + operator);
    //             return new ArrayList<>(); // Prevent deletion if an invalid operator is used
    //         }

            
    
    //         columnNames.add(columnName);
    //         operators.add(operator);
    //         values.add(value);
    
    //         i += 3; // Move to the next condition or logical operator
    //     }
    
    //     return filterRowsWithLogic(table, columnNames, operators, values, logicalOperators);
    // }

    
    private List<List<Object>> filterRowsWithLogic(
    Table table, List<String> columnNames, List<String> operators, List<Object> values, List<String> logicalOperators) {
    
    List<List<Object>> filteredRows = new ArrayList<>();

    for (List<Object> row : table.getAllRows()) {
        boolean matches = true; // Start with true to allow initial condition check

        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            String operator = operators.get(i);
            Object value = values.get(i);

            // Find the index of the current column
            int columnIndex = table.getColumns().indexOf(columnName);
            if (columnIndex == -1) {
                System.out.println("ERROR: Column " + columnName + " does not exist.");
                matches = false;
                break; // Skip this row if the column doesn't exist
            }

            Object cellValue = row.get(columnIndex);
            boolean conditionMatch = evaluateCondition(cellValue.toString(), operator, value.toString());

            if (i == 0) {
                matches = conditionMatch; // Initialize with the first condition
            } else if (i - 1 < logicalOperators.size()) { 
                // Ensure logical operator is in bounds
                String logicalOperator = logicalOperators.get(i - 1);
                if (logicalOperator.equals("AND")) {
                    matches &= conditionMatch;
                } else if (logicalOperator.equals("OR")) {
                    matches |= conditionMatch;
                }
            }

            // Short-circuit if an AND condition fails
            if (!matches && i > 0 && logicalOperators.get(i - 1).equals("AND")) {
                break;
            }
        }

        if (matches) {
            filteredRows.add(row);
        }
    }

        return filteredRows;
    }

    private boolean isValidOperator(String operator) {
        return operator.equals("=") || operator.equals(">") || operator.equals("<") ||
            operator.equals(">=") || operator.equals("<=") || operator.equals("!=");
    }


    private boolean evaluateCondition(String cellValue, String operator, String value) {
        value = value.replaceAll("^['\"]|['\"]$", ""); // Remove quotes around the value if present
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


                

                

                

                