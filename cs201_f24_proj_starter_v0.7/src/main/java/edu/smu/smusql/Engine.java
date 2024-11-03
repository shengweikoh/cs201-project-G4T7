package edu.smu.smusql;

import java.util.*;

public class Engine {
      private Database database = new Database();
      private Parser parser = new Parser();
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
            if (!tokens[1].equalsIgnoreCase("INTO")) {
                  return "ERROR: Invalid INSERT INTO syntax";
            }

            // Get table name and list of values from parsed data
            String tableName = tokens[2];

            // Get table from database
            Table table;
            try {
                  table = database.getTable(tableName);
            } catch (IllegalArgumentException e) {
                  return "ERROR: " + e.getMessage() + ": " + tableName;
            }

            // Extract values between parentheses
            String valueList = queryBetweenParentheses(tokens, 4);
            List<String> values = Arrays.asList(valueList.split(","));
            values.replaceAll(String::trim); // Trim whitespace from each value

            // Get primary key from values
            String primaryKey = values.get(0); // Primary key is the first value

            try {
                  table.insertRow(primaryKey, values); // Pass raw values, conversion happens in insertRow
            } catch (IllegalArgumentException e) {
                  return "ERROR: " + e.getMessage(); // Return specific error messages
            }

            return "Row inserted into " + tableName;
      }

      public String update(String[] tokens) {
            // Check syntax
            if (tokens.length < 4 || !tokens[2].equalsIgnoreCase("SET")) {
                  return "ERROR: Invalid UPDATE syntax";
            }

            String tableName = tokens[1];

            Table table;
            try {
                  table = database.getTable(tableName);
            } catch (IllegalArgumentException e) {
                  return "ERROR: " + e.getMessage() + ": " + tableName;
            }

            // Parse the columns and values to be updated
            Map<String, String> updatedColumns = new HashMap<>();

            int i = 3; // Start after 'SET'
            while (i < tokens.length && !tokens[i].equalsIgnoreCase("WHERE")) {
                  // Parse column name
                  String columnName = tokens[i];
                  if (columnName.endsWith(",")) {
                        columnName = columnName.substring(0, columnName.length() - 1);
                  }
                  if (!table.getColumns().contains(columnName)) {
                        return "ERROR: Column not found: " + columnName;
                  }
                  i++;

                  // Expect '='
                  if (i >= tokens.length || !tokens[i].equals("=")) {
                        return "ERROR: Invalid assignment in SET clause";
                  }
                  i++;

                  // Parse value
                  if (i >= tokens.length) {
                        return "ERROR: Missing value in SET clause";
                  }
                  String value = tokens[i];

                  // Check if value ends with a comma
                  boolean hasComma = false;
                  if (value.endsWith(",")) {
                        hasComma = true;
                        value = value.substring(0, value.length() - 1);
                  }

                  value = stripQuotes(value);

                  updatedColumns.put(columnName, value);

                  i++;

                  // If value had a comma, we continue to next column
                  if (hasComma) {
                        continue;
                  }

                  // If next token is a comma, skip it
                  if (i < tokens.length && tokens[i].equals(",")) {
                        i++;
                  }
            }

            // Parse WHERE clause conditions
            List<String[]> whereClauseConditions = new ArrayList<>();
            List<Boolean> andOrConditions = new ArrayList<>();

            if (i < tokens.length && tokens[i].equalsIgnoreCase("WHERE")) {
                  i++; // Move past 'WHERE'
                  while (i < tokens.length) {
                        if (tokens[i].equalsIgnoreCase("AND")) {
                              andOrConditions.add(true);
                              i++;
                        } else if (tokens[i].equalsIgnoreCase("OR")) {
                              andOrConditions.add(false);
                              i++;
                        } else {
                              // Parse column name
                              if (i >= tokens.length) {
                                    return "ERROR: Missing column in WHERE clause";
                              }
                              String column = tokens[i];
                              if (!table.getColumns().contains(column)) {
                                    return "ERROR: Column not found: " + column;
                              }
                              i++;

                              // Parse operator
                              if (i >= tokens.length || !isOperator(tokens[i])) {
                                    return "ERROR: Invalid operator in WHERE clause";
                              }
                              String operator = tokens[i];
                              i++;

                              // Parse value, which may include tokens with spaces
                              if (i >= tokens.length) {
                                    return "ERROR: Missing value in WHERE clause";
                              }
                              StringBuilder valueBuilder = new StringBuilder();
                              boolean inQuotes = false;
                              while (i < tokens.length && (inQuotes || (!tokens[i].equalsIgnoreCase("AND")
                                          && !tokens[i].equalsIgnoreCase("OR")))) {
                                    String token = tokens[i];
                                    if (token.startsWith("'")) {
                                          inQuotes = true;
                                          if (token.endsWith("'") && token.length() > 1) {
                                                inQuotes = false;
                                          }
                                    } else if (token.endsWith("'")) {
                                          inQuotes = false;
                                    }
                                    valueBuilder.append(token).append(" ");
                                    i++;
                              }
                              String value = valueBuilder.toString().trim();
                              value = stripQuotes(value);

                              whereClauseConditions.add(new String[] { column, operator, value });
                        }
                  }
            }

            // Get rows that satisfy the WHERE clause
            Set<String> rowsToUpdate;
            if (whereClauseConditions.isEmpty()) {
                  // No WHERE clause: update all rows
                  rowsToUpdate = new HashSet<>(table.getPrimaryKeyMap().keySet());
            } else {
                  rowsToUpdate = evaluateWhereCondition(whereClauseConditions.get(0), table);
                  for (int j = 1; j < whereClauseConditions.size(); j++) {
                        Set<String> newRows = evaluateWhereCondition(whereClauseConditions.get(j), table);
                        if (andOrConditions.get(j - 1)) {
                              rowsToUpdate.retainAll(newRows);
                        } else {
                              rowsToUpdate.addAll(newRows);
                        }
                  }
            }

            Map<String, Map<String, String>> rows = table.getPrimaryKeyMap();

            // Update each specified column
            for (String updatedColumn : updatedColumns.keySet()) {
                  String updatedValue = updatedColumns.get(updatedColumn);
                  TreeMap<String, List<String>> columnMap = table.getColumnTreeMap(updatedColumn);

                  for (String primaryKey : rowsToUpdate) {
                        Map<String, String> row = rows.get(primaryKey);
                        String oldValue = row.get(updatedColumn);

                        // Remove primaryKey from old value in columnMap
                        if (columnMap.containsKey(oldValue)) {
                              columnMap.get(oldValue).remove(primaryKey);
                              if (columnMap.get(oldValue).isEmpty()) {
                                    columnMap.remove(oldValue);
                              }
                        }

                        // Update the row with the new value
                        row.put(updatedColumn, updatedValue);

                        // Add primaryKey to new value in columnMap
                        columnMap.computeIfAbsent(updatedValue, k -> new ArrayList<>()).add(primaryKey);
                  }
            }

            return rowsToUpdate.size() + " row(s) updated in " + tableName;
      }

      public String delete(String[] tokens) {
            // Check syntax
            if (!tokens[1].equalsIgnoreCase("FROM")) {
                  return "ERROR: Invalid DELETE syntax";
            }

            String tableName = tokens[2];

            // Fetch the table
            Table table;
            try {
                  table = database.getTable(tableName);
            } catch (IllegalArgumentException e) {
                  return "ERROR: " + e.getMessage() + ": " + tableName;
            }

            // Parse WHERE clause conditions
            List<String[]> whereClauseConditions = new ArrayList<>();
            List<Boolean> andOrConditions = new ArrayList<>();

            int i = 3; // Start after 'FROM tableName'
            if (i < tokens.length && tokens[i].equalsIgnoreCase("WHERE")) {
                  i++; // Move past 'WHERE'
                  while (i < tokens.length) {
                        if (tokens[i].equalsIgnoreCase("AND")) {
                              andOrConditions.add(true);
                              i++;
                        } else if (tokens[i].equalsIgnoreCase("OR")) {
                              andOrConditions.add(false);
                              i++;
                        } else {
                              String column = tokens[i];
                              if (!table.getColumns().contains(column)) {
                                    return "ERROR: Column not found: " + column;
                              }
                              i++;
                              if (i >= tokens.length || !isOperator(tokens[i])) {
                                    return "ERROR: Invalid operator in WHERE clause";
                              }
                              String operator = tokens[i];
                              i++;
                              if (i >= tokens.length) {
                                    return "ERROR: Missing value in WHERE clause";
                              }
                              String value = tokens[i];
                              whereClauseConditions.add(new String[] { column, operator, value });
                              i++;
                        }
                  }
            }

            // Evaluate WHERE conditions to get rows to delete
            Set<String> rowsToDelete;
            if (whereClauseConditions.isEmpty()) {
                  // No WHERE clause: delete all rows
                  rowsToDelete = new HashSet<>(table.getPrimaryKeyMap().keySet());
            } else {
                  rowsToDelete = evaluateWhereCondition(whereClauseConditions.get(0), table);
                  for (int j = 1; j < whereClauseConditions.size(); j++) {
                        Set<String> newRows = evaluateWhereCondition(whereClauseConditions.get(j), table);
                        if (andOrConditions.get(j - 1)) {
                              rowsToDelete.retainAll(newRows);
                        } else {
                              rowsToDelete.addAll(newRows);
                        }
                  }
            }

            List<String> columns = table.getColumns();
            Map<String, Map<String, String>> rows = table.getPrimaryKeyMap();

            // Delete the rows from the column TreeMaps
            for (String column : columns) {
                  TreeMap<String, List<String>> columnMap = table.getColumnTreeMap(column);
                  for (String rowId : rowsToDelete) {
                        Map<String, String> row = rows.get(rowId);
                        String value = row.get(column);
                        if (columnMap.containsKey(value)) {
                              columnMap.get(value).remove(rowId);
                              if (columnMap.get(value).isEmpty()) {
                                    columnMap.remove(value);
                              }
                        }
                  }
            }

            // Delete the rows from the primaryKeyMap
            for (String rowId : rowsToDelete) {
                  rows.remove(rowId);
            }

            return "Rows deleted from " + tableName + ". " + rowsToDelete.size() + " row(s) affected.";
      }

      public String select(String[] tokens) {
            // Check syntax
            if (tokens.length < 4 || !tokens[1].equals("*") || !tokens[2].equalsIgnoreCase("FROM")) {
                  return "ERROR: Invalid SELECT syntax";
            }

            String tableName = tokens[3];

            // Retrieve the table
            Table table;
            try {
                  table = database.getTable(tableName);
            } catch (IllegalArgumentException e) {
                  return "ERROR: " + e.getMessage() + ": " + tableName;
            }

            List<String> columns = table.getColumns();
            Map<String, Map<String, String>> allRows = table.getPrimaryKeyMap();

            // Parse WHERE clause conditions
            List<String[]> whereClauseConditions = new ArrayList<>();
            List<Boolean> andOrConditions = new ArrayList<>();

            int i = 4; // Start after 'FROM tableName'
            if (i < tokens.length && tokens[i].equalsIgnoreCase("WHERE")) {
                  i++; // Move past 'WHERE'
                  while (i < tokens.length) {
                        if (tokens[i].equalsIgnoreCase("AND")) {
                              andOrConditions.add(true);
                              i++;
                        } else if (tokens[i].equalsIgnoreCase("OR")) {
                              andOrConditions.add(false);
                              i++;
                        } else {
                              String column = tokens[i];
                              if (!table.getColumns().contains(column)) {
                                    return "ERROR: Column not found: " + column;
                              }
                              i++;
                              if (i >= tokens.length || !isOperator(tokens[i])) {
                                    return "ERROR: Invalid operator in WHERE clause";
                              }
                              String operator = tokens[i];
                              i++;
                              if (i >= tokens.length) {
                                    return "ERROR: Missing value in WHERE clause";
                              }
                              String value = tokens[i];
                              whereClauseConditions.add(new String[] { column, operator, value });
                              i++;
                        }
                  }
            }

            // Evaluate WHERE conditions
            Set<String> matchingRows;
            if (whereClauseConditions.isEmpty()) {
                  // No WHERE clause: select all rows
                  matchingRows = new HashSet<>(allRows.keySet());
            } else {
                  matchingRows = evaluateWhereCondition(whereClauseConditions.get(0), table);
                  for (int j = 1; j < whereClauseConditions.size(); j++) {
                        Set<String> newRows = evaluateWhereCondition(whereClauseConditions.get(j), table);
                        if (andOrConditions.get(j - 1)) {
                              matchingRows.retainAll(newRows);
                        } else {
                              matchingRows.addAll(newRows);
                        }
                  }
            }

            // Collect the matching rows
            List<Map<String, String>> selectedRows = new ArrayList<>();
            for (String primaryKey : matchingRows) {
                  selectedRows.add(allRows.get(primaryKey));
            }

            return buildResultWithRows(columns, selectedRows);
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
            database.createTable(tableName, columns, false); // Set to true if you want to use BTree

            return "Table " + tableName + " created";
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

      // HELPER METHODS

      private String stripQuotes(String value) {
            value = value.trim();
            if (value.startsWith("'") && value.endsWith("'") && value.length() >= 2) {
                  return value.substring(1, value.length() - 1);
            }
            return value;
      }

      private String queryBetweenParentheses(String[] tokens, int startIndex) {
            StringBuilder result = new StringBuilder();
            for (int i = startIndex; i < tokens.length; i++) {
                  result.append(tokens[i]).append(" ");
            }
            return result.toString().trim().replaceAll("\\(", "").replaceAll("\\)", "");
      }

      private boolean isOperator(String token) {
            return token.equals("=") || token.equals(">") || token.equals("<") || token.equals(">=")
                        || token.equals("<=");
      }

      private Set<String> evaluateWhereCondition(String[] whereClauseCondition, Table table) {
            String column = whereClauseCondition[0].trim(); // Column name
            String operator = whereClauseCondition[1].trim(); // Operator
            String valueStr = whereClauseCondition[2].trim(); // Value

            Set<String> matchingRows = new TreeSet<>();

            TreeMap<String, List<String>> columnTreeMap = table.getColumnTreeMap(column);
            if (columnTreeMap == null) {
                  throw new IllegalArgumentException("Column not found: " + column);
            }

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

            return matchingRows;
      }

      private String buildResultWithRows(List<String> columns, Collection<Map<String, String>> rows) {
            StringBuilder result = new StringBuilder();
            result.append(String.join("\t", columns)).append("\n"); // Print column headers

            for (Map<String, String> rowData : rows) {
                  for (int i = 0; i < columns.size(); i++) {
                        String column = columns.get(i);
                        String value = rowData.getOrDefault(column, "NULL");
                        result.append(value);
                        if (i < columns.size() - 1) {
                              result.append("\t");
                        }
                  }
                  result.append("\n");
            }

            return result.toString();
      }
}
