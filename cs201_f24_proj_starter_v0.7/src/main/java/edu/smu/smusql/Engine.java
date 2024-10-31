package edu.smu.smusql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

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

        String valueList = queryBetweenParentheses(tokens, 4); // Get values list between parentheses
        List<String> values = Arrays.asList(valueList.split(","));
    
        // Get table from database
        Table table = null;
        try {
            table = database.getTable(tableName);
        } catch (IllegalArgumentException e) {
            return "ERROR: " + e.getMessage();
        }
    
        // Get primary key from values
        String primaryKey = values.get(0).toString(); // Convert primary key to String
    
        // Insert row to table
        System.out.println("Inserting into table: " + tableName + " , Primary key: " + primaryKey + " , Values: " + values);
    
        try {
            table.insertRow(primaryKey, values);  // Pass raw values, conversion happens in insertRow
        } catch (IllegalArgumentException e) {
            return "ERROR: " + e.getMessage(); // Return specific error messages
        }
    
        return "Insertion Successful";
    }

    public String update(String[] tokens) {
        // TODO
        return "not implemented";
    }

    public String delete(String[] tokens) {
      //   // Check syntax
      //   if (!tokens[1].toUpperCase().equals("FROM") || !tokens[3].toUpperCase().equals("WHERE")) {
      //       return "ERROR: Invalid DELETE syntax";
      //   }

      //   String tableName = tokens[2];

      //   // Check if table doesn't exist
      //   if(!database.listTables().contains(tableName)) {
      //       return "ERROR: No such table";
      //   }

      //   // If table exists, fetch the table
      //   Table table = null;
      //   try {
      //       table = database.getTable(tableName); // database is where tables are stored after being created - not the same as cache
      //   } catch (IllegalArgumentException e) {
      //       return e.getMessage();
      //   }

      //   List<String> columns = table.getColumns();

      //   // Initialize whereClauseConditions list
      //   List<String[]> whereClauseConditions = new ArrayList<>();
      //   List<Boolean> andOrConditions = new ArrayList<>();

      //   // Iterate through the where condition
      //   if(tokens.length > 3 && tokens[3].toUpperCase().equals("WHERE")) {
      //       for(int i = 4; i < tokens.length; i++) {
      //           if(tokens[i].toUpperCase().equals("AND")) {
      //               // true for AND
      //               andOrConditions.add(true);
      //           } else if (tokens[i].toUpperCase().equals("OR")) {
      //               // false for OR
      //               andOrConditions.add(false);
      //           } else if (isOperator(tokens[i])) {
      //               // eg where gpa < 2.0
      //               // col, operator, value

      //               String column = tokens[i - 1]; // idx is at operator so -1 to go back
                    
      //               if(!table.getColumns().contains(column)) {
      //                   return "ERROR: Column not found: " + column;
      //               }

      //               String operator = tokens[i];
      //               String value = tokens[i+1];

      //               whereClauseConditions.add(new String[]{column, operator, value});

      //               i++; // increment i by 1 because i+1 stored in token
      //           }
      //       }
        return "not implemented";
        }

//         // Evaluate WHERE conditions to get rows to delete
//         Set<Map<String, Object>> rowsToDelete = evaluateWhereCondition(whereClauseConditions.get(0), table);

//         for (int i = 1; i < whereClauseConditions.size(); i++) {
//             Set<Map<String, Object>> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
//             if (andOrConditions.get(i - 1)) {
//                 rowsToDelete.retainAll(newRows); // AND condition
//             } else {
//                 rowsToDelete.addAll(newRows); // OR condition
//             }
//         }

//         // Delete the matched rows
//         // for (Map<String, Object> row : rowsToDelete) {
//         //     table.getPrimaryKeyMap().remove(row.get(table.getPrimaryKeyColumn()));

//         // }
//         for (Map<String, Object> rowToDelete : rowsToDelete) {
//             table.getPrimaryKeyMap().entrySet().removeIf(entry -> entry.getValue().equals(rowToDelete));
//         }
        


//         return rowsToDelete.size() + " row(s) deleted from " + tableName;
//     }

    public String select(String[] tokens) {
        // Check if the query syntax is valid
        if (!tokens[1].equals("*") || !tokens[2].toUpperCase().equals("FROM")) {
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

        // Fetch all rows from the primary key map
        Map<String, Map<String, String>> allRows = table.getPrimaryKeyMap();

        if (tokens.length == 4) {
    
            // Build the result with column headers and rows
            StringBuilder result = new StringBuilder();
            result.append(String.join("\t", columns)).append("\n"); // Print column headers
    
            // Loop through the rows and append the values without the extra tab at the end
            for (Map<String, String> row : allRows.values()) {
                for (int i = 0; i < columns.size(); i++) {
                    String column = columns.get(i);
                    String value = row.getOrDefault(column, "NULL"); // Use "NULL" if the value is missing

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
                    whereClauseConditions.add(new String[] {column, operator, value});
                    i += 1; // Skip the value since it has been processed
                }
            }
        }

        Set<String> rows = evaluateWhereCondition(whereClauseConditions.get(0), table);
        
        for (int i = 1; i < whereClauseConditions.size(); i++) {
            Set<String> newRows = evaluateWhereCondition(whereClauseConditions.get(i), table);
            if (andOrConditions.get(i - 1)) {
                rows.retainAll(newRows);
            } else {
                rows.addAll(newRows);
            }
        }

        StringBuilder result = new StringBuilder();
        result.append(String.join("\t", columns)).append("\n"); // Print column headers
        

        // Loop through the rows and append the values without the extra tab at the end
        for (String row : rows) {
            Map<String, String> rowData = allRows.get(row);
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
        String column = whereClauseCondition[0].trim();    // Column name (e.g., "gpa")
        String operator = whereClauseCondition[1].trim();  // Operator (e.g., ">", "<", "=", etc.)
        String valueStr = whereClauseCondition[2].trim();  // Value (e.g., "3.8")
        
        // We will store the keys to the matching rows in a TreeSet to avoid duplicates and auto sort keys in ascending order
        Set<String> matchingRows = new TreeSet<>();
        
        // Assume we are dealing with TreeMap that stores keys as Strings
        //TreeMap<String, List<String>> columnTreeMap = table.getColumnTreeMap(column);
        // if (columnTreeMap == null) {
        //     throw new IllegalArgumentException("Column not found: " + column);
        // }
        // System.out.println(columnTreeMap.toString());
    
        // Handle different operators
        if (table.useBTree()) {
            // Use BTree indexing if enabled
            BTree<String> bTree = table.getColumnBTree(column);
            if (bTree == null) {
                throw new IllegalArgumentException("Column not found: " + column);
            }
    
            // Handle different operators for BTree
            switch (operator) {
                case "=":
                    // Exact match in BTree
                    List<String> exactMatches = bTree.search(valueStr);
                    if (exactMatches != null) {
                        matchingRows.addAll(exactMatches);  // Add all primary key references directly
                    }
                    break;
                case ">":
                case ">=":
                case "<":
                case "<=":
                    // For range queries in BTree, create a custom range search
                    List<String> rangeMatches = bTree.rangeSearch(valueStr, operator);
                    if (rangeMatches != null) {
                        matchingRows.addAll(rangeMatches);  // Add all primary key references directly
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported operator: " + operator);
            }
        } else {
            // Use TreeMap (Red-Black Tree) indexing if BTree is not used
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
        }
    
        return matchingRows;
    }
}