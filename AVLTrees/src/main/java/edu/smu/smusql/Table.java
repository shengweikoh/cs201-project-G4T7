package edu.smu.smusql;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


public class Table {
    private final String tableName;
    private final List<String> columns; // List of column names
    private final String primaryKeyName;
    private Map<String, Map<String, Object>> rows; // Stores the actual rows, keyed by primary key
    private AVLTree<String> primaryKeyTree; // AVL Tree to index by primary key (assuming primary key is a String)

    // Constructor
    public Table(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.primaryKeyName = columns.get(0);

        Set<String> columnSet = new HashSet<>(columns);
        if (columnSet.size() != columns.size()) {
            throw new IllegalArgumentException("Duplicate column names found");
        }
        this.columns = columns;
        this.rows = new HashMap<>();
        this.primaryKeyTree = new AVLTree<>(); 
    }

    // Getters for the table name and columns
    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<Map<String, Object>> getRows() {

        return new ArrayList<>();

    }

    

    // Method to get the primary key tree (for checking or debugging)
    public AVLTree<String> getPrimaryKeyTree() {
        return primaryKeyTree;
    }

    public String getPrimaryKeyColumn() {
        return primaryKeyName;
    }
    

    // Method to insert a row into the table
    public void insertRow(String primaryKey, List<Object> values) {
        
        if (primaryKeyTree != null && primaryKeyTree.search(primaryKey) != null) {
            throw new IllegalArgumentException("Primary key already exists: " + primaryKey);
        }

        if(values.size() != columns.size()){
            throw new IllegalArgumentException("Number of values doesn't match number of columns");
        }

        // Create a map representing the row (column -> value)
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }

        // Insert the row into the table (map of rows)
        rows.put(primaryKey, row);

        // Insert the primary key into the AVL Tree
        if (primaryKeyTree != null) {
            primaryKeyTree.insert(primaryKey, primaryKey);
        }

        System.out.println("Inserting into AVLTree: " + primaryKey);

    }

    // Method to delete a row by primary key
// Method to delete a row by primary key
public void deleteRow(String primaryKey) {
     /* if (primaryKey == null) {
        throw new IllegalArgumentException("Primary key not found");
    }

    if (primaryKeyTree.search(primaryKey) == null) {
        throw new IllegalArgumentException("Primary key not found: " + primaryKey);
    }
*/
    // Remove the row from the table (map of rows)
    rows.remove(primaryKey);

    // Remove the primary key from the AVL Tree
    primaryKeyTree.delete(primaryKey);

    System.out.println("Deleting from AVLTree: " + primaryKey);
}

    // Method to update a row by primary key
    public void updateRow(String primaryKey, Map<String, Object> newValues) {
        if (primaryKeyTree != null && primaryKeyTree.search(primaryKey) == null) {
            throw new IllegalArgumentException("Primary key not found: " + primaryKey);
        }

        // Update the row data
        Map<String, Object> row = rows.get(primaryKey);
        if (row == null) {
            throw new IllegalArgumentException("Row not found for primary key: " + primaryKey);
        }

        // Update the row with new values (only updating specified columns)
        for (String column : newValues.keySet()) {
            if (!columns.contains(column)) {
                throw new IllegalArgumentException("Column not found: " + column);
            }
            row.put(column, newValues.get(column));
        }

        // The AVL Tree remains unchanged since the primary key is not modified
    }

    // Method to retrieve a row by primary key
    public Map<String, Object> getRow(String primaryKey) {
        if (primaryKeyTree != null && primaryKeyTree.search(primaryKey) == null) {
            return null;
        }
        return rows.get(primaryKey);

    }

    // Method to perform a SELECT (retrieving all rows)
    public Set<Map<String, Object>> selectAll() {
        return new HashSet<>(rows.values());
    }

    // Method to perform a SELECT with a WHERE clause (condition on a column)
    public List<Map<String, Object>> selectWhere(String columnName, Object value) {
        if (!columns.contains(columnName)) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }

        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows.values()) {
            if (value.equals(row.get(columnName))) {
                result.add(row);
            }
        }

        return result;
    }

    // Optional: Print all rows (for debugging)
    public void printAllRows() {
        for (Map<String, Object> row : rows.values()) {
            System.out.println(row);
        }
    }
}
