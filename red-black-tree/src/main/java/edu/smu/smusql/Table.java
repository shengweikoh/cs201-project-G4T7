package edu.smu.smusql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Table {
    private final String tableName;
    private final List<String> columns;
    private final String primaryKey;

    // HashMap for exact primary key lookups
    private Map<String, Map<String, String>> primaryKeyMap;

    // Data structures for range queries on other columns
    private Map<String, TreeMap<String, List<String>>> columnRedBlackTrees; // For Red-Black tree indexing

    // Constructor to initialize the table with a name, columns, and primary key
    public Table(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.primaryKey = columns.get(0); // The first column is used as the primary key

        // Validate that columns do not contain duplicates
        Set<String> columnSet = new HashSet<>(columns);
        if (columnSet.size() != columns.size()) {
            throw new IllegalArgumentException("Duplicate column names found");
        }

        this.columns = new ArrayList<>(columns);
        this.primaryKeyMap = new HashMap<>();

        this.columnRedBlackTrees = new HashMap<>();
        for (String column : columns) {
            columnRedBlackTrees.put(column, new TreeMap<>()); // TreeMap is a Red-Black tree
        }
    }

    // Get the TreeMap for a specific column for Red-Black tree indexing
    public TreeMap<String, List<String>> getColumnTreeMap(String column) {
        if (columnRedBlackTrees == null) {
            throw new IllegalStateException("TreeMap indexing is not enabled for this table.");
        }
        if (!columns.contains(column)) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (columnRedBlackTrees == null) {
            throw new IllegalStateException("TreeMap-based indexing is not enabled for this table.");
        }
        return columnRedBlackTrees.get(column);
    }

    // Get the primaryKeyMap
    public Map<String, Map<String, String>> getPrimaryKeyMap() {
        return primaryKeyMap;
    }

    // Insert a row into the table
    public void insertRow(String primaryKeyValue, Map<String, String> row) {

        if (primaryKeyMap.containsKey(primaryKeyValue)) {
            throw new IllegalArgumentException("Duplicate primary key: " + primaryKeyValue);
        }

        // Insert the row into primaryKeyMap with a generated row ID
        primaryKeyMap.put(primaryKeyValue, row);

        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            String value = row.get(column).toString(); // Store value as String
            TreeMap<String, List<String>> treeMap = columnRedBlackTrees.get(column);

            // Check if the value already has a list in the TreeMap
            if (!treeMap.containsKey(value)) {
                // If the list does not exist, create a new list and put it to the TreeMap
                treeMap.put(value, new ArrayList<>());
            }
            // Add the primaryKeyValue to the list
            treeMap.get(value).add(primaryKeyValue);
        }
        // }
    }

    public void updateRows(Set<String> rowsToUpdate, String updatedColumn, String updatedValue) {
        TreeMap<String, List<String>> columnMap = getColumnTreeMap(updatedColumn);
        Map<String, Map<String, String>> rows = getPrimaryKeyMap();

        // Loop through each row to update
        for (String primaryKey : rowsToUpdate) {
            Map<String, String> row = rows.get(primaryKey);

            // Remove the primary key from the previous value's list in the column map
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
    }

    public void deleteRows(Set<String> rowsToDelete) {
        // Delete the rows from the column TreeMaps
        for (String column : columns) {
            TreeMap<String, List<String>> columnMap = getColumnTreeMap(column);
            for (String rowId : rowsToDelete) {
                Map<String, String> row = primaryKeyMap.get(rowId);
                columnMap.get(row.get(column)).remove(rowId);
            }
        }

        // Delete the rows from the primaryKeyMap
        for (String rowId : rowsToDelete) {
            primaryKeyMap.remove(rowId);
        }

    }

    // Get row by primary key (exact match)
    public Map<String, String> getRowByPrimaryKey(String primaryKeyValue) {
        return primaryKeyMap.get(primaryKeyValue); // O(1) average time
    }

    public String getTableName() {
        return tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public List<String> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        return "Table{" +
                "tableName='" + tableName + '\'' +
                ", primaryKey='" + primaryKey + '\'' +
                ", columns=" + columns +
                '}';
    }
}