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
    private boolean useBTree; // set to true if wan to use btree

    // HashMap for exact primary key lookups
    private Map<String, Map<String, Object>> primaryKeyMap;

    // Data structures for range queries on other columns
    private Map<String, BTree<String>> columnBTrees;
    private Map<String, TreeMap<String, List<Map<String, Object>>>> columnRedBlackTrees;

    // Constructor to initialize the table with a name, columns, and primary key
    public Table(String tableName, List<String> columns, boolean useBTree) {
        this.tableName = tableName;
        this.primaryKey = columns.get(0); // The first column is used as the primary key
        this.useBTree = false;

        // Validate that columns do not contain duplicates
        Set<String> columnSet = new HashSet<>(columns);
        if (columnSet.size() != columns.size()) {
            throw new IllegalArgumentException("Duplicate column names found");
        }

        this.columns = new ArrayList<>(columns);
        this.primaryKeyMap = new HashMap<>();

        // Initialize both data structures, regardless of useBTree
        this.columnBTrees = new HashMap<>();
        this.columnRedBlackTrees = new HashMap<>();

        // Populate each column with the appropriate structure
        for (String column : columns) {
            columnBTrees.put(column, new BTree<>(3)); // Initialize BTree with a minimum degree of 3
            columnRedBlackTrees.put(column, new TreeMap<>()); // Initialize TreeMap (Red-Black Tree)
        }
    }

    // Get the TreeMap for a specific column
    public TreeMap<String, List<Map<String, Object>>> getColumnTreeMap(String column) {
        if (!columns.contains(column)) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        if (columnRedBlackTrees == null) {
            throw new IllegalStateException("TreeMap-based indexing is not enabled for this table.");
        }
        return columnRedBlackTrees.get(column);
    }

    // Get the primaryKeyMap
    public Map<String, Map<String, Object>> getPrimaryKeyMap() {
        return primaryKeyMap;
    }

    // Insert a row into the table, with all values stored as Strings
    public void insertRow(String primaryKey, List<Object> values) {
        if (values.size() != columns.size()) {
            throw new IllegalArgumentException("Number of values doesn't match number of columns");
        }

        if (primaryKeyMap.containsKey(primaryKey)) {
            throw new IllegalArgumentException("Duplicate primary key: " + primaryKey);
        }

        // Convert all values to Strings within this method
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String value = values.get(i).toString().trim(); // Convert each value to String and trim whitespace
            row.put(columns.get(i), value);
        }
        primaryKeyMap.put(primaryKey, row);

        // Insert into the appropriate data structure for range queries
        if (this.useBTree) {
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                String value = row.get(column).toString(); // Store value as String
                columnBTrees.get(column).insert(value, row);
            }
        } else {
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                String value = row.get(column).toString(); // Store value as String
                TreeMap<String, List<Map<String, Object>>> treeMap = columnRedBlackTrees.get(column);
                treeMap.computeIfAbsent(value, k -> new ArrayList<>()).add(row);
            }
        }
    }

    // Get row by primary key (exact match)
    public Map<String, Object> getRowByPrimaryKey(String primaryKey) {
        return primaryKeyMap.get(primaryKey); // O(1) average time
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

    public BTree<String> getColumnBTree(String column) {
        return columnBTrees.get(column);
    }

    public boolean useBTree() {
        return useBTree;
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