package edu.smu.smusql;

import java.util.*;

public class Table {
    private final String tableName;
    private final List<String> columns;
    private final String primaryKey;
    private final boolean useBTree;

    // HashMap for exact primary key lookups
    private Map<String, Map<String, String>> primaryKeyMap;
    
    // Data structures for range queries on other columns
    private Map<String, BTree<String>> columnBTrees; // For B-tree-based indexing
    private Map<String, TreeMap<String, List<String>>> columnRedBlackTrees; // For Red-Black tree indexing

    // Constructor to initialize the table with a name, columns, and primary key
    public Table(String tableName, List<String> columns, boolean useBTree) {
        this.tableName = tableName;
        this.primaryKey = columns.get(0); // The first column is used as the primary key
        this.useBTree = useBTree;

        // Validate that columns do not contain duplicates
        Set<String> columnSet = new HashSet<>(columns);
        if (columnSet.size() != columns.size()) {
            throw new IllegalArgumentException("Duplicate column names found");
        }

        this.columns = new ArrayList<>(columns); // Copy the columns list
        this.primaryKeyMap = new HashMap<>();

        // Initialize appropriate index structures
        if (useBTree) {
            this.columnBTrees = new HashMap<>();
            for (String column : columns) {
                columnBTrees.put(column, new BTree<>(3)); // Example: B-tree with minimum degree 3
            }
        } else {
            this.columnRedBlackTrees = new HashMap<>();
            for (String column : columns) {
                columnRedBlackTrees.put(column, new TreeMap<>()); // TreeMap is a Red-Black tree
            }
        }
    }

    // Get the TreeMap for a specific column for Red-Black tree indexing
    public TreeMap<String, List<String>> getColumnTreeMap(String column) {
        if (!columns.contains(column)) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        return columnRedBlackTrees.get(column);
    }

    // Get the primaryKeyMap
    public Map<String, Map<String, String>> getPrimaryKeyMap() {
        return primaryKeyMap;
    }

    // Insert a row into the table
    public void insertRow(String primaryKeyValue, List<String> values) {
        if (values.size() != columns.size()) {
            throw new IllegalArgumentException("Number of values doesn't match number of columns");
        }

        if (primaryKeyMap.containsKey(primaryKeyValue)) {
            throw new IllegalArgumentException("Duplicate primary key: " + primaryKeyValue);
        }

        // Convert all values to Strings within this method
        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String value = values.get(i).toString().trim();  // Convert each value to String and trim whitespace
            row.put(columns.get(i), value);
        }
        
        // Insert the row into primaryKeyMap with a generated row ID
        primaryKeyMap.put(primaryKeyValue, row);

        // Insert into the appropriate data structure for range queries
        if (useBTree) {
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                String value = row.get(column).toString();
                // columnBTrees.get(column).insert(value, row); // Store the row in the BTree
            }
        } else {
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                String value = row.get(column).toString(); // Store value as String
                TreeMap<String, List<String>> treeMap = columnRedBlackTrees.get(column);
            
                // Check if the value already has a list in the TreeMap
                if (treeMap.containsKey(value)) {
                    // If the list exists, add the primaryKeyValue to it
                    treeMap.get(value).add(primaryKeyValue);
                } else {
                    // If the list does not exist, create a new list, add the primaryKeyValue, and put it in the TreeMap
                    List<String> list = new ArrayList<>();
                    list.add(primaryKeyValue);
                    treeMap.put(value, list);
                }
            }
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