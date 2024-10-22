package edu.smu.smusql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class Table {
    private String tableName;
    private List<String> columns;
    private boolean useBTree;  // To toggle between B-tree and TreeMap (Red-Black tree)

    // HashMap for exact primary key lookups
    private Map<Object, Map<String, Object>> primaryKeyMap;

    // Data structures for range queries on other columns
    private Map<String, BTree<String>> columnBTrees;  // Using String as an example
    private Map<String, TreeMap<String, List<Map<String, Object>>>> columnRedBlackTrees;

    // Constructor to initialize the table with a name and columns
    public Table(String tableName, List<String> columns, boolean useBTree) {
        this.tableName = tableName;
        this.columns = columns;
        this.useBTree = useBTree;
        this.primaryKeyMap = new HashMap<>();

        // Initialize appropriate index structures
        if (useBTree) {
            this.columnBTrees = new HashMap<>();
            for (String column : columns) {
                columnBTrees.put(column, new BTree<>(3));  // Example: B-tree with minimum degree 3
            }
        } else {
            this.columnRedBlackTrees = new HashMap<>();
            for (String column : columns) {
                columnRedBlackTrees.put(column, new TreeMap<>());  // TreeMap is a Red-Black tree
            }
        }
    }

    // Insert a row into the table
    public void insertRow(Object primaryKey, List<Object> values) {
        if (values.size() != columns.size()) {
            throw new IllegalArgumentException("Number of values doesn't match number of columns");
        }

        // Insert the row into the primary key map for exact lookups
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }
        primaryKeyMap.put(primaryKey, row);

        // Insert into the appropriate data structure for range queries
        if (useBTree) {
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                String value = (String) values.get(i);  // Assuming values are strings
                columnBTrees.get(column).insert(value, row);
            }
        } else {
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                String value = (String) values.get(i);  // Assuming values are strings
                TreeMap<String, List<Map<String, Object>>> treeMap = columnRedBlackTrees.get(column);
                treeMap.computeIfAbsent(value, k -> new ArrayList<>()).add(row);
            }
        }
    }

    // Get row by primary key (exact match)
    public Map<String, Object> getRowByPrimaryKey(Object primaryKey) {
        return primaryKeyMap.get(primaryKey); // O(1) average time
    }

    // Select rows based on range conditions (e.g., GPA > 3.0)
    public List<Map<String, Object>> selectRowsWithRange(String column, Object lowerBound, Object upperBound) {
        List<Map<String, Object>> result = new ArrayList<>();

        if (useBTree) {
            BTree<String> bTree = columnBTrees.get(column);
            result = bTree.rangeQuery((String) lowerBound, (String) upperBound);
        } else {
            TreeMap<String, List<Map<String, Object>>> treeMap = columnRedBlackTrees.get(column);
            SortedMap<String, List<Map<String, Object>>> range = treeMap.subMap((String) lowerBound, true, (String) upperBound, true);
            for (List<Map<String, Object>> rows : range.values()) {
                result.addAll(rows);
            }
        }

        return result;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }
}
