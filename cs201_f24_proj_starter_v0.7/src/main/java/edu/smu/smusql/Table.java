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

    // hashMap for exact primary key lookups
    private Map<Object, Map<String, Object>> primaryKeyMap;

    // (B-tree) for range queries on other columns
    private Map<String, TreeMap<Object, List<Map<String, Object>>>> columnTreeMaps;

    // constructor to initialize the table with a name and columns
    public Table(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.columns = columns;
        this.primaryKeyMap = new HashMap<>();  // For primary key exact match
        this.columnTreeMaps = new HashMap<>(); // For range queries

        // Initialize a TreeMap for each column that might need range queries
        for (String column : columns) {
            columnTreeMaps.put(column, new TreeMap<>());
        }
    }

    // insert a row into the table
    public void insertRow(Object primaryKey, List<Object> values) {
        if (values.size() != columns.size()) {
            throw new IllegalArgumentException("Number of values doesn't match number of columns");
        }

        // create a row to store data
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }

        // insert into HashMap for primary key exact matching
        primaryKeyMap.put(primaryKey, row);

        // insert into TreeMaps for range queries
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            Object value = values.get(i);

            // Initialize TreeMap if not already done
            columnTreeMaps.computeIfAbsent(column, k -> new TreeMap<>());

            // For each column, store the value and associate it with the row in the TreeMap
            TreeMap<Object, List<Map<String, Object>>> treeMap = columnTreeMaps.get(column);
            treeMap.computeIfAbsent(value, k -> new ArrayList<>()).add(row);

            // Debugging output for TreeMap
        System.out.println("For column: " + column + ", added value: " + value + " associated with row: " + row);
        System.out.println("Current TreeMap for column " + column + ": " + treeMap);
        System.out.println();
        }

        System.out.println("Inserted row: " + row);
        System.out.println("Current primary key map: " + primaryKeyMap);
    }

    //  get row by primary key (exact match)
    public Map<String, Object> getRowByPrimaryKey(Object primaryKey) {
        return primaryKeyMap.get(primaryKey); // O(1) average time
    }

    // select rows based on range conditions (e.g., GPA > 3.0)
    public List<Map<String, Object>> selectRowsWithRange(String column, Object lowerBound, Object upperBound) {
        if (!columnTreeMaps.containsKey(column)) {
            throw new IllegalArgumentException("Column not found: " + column);
        }
        
        TreeMap<Object, List<Map<String, Object>>> treeMap = columnTreeMaps.get(column);

        // Inclusive range query (including both bounds)
        SortedMap<Object, List<Map<String, Object>>> range = treeMap.subMap(lowerBound, true, upperBound, true);
        
        // Flatten the result to get a single list of rows
        List<Map<String, Object>> result = new ArrayList<>();
        for (List<Map<String, Object>> rows : range.values()) {
            result.addAll(rows);  // Add each list of rows to the result list
        }

        return result;
    }

     //other method go here? idk 
    // Getter methods for table name, columns, etc.
    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    @Override
public String toString() {
    return "Table{" +
           "columns=" + columns +
           ", primaryKeyMap=" + primaryKeyMap +
           '}';
}

}
