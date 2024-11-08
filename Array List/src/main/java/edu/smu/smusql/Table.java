package edu.smu.smusql;

import java.util.*;

public class Table {
    private final String tableName;
    private final List<String> columns;
    private final String primaryKey;

    // HashMap for exact primary key lookups
    private Map<String, Map<String, Object>> primaryKeyMap;

    // List to store all rows
    private List<Map<String, Object>> rows;

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
        this.rows = new ArrayList<>();
    }

    // Insert a row into the table
    public void insertRow(String primaryKeyValue, List<Object> values) {
        if (values.size() != columns.size()) {
            throw new IllegalArgumentException("Number of values doesn't match number of columns");
        }

        if (primaryKeyMap.containsKey(primaryKeyValue)) {
            throw new IllegalArgumentException("Duplicate primary key: " + primaryKeyValue);
        }

        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String value = values.get(i).toString().trim(); // Convert each value to String and trim whitespace
            row.put(columns.get(i), value);
        }
        primaryKeyMap.put(primaryKeyValue, row);
        rows.add(row);
    }

    // Get row by primary key (exact match)
    public Map<String, Object> getRowByPrimaryKey(String primaryKeyValue) {
        return primaryKeyMap.get(primaryKeyValue); // O(1) average time
    }

    // Range query on a column
    public List<Map<String, Object>> rangeQuery(String column, String lowerBound, String upperBound) {
        if (!columns.contains(column)) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        // Comparator to sort rows based on the specified column
        Comparator<Map<String, Object>> comparator = (row1, row2) -> {
            String val1 = row1.get(column).toString();
            String val2 = row2.get(column).toString();
            return val1.compareTo(val2);
        };

        // Create a copy of the rows list and sort it
        List<Map<String, Object>> sortedRows = new ArrayList<>(rows);
        sortedRows.sort(comparator);

        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> row : sortedRows) {
            String value = row.get(column).toString();
            if (value.compareTo(lowerBound) >= 0 && value.compareTo(upperBound) <= 0) {
                result.add(row);
            } else if (value.compareTo(upperBound) > 0) {
                // Since rows are sorted, we can break early
                break;
            }
        }

        return result;
    }

    // Method to get rows by exact match on a column
    public List<Map<String, Object>> getRowsByColumnValue(String column, String value) {
        if (!columns.contains(column)) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            if (row.get(column).toString().equals(value)) {
                result.add(row);
            }
        }
        return result;
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

    public List<Map<String, Object>> getAllRows() {
        return new ArrayList<>(rows);
    }

    public void deleteRow(String primaryKeyValue) {
        Map<String, Object> row = primaryKeyMap.remove(primaryKeyValue);
        if (row != null) {
            rows.remove(row);
        }
    }

}