package edu.smu.smusql;

import java.util.*;

public class Table {
    private final String tableName;
    private final List<String> columns; // List of column names
    private final String primaryKey;

    // List to store all rows, each row is a List of Objects
    private List<List<Object>> rows;

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
        this.rows = new ArrayList<>();
    }

    // Insert a row into the table
    public void insertRow(List<Object> values) {
        if (values.size() != columns.size()) {
            throw new IllegalArgumentException("Number of values doesn't match number of columns");
        }

        // Check for duplicate primary key
        String primaryKeyValue = values.get(0).toString();
        if (getRowByPrimaryKey(primaryKeyValue) != null) {
            throw new IllegalArgumentException("Duplicate primary key: " + primaryKeyValue);
        }

        // Trim whitespace from String values
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) instanceof String) {
                values.set(i, values.get(i).toString().trim());
            }
        }

        rows.add(values);
    }

    // Get row by primary key (exact match)
    public List<Object> getRowByPrimaryKey(String primaryKeyValue) {
        int primaryKeyIndex = 0; // Since the primary key is the first column

        for (List<Object> row : rows) {
            if (row.get(primaryKeyIndex).toString().equals(primaryKeyValue)) {
                return row;
            }
        }
        return null;
    }

    // Delete a row by primary key
    public void deleteRow(String primaryKeyValue) {
        Iterator<List<Object>> iterator = rows.iterator();
        int primaryKeyIndex = 0; // Since the primary key is the first column

        while (iterator.hasNext()) {
            List<Object> row = iterator.next();
            if (row.get(primaryKeyIndex).toString().equals(primaryKeyValue)) {
                iterator.remove();
                return;
            }
        }
    }

    // Range query on a column
    public List<List<Object>> rangeQuery(String column, String lowerBound, String upperBound) {
        int columnIndex = columns.indexOf(column);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        // Comparator to sort rows based on the specified column
        Comparator<List<Object>> comparator = (row1, row2) -> {
            String val1 = row1.get(columnIndex).toString();
            String val2 = row2.get(columnIndex).toString();
            return val1.compareTo(val2);
        };

        // Create a copy of the rows list and sort it
        List<List<Object>> sortedRows = new ArrayList<>(rows);
        sortedRows.sort(comparator);

        List<List<Object>> result = new ArrayList<>();

        for (List<Object> row : sortedRows) {
            String value = row.get(columnIndex).toString();
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
    public List<List<Object>> getRowsByColumnValue(String column, String value) {
        int columnIndex = columns.indexOf(column);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column not found: " + column);
        }

        List<List<Object>> result = new ArrayList<>();
        for (List<Object> row : rows) {
            if (row.get(columnIndex).toString().equals(value)) {
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

    public List<List<Object>> getAllRows() {
        return new ArrayList<>(rows);
    }
}
