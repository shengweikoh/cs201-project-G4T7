package edu.smu.smusql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Database {

    private Map<String, Table> tables; // Store tables by name

    public Database() {
        this.tables = new HashMap<>();
    }

    // Method to create a new table
    public void createTable(String tableName, List<String> columns, boolean useBTree) {
        if (tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table already exists");
        }
        tables.put(tableName, new Table(tableName, columns, useBTree));
    }

    // Method to retrieve a table by name
    public Table getTable(String tableName) {
        if (!tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table not found");
        }
        return tables.get(tableName);
    }

    // Method to delete a table by name
    public void deleteTable(String tableName) {
        if (!tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table not found");
        }
        tables.remove(tableName);
    }

    // Method to list all tables
    public Set<String> listTables() {
        return tables.keySet();
    }
}
