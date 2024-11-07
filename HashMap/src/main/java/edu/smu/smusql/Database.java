package edu.smu.smusql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Database {

    private Map<String, Table> tables;

    public Database() {
        this.tables = new HashMap<>();
    }

    // Method to create a new table
    public void createTable(String tableName, List<String> columns) {
        if (tables.containsKey(tableName.toLowerCase())) {
            throw new IllegalArgumentException("Table already exists");
        }
        tables.put(tableName.toLowerCase(), new Table(tableName, columns));
    }

    // Method to retrieve a table by name
    public Table getTable(String tableName) {
        Table table = tables.get(tableName.toLowerCase());
        if (table == null) {
            throw new IllegalArgumentException("Table not found");
        }
        return table;
    }

    // Method to delete a table by name
    public void deleteTable(String tableName) {
        if (!tables.containsKey(tableName.toLowerCase())) {
            throw new IllegalArgumentException("Table not found");
        }
        tables.remove(tableName.toLowerCase());
    }

    // Method to list all tables
    public Set<String> listTables() {
        return new HashSet<>(tables.keySet());
    }
}
