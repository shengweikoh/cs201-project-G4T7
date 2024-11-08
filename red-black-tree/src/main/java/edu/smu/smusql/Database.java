package edu.smu.smusql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Database {

    private Map<String, Table> tables; // Store tables by name

    public Database() {
        this.tables = new HashMap<>();
    }

    // Method to create a new table
    public void createTable(String tableName, List<String> columns) {
        if (tables.containsKey(tableName)) {
            throw new IllegalArgumentException("ERROR: Table already exists");
        }
        tables.put(tableName, new Table(tableName, columns));
    }

    // Method to retrieve a table by name
    public Table getTable(String tableName) {
        if (!tables.containsKey(tableName)) {
            throw new IllegalArgumentException("ERROR: No such table: " + tableName);
        }
        return tables.get(tableName);
    }
}
