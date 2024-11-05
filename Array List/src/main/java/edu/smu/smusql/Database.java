package edu.smu.smusql;

import java.util.*;

public class Database {

    private List<Table> tables;

    public Database() {
        this.tables = new ArrayList<>();
    }

    // Method to create a new table
    public void createTable(String tableName, List<String> columns) {
        if (getTableByName(tableName) != null) {
            throw new IllegalArgumentException("Table already exists");
        }
        tables.add(new Table(tableName, columns));
    }

    // Method to retrieve a table by name
    public Table getTable(String tableName) {
        Table table = getTableByName(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found");
        }
        return table;
    }

    // Helper method to find a table by name
    private Table getTableByName(String tableName) {
        for (Table table : tables) {
            if (table.getTableName().equalsIgnoreCase(tableName)) {
                return table;
            }
        }
        return null;
    }

    // Method to delete a table by name
    public void deleteTable(String tableName) {
        Iterator<Table> iterator = tables.iterator();
        while (iterator.hasNext()) {
            Table table = iterator.next();
            if (table.getTableName().equalsIgnoreCase(tableName)) {
                iterator.remove();
                return;
            }
        }
        throw new IllegalArgumentException("Table not found");
    }

    // Method to list all tables
    public Set<String> listTables() {
        Set<String> tableNames = new HashSet<>();
        for (Table table : tables) {
            tableNames.add(table.getTableName());
        }
        return tableNames;
    }
}
