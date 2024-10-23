package edu.smu.smusql;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Engine {
    public Database getDatabase() {
        return database;
    }

    private Database database = new Database();
    private Parser parser = new Parser();

    public String executeSQL(String query) {
        String[] tokens = query.trim().split("\\s+");
        String command = tokens[0].toUpperCase();

        switch (command) {
            case "CREATE":
                return create(tokens);
            case "INSERT":
                return insert(tokens);
            case "SELECT":
                return select(tokens);
            case "UPDATE":
                return update(tokens);
            case "DELETE":
                return delete(tokens);
            default:
                return "ERROR: Unknown command";
        }
    }

    public String insert(String[] tokens) {
        //Parse tokens 
        List<Object> parsedData = parser.parseInsert(tokens);

        //Get table name and list of  values from parsed data
        String tableName = (String) parsedData.get(0);
        List<Object> values = (List<Object>) parsedData.get(1);

        //Get table from database
        Table table = database.getTable(tableName);
        if(table == null){
            return  "ERROR: table " + tableName + " not found.";
        }

        //Get primary key
        Object primaryKey = values.get(0);

        //Insert row to table
        System.out.println("Inserting into table: " + tableName 
                                + " , Primary key: " + primaryKey + " , Values: " + values);
                                
        try {
                table.insertRow(primaryKey, values);
            } catch (IllegalArgumentException e) {
                return "ERROR: " + e.getMessage(); // Return specific error messages
            }
        
        return "Insertion Successful";
    }
    
    
    public String delete(String[] tokens) {
        //TODO
        return "not implemented";
    }

    public String select(String[] tokens) {
        //TODO
        return "not implemented";
    }
    public String update(String[] tokens) {
        //TODO
        return "not implemented";
    }

    public String create(String[] tokens) {
        if (!tokens[1].equalsIgnoreCase("TABLE")) {
            return "ERROR: Invalid CREATE TABLE syntax";
        }
        
        String tableName = tokens[2];
        if (database.listTables().contains(tableName)) {
            return "ERROR: Table already exists";
        }
        
        String columnList = queryBetweenParentheses(tokens, 3);
        List<String> columns = Arrays.asList(columnList.split(","));
        columns.replaceAll(String::trim);

        database.createTable(tableName, columns);

        return "Table " + tableName + " created";
    }

    // HELPER METHODS
    
    private String queryBetweenParentheses(String[] tokens, int startIndex) {
        StringBuilder result = new StringBuilder();
        for (int i = startIndex; i < tokens.length; i++) {
            result.append(tokens[i]).append(" ");
        }
        return result.toString().trim().replaceAll("\\(", "").replaceAll("\\)", "");
    }

}
