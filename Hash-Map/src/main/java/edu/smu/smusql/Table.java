package edu.smu.smusql;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;



public class Table {
    private String tableName;
    private List<String> columns;
    private Map<Object, List<Object>> rows;

    public Table(String tableName, List<String> columns){
        this.tableName = tableName;
        this.columns = columns;
        this.rows = new HashMap<>();
    }

    //insert row with unique key,
    public String insertRow(List<Object> rowValues){
        if(rowValues.size() != columns.size()){
            return "Mismatch between columns and values provided";
        }
        if (rowValues.contains(null)) {
            return "Null values not allowed";
        }
        Object key = rowValues.get(0);
        if(rows.containsKey(key)){
            return "Primary key " + key + " existed";
        }
        rows.put(key,rowValues);
        return "Row Inserted";
    }

    //select row with matching criteria
    public List<List<Object>> getAllRows(){
        return new ArrayList<>(rows.values());
    }
    //select row by condition
    // public List<List<Object>> selectWhere(String columnName, Object value){
    //     List<List<Object>> result = new ArrayList<>();
    //     int columnsIndex = columns.indexOf(columnName);
    //     if(columnsIndex == -1){
    //         return result;
    //     }
    //     for(List<Object> row : rows.values()){
    //         if(row.get(columnsIndex).equals(value)){
    //             result.add(row);
    //         }
    //     }
    //     return result;
    // }
    public String updateWhere(String columnName, Object value, String targetColumn, Object newValue){
        int columnsIndex = columns.indexOf(columnName);
        int targetIndex = columns.indexOf(targetColumn);
        if(columnsIndex == -1 || targetIndex == -1 ){
            return "Invalid Column";
        }
        int updatedRows = 0;
        for(List<Object> row : rows.values()){
            if(row.get(columnsIndex).equals(value)){
                row.set(targetIndex,newValue);
                updatedRows++; //increment
            }
        }
        return "updated " + updatedRows + " rows";
    }

    public String deleteWhere(String columnName, String operator, String value) {
        int columnIndex = columns.indexOf(columnName);
        if (columnIndex == -1) {
            return "Column Not found";
        }
        // value = value.replaceAll("^['\"]|['\"]$", ""); // Strip quotes if present
        int deletedRows = 0;
        Iterator<Map.Entry<Object, List<Object>>> iterator = rows.entrySet().iterator();
    
        while (iterator.hasNext()) {
            Map.Entry<Object, List<Object>> entry = iterator.next();
            Object cellValue = entry.getValue().get(columnIndex);
            
            if (evaluateCondition(cellValue.toString(), operator, value)) {
                iterator.remove(); // Safely removes the row
                deletedRows++;
            }
        }
        return "Deleted " + deletedRows + " rows";
    }
    private boolean evaluateCondition(Object cellValue, String operator, String value) {
        if (cellValue instanceof Number && value.matches("-?\\d+(\\.\\d+)?")) {
            // If the value is numeric (either Integer or Double), perform numeric comparison
            double cellNumber = ((Number) cellValue).doubleValue();
            double compareValue = Double.parseDouble(value);
    
            switch (operator) {
                case "=": return cellNumber == compareValue;
                case ">": return cellNumber > compareValue;
                case "<": return cellNumber < compareValue;
                case ">=": return cellNumber >= compareValue;
                case "<=": return cellNumber <= compareValue;
                default: return false;
            }
        } else if (cellValue instanceof String && value instanceof String) {
            // If both are strings, perform string comparison
            switch (operator) {
                case "=": return cellValue.equals(value);
                case "!=": return !cellValue.equals(value);
                default: return false;
            }
        }
        // If types don't match, return false
        return false;
    }

    public int deleteAllRows() {
        int count = rows.size();
        rows.clear();
        return count;
    }

    // public List<Object> getRowByPrimaryKey(Object primaryKey){
    //     return rows.get(primaryKey);
    // }

    public boolean deleteRow(Object primaryKey) {
        return rows.remove(primaryKey) != null;
    }

    public String getTableName(){
        return tableName;
    }
    public List<String> getColumns(){
        return columns;
    }
    public Map<Object, List<Object>> getRows() {
        return rows;
    }
    

    public List<List<Object>> filterRows(String columnName, String operator, Object value) {
        List<List<Object>> result = new ArrayList<>();
        int columnIndex = columns.indexOf(columnName);

        if (columnIndex == -1) {
            return result; // Column not found, return empty list
        }

        for (List<Object> row : rows.values()) {
            Object cellValue = row.get(columnIndex);

            // Perform the appropriate comparison based on the operator
            boolean matches = switch (operator) {
                case "=" -> cellValue.equals(value);
                case ">" -> ((Comparable<Object>) cellValue).compareTo(value) > 0;
                case "<" -> ((Comparable<Object>) cellValue).compareTo(value) < 0;
                case ">=" -> ((Comparable<Object>) cellValue).compareTo(value) >= 0;
                case "<=" -> ((Comparable<Object>) cellValue).compareTo(value) <= 0;
                default -> false;
            };

            if (matches) {
                result.add(row);
            }
        }
        return result;
    }

    // New method to update a specific row by setting a new value in a target column
    public void updateRow(List<Object> row, String targetColumn, Object newValue) {
        int targetIndex = columns.indexOf(targetColumn);
        if (targetIndex != -1) {
            row.set(targetIndex, newValue);
        }
    }

}
