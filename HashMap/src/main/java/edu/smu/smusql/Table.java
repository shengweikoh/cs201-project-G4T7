package edu.smu.smusql;


import java.util.ArrayList;
import java.util.HashMap;
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
    public List<List<Object>> selectAllRows(){
        return new ArrayList<>(rows.values());
    }
    //select row by condition
    public List<List<Object>> selectWhere(String columnName, Object value){
        List<List<Object>> result = new ArrayList<>();
        int columnsIndex = columns.indexOf(columnName);
        if(columnsIndex == -1){
            return result;
        }
        for(List<Object> row : rows.values()){
            if(row.get(columnsIndex).equals(value)){
                result.add(row);
            }
        }
        return result;
    }
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

    public String deleteWhere(String columnName, Object value){
        int columnsIndex = columns.indexOf(columnName);
        if(columnsIndex == -1){
            return "Column Not found";
        }
        int deletedRows = 0;

        for(Map.Entry<Object,List<Object>> entry: new HashMap<>(rows).entrySet()){
            if(entry.getValue().get(columnsIndex).equals(value)){
                rows.remove(entry.getKey());
                deletedRows++;
            }
        }
        return "Deleted " + deletedRows + " row";
    }

    public List<Object> getRowByPrimaryKey(Object primaryKey){
        return rows.get(primaryKey);
    }

    public boolean deleteRow(Object primaryKey) {
        return rows.remove(primaryKey) != null;
    }

    public String getTableName(){
        return tableName;
    }
    public List<String> getColumns(){
        return columns;
    }

}
