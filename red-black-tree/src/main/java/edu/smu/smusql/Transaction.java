package edu.smu.smusql;

import java.util.Map;
import java.util.Set;

public class Transaction {
    private String operationType;
    private String tableName;
    private Set<String> primaryKeys; // Stores primary keys affected by the transaction
    private Map<String, Map<String, String>> previousStates; // Previous states for each affected row
    private String columnName;

    public Transaction(String operationType, String tableName, Set<String> primaryKeys, Map<String, Map<String, String>> previousStates) {
        this.operationType = operationType;
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
        this.previousStates = previousStates;
    }

    public Transaction(String operationType, String tableName, Set<String> primaryKeys, Map<String, Map<String, String>> previousStates, String columnName) {
        this.operationType = operationType;
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
        this.previousStates = previousStates;
        this.columnName = columnName;
    }

    public String getOperationType() {
        return operationType;
    }

    public String getTableName() {
        return tableName;
    }

    public Set<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public Map<String, Map<String, String>> getPreviousStates() {
        return previousStates;
    }

    public String getColumnName() {
        return columnName;
    }
}