package edu.smu.smusql;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InsertTest {
    private Table table;

    @BeforeEach
    void setUp() {
        List<String> columns = Arrays.asList("id", "name", "age");
        table = new Table("Users", columns);
    }

    @Test
    void testInsertRowSuccess() {
        List<Object> row = Arrays.asList(1, "Alice", 30);
        table.insertRow(row);
        assertNotNull(table.getRowByPrimaryKey(1), "Row should be inserted successfully");
        assertEquals("Alice", table.getRowByPrimaryKey(1).get(1), "Name should match inserted value");
    }

    @Test
    void testInsertDuplicatePrimaryKey() {
        List<Object> row1 = Arrays.asList(1, "Alice", 30);
        List<Object> row2 = Arrays.asList(1, "Bob", 25); // Duplicate primary key

        table.insertRow(row1);
        table.insertRow(row2); // Attempt duplicate insert

        assertNotNull(table.getRowByPrimaryKey(1), "Original row should still exist after duplicate insert");
        assertEquals("Alice", table.getRowByPrimaryKey(1).get(1), "Original row should not be overwritten");
    }

    @Test
    void testInsertRowColumnMismatch() {
        List<Object> row = Arrays.asList(1, "Bob"); // Missing 'age' column

        table.insertRow(row); // Attempt to insert row with column mismatch
        assertNull(table.getRowByPrimaryKey(1), "Row should not be inserted due to column mismatch");
    }

    @Test
    void testInsertRowWithNullValues() {
        List<Object> row = Arrays.asList(2, null, 22); // Null in 'name' column

        table.insertRow(row); // Attempt to insert row with null values
        assertNull(table.getRowByPrimaryKey(2), "Row should not be inserted due to null values");
    }
}
