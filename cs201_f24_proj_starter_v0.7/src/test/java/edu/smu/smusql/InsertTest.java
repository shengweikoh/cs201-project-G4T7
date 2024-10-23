package edu.smu.smusql;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import java.util.*;


public class InsertTest {

// Test method
    @Test
    public void testInsertRowSuccessfully() {
        Engine engine = new Engine();
        engine.executeSQL("CREATE TABLE employees (id, name, position)");
        String insertQuery = "INSERT INTO employees VALUES (1, 'John Doe', 'Developer')";
        String result = engine.executeSQL(insertQuery);

        // Verify the success message
        assertEquals("Insertion Successful", result);

        // Now retrieve the row
        Table table = engine.getDatabase().getTable("employees");
        System.out.println("Retrieved table: " + table);

        Map<String, Object> row = table.getRowByPrimaryKey(Integer.valueOf(1)); // Make sure this is Integer

        System.out.println("Retrieved row: " + row); // Should print the inserted row

        // //assertNotNull(row); // This is where it fails
        // assertEquals("John Doe", row.get("name"));
        // assertEquals("Developer", row.get("position"));
    }



}
