package edu.smu.smusql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class InsertTest {
    private Engine engine;
    private Database database;

    @BeforeEach
    void setUp() {
        engine = new Engine();
        database = engine.getDatabase();

        // Create a table with columns
        List<String> columns = Arrays.asList("id", "name", "age");
        database.createTable("Users", columns, false); // Adjust to remove the B-Tree if it's not needed here
    }

    @Test
    void testInsertCommand() {
        // System.out.println("---INSERT COMMAND TEST---\n");
        // Execute insert command
        String result = engine.executeSQL("INSERT INTO Users VALUES (1, Alice, 30)");
        assertEquals("Insertion Successful", result);

        // Retrieve the table and check the inserted row
        Table usersTable = database.getTable("Users");
        // System.out.println(usersTable);
        Map<String, String> insertedRow = usersTable.getRowByPrimaryKey("1");

        // System.out.println("Inserted row: " + insertedRow + "\n");
        assertNotNull(insertedRow, "Inserted row should not be null");
        assertEquals("Alice", insertedRow.get("name"));
        assertEquals("30", insertedRow.get("age"));
    }

    @Test
    void testInsertWithInvalidData() {
        // Attempt to insert invalid data (not enough values)
        String result = engine.executeSQL("INSERT INTO Users VALUES (2, Bob)"); // Only 2 values, but 3 expected
        assertTrue(result.startsWith("ERROR:"), "Should return an error for invalid data");
        assertTrue(result.contains("Number of values doesn't match number of columns"), "Should specify the error reason");
    }

    @Test
    void testInsertDuplicatePrimaryKey() {
        // First insert
        String result1 = engine.executeSQL("INSERT INTO Users VALUES (1, Alice, 30)");
        assertEquals("Insertion Successful", result1);

        // Attempt to insert with the same primary key
        String result2 = engine.executeSQL("INSERT INTO Users VALUES (1, 'Charlie', 25)");
        assertTrue(result2.startsWith("ERROR:"), "Should return an error for duplicate primary key");
        assertTrue(result2.contains("Duplicate primary key"), "Should specify the error reason");
    }

    @Test
    void testInsertWithNonexistentTable() {
        // Attempt to insert into a nonexistent table
        String result = engine.executeSQL("INSERT INTO NonexistentTable VALUES (4, 'David', 22)");
        assertEquals("ERROR: No such table: NonexistentTable", result);
    }
}
