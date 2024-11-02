package edu.smu.smusql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class UpdateTest {

    private Engine engine;

    @BeforeEach
    public void setUp() {
        engine = new Engine();

        // Create a table for testing
        String createTableQuery = "CREATE TABLE student (id, name, gpa, age)";
        engine.executeSQL(createTableQuery);

        // Insert some sample data
        engine.executeSQL("INSERT INTO student VALUES (1, 'John Doe', 3.5, 20)");
        engine.executeSQL("INSERT INTO student VALUES (2, 'Jane Smith', 3.9, 19)");
        engine.executeSQL("INSERT INTO student VALUES (3, 'Mark Davis', 3.2, 21)");
        engine.executeSQL("INSERT INTO student VALUES (4, 'Emma Wilson', 3.8, 22)");
    }

    @Test
    public void testUpdateSingleRow() {
        // Update a single row where id = 2
        String updateQuery = "UPDATE student SET age = 21 WHERE id = 2";
        String result = engine.executeSQL(updateQuery);

        assertEquals("Table student updated. 1 row(s) updated in student", result);

        // Verify that Jane Smith's age is now updated
        String selectQuery = "SELECT * FROM student WHERE id = 2";
        String expected = "id\tname\tgpa\tage\n" +
                          "2\t'Jane Smith'\t3.9\t21\n";
        assertEquals(expected, engine.executeSQL(selectQuery));
    }

    @Test
    public void testUpdateMultipleRowsWithCondition() {
        // Update multiple rows where gpa < 3.8
        String updateQuery = "UPDATE student SET age = 23 WHERE gpa < 3.8";
        String result = engine.executeSQL(updateQuery);

        assertEquals("Table student updated. 2 row(s) updated in student", result);

        // Verify that Mark Davis and John Doe's ages are updated
        String selectQuery = "SELECT * FROM student WHERE age = 23";
        String expected = "id\tname\tgpa\tage\n" +
                          "1\t'John Doe'\t3.5\t23\n" +
                          "3\t'Mark Davis'\t3.2\t23\n";
        assertEquals(expected, engine.executeSQL(selectQuery));
    }

    @Test
    public void testUpdateWithAndCondition() {
        // Update with AND condition (gpa > 3.5 AND age < 22)
        String updateQuery = "UPDATE student SET name = 'Updated' WHERE gpa > 3.5 AND age < 22";
        String result = engine.executeSQL(updateQuery);

        assertEquals("Table student updated. 1 row(s) affected.", result);

        // Verify that only Jane Smith's name is updated
        String selectQuery = "SELECT * FROM student WHERE id = 2";
        String expected = "id\tname\tgpa\tage\n" +
                          "2\t'Updated'\t3.9\t19\n";
        assertEquals(expected, engine.executeSQL(selectQuery));
    }

    @Test
    public void testUpdateWithOrCondition() {
        // Update with OR condition (age = 20 OR gpa = 3.2)
        String updateQuery = "UPDATE student SET gpa = 3.0 WHERE age = 20 OR gpa = 3.2";
        String result = engine.executeSQL(updateQuery);

        assertEquals("Table student updated. 2 row(s) affected.", result);

        // Verify that John Doe and Mark Davis' gpa values are updated
        String selectQuery = "SELECT * FROM student WHERE gpa = 3.0";
        String expected = "id\tname\tgpa\tage\n" +
                          "1\t'John Doe'\t3.0\t20\n" +
                          "3\t'Mark Davis'\t3.0\t21\n";
        assertEquals(expected, engine.executeSQL(selectQuery));
    }

    @Test
    public void testUpdateNonExistentColumn() {
        // Update a non-existent column
        String updateQuery = "UPDATE student SET nonExistentColumn = 25 WHERE id = 1";
        String result = engine.executeSQL(updateQuery);

        assertEquals("ERROR: Column not found: nonExistentColumn", result);
    }

    @Test
    public void testUpdateInvalidSyntax() {
        // Test invalid UPDATE syntax
        String invalidUpdateQuery = "UPDATE student WHERE id = 1";
        String result = engine.executeSQL(invalidUpdateQuery);

        assertEquals("ERROR: Invalid UPDATE syntax", result);
    }

    @Test
    public void testUpdateNonExistentTable() {
        // Update a non-existent table
        String invalidTableQuery = "UPDATE nonExistentTable SET age = 25 WHERE id = 1";
        String result = engine.executeSQL(invalidTableQuery);

        assertEquals("ERROR: No such table: nonExistentTable", result);
    }
}