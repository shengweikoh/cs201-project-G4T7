package edu.smu.smusql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class SelectTest {

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
    public void testSelectAll() {
        // SELECT all rows
        String selectAllQuery = "SELECT * FROM student";
        String result = engine.executeSQL(selectAllQuery);

        // Check that all rows are returned, with names enclosed in single quotes
        String expected = "id\tname\tgpa\tage\n" +
                          "1\t'John Doe'\t3.5\t20\n" +
                          "2\t'Jane Smith'\t3.9\t19\n" +
                          "3\t'Mark Davis'\t3.2\t21\n" +
                          "4\t'Emma Wilson'\t3.8\t22\n";

        assertEquals(expected, result);
    }

    @Test
    public void testSelectWithWhereEqual() {
        // SELECT with a WHERE clause (gpa = 3.5)
        String selectWhereQuery = "SELECT * FROM student WHERE gpa = 3.5";
        String result = engine.executeSQL(selectWhereQuery);

        // Check that the correct row is returned, with names enclosed in single quotes
        String expected = "id\tname\tgpa\tage\n" +
                          "1\t'John Doe'\t3.5\t20\n";

        assertEquals(expected, result);
    }

    @Test
    public void testSelectWithWhereGreaterThan() {
        // SELECT with a WHERE clause (gpa > 3.5)
        String selectWhereQuery = "SELECT * FROM student WHERE gpa > 3.5";
        String result = engine.executeSQL(selectWhereQuery);

        // Check that the correct rows are returned, with names enclosed in single quotes
        String expected = "id\tname\tgpa\tage\n" +
                          "2\t'Jane Smith'\t3.9\t19\n" +
                          "4\t'Emma Wilson'\t3.8\t22\n";

        assertEquals(expected, result);
    }

    @Test
    public void testSelectWithWhereLessThan() {
        // SELECT with a WHERE clause (age < 21)
        String selectWhereQuery = "SELECT * FROM student WHERE age < 21";
        String result = engine.executeSQL(selectWhereQuery);

        // Check that the correct rows are returned, with names enclosed in single quotes
        String expected = "id\tname\tgpa\tage\n" +
                          "1\t'John Doe'\t3.5\t20\n" +
                          "2\t'Jane Smith'\t3.9\t19\n";

        assertEquals(expected, result);
    }

    @Test
    public void testSelectWithWhereAndCondition() {
        // SELECT with a WHERE clause using AND (gpa > 3.5 AND age < 22)
        String selectWhereQuery = "SELECT * FROM student WHERE gpa > 3.5 AND age < 22";
        String result = engine.executeSQL(selectWhereQuery);

        // Check that the correct rows are returned, with names enclosed in single quotes
        String expected = "id\tname\tgpa\tage\n" +
                          "2\t'Jane Smith'\t3.9\t19\n";

        assertEquals(expected, result);
    }

    @Test
    public void testSelectInvalidSyntax() {
        // Test invalid SELECT syntax
        String invalidSelectQuery = "SELECT FROM student";
        String result = engine.executeSQL(invalidSelectQuery);

        assertEquals("ERROR: Invalid SELECT syntax", result);
    }

    @Test
    public void testSelectNonExistentTable() {
        // SELECT from a non-existent table
        String invalidTableQuery = "SELECT * FROM nonExistentTable";
        String result = engine.executeSQL(invalidTableQuery);

        assertEquals("ERROR: No such table", result);
    }

    @Test
    public void testSelectWithWhereNonExistentColumn() {
        // SELECT with a WHERE clause using a non-existent column
        String selectWhereQuery = "SELECT * FROM student WHERE nonExistentColumn = 3.5";
        String result = engine.executeSQL(selectWhereQuery);

        assertEquals("ERROR: Column not found: nonExistentColumn", result);
    }
}