package edu.smu.smusql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class DeleteTest {
    private Engine engine;

    /*All test cases pass */

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
    public void testDeleteSpecificRow() {
        // DELETE row with id = 1
        String deleteQuery = "DELETE FROM student WHERE id = 1";
        String deleteResult = engine.executeSQL(deleteQuery);

        // Verify DELETE was successful
        assertEquals("DELETE successful", deleteResult);

        // Check that row with id=1 is no longer present
        String selectQuery = "SELECT * FROM student WHERE id = 1";
        String selectResult = engine.executeSQL(selectQuery);
        assertEquals("id\tname\tgpa\tage\n", selectResult); // Expecting no rows

        // Verify other rows still exist
        String selectAllQuery = "SELECT * FROM student";
        String selectAllResult = engine.executeSQL(selectAllQuery);
        String expected = "id\tname\tgpa\tage\n" +
                          "2\t'Jane Smith'\t3.9\t19\n" +
                          "3\t'Mark Davis'\t3.2\t21\n" +
                          "4\t'Emma Wilson'\t3.8\t22\n";
        assertEquals(expected, selectAllResult);
    }

    @Test
    public void testDeleteWithCondition() {
        // DELETE rows where gpa < 3.8
        String deleteQuery = "DELETE FROM student WHERE gpa < 3.8";
        String deleteResult = engine.executeSQL(deleteQuery);
        assertEquals("DELETE successful", deleteResult);

        // Verify only the expected rows remain
        String selectAllQuery = "SELECT * FROM student";
        String selectAllResult = engine.executeSQL(selectAllQuery);
        String expected = "id\tname\tgpa\tage\n" +
                          "2\t'Jane Smith'\t3.9\t19\n" +
                          "4\t'Emma Wilson'\t3.8\t22\n";
        assertEquals(expected, selectAllResult);
    }

    @Test
    public void testDeleteNonExistentRow() {
        // Attempt to DELETE a non-existent row
        String deleteQuery = "DELETE FROM student WHERE id = 10";
        String deleteResult = engine.executeSQL(deleteQuery);
        assertEquals("DELETE successful", deleteResult); // Should still return "DELETE successful"

        // Verify no rows were actually deleted
        String selectAllQuery = "SELECT * FROM student";
        String selectAllResult = engine.executeSQL(selectAllQuery);
        String expected = "id\tname\tgpa\tage\n" +
                          "1\t'John Doe'\t3.5\t20\n" +
                          "2\t'Jane Smith'\t3.9\t19\n" +
                          "3\t'Mark Davis'\t3.2\t21\n" +
                          "4\t'Emma Wilson'\t3.8\t22\n";
        assertEquals(expected, selectAllResult);
    }

    @Test
    public void testDeleteWithInvalidColumn() {
        // DELETE with an invalid column
        String deleteQuery = "DELETE FROM student WHERE nonExistentColumn = 3.5";
        String deleteResult = engine.executeSQL(deleteQuery);

        assertEquals("ERROR: Column not found: nonExistentColumn", deleteResult);
    }

    // Test case using AND OR
}
