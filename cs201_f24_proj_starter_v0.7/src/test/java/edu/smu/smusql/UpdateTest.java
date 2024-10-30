package edu.smu.smusql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class UpdateTest {
    private Engine engine;

    @BeforeEach
    public void setUp() {
        engine = new Engine();

        // Create table and add sample data for testing
        engine.executeSQL("CREATE TABLE employees (id, name, salary, department)");
        engine.executeSQL("INSERT INTO employees VALUES (1, 'Alice', 55000, 'HR')");
        engine.executeSQL("INSERT INTO employees VALUES (2, 'Bob', 60000, 'Engineering')");
        engine.executeSQL("INSERT INTO employees VALUES (3, 'Charlie', 50000, 'HR')");
    }

    @Test
    public void testUpdateSingleRow() {
        // Update the salary for a specific row where id = 1
        String result = engine.executeSQL("UPDATE employees SET salary = 58000 WHERE id = 1");
        assertEquals("Update Successful", result);

        // Verify the row update
        String selectResult = engine.executeSQL("SELECT * FROM employees WHERE id = 1");
        String expected = "id\tname\tsalary\tdepartment\n1\t'Alice'\t58000\t'HR'\n";
        assertEquals(expected, selectResult);
    }

    @Test
    public void testUpdateWithCondition() {
        // Update the salary for all employees in the HR department
        String result = engine.executeSQL("UPDATE employees SET salary = 53000 WHERE department = 'HR'");
        assertEquals("Update Successful", result);

        // Verify only HR employees' salaries were updated
        String selectResult = engine.executeSQL("SELECT * FROM employees");
        String expected = "id\tname\tsalary\tdepartment\n" +
                "1\t'Alice'\t53000\t'HR'\n" +
                "2\t'Bob'\t60000\t'Engineering'\n" +
                "3\t'Charlie'\t53000\t'HR'\n";
        assertEquals(expected, selectResult);
    }

    @Test
    public void testUpdateNonExistentRow() {
        // Attempt to update a row with an id that doesn't exist
        String result = engine.executeSQL("UPDATE employees SET salary = 70000 WHERE id = 10");
        assertEquals("No Rows Affected", result); // Return no rows affected if there's no row according to the
                                                  // condition

        // Ensure no rows were modified
        String selectResult = engine.executeSQL("SELECT * FROM employees");
        String expected = "id\tname\tsalary\tdepartment\n" +
                "1\t'Alice'\t55000\t'HR'\n" +
                "2\t'Bob'\t60000\t'Engineering'\n" +
                "3\t'Charlie'\t50000\t'HR'\n";
        assertEquals(expected, selectResult);
    }

    @Test
    public void testUpdateInvalidColumn() {
        // Expect IllegalArgumentException with specific message when the column is
        // invalid
        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> engine.executeSQL("UPDATE employees SET invalidColumn = 80000 WHERE id = 1"));
        assertEquals("Column invalidColumn does not exist in table employees", exception.getMessage());
    }

    @Test
    public void testUpdateWithoutWhere() {
        // Update all employees' salaries without specifying WHERE
        String result = engine.executeSQL("UPDATE employees SET salary = 75000");
        assertEquals("Update Successful", result);

        // Verify all rows were updated
        String selectResult = engine.executeSQL("SELECT * FROM employees");
        String expected = "id\tname\tsalary\tdepartment\n" +
                "1\t'Alice'\t75000\t'HR'\n" +
                "2\t'Bob'\t75000\t'Engineering'\n" +
                "3\t'Charlie'\t75000\t'HR'\n";
        assertEquals(expected, selectResult);
    }
}
