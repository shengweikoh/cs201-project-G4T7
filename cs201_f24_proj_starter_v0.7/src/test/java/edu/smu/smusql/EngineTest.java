// package edu.smu.smusql;

// import org.junit.jupiter.api.Test;
// import static org.junit.jupiter.api.Assertions.*;

// import java.util.List;

// public class EngineTest {

//     // Create table
//     @Test
//     public void testCreateTableSuccessfully() {
//         Engine engine = new Engine();
//         String query = "CREATE TABLE employees (id, name, position)";
//         String result = engine.executeSQL(query);
        
//         // Check the success message
//         assertEquals("Table employees created", result);

//         // Check if the table was indeed created in the database
//         Database database = engine.getDatabase(); // Ensure Engine has a getDatabase method
//         Table table = database.getTable("employees");
        
//         // Validate that the table was created with the expected columns
//         List<String> expectedColumns = List.of("id", "name", "position");
//         assertEquals(expectedColumns, table.getColumns());
//     }

//     @Test
//     public void testCreateTableAlreadyExists() {
//         Engine engine = new Engine();
        
//         // Create the first table
//         String query1 = "CREATE TABLE employees (id, name, position)";
//         engine.executeSQL(query1);

//         // Try creating the same table again
//         String query2 = "CREATE TABLE employees (id, name, position)";
//         String result = engine.executeSQL(query2);
        
//         // Check the error message
//         assertEquals("ERROR: Table already exists", result);
//     }

//     @Test
//     public void testCreateTableInvalidSyntax() {
//         Engine engine = new Engine();
        
//         // Invalid syntax (missing TABLE keyword)
//         String query = "CREATE employees (id, name, position)";
//         String result = engine.executeSQL(query);
        
//         // Check for the appropriate error message
//         assertEquals("ERROR: Invalid CREATE TABLE syntax", result);
//     }
// }